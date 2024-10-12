import datetime
from enum import (
    Enum,
)
import logging
from operator import (
    itemgetter,
)
from typing import (
    AbstractSet,
    Callable,
    Iterable,
)

import attrs
from more_itertools import (
    one,
)

from azul import (
    cached_property,
    config,
    require,
    uuids,
)
from azul.bigquery import (
    backtick,
)
from azul.drs import (
    DRSURI,
)
from azul.indexer import (
    SourcedBundleFQIDJSON,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    EntityLink,
    Key,
    KeyLink,
    KeyReference,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schema,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRPlugin,
)
from azul.terra import (
    TDRSourceRef,
    TDRSourceSpec,
)
from azul.types import (
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    change_version,
)

log = logging.getLogger(__name__)

Keys = AbstractSet[KeyReference]
MutableKeys = set[KeyReference]
KeysByType = dict[EntityType, AbstractSet[Key]]
MutableKeysByType = dict[EntityType, set[Key]]
KeyLinks = set[KeyLink]


class BundleEntityType(Enum):
    """
    AnVIL snapshots have no inherent notion of a "bundle". When indexing these
    snapshots, we dynamically construct bundles by selecting individual entities
    and following their foreign keys to discover associated entities. The
    initial entity from which this graph traversal begins is termed the
    "bundle entity", and its FQID serves as the basis for the bundle FQID. Each
    member of this enumeration represents a strategy for selecting bundle
    entities.

    Our primary such strategy is to use every biosample in a given snapshot as a
    bundle entity. Biosamples were chosen for this role based on a desirable
    balance between the size and number of the resulting bundles as well as the
    degree of overlap between them. The implementation of the graph traversal is
    tightly coupled to this choice, and switching to a different entity type
    would require re-implementing much of the Plugin code. Primary bundles
    consist of at least one biosample (the bundle entity), exactly one dataset,
    and zero or more other entities of assorted types.

    Some snapshots include file entities that lack any foreign keys that
    associate the file with any other entity. To ensure that these "orphaned"
    files are indexed, they are also used as bundle entities. As with primary
    bundles, the creation of these supplementary bundles depends on a
    specifically tailored traversal implementation. Supplementary bundles always
    consist of exactly two entities: one file (the bundle entity) and one
    dataset.

    The `dataset.description` field is unusual in that it is not stored in
    BigQuery and must be retrieved via Terra's DUOS API. There is only one
    dataset per snapshot, which is referenced in all primary and supplementary
    bundles. Therefore, only one request to DUOS per *snapshot* is necessary,
    but if `description` is retrieved at the same time as the other dataset
    fields, we will make one request per *bundle* instead, potentially
    overloading the DUOS service. Our solution is to retrieve `description` only
    in a dedicated bundle format, once per snapshot, and merge it with the other
    dataset fields during aggregation. This bundle contains only a single
    dataset entity with only the `description` field populated.
    """
    primary: EntityType = 'biosample'
    supplementary: EntityType = 'file'
    duos: EntityType = 'dataset'


class TDRAnvilBundleFQIDJSON(SourcedBundleFQIDJSON):
    entity_type: str


@attrs.frozen(kw_only=True)
class TDRAnvilBundleFQID(TDRBundleFQID):
    entity_type: BundleEntityType = attrs.field(converter=BundleEntityType)

    def to_json(self) -> TDRAnvilBundleFQIDJSON:
        return dict(super().to_json(),
                    entity_type=self.entity_type.value)


class TDRAnvilBundle(AnvilBundle[TDRAnvilBundleFQID], TDRBundle):

    @classmethod
    def canning_qualifier(cls) -> str:
        return super().canning_qualifier() + '.anvil'

    def add_entity(self,
                   entity: EntityReference,
                   version: str,
                   row: MutableJSON
                   ) -> None:
        assert entity not in self.entities, entity
        metadata = dict(row,
                        version=version)
        if entity.entity_type == 'file':
            drs_uri = row['file_ref']
            # Validate URI syntax
            DRSURI.parse(drs_uri)
            metadata.update(drs_uri=drs_uri,
                            sha256='',
                            crc32='')
        self.entities[entity] = metadata

    def add_links(self, links: Iterable[EntityLink]):
        self.links.update(links)
        EntityLink.group_by_activity(self.links)


class Plugin(TDRPlugin[TDRAnvilBundle, TDRSourceSpec, TDRSourceRef, TDRAnvilBundleFQID]):

    @cached_property
    def _version(self):
        return self.format_version(datetime.datetime(year=2022,
                                                     month=6,
                                                     day=1,
                                                     hour=0,
                                                     tzinfo=datetime.timezone.utc))

    datarepo_row_uuid_version = 4
    bundle_uuid_version = 10

    def _count_subgraphs(self, source: TDRSourceSpec) -> int:
        rows = self._run_sql(f'''
            SELECT COUNT(*) AS count
            FROM {backtick(self._full_table_name(source, BundleEntityType.primary.value))}
            UNION ALL
            SELECT COUNT(*) AS count
            FROM {backtick(self._full_table_name(source, BundleEntityType.supplementary.value))}
            WHERE is_supplementary
        ''')
        return sum(row['count'] for row in rows)

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRAnvilBundleFQID]:
        spec = source.spec
        primary = BundleEntityType.primary.value
        supplementary = BundleEntityType.supplementary.value
        duos = BundleEntityType.duos.value
        rows = list(self._run_sql(f'''
            SELECT datarepo_row_id, {primary!r} AS entity_type
            FROM {backtick(self._full_table_name(spec, primary))}
            WHERE STARTS_WITH(datarepo_row_id, '{prefix}')
            UNION ALL
            SELECT datarepo_row_id, {supplementary!r} AS entity_type
            FROM {backtick(self._full_table_name(spec, supplementary))} AS supp
            WHERE supp.is_supplementary AND STARTS_WITH(datarepo_row_id, '{prefix}')
        ''' + (
            ''
            if config.duos_service_url is None else
            f'''
            UNION ALL
            SELECT datarepo_row_id, {duos!r} AS entity_type
            FROM {backtick(self._full_table_name(spec, duos))}
            '''
        )))
        bundles = []
        duos_count = 0
        for row in rows:
            # Reversibly tweak the entity UUID to prevent
            # collisions between entity IDs and bundle IDs
            bundle_uuid = uuids.change_version(row['datarepo_row_id'],
                                               self.datarepo_row_uuid_version,
                                               self.bundle_uuid_version)
            # We intentionally omit the WHERE clause for datasets so that we can
            # verify our assumption that each snapshot only contains rows for a
            # single dataset. This verification is performed independently and
            # concurrently for every partition, but only one partition actually
            # emits the bundle.
            if row['entity_type'] == duos:
                require(0 == duos_count)
                duos_count += 1
                # Ensure that one partition will always contain the DUOS bundle
                # regardless of the choice of common prefix
                if not bundle_uuid.startswith(prefix):
                    continue
            bundles.append(TDRAnvilBundleFQID(
                source=source,
                uuid=bundle_uuid,
                version=self._version,
                entity_type=BundleEntityType(row['entity_type'])
            ))
        return bundles

    def resolve_bundle(self, fqid: SourcedBundleFQIDJSON) -> TDRAnvilBundleFQID:
        if 'entity_type' not in fqid:
            # Resolution of bundles without entity type is expensive, so we only
            # support it during canning.
            assert not config.is_in_lambda, ('Bundle FQID lacks entity type', fqid)
            source = self.source_from_json(fqid['source'])
            entity_id = uuids.change_version(fqid['uuid'],
                                             self.bundle_uuid_version,
                                             self.datarepo_row_uuid_version)
            rows = self._run_sql(' UNION ALL '.join((
                f'''
                SELECT {entity_type.value!r} AS entity_type
                FROM {backtick(self._full_table_name(source.spec, entity_type.value))}
                WHERE datarepo_row_id = {entity_id!r}
                '''
                for entity_type in BundleEntityType
            )))
            fqid = {**fqid, **one(rows)}
        return super().resolve_bundle(fqid)

    def _emulate_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        if bundle_fqid.entity_type is BundleEntityType.primary:
            log.info('Bundle %r is a primary bundle', bundle_fqid.uuid)
            return self._primary_bundle(bundle_fqid)
        elif bundle_fqid.entity_type is BundleEntityType.supplementary:
            log.info('Bundle %r is a supplementary bundle', bundle_fqid.uuid)
            return self._supplementary_bundle(bundle_fqid)
        elif bundle_fqid.entity_type is BundleEntityType.duos:
            assert config.duos_service_url is not None, bundle_fqid
            log.info('Bundle %r is a DUOS bundle', bundle_fqid.uuid)
            return self._duos_bundle(bundle_fqid)
        else:
            assert False, bundle_fqid.entity_type

    def _primary_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        source = bundle_fqid.source
        bundle_entity = self._bundle_entity(bundle_fqid)

        keys: MutableKeys = {bundle_entity}
        links: KeyLinks = set()

        for method in [self._follow_downstream, self._follow_upstream]:
            method: Callable[[TDRSourceSpec, KeysByType], KeyLinks]
            n = len(keys)
            frontier: Keys = keys
            while frontier:
                new_links = method(source.spec, self._consolidate_by_type(frontier))
                links.update(new_links)
                frontier = frozenset().union(*(link.all_entities for link in new_links)) - keys
                keys.update(frontier)
            log.debug('Found %r linked entities via %r', len(keys) - n, method)

        keys_by_type: KeysByType = self._consolidate_by_type(keys)
        if log.isEnabledFor(logging.DEBUG):
            arg = keys_by_type
        else:
            arg = {entity_type: len(keys) for entity_type, keys in keys_by_type.items()}
        log.info('Found %i entities linked to bundle %r: %r',
                 len(keys), bundle_fqid.uuid, arg)

        result = TDRAnvilBundle(fqid=bundle_fqid)
        entities_by_key: dict[KeyReference, EntityReference] = {}
        for entity_type, typed_keys in sorted(keys_by_type.items()):
            pk_column = entity_type + '_id'
            rows = self._retrieve_entities(source.spec, entity_type, typed_keys)
            if entity_type == 'donor':
                # We expect that the foreign key `part_of_dataset_id` is
                # redundant for biosamples and donors. To simplify our queries,
                # we do not follow the latter during the graph traversal.
                # Here, we validate our expectation. Note that the key is an
                # array for biosamples, but not for donors.
                dataset_id: Key = one(keys_by_type['dataset'])
                for row in rows:
                    donor_dataset_id = row['part_of_dataset_id']
                    require(donor_dataset_id == dataset_id, donor_dataset_id, dataset_id)
            for row in sorted(rows, key=itemgetter(pk_column)):
                key = KeyReference(key=row[pk_column], entity_type=entity_type)
                entity = EntityReference(entity_id=row['datarepo_row_id'], entity_type=entity_type)
                entities_by_key[key] = entity
                result.add_entity(entity, self._version, row)
        result.add_links((link.to_entity_link(entities_by_key) for link in links))
        return result

    def _supplementary_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        entity_id = uuids.change_version(bundle_fqid.uuid,
                                         self.bundle_uuid_version,
                                         self.datarepo_row_uuid_version)
        source = bundle_fqid.source.spec
        bundle_entity_type = bundle_fqid.entity_type.value
        result = TDRAnvilBundle(fqid=bundle_fqid)
        columns = self._columns(bundle_entity_type)
        bundle_entity = dict(one(self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source, bundle_entity_type))}
            WHERE datarepo_row_id = '{entity_id}'
        ''')))
        linked_entity_type = 'dataset'
        columns = self._columns(linked_entity_type)
        linked_entity = dict(one(self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source, linked_entity_type))}
        ''')))
        link_args = {}
        for entity_type, row, arg in [
            (bundle_entity_type, bundle_entity, 'outputs'),
            (linked_entity_type, linked_entity, 'inputs')
        ]:
            entity_ref = EntityReference(entity_type=entity_type, entity_id=row['datarepo_row_id'])
            result.add_entity(entity_ref, self._version, row)
            link_args[arg] = {entity_ref}
        result.add_links({EntityLink(**link_args)})
        return result

    def _duos_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        duos_info = self.tdr.get_duos(bundle_fqid.source)
        description = None if duos_info is None else duos_info.get('studyDescription')
        entity_id = change_version(bundle_fqid.uuid,
                                   self.bundle_uuid_version,
                                   self.datarepo_row_uuid_version)
        entity = EntityReference(entity_type=bundle_fqid.entity_type.value,
                                 entity_id=entity_id)
        bundle = TDRAnvilBundle(fqid=bundle_fqid)
        bundle.add_entity(entity=entity,
                          version=self._version,
                          row={'description': description})
        return bundle

    def _bundle_entity(self, bundle_fqid: TDRAnvilBundleFQID) -> KeyReference:
        source = bundle_fqid.source
        bundle_uuid = bundle_fqid.uuid
        entity_id = uuids.change_version(bundle_uuid,
                                         self.bundle_uuid_version,
                                         self.datarepo_row_uuid_version)
        entity_type = bundle_fqid.entity_type.value
        pk_column = entity_type + '_id'
        bundle_entity = one(self._run_sql(f'''
            SELECT {pk_column}
            FROM {backtick(self._full_table_name(source.spec, entity_type))}
            WHERE datarepo_row_id = '{entity_id}'
        '''))[pk_column]
        bundle_entity = KeyReference(key=bundle_entity, entity_type=entity_type)
        log.info('Bundle UUID %r resolved to primary key %r in table %r',
                 bundle_uuid, bundle_entity.key, entity_type)
        return bundle_entity

    def _full_table_name(self, source: TDRSourceSpec, table_name: str) -> str:
        if not table_name.startswith('INFORMATION_SCHEMA'):
            table_name = 'anvil_' + table_name
        return super()._full_table_name(source, table_name)

    def _consolidate_by_type(self, entities: Keys) -> MutableKeysByType:
        result = {
            table['name'].removeprefix('anvil_'): set()
            for table in anvil_schema['tables']
        }
        for e in entities:
            result[e.entity_type].add(e.key)
        return result

    def _follow_upstream(self,
                         source: TDRSourceSpec,
                         entities: KeysByType
                         ) -> KeyLinks:
        return set.union(
            self._upstream_from_files(source, entities['file']),
            self._upstream_from_biosamples(source, entities['biosample']),
            # The direction of the edges linking donors to diagnoses is
            # contentious. Currently, we model diagnoses as being upstream from
            # donors. This is counterintuitive, but has two important practical
            # benefits.
            #
            # First, it greatly simplifies the process of discovering the
            # diagnoses while building the bundle, because performing a complete
            # *downstream* search with donors as input would be tantamount to
            # using donors as bundle entities instead of biosamples, leading to
            # increased bundle size and increased overlap between bundles.
            #
            # Each diagnosis is linked to exactly one other entity (the donor),
            # so the direction in which the donor-diagnosis links are followed
            # won't affect the discovery of other entities. However, edge
            # direction *is* important for deciding which entities in the bundle
            # are linked to each other (and thus constitute each other's
            # inner/outer entities). This leads to the second and more important
            # benefit of our decision to model diagnoses as being upstream from
            # donors: it creates continuous directed paths through the graph
            # from the diagnoses to all entities downstream of the donor.
            # Without such a path, we would be unable to associate biosamples or
            # files with diagnoses without adding cumbersome diagnosis-specific
            # logic to the transformers' graph traversal algorithm. The only
            # entities that are upstream from donors are datasets, which do not
            # perform a traversal and are treated as being linked to every
            # entity in the bundle regardless of the edges in the graph.
            self._diagnoses_from_donors(source, entities['donor'])
        )

    def _follow_downstream(self,
                           source: TDRSourceSpec,
                           entities: KeysByType
                           ) -> KeyLinks:
        return set.union(
            self._downstream_from_biosamples(source, entities['biosample']),
            self._downstream_from_files(source, entities['file'])
        )

    def _upstream_from_biosamples(self,
                                  source: TDRSourceSpec,
                                  biosample_ids: AbstractSet[Key]
                                  ) -> KeyLinks:
        if biosample_ids:
            rows = self._run_sql(f'''
                SELECT b.biosample_id, b.donor_id, b.part_of_dataset_id
                FROM {backtick(self._full_table_name(source, 'biosample'))} AS b
                WHERE b.biosample_id IN ({', '.join(map(repr, biosample_ids))})
            ''')
            result: KeyLinks = set()
            for row in rows:
                downstream_ref = KeyReference(entity_type='biosample',
                                              key=row['biosample_id'])
                result.add(KeyLink(outputs={downstream_ref},
                                   inputs={KeyReference(entity_type='dataset',
                                                        key=one(row['part_of_dataset_id']))}))
                for donor_id in row['donor_id']:
                    result.add(KeyLink(outputs={downstream_ref},
                                       inputs={KeyReference(entity_type='donor',
                                                            key=donor_id)}))
            return result
        else:
            return set()

    def _upstream_from_files(self,
                             source: TDRSourceSpec,
                             file_ids: AbstractSet[Key]
                             ) -> KeyLinks:
        if file_ids:
            rows = self._run_sql(f'''
                WITH file AS (
                  SELECT f.file_id FROM {backtick(self._full_table_name(source, 'file'))} AS f
                  WHERE f.file_id IN ({', '.join(map(repr, file_ids))})
                )
                SELECT
                      f.file_id AS generated_file_id,
                      'alignmentactivity' AS activity_table,
                      ama.alignmentactivity_id AS activity_id,
                      ama.used_file_id AS uses_file_id,
                      [] AS uses_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'alignmentactivity'))} AS ama
                    ON f.file_id IN UNNEST(ama.generated_file_id)
                UNION ALL SELECT
                      f.file_id,
                      'assayactivity',
                      aya.assayactivity_id,
                      [],
                      aya.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'assayactivity'))} AS aya
                    ON f.file_id IN UNNEST(aya.generated_file_id)
                UNION ALL SELECT
                      f.file_id,
                      'sequencingactivity',
                      sqa.sequencingactivity_id,
                      [],
                      sqa.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'sequencingactivity'))} AS sqa
                    ON f.file_id IN UNNEST(sqa.generated_file_id)
                UNION ALL SELECT
                    f.file_id,
                    'variantcallingactivity',
                    vca.variantcallingactivity_id,
                    vca.used_file_id,
                    []
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'variantcallingactivity'))} AS vca
                    ON f.file_id IN UNNEST(vca.generated_file_id)
                UNION ALL SELECT
                    f.file_id,
                    'activity',
                    a.activity_id,
                    a.used_file_id,
                    a.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'activity'))} AS a
                    ON f.file_id IN UNNEST(a.generated_file_id)
            ''')
            return {
                KeyLink(
                    activity=KeyReference(entity_type=row['activity_table'], key=row['activity_id']),
                    # The generated link is not a complete representation of the
                    # upstream activity because it does not include generated files
                    # that are not ancestors of the downstream file
                    outputs={KeyReference(entity_type='file', key=row['generated_file_id'])},
                    inputs={
                        KeyReference(entity_type=entity_type, key=key)
                        for entity_type, column in [('file', 'uses_file_id'),
                                                    ('biosample', 'uses_biosample_id')]
                        for key in row[column]
                    }
                )
                for row in rows
            }
        else:
            return set()

    def _diagnoses_from_donors(self,
                               source: TDRSourceSpec,
                               donor_ids: AbstractSet[Key]
                               ) -> KeyLinks:
        if donor_ids:
            rows = self._run_sql(f'''
                SELECT dgn.donor_id, dgn.diagnosis_id
                FROM {backtick(self._full_table_name(source, 'diagnosis'))} as dgn
                WHERE dgn.donor_id IN ({', '.join(map(repr, donor_ids))})
            ''')
            return {
                KeyLink(inputs={KeyReference(key=row['diagnosis_id'], entity_type='diagnosis')},
                        outputs={KeyReference(key=row['donor_id'], entity_type='donor')},
                        activity=None)
                for row in rows
            }
        else:
            return set()

    def _downstream_from_biosamples(self,
                                    source: TDRSourceSpec,
                                    biosample_ids: AbstractSet[Key],
                                    ) -> KeyLinks:
        if biosample_ids:
            rows = self._run_sql(f'''
                WITH activities AS (
                    SELECT
                        sqa.sequencingactivity_id as activity_id,
                        'sequencingactivity' as activity_table,
                        sqa.used_biosample_id,
                        sqa.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'sequencingactivity'))} AS sqa
                    UNION ALL
                    SELECT
                        aya.assayactivity_id,
                        'assayactivity',
                        aya.used_biosample_id,
                        aya.generated_file_id,
                    FROM {backtick(self._full_table_name(source, 'assayactivity'))} AS aya
                    UNION ALL
                    SELECT
                        a.activity_id,
                        'activity',
                        a.used_biosample_id,
                        a.generated_file_id,
                    FROM {backtick(self._full_table_name(source, 'activity'))} AS a
                )
                SELECT
                    biosample_id,
                    a.activity_id,
                    a.activity_table,
                    a.generated_file_id
                FROM activities AS a, UNNEST(a.used_biosample_id) AS biosample_id
                WHERE biosample_id IN ({', '.join(map(repr, biosample_ids))})
            ''')
            return {
                KeyLink(inputs={KeyReference(key=row['biosample_id'], entity_type='biosample')},
                        outputs={
                            KeyReference(key=output_id, entity_type='file')
                            for output_id in row['generated_file_id']
                        },
                        activity=KeyReference(key=row['activity_id'], entity_type=row['activity_table']))
                for row in rows
            }
        else:
            return set()

    def _downstream_from_files(self,
                               source: TDRSourceSpec,
                               file_ids: AbstractSet[Key]
                               ) -> KeyLinks:
        if file_ids:
            rows = self._run_sql(f'''
                WITH activities AS (
                    SELECT
                        ala.alignmentactivity_id AS activity_id,
                        'alignmentactivity' AS activity_table,
                        ala.used_file_id,
                        ala.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'alignmentactivity'))} AS ala
                    UNION ALL SELECT
                        vca.variantcallingactivity_id,
                        'variantcallingactivity',
                        vca.used_file_id,
                        vca.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'variantcallingactivity'))} AS vca
                    UNION ALL SELECT
                        a.activity_id,
                        'activity',
                        a.used_file_id,
                        a.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'activity'))} AS a
                )
                SELECT
                    used_file_id,
                    a.generated_file_id,
                    a.activity_id,
                    a.activity_table
                FROM activities AS a, UNNEST(a.used_file_id) AS used_file_id
                WHERE used_file_id IN ({', '.join(map(repr, file_ids))})
            ''')
            return {
                KeyLink(inputs={KeyReference(key=row['used_file_id'], entity_type='file')},
                        outputs={
                            KeyReference(key=file_id, entity_type='file')
                            for file_id in row['generated_file_id']
                        },
                        activity=KeyReference(key=row['activity_id'], entity_type=row['activity_table']))
                for row in rows
            }
        else:
            return set()

    def _retrieve_entities(self,
                           source: TDRSourceSpec,
                           entity_type: EntityType,
                           keys: AbstractSet[Key],
                           ) -> MutableJSONs:
        if keys:
            table_name = self._full_table_name(source, entity_type)
            columns = self._columns(entity_type)
            pk_column = entity_type + '_id'
            assert pk_column in columns, entity_type
            log.debug('Retrieving %i entities of type %r ...', len(keys), entity_type)
            rows = self._run_sql(f'''
                SELECT {', '.join(sorted(columns))}
                FROM {backtick(table_name)}
                WHERE {pk_column} IN ({', '.join(map(repr, keys))})
            ''')

            def convert_column(value):
                if isinstance(value, list):
                    value.sort()
                if isinstance(value, datetime.datetime):
                    return self.format_version(value)
                else:
                    return value

            rows = [
                {k: convert_column(v) for k, v in row.items()}
                for row in rows
            ]
            log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
            missing = keys - {row[pk_column] for row in rows}
            require(not missing,
                    f'Required entities not found in {table_name}: {missing}')
            return rows
        else:
            return []

    _schema_columns = {
        table['name']: [column['name'] for column in table['columns']]
        for table in anvil_schema['tables']
    }

    def _columns(self, entity_type: EntityType) -> set[str]:
        columns = set(self._schema_columns[f'anvil_{entity_type}'])
        columns.add('datarepo_row_id')
        return columns
