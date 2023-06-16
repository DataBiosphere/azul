from collections import (
    defaultdict,
)
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
    Mapping,
    Optional,
)

import attr
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
from azul.indexer import (
    SourcedBundleFQIDJSON,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    Key,
    KeyReference,
    Link,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRPlugin,
    TDRSourceRef,
)
from azul.terra import (
    TDRSourceSpec,
)
from azul.types import (
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

Keys = AbstractSet[KeyReference]
MutableKeys = set[KeyReference]
KeysByType = dict[EntityType, AbstractSet[Key]]
MutableKeysByType = dict[EntityType, set[Key]]
KeyLinks = set[Link[KeyReference]]


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
    """
    primary: EntityType = 'biosample'
    supplementary: EntityType = 'file'


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class AnvilBundleFQID(TDRBundleFQID):
    entity_type: BundleEntityType = attr.ib(converter=BundleEntityType)

    def to_json(self) -> SourcedBundleFQIDJSON:
        return dict(super().to_json(),
                    entity_type=self.entity_type.value)


class TDRAnvilBundle(AnvilBundle[AnvilBundleFQID], TDRBundle):

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
            metadata.update(drs_path=self._parse_drs_uri(row.get('file_ref')),
                            sha256='',
                            crc32='')
        self.entities[entity] = metadata

    def add_links(self,
                  links: KeyLinks,
                  entities_by_key: Mapping[KeyReference, EntityReference]) -> None:
        def key_ref_to_entity_ref(key_ref: KeyReference) -> EntityReference:
            return entities_by_key[key_ref]

        def optional_key_ref_to_entity_ref(key_ref: Optional[KeyReference]) -> Optional[EntityReference]:
            return None if key_ref is None else key_ref_to_entity_ref(key_ref)

        self.links.update(
            Link(inputs=set(map(key_ref_to_entity_ref, link.inputs)),
                 activity=optional_key_ref_to_entity_ref(link.activity),
                 outputs=set(map(key_ref_to_entity_ref, link.outputs)))
            for link in links
        )

    def _parse_drs_uri(self, file_ref: Optional[str]) -> Optional[str]:
        if file_ref is None:
            return None
        else:
            return self._parse_drs_path(file_ref)


class Plugin(TDRPlugin[TDRAnvilBundle, TDRSourceSpec, TDRSourceRef, AnvilBundleFQID]):

    @cached_property
    def _version(self):
        return self.format_version(datetime.datetime(year=2022,
                                                     month=6,
                                                     day=1,
                                                     hour=0,
                                                     tzinfo=datetime.timezone.utc))

    datarepo_row_uuid_version = 4
    bundle_uuid_version = 10

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[AnvilBundleFQID]:
        spec = source.spec
        partition_prefix = spec.prefix.common + prefix
        validate_uuid_prefix(partition_prefix)
        primary = BundleEntityType.primary.value
        supplementary = BundleEntityType.supplementary.value
        rows = self._run_sql(f'''
            SELECT datarepo_row_id, {primary!r} AS entity_type
            FROM {backtick(self._full_table_name(spec, primary))}
            WHERE STARTS_WITH(datarepo_row_id, '{partition_prefix}')
            UNION ALL
            SELECT datarepo_row_id, {supplementary!r} AS entity_type
            FROM {backtick(self._full_table_name(spec, supplementary))} AS supp
            WHERE supp.is_supplementary AND STARTS_WITH(datarepo_row_id, '{partition_prefix}')
        ''')
        return [
            AnvilBundleFQID(source=source,
                            # Reversibly tweak the entity UUID to prevent
                            # collisions between entity IDs and bundle IDs
                            uuid=uuids.change_version(row['datarepo_row_id'],
                                                      self.datarepo_row_uuid_version,
                                                      self.bundle_uuid_version),
                            version=self._version,
                            entity_type=BundleEntityType(row['entity_type']))
            for row in rows
        ]

    def list_partitions(self,
                        source: TDRSourceRef
                        ) -> Mapping[str, int]:
        prefix = source.spec.prefix
        prefixes = [
            prefix.common + partition_prefix
            for partition_prefix in prefix.partition_prefixes()
        ]
        assert prefixes, prefix
        primary = BundleEntityType.primary.value
        supplementary = BundleEntityType.supplementary.value
        rows = self._run_sql(f'''
            SELECT prefix, COUNT(datarepo_row_id) AS subgraph_count
            FROM (
                SELECT datarepo_row_id FROM {backtick(self._full_table_name(source.spec, primary))}
                UNION ALL
                SELECT datarepo_row_id FROM {backtick(self._full_table_name(source.spec, supplementary))}
                WHERE is_supplementary
            )
            JOIN UNNEST({prefixes}) AS prefix ON STARTS_WITH(datarepo_row_id, prefix)
            GROUP BY prefix
        ''')
        return {row['prefix']: row['subgraph_count'] for row in rows}

    def resolve_bundle(self, fqid: SourcedBundleFQIDJSON) -> AnvilBundleFQID:
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

    def _emulate_bundle(self, bundle_fqid: AnvilBundleFQID) -> TDRAnvilBundle:
        if bundle_fqid.entity_type is BundleEntityType.primary:
            log.info('Bundle %r is a primary bundle', bundle_fqid.uuid)
            return self._primary_bundle(bundle_fqid)
        elif bundle_fqid.entity_type is BundleEntityType.supplementary:
            log.info('Bundle %r is a supplementary bundle', bundle_fqid.uuid)
            return self._supplementary_bundle(bundle_fqid)
        else:
            assert False, bundle_fqid.entity_type

    def _primary_bundle(self, bundle_fqid: AnvilBundleFQID) -> TDRAnvilBundle:
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

        self._simplify_links(links)
        result = TDRAnvilBundle(fqid=bundle_fqid)
        entities_by_key: dict[KeyReference, EntityReference] = {}
        for entity_type, typed_keys in sorted(keys_by_type.items()):
            pk_column = entity_type + '_id'
            rows = self._retrieve_entities(source.spec, entity_type, typed_keys)
            if entity_type == 'donors':
                # We expect that the foreign key `part_of_dataset_id` is
                # redundant for biosamples and donors. To simplify our queries,
                # we do not follow the latter during the graph traversal.
                # Here, we validate our expectation.
                dataset_id: Key = one(keys_by_type['datasets'])
                for row in rows:
                    require(row.pop('part_of_dataset_id') == dataset_id)
            for row in sorted(rows, key=itemgetter(pk_column)):
                key = KeyReference(key=row[pk_column], entity_type=entity_type)
                entity = EntityReference(entity_id=row['datarepo_row_id'], entity_type=entity_type)
                entities_by_key[key] = entity
                result.add_entity(entity, self._version, row)
        result.add_links(links, entities_by_key)
        return result

    def _supplementary_bundle(self, bundle_fqid: AnvilBundleFQID) -> TDRAnvilBundle:
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
        entities_by_key = {}
        link_args = {}
        for entity_type, row, arg in [
            (bundle_entity_type, bundle_entity, 'outputs'),
            (linked_entity_type, linked_entity, 'inputs')
        ]:
            entity_ref = EntityReference(entity_type=entity_type, entity_id=row['datarepo_row_id'])
            key_ref = KeyReference(key=row[entity_type + '_id'], entity_type=entity_type)
            entities_by_key[key_ref] = entity_ref
            result.add_entity(entity_ref, self._version, row)
            link_args[arg] = {key_ref}
        result.add_links({Link(**link_args)}, entities_by_key)
        return result

    def _bundle_entity(self, bundle_fqid: AnvilBundleFQID) -> KeyReference:
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
        result = {entity_type: set() for entity_type in self.indexed_columns_by_entity_type}
        for e in entities:
            result[e.entity_type].add(e.key)
        return result

    def _simplify_links(self, links: KeyLinks) -> None:
        grouped_links: Mapping[KeyReference, KeyLinks] = defaultdict(set)
        for link in links:
            grouped_links[link.activity].add(link)
        for activity, convergent_links in grouped_links.items():
            if activity is not None and len(convergent_links) > 1:
                links -= convergent_links
                links.add(Link.merge(convergent_links))

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
                result.add(Link(outputs={downstream_ref},
                                inputs={KeyReference(entity_type='dataset',
                                                     key=one(row['part_of_dataset_id']))}))
                for donor_id in row['donor_id']:
                    result.add(Link(outputs={downstream_ref},
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
                Link(
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
                Link(inputs={KeyReference(key=row['diagnosis_id'], entity_type='diagnosis')},
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
                Link(inputs={KeyReference(key=row['biosample_id'], entity_type='biosample')},
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
                Link(inputs={KeyReference(key=row['used_file_id'], entity_type='file')},
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

    def _columns(self, entity_type: EntityType) -> set[str]:
        entity_columns = self.indexed_columns_by_entity_type[entity_type]
        return self.common_indexed_columns | entity_columns

    common_indexed_columns = {
        'datarepo_row_id',
        'source_datarepo_row_ids'
    }

    # This could be consolidated with similar info from the metadata plugin?
    indexed_columns_by_entity_type = {
        'biosample': {
            'biosample_id',
            'anatomical_site',
            'apriori_cell_type',
            'biosample_type',
            'disease',
            'donor_age_at_collection_unit',
            'donor_age_at_collection_lower_bound',
            'donor_age_at_collection_upper_bound',
        },
        'dataset': {
            'dataset_id',
            'consent_group',
            'data_use_permission',
            'owner',
            'principal_investigator',
            'registered_identifier',
            'title',
            'data_modality'
        },
        'diagnosis': {
            'diagnosis_id',
            'disease',
            'diagnosis_age_unit',
            'diagnosis_age_lower_bound',
            'diagnosis_age_upper_bound',
            'onset_age_unit',
            'onset_age_lower_bound',
            'onset_age_upper_bound',
            'phenotype',
            'phenopacket'
        },
        'donor': {
            'donor_id',
            'organism_type',
            'phenotypic_sex',
            'reported_ethnicity',
            'genetic_ancestry',
            # Not stored in index; only retrieved to verify redundancy with
            # biosample.part_of_dataset_id
            'part_of_dataset_id'
        },
        'file': {
            'file_id',
            'data_modality',
            'file_format',
            'file_size',
            'file_md5sum',
            'reference_assembly',
            'file_name',
            'file_ref',
            'is_supplementary',
        },
        'activity': {
            'activity_id',
            'activity_type',
        },
        'alignmentactivity': {
            'alignmentactivity_id',
            'activity_type',
            'data_modality',
            'reference_assembly',
            # Not in schema
            'date_created',
        },
        'assayactivity': {
            'assayactivity_id',
            'activity_type',
            'assay_type',
            'data_modality',
            # Not in schema
            'date_created',
        },
        'sequencingactivity': {
            'sequencingactivity_id',
            'activity_type',
            'assay_type',
            'data_modality',
        },
        'variantcallingactivity': {
            'variatncallingactivity_id',
            'activity_type',
            'reference_assembly',
            'data_modality'
        }
    }
