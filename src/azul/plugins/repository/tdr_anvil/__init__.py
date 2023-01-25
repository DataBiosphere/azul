from collections import (
    defaultdict,
)
import datetime
import logging
from operator import (
    itemgetter,
)
from typing import (
    AbstractSet,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Union,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    cached_property,
    require,
    uuids,
)
from azul.bigquery import (
    backtick,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
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
    AnyMutableJSON,
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

# AnVIL snapshots do not use UUIDs for primary/foreign keys.
# This type alias helps us distinguish these keys from the document UUIDs,
# which are drawn from the `datarepo_row_id` column.
# Note that entities from different tables may have the same key, so
# `KeyReference` should be used when mixing keys from different entity types.
Key = str


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class KeyReference:
    key: Key
    entity_type: EntityType


Keys = AbstractSet[KeyReference]
MutableKeys = set[KeyReference]
KeysByType = dict[EntityType, AbstractSet[Key]]
MutableKeysByType = dict[EntityType, set[Key]]


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class Link:
    inputs: Keys
    activity: Optional[KeyReference]
    outputs: Keys

    @property
    def all_entities(self) -> Keys:
        return self.inputs | self.outputs | (set() if self.activity is None else {self.activity})

    @classmethod
    def create(cls,
               *,
               inputs: Union[KeyReference, Iterable[KeyReference]],
               outputs: Union[KeyReference, Iterable[KeyReference]],
               activity: Optional[KeyReference] = None
               ) -> 'Link':
        if isinstance(inputs, KeyReference):
            inputs = (inputs,)
        if isinstance(outputs, KeyReference):
            outputs = (outputs,)
        return cls(inputs=frozenset(inputs),
                   outputs=frozenset(outputs),
                   activity=activity)

    @classmethod
    def merge(cls, links: Iterable['Link']) -> 'Link':
        return cls(inputs=frozenset.union(*[link.inputs for link in links]),
                   activity=one({link.activity for link in links}),
                   outputs=frozenset.union(*[link.outputs for link in links]))


Links = set[Link]


class Plugin(TDRPlugin):

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
                      ) -> list[TDRBundleFQID]:
        spec = source.spec
        partition_prefix = spec.prefix.common + prefix
        validate_uuid_prefix(partition_prefix)
        entity_type = TDRAnvilBundle.entity_type
        rows = self._run_sql(f'''
            SELECT datarepo_row_id
            FROM {backtick(self._full_table_name(spec, entity_type))}
            WHERE STARTS_WITH(datarepo_row_id, '{partition_prefix}')
        ''')
        return [
            TDRBundleFQID(source=source,
                          # Reversibly tweak the entity UUID to prevent
                          # collisions between entity IDs and bundle IDs
                          uuid=uuids.change_version(row['datarepo_row_id'],
                                                    self.datarepo_row_uuid_version,
                                                    self.bundle_uuid_version),
                          version=self._version)
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
        entity_type = TDRAnvilBundle.entity_type
        pk_column = entity_type + '_id'
        rows = self._run_sql(f'''
            SELECT prefix, COUNT({pk_column}) AS subgraph_count
            FROM {backtick(self._full_table_name(source.spec, entity_type))}
            JOIN UNNEST({prefixes}) AS prefix ON STARTS_WITH({pk_column}, prefix)
            GROUP BY prefix
        ''')
        return {row['prefix']: row['subgraph_count'] for row in rows}

    def _emulate_bundle(self, bundle_fqid: SourcedBundleFQID) -> Bundle:
        source = bundle_fqid.source
        bundle_entity = self._bundle_entity(bundle_fqid)

        keys: MutableKeys = {bundle_entity}
        links: Links = set()

        for method in [self._follow_downstream, self._follow_upstream]:
            method: Callable[[TDRSourceSpec, KeysByType], Links]
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
        result = TDRAnvilBundle(fqid=bundle_fqid, manifest=[], metadata_files={})
        entities_by_key: dict[KeyReference, EntityReference] = {}
        for entity_type, typed_keys in sorted(keys_by_type.items()):
            pk_column = entity_type + '_id'
            rows = self._retrieve_entities(source.spec, entity_type, typed_keys)
            for row in sorted(rows, key=itemgetter(pk_column)):
                key = KeyReference(key=row[pk_column], entity_type=entity_type)
                entity = EntityReference(entity_id=row['datarepo_row_id'], entity_type=entity_type)
                entities_by_key[key] = entity
                result.add_entity(entity, self._version, row)
        result.add_links(bundle_fqid, links, entities_by_key)

        return result

    def _bundle_entity(self, bundle_fqid: SourcedBundleFQID) -> KeyReference:
        source = bundle_fqid.source
        bundle_uuid = bundle_fqid.uuid
        entity_id = uuids.change_version(bundle_uuid,
                                         self.bundle_uuid_version,
                                         self.datarepo_row_uuid_version)
        entity_type = TDRAnvilBundle.entity_type
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

    def _simplify_links(self, links: Links) -> None:
        grouped_links = defaultdict(set)
        for link in links:
            grouped_links[link.activity].add(link)
        for activity, convergent_links in grouped_links.items():
            if activity is not None and len(convergent_links) > 1:
                links -= convergent_links
                links.add(Link.merge(convergent_links))

    def _follow_upstream(self,
                         source: TDRSourceSpec,
                         entities: KeysByType
                         ) -> Links:
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
                           ) -> Links:
        return set.union(
            self._downstream_from_biosamples(source, entities['biosample']),
            self._downstream_from_files(source, entities['file'])
        )

    def _upstream_from_biosamples(self,
                                  source: TDRSourceSpec,
                                  biosample_ids: AbstractSet[Key]
                                  ) -> Links:
        if biosample_ids:
            rows = self._run_sql(f'''
                SELECT b.biosample_id, b.donor_id, b.part_of_dataset_id
                FROM {backtick(self._full_table_name(source, 'biosample'))} AS b
                WHERE b.biosample_id IN ({', '.join(map(repr, biosample_ids))})
            ''')
            result: Links = set()
            for row in rows:
                downstream_ref = KeyReference(entity_type='biosample',
                                              key=row['biosample_id'])
                result.add(Link.create(outputs=downstream_ref,
                                       inputs=KeyReference(entity_type='dataset',
                                                           key=one(row['part_of_dataset_id']))))
                for donor_id in row['donor_id']:
                    result.add(Link.create(outputs=downstream_ref,
                                           inputs=KeyReference(entity_type='donor',
                                                               key=donor_id)))
            return result
        else:
            return set()

    def _upstream_from_files(self,
                             source: TDRSourceSpec,
                             file_ids: AbstractSet[Key]
                             ) -> Links:
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
                Link.create(
                    activity=KeyReference(entity_type=row['activity_table'], key=row['activity_id']),
                    # The generated link is not a complete representation of the
                    # upstream activity because it does not include generated files
                    # that are not ancestors of the downstream file
                    outputs=KeyReference(entity_type='file', key=row['generated_file_id']),
                    inputs=[
                        KeyReference(entity_type=entity_type, key=key)
                        for entity_type, column in [('file', 'uses_file_id'),
                                                    ('biosample', 'uses_biosample_id')]
                        for key in row[column]
                    ]
                )
                for row in rows
            }
        else:
            return set()

    def _diagnoses_from_donors(self,
                               source: TDRSourceSpec,
                               donor_ids: AbstractSet[Key]
                               ) -> Links:
        if donor_ids:
            rows = self._run_sql(f'''
                SELECT dgn.donor_id, dgn.diagnosis_id
                FROM {backtick(self._full_table_name(source, 'diagnosis'))} as dgn
                WHERE dgn.donor_id IN ({', '.join(map(repr, donor_ids))})
            ''')
            return {
                Link.create(inputs={KeyReference(key=row['diagnosis_id'], entity_type='diagnosis')},
                            outputs={KeyReference(key=row['donor_id'], entity_type='donor')},
                            activity=None)
                for row in rows
            }
        else:
            return set()

    def _downstream_from_biosamples(self,
                                    source: TDRSourceSpec,
                                    biosample_ids: AbstractSet[Key],
                                    ) -> Links:
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
                Link.create(inputs={KeyReference(key=row['biosample_id'], entity_type='biosample')},
                            outputs=[
                                KeyReference(key=output_id, entity_type='file')
                                for output_id in row['generated_file_id']
                            ],
                            activity=KeyReference(key=row['activity_id'], entity_type=row['activity_table']))
                for row in rows
            }
        else:
            return set()

    def _downstream_from_files(self,
                               source: TDRSourceSpec,
                               file_ids: AbstractSet[Key]
                               ) -> Links:
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
                Link.create(inputs=KeyReference(key=row['used_file_id'], entity_type='file'),
                            outputs=[
                                KeyReference(key=file_id, entity_type='file')
                                for file_id in row['generated_file_id']
                            ],
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
            columns = set.union(
                self.common_indexed_columns,
                self.indexed_columns_by_entity_type[entity_type]
            )
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


class TDRAnvilBundle(TDRBundle):
    entity_type: EntityType = 'biosample'

    def add_entity(self,
                   entity: EntityReference,
                   version: str,
                   row: MutableJSON
                   ) -> None:
        pk_column = entity.entity_type + '_id'
        self._add_entity(
            manifest_entry={
                'uuid': entity.entity_id,
                'version': version,
                'name': f'{entity.entity_type}_{row[pk_column]}',
                'indexed': True,
                'crc32': '',
                'sha256': '',
                **(
                    {'drs_path': self._parse_drs_uri(row.get('file_ref'))}
                    if entity.entity_type == 'file' else {}
                )
            },
            metadata=row
        )

    def add_links(self,
                  bundle_fqid: BundleFQID,
                  links: Links,
                  entities_by_key: Mapping[KeyReference, EntityReference]) -> None:
        def link_sort_key(link: JSON):
            return link['activity'] or '', link['inputs'], link['outputs']

        def key_ref_to_entity_ref(key_ref: KeyReference) -> str:
            return str(entities_by_key[key_ref])

        self._add_entity(
            manifest_entry={
                'uuid': bundle_fqid.uuid,
                'version': bundle_fqid.version,
                'name': 'links',
                'indexed': True
            },
            metadata=sorted((
                {
                    'inputs': sorted(map(key_ref_to_entity_ref, link.inputs)),
                    'activity': None if link.activity is None else key_ref_to_entity_ref(link.activity),
                    'outputs': sorted(map(key_ref_to_entity_ref, link.outputs))
                }
                for link in links
            ), key=link_sort_key)
        )

    def _add_entity(self,
                    *,
                    manifest_entry: MutableJSON,
                    metadata: AnyMutableJSON
                    ) -> None:
        name = manifest_entry['name']
        assert name not in self.metadata_files, name
        self.manifest.append(manifest_entry)
        self.metadata_files[name] = metadata

    def _parse_drs_uri(self, file_ref: Optional[str]) -> Optional[str]:
        if file_ref is None:
            return None
        else:
            return self._parse_drs_path(file_ref)
