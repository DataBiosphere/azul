from collections import (
    defaultdict,
)
import datetime
from hashlib import (
    sha1,
)
import logging
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
    JSON,
    cached_property,
    require,
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
    EntityID,
    EntityReference,
    EntityType,
)
from azul.plugins.repository.tdr import (
    TDRBundleFQID,
    TDRPlugin,
    TDRSourceRef,
)
from azul.strings import (
    pluralize,
)
from azul.terra import (
    TDRSourceSpec,
)
from azul.types import (
    AnyMutableJSON,
    MutableJSON,
    MutableJSONs,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

# The primary keys of AnVIL entities stored in TDR do not correspond to the
# entity IDs used by Azul
Key = str


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class KeyReference:
    key: Key
    entity_type: EntityType

    def __str__(self) -> str:
        return f'{self.entity_type}/{self.key}'

    @property
    def entity_id(self) -> EntityID:
        return sha1(f'{pluralize(self.entity_type)}:{self.key}'.encode()).hexdigest()[:32]

    def as_entity_reference(self) -> EntityReference:
        return EntityReference(entity_type=self.entity_type, entity_id=self.entity_id)


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

    def _bigquery_entity_id(self, key_str: str, entity_type: str) -> str:
        return f"CAST(LEFT(SHA1('{pluralize(entity_type)}:' || {key_str}), 16) AS STRING FORMAT 'hex')"

    def validate_entity_id(self, entity_id: str) -> None:
        # FIXME: Switch to using datarepo_row_id for partitioning and entity IDs
        #        https://github.com/DataBiosphere/azul/issues/4341
        require(len(entity_id) == 32 and set(entity_id) <= set('0123456789abcdef'),
                'The entity ID must be a string of 32 hexademical characters')

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRBundleFQID]:
        spec = source.spec
        partition_prefix = spec.prefix.common + prefix
        validate_uuid_prefix(partition_prefix)
        entity_type = TDRAnvilBundle.entity_type
        pk_column = entity_type + '_id'
        # FIXME: Switch to using datarepo_row_id for partitioning and entity IDs
        #        https://github.com/DataBiosphere/azul/issues/4341
        rows = self._run_sql(f'''
            SELECT {pk_column}
            FROM {backtick(self._full_table_name(spec, entity_type))}
            WHERE STARTS_WITH({self._bigquery_entity_id(pk_column, entity_type)}, '{partition_prefix}')
        ''')

        return [
            TDRBundleFQID(source=source,
                          uuid=KeyReference(key=row[pk_column], entity_type='bundle').entity_id,
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
        result.add_links(bundle_fqid, links)
        for entity_type, typed_keys in keys_by_type.items():
            pk_column = entity_type + '_id'
            for row in self._retrieve_entities(source.spec, entity_type, typed_keys):
                result.add_entity(KeyReference(key=row[pk_column], entity_type=entity_type),
                                  self._version,
                                  row)

        return result

    def _bundle_entity(self, bundle_fqid: SourcedBundleFQID) -> KeyReference:
        source = bundle_fqid.source
        entity_type = TDRAnvilBundle.entity_type
        pk_column = entity_type + '_id'
        # FIXME: Switch to using datarepo_row_id for partitioning and entity IDs
        #        https://github.com/DataBiosphere/azul/issues/4341
        bundle_entity = one(self._run_sql(f'''
            SELECT {pk_column}
            FROM {backtick(self._full_table_name(source.spec, entity_type))}
            WHERE {self._bigquery_entity_id(pk_column, 'bundle')} = '{bundle_fqid.uuid}'
        '''))[pk_column]
        bundle_entity = KeyReference(key=bundle_entity, entity_type=entity_type)
        log.info('Bundle ID %r resolved to native %s ID %r',
                 bundle_fqid.uuid, entity_type, bundle_entity.key)
        return bundle_entity

    def _consolidate_by_type(self, entities: Keys) -> MutableKeysByType:
        result = defaultdict(set)
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
            self._upstream_from_libraries(source, entities['library'])
            # Should we also follow donor.parent_donor?
        )

    def _follow_downstream(self,
                           source: TDRSourceSpec,
                           entities: KeysByType
                           ) -> Links:
        return set.union(
            self._downstream_from_libraries(source, entities['library']),
            self._downstream_from_files(source, entities['files'])
        )

    def _upstream_from_biosamples(self,
                                  source: TDRSourceSpec,
                                  biosample_ids: AbstractSet[Key]
                                  ) -> Links:
        if not biosample_ids:
            return set()
        rows = self._run_sql(f'''
            SELECT b.biosample_id, b.derived_from_biosample_id, b.donor_id
            FROM {backtick(self._full_table_name(source, 'biosample'))} AS b
            WHERE b.biosample_id IN ({', '.join(map(repr, biosample_ids))})
        ''')
        result: Links = set()
        for row in rows:
            downstream_ref = KeyReference(entity_type='biosample',
                                          key=row['biosample_id'])
            donor_id = row['donor_id']
            if donor_id is not None:
                result.add(Link.create(outputs=downstream_ref,
                                       inputs=KeyReference(entity_type='donor',
                                                           key=donor_id)))
            upstream_biosample_id = row['derived_from_biosample_id']
            if upstream_biosample_id is not None:
                result.add(Link.create(outputs=downstream_ref,
                                       inputs=KeyReference(entity_type='biosample',
                                                           key=upstream_biosample_id)))
        return result

    def _upstream_from_libraries(self,
                                 source: TDRSourceSpec,
                                 library_ids: AbstractSet[Key]
                                 ) -> Links:
        if not library_ids:
            return set()
        rows = self._run_sql(f'''
            SELECT lpa.generated_library_id, lpa.librarypreparationactivity_id, lpa.uses_sample_biosample_id
            FROM {backtick(self._full_table_name(source, 'librarypreparationactivity'))} AS lpa
            WHERE lpa.generated_library_id IN ({', '.join(map(repr, library_ids))})
        ''')
        return {
            Link.create(inputs=[
                KeyReference(entity_type='biosample',
                             key=biosample_id)
                for biosample_id in row['uses_sample_biosample_id']
            ],
                activity=KeyReference(entity_type='librarypreparationactivity',
                                      key=row['librarypreparationactivity_id']),
                outputs=KeyReference(entity_type='library',
                                     key=row['generated_library_id']))
            for row in rows
        }

    def _upstream_from_files(self,
                             source: TDRSourceSpec,
                             file_ids: AbstractSet[Key]
                             ) -> Links:
        if not file_ids:
            return set()
        rows = self._run_sql(f'''
            WITH file AS (
              SELECT f.file_id FROM {backtick(self._full_table_name(source, 'file'))} AS f
              WHERE f.file_id IN ({', '.join(map(repr, file_ids))})
            )
            SELECT
                  f.file_id AS generated_file_id,
                  'alignmentactivity' AS activity_type,
                  ama.alignmentactivity_id AS activity_id,
                  ama.uses_file_id AS uses_file_id,
                  [] AS uses_biosample_id,
                  [] AS library_id
              FROM file AS f
              JOIN {backtick(self._full_table_name(source, 'alignmentactivity'))} AS ama
                ON f.file_id IN UNNEST(ama.generated_file_id)
            UNION ALL SELECT
                  f.file_id,
                  'analysisactivity',
                  asa.analysisactivity_id,
                  asa.derived_from_file_id,
                  [],
                  []
              FROM file AS f
              JOIN {backtick(self._full_table_name(source, 'analysisactivity'))} AS asa
                ON f.file_id IN UNNEST(asa.generated_file_id)
            UNION ALL SELECT
                  f.file_id,
                  'assayactivity',
                  aya.assayactivity_id,
                  [],
                  aya.uses_sample_biosample_id,
                  aya.library_id
              FROM file AS f
              JOIN {backtick(self._full_table_name(source, 'assayactivity'))} AS aya
                ON f.file_id IN UNNEST(aya.generated_file_id)
            UNION ALL SELECT
                  f.file_id,
                  'sequencingactivity',
                  sqa.sequencingactivity_id,
                  [],
                  sqa.uses_sample_biosample_id,
                  sqa.library_id
              FROM file AS f
              JOIN {backtick(self._full_table_name(source, 'sequencingactivity'))} AS sqa
                ON f.file_id IN UNNEST(sqa.generated_file_id)
            UNION ALL SELECT
                  f.file_id,
                  'experimentactivity',
                  exa.experimentactivity_id,
                  exa.used_file_id,
                  exa.uses_sample_biosample_id,
                  exa.library_id
              FROM file AS f
              JOIN {backtick(self._full_table_name(source, 'experimentactivity'))} AS exa
                ON f.file_id IN UNNEST(exa.generated_file_id)
        ''')
        return {
            Link.create(
                activity=KeyReference(entity_type=row['activity_type'], key=row['activity_id']),
                # The generated link is not a complete representation of the
                # upstream activity because it does not include generated files
                # that are not ancestors of the downstream file
                outputs=KeyReference(entity_type='file', key=row['generated_file_id']),
                inputs=[
                    KeyReference(entity_type=entity_type, key=key)
                    for entity_type, column in [('file', 'uses_file_id'),
                                                ('biosample', 'uses_biosample_id'),
                                                ('library', 'library_id')]
                    for key in row[column]
                ]
            )
            for row in rows
        }

    def _downstream_from_libraries(self,
                                   source: TDRSourceSpec,
                                   library_ids: AbstractSet[Key]
                                   ) -> Links:
        if not library_ids:
            return set()
        rows = self._run_sql(f'''
            WITH activities AS (
                SELECT
                    exa.experimentactivity_id AS activity_id,
                    'experimentactivity' AS activity_type,
                    exa.library_id AS library_ids,
                    exa.generated_file_id AS file_ids
                FROM {backtick(self._full_table_name(source, 'experimentactivity'))} AS exa
                UNION ALL
                SELECT
                    sqa.sequencingactivity_id,
                    'sequencingactivity',
                    sqa.library_id,
                    sqa.generated_file_id
                FROM {backtick(self._full_table_name(source, 'sequencingactivity'))} AS sqa
                UNION ALL
                SELECT
                    aya.assayactivity_id,
                    'assayactivity',
                    aya.library_id,
                    aya.generated_file_id
                FROM {backtick(self._full_table_name(source, 'assayactivity'))} AS aya
            )
            SELECT
                library_id,
                a.activity_id,
                a.activity_type,
                a.file_ids
            FROM activities AS a, UNNEST(a.library_ids) AS library_id
            WHERE library_id IN ({', '.join(map(repr, library_ids))})
        ''')
        return {
            Link.create(inputs=KeyReference(key=row['library_id'], entity_type='library'),
                        outputs=[
                            KeyReference(key=file_id, entity_type='file')
                            for file_id in row['file_ids']
                        ],
                        activity=KeyReference(key=row['activity_id'], entity_type=row['activity_type']))
            for row in rows
        }

    def _downstream_from_files(self,
                               source: TDRSourceSpec,
                               file_ids: AbstractSet[Key]
                               ) -> Links:
        if not file_ids:
            return set()
        rows = self._run_sql(f'''
            WITH activities AS (
                SELECT
                    exa.experimentactivity_id AS activity_id,
                    'experimentactivity' AS activity_type,
                    exa.used_file_id AS used_file_ids,
                    exa.generated_file_id AS generated_file_ids
                FROM {backtick(self._full_table_name(source, 'experimentactivity'))} AS exa
                UNION ALL
                SELECT
                    asa.analysisactivity_id,
                    'analysisactivity',
                    asa.uses_file_id,
                    asa.generated_file_id
                FROM {backtick(self._full_table_name(source, 'analysisactivity'))} AS asa
                UNION ALL
                SELECT
                    ala.alignmentactivity_id,
                    'alignmentactivity',
                    ala.derived_file_id,
                    ala.generated_file_id
                FROM {backtick(self._full_table_name(source, 'alignmentactivity'))} AS ala
            )
            SELECT
                used_file_id,
                a.generated_file_ids,
                a.activity_id,
                a.activity_type
            FROM activities AS a, UNNEST(a.used_file_ids) AS used_file_id
            WHERE used_file_id IN ({', '.join(map(repr, file_ids))})
        ''')
        return {
            Link.create(inputs=KeyReference(key=row['used_file_id'],
                                            entity_type='file'),
                        outputs=[
                            KeyReference(key=file_id,
                                         entity_type='file')
                            for file_id in row['generated_file_ids']
                        ],
                        activity=KeyReference(key=row['actvity_id'], entity_type=row['activity_type']))
            for row in rows
        }

    def _retrieve_entities(self,
                           source: TDRSourceSpec,
                           entity_type: EntityType,
                           keys: AbstractSet[Key],
                           ) -> MutableJSONs:
        table_name = self._full_table_name(source, entity_type)
        columns = self.indexed_columns_by_entity_type[entity_type]
        pk_column = entity_type + '_id'
        assert pk_column in columns, entity_type
        log.debug('Retrieving %i entities of type %r ...', len(keys), entity_type)
        rows = self._run_sql(f'''
            SELECT {', '.join(columns)}
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

    # This could be consolidated with similar info from the metadata plugin?
    indexed_columns_by_entity_type = {
        'biosample': {
            'biosample_id',
            'biosample_type',
            'anatomical_site',
            'date_created',
            'date_obtained',
            'donor_age_at_collection_age_lowerbound',
            'donor_age_at_collection_age_upperbound',
            'donor_age_at_collection_age_stage',
            'donor_age_at_collection_age_unit',
            'health_status',
            'lab',
            'preservation_state',
            'xref'
        },
        'donor': {
            'donor_id',
            'date_created',
            'organism_type',
            'phenotypic_sex',
            'reported_ethnicity',
            'xref'
        },
        'file': {
            'file_id',
            'data_modality',
            'date_created',
            'file_format',
            'file_format_type',
            'file_type',
            'genome_annotation',
            'reference_assembly'
        },
        'library': {
            'library_id',
            'date_created',
            'prep_material_name',
            'xref'
        },
        'alignmentactivity': {
            'alignmentactivity_id',
            'data_modality',
            'date_created',
            'xref'
        },
        'analysisactivity': {
            'analysisactivity_id',
            'analysis_type',
            'xref'
        },
        'assayactivity': {
            'assayactivity_id',
            'assay_category',
            'data_modality',
            'date_created',
            'xref'
        },
        'experimentactivity': {
            'experimentactivity_id',
            'date_created',
            'date_submitted',
            'xref'
        },
        'librarypreparationactivity': {
            'librarypreparationactivity_id',
            'date_created'
        },
        'sequencingactivity': {
            'sequencingactivity_id',
            'data_modality',
            'date_created',
            'xref'
        }
    }

    def drs_uri(self, drs_path: Optional[str]) -> Optional[str]:
        assert drs_path is None, drs_path
        return None


class TDRAnvilBundle(Bundle[TDRSourceRef]):
    entity_type: EntityType = 'library'

    def drs_path(self, manifest_entry: JSON) -> Optional[str]:
        return None

    def add_entity(self,
                   entity: KeyReference,
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
                **({'drs_path': None} if entity.entity_type == 'file' else {})
            },
            metadata=row
        )

    def add_links(self, bundle_fqid: BundleFQID, links: Links) -> None:
        self._add_entity(
            manifest_entry={
                'uuid': bundle_fqid.uuid,
                'version': bundle_fqid.version,
                'name': 'links',
                'indexed': True
            },
            metadata=[
                {
                    'inputs': sorted(str(i.as_entity_reference()) for i in link.inputs),
                    'activity': None if link.activity is None else str(link.activity.as_entity_reference()),
                    'outputs': sorted(str(o.as_entity_reference()) for o in link.outputs)
                }
                for link in links
            ]
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
