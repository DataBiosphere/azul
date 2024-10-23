import datetime
from enum import (
    Enum,
)
import itertools
import logging
from operator import (
    itemgetter,
)
from typing import (
    AbstractSet,
    Callable,
    Iterable,
    Self,
    cast,
)
import uuid

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
    BigQueryRow,
    backtick,
)
from azul.drs import (
    DRSURI,
)
from azul.indexer import (
    Prefix,
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


class BundleType(Enum):
    """
    AnVIL snapshots have no inherent notion of a "bundle". During indexing, we
    dynamically construct bundles by querying each table in the snapshot. This
    class enumerates the tables that require special strategies for listing and
    fetching their bundles.

    Primary bundles are defined by a biosample entity, termed the bundle entity.
    Each primary bundle includes all entities derived from the bundle entity
    and all the ancestors of those entities, which are discovered by iteratively
    following foreign keys. Biosamples were chosen for this role based on a
    desirable balance between the size and number of the resulting bundles as
    well as the degree of overlap between them. The implementation of the graph
    traversal is tightly coupled to this choice, and switching to a different
    entity type would require re-implementing much of the Plugin code. Primary
    bundles consist of at least one biosample (the bundle entity), exactly one
    dataset, and zero or more other entities of assorted types. Primary bundles
    never contain orphans because they are bijective to rows in the biosample
    table.

    Supplementary bundles consist of batches of file entities, which may include
    supplementary files, which lack any foreign keys that associate them with
    any other entity. Non-supplementary files in the bundle are classified as
    orphans. The bundle also includes a dataset entity linked to the
    supplementary bundles.

    Duos bundles consist of a single dataset entity. The "entity" includes only
    the dataset description retrieved from DUOS, while a copy of the BigQuery
    row for this dataset is also included as an orphan. We chose this design
    because there is only one dataset per snapshot, which is referenced in all
    primary and supplementary bundles. Therefore, only one request to DUOS per
    *snapshot* is necessary, but if `description` is retrieved at the same time
    as the other dataset fields, we will make one request per *bundle* instead,
    potentially overloading the DUOS service. Our solution is to retrieve
    `description` only in a dedicated bundle format, once per snapshot, and
    merge it with the other dataset fields during aggregation.

    All other tables are batched like supplementary bundles, but only include
    orphans and have no links.
    """
    primary = 'anvil_biosample'
    supplementary = 'anvil_file'
    duos = 'anvil_dataset'

    def is_batched(self: Self | str) -> bool:
        """
        >>> BundleType.primary.is_batched()
        False

        >>> BundleType.is_batched('anvil_activity')
        True
        """
        if isinstance(self, str):
            try:
                self = BundleType(self)
            except ValueError:
                return True
        return self not in (BundleType.primary, BundleType.duos)


class TDRAnvilBundleFQIDJSON(SourcedBundleFQIDJSON):
    table_name: str
    batch_prefix: str | None


@attrs.frozen(kw_only=True, eq=False)
class TDRAnvilBundleFQID(TDRBundleFQID):
    table_name: str
    batch_prefix: str | None

    def to_json(self) -> TDRAnvilBundleFQIDJSON:
        return cast(TDRAnvilBundleFQIDJSON, super().to_json())

    def __attrs_post_init__(self):
        should_be_batched = BundleType.is_batched(self.table_name)
        is_batched = self.is_batched
        assert is_batched == should_be_batched, self
        if is_batched:
            assert len(self.batch_prefix) <= 8, self

    @property
    def is_batched(self) -> bool:
        return self.batch_prefix is not None


class TDRAnvilBundle(AnvilBundle[TDRAnvilBundleFQID], TDRBundle):

    @classmethod
    def canning_qualifier(cls) -> str:
        return super().canning_qualifier() + '.anvil'

    def add_entity(self,
                   entity: EntityReference,
                   version: str,
                   row: MutableJSON,
                   *,
                   is_orphan: bool = False
                   ) -> None:
        dst = self.orphans if is_orphan else self.entities
        # In DUOS bundles, the dataset is represented as both as entity and an
        # orphan
        assert entity not in dst, entity
        metadata = dict(row,
                        version=version)
        if entity.entity_type == 'anvil_file':
            drs_uri = row['file_ref']
            # Validate URI syntax
            DRSURI.parse(drs_uri)
            metadata.update(drs_uri=drs_uri,
                            sha256='',
                            crc32='')
        dst[entity] = metadata

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
    batch_uuid_version = 5
    bundle_uuid_version = 10

    def _batch_uuid(self,
                    source: TDRSourceSpec,
                    table_name: str,
                    batch_prefix: str
                    ) -> str:
        namespace = uuid.UUID('b8b3ac80-e035-4904-8b02-2d04f9e9a369')
        batch_uuid = uuid.uuid5(namespace, f'{source}:{table_name}:{batch_prefix}')
        return change_version(str(batch_uuid),
                              self.batch_uuid_version,
                              self.bundle_uuid_version)

    def _count_subgraphs(self, source: TDRSourceSpec) -> int:
        total = 0
        for table_name in self._list_tables(source):
            if BundleType.is_batched(table_name):
                _, bundle_count = self._batch_table(source, table_name, prefix='')
            else:
                rows = self._run_sql(f'''
                    SELECT COUNT(*) AS count
                    FROM {backtick(self._full_table_name(source, table_name))}
                ''')
                bundle_count = one(rows)['count']
            total += bundle_count
        return total

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRAnvilBundleFQID]:
        bundles = []
        spec = source.spec
        for table_name in self._list_tables(source.spec):

            def _yield(bundle_uuid: str, batch_prefix: str | None = None):
                bundles.append(TDRAnvilBundleFQID(
                    source=source,
                    uuid=bundle_uuid,
                    version=self._version,
                    table_name=table_name,
                    batch_prefix=batch_prefix,
                ))

            if table_name == BundleType.duos.value:
                row = one(self._run_sql(f'''
                    SELECT datarepo_row_id
                    FROM {backtick(self._full_table_name(spec, table_name))}
                '''))
                dataset_row_id = row['datarepo_row_id']
                # We intentionally omit the WHERE clause for datasets in order
                # to verify our assumption that each snapshot only contains rows
                # for a single dataset. This verification is performed
                # independently and concurrently for every partition, but only
                # one partition actually emits the bundle.
                if dataset_row_id.startswith(prefix):
                    _yield(bundle_uuid=change_version(dataset_row_id,
                                                      self.datarepo_row_uuid_version,
                                                      self.bundle_uuid_version))
            elif table_name == BundleType.primary.value:
                for row in self._run_sql(f'''
                    SELECT datarepo_row_id
                    FROM {backtick(self._full_table_name(spec, table_name))}
                    WHERE STARTS_WITH(datarepo_row_id, {prefix!r})
                '''):
                    _yield(bundle_uuid=change_version(row['datarepo_row_id'],
                                                      self.datarepo_row_uuid_version,
                                                      self.bundle_uuid_version))
            else:
                batch_prefix_length, _ = self._batch_table(spec, table_name, prefix)
                if batch_prefix_length is not None:
                    batch_prefixes = Prefix(common=prefix,
                                            partition=batch_prefix_length - len(prefix)).partition_prefixes()
                    for batch_prefix in batch_prefixes:
                        _yield(bundle_uuid=self._batch_uuid(spec, table_name, batch_prefix),
                               batch_prefix=batch_prefix)
        return bundles

    def _emulate_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        if bundle_fqid.table_name == BundleType.primary.value:
            log.info('Bundle %r is a primary bundle', bundle_fqid.uuid)
            return self._primary_bundle(bundle_fqid)
        elif bundle_fqid.table_name == BundleType.supplementary.value:
            log.info('Bundle %r is a supplementary bundle', bundle_fqid.uuid)
            return self._supplementary_bundle(bundle_fqid)
        elif bundle_fqid.table_name == BundleType.duos.value:
            assert config.duos_service_url is not None, bundle_fqid
            log.info('Bundle %r is a DUOS bundle', bundle_fqid.uuid)
            return self._duos_bundle(bundle_fqid)
        else:
            log.info('Bundle %r is a replica bundle', bundle_fqid.uuid)
            return self._replica_bundle(bundle_fqid)

    def _batch_table(self,
                     source: TDRSourceSpec,
                     table_name: str,
                     prefix: str,
                     ) -> tuple[int | None, int]:
        """
        Find a batch prefix length that yields as close to 256 rows per batch
        as possible within the specified table partition. The first element is
        the prefix length (*including* the partition prefix), or None if the
        table contains no rows, and the second element is the resulting number
        of batches.

        Because the partitions of a table do not contain exactly the same number
        of bundles, calculating the batch size statistics for the entire table
        at once produces a different result than performing the same calculation
        for any individual partition. We expect the inconsistencies to average
        out across partitions so that `_count_subgraphs` and `_list_bundles`
        give consistent results.

        This method relies on BigQuery's `AVG` function, which is
        nondeterministic for floating-point return values. The probability that
        this affects this method's return value is very small, but nonzero.
        https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#avg
        """
        max_length = 4

        def repeat(fmt):
            return ', '.join(fmt.format(i=i) for i in range(1, max_length + 1))

        prefix_len = len(prefix)
        log.info('Calculating batch prefix length for partition %r of table %r '
                 'in source %s', prefix, table_name, source)
        query = f'''
        WITH counts AS (
            SELECT
                {repeat(f'SUBSTR(datarepo_row_id, {prefix_len} + {{i}}, 1) AS p{{i}}')},
                COUNT(*) AS num_rows
            FROM {backtick(self._full_table_name(source, table_name))}
            WHERE STARTS_WITH(datarepo_row_id, {prefix!r})
            GROUP BY ROLLUP ({repeat('p{i}')})
        )
        SELECT
            {prefix_len} + LENGTH(CONCAT(
                {repeat('IFNULL(p{i}, "")')}
            )) AS batch_prefix_length,
            AVG(num_rows) AS average_batch_size,
            COUNT(*) AS num_batches
        FROM counts
        GROUP BY batch_prefix_length
        '''
        rows = list(self._run_sql(query))
        target_size = 256
        # `average_batch_size` is nondeterministic here
        if len(rows) > 0:
            row = min(rows, key=lambda row: abs(target_size - row['average_batch_size']))
            prefix_length = row['batch_prefix_length']
            average_size = row['average_batch_size']
            count = row['num_batches']
            log.info('Selected batch prefix length %d (average batch size %.1f, '
                     'num batches %d)', prefix_length, average_size, count)
        else:
            log.info('Table is empty, emitting no batches')
            prefix_length = None
            count = 0

        return prefix_length, count

    def _primary_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert not bundle_fqid.is_batched, bundle_fqid
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
            pk_column = entity_type.removeprefix('anvil_') + '_id'
            rows = self._retrieve_entities(source.spec, entity_type, typed_keys)
            if entity_type == 'anvil_donor':
                # We expect that the foreign key `part_of_dataset_id` is
                # redundant for biosamples and donors. To simplify our queries,
                # we do not follow the latter during the graph traversal.
                # Here, we validate our expectation. Note that the key is an
                # array for biosamples, but not for donors.
                dataset_id: Key = one(keys_by_type['anvil_dataset'])
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
        assert bundle_fqid.is_batched, bundle_fqid
        source = bundle_fqid.source.spec
        result = TDRAnvilBundle(fqid=bundle_fqid)
        linked_file_refs = set()
        for file_ref, file_row in self._get_batch(bundle_fqid):
            is_supplementary = file_row['is_supplementary']
            result.add_entity(file_ref,
                              self._version,
                              dict(file_row),
                              is_orphan=not is_supplementary)
            if is_supplementary:
                linked_file_refs.add(file_ref)
        dataset_ref, dataset_row = self._get_dataset(source)
        result.add_entity(dataset_ref, self._version, dict(dataset_row))
        result.add_links([EntityLink(inputs={dataset_ref}, outputs=linked_file_refs)])
        return result

    def _duos_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert not bundle_fqid.is_batched, bundle_fqid
        duos_info = self.tdr.get_duos(bundle_fqid.source)
        description = None if duos_info is None else duos_info.get('studyDescription')
        ref, row = self._get_dataset(bundle_fqid.source.spec)
        expected_entity_id = change_version(bundle_fqid.uuid,
                                            self.bundle_uuid_version,
                                            self.datarepo_row_uuid_version)
        assert ref.entity_id == expected_entity_id, (ref, bundle_fqid)
        bundle = TDRAnvilBundle(fqid=bundle_fqid)
        bundle.add_entity(ref, self._version, {'description': description})
        # Classify as orphan to suppress the emission of a contribution
        bundle.add_entity(ref, self._version, dict(row), is_orphan=True)
        return bundle

    def _replica_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert bundle_fqid.is_batched, bundle_fqid
        source = bundle_fqid.source.spec
        result = TDRAnvilBundle(fqid=bundle_fqid)
        batch = self._get_batch(bundle_fqid)
        dataset = self._get_dataset(source)
        for (ref, row) in itertools.chain([dataset], batch):
            result.add_entity(ref, self._version, dict(row), is_orphan=True)
        return result

    def _get_dataset(self, source: TDRSourceSpec) -> tuple[EntityReference, BigQueryRow]:
        table_name = 'anvil_dataset'
        columns = self._columns(table_name)
        row = one(self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source, table_name))}
        '''))
        ref = EntityReference(entity_type=table_name, entity_id=row['datarepo_row_id'])
        return ref, row

    def _get_batch(self,
                   bundle_fqid: TDRAnvilBundleFQID
                   ) -> Iterable[tuple[EntityReference, BigQueryRow]]:
        source = bundle_fqid.source.spec
        batch_prefix = bundle_fqid.batch_prefix
        table_name = bundle_fqid.table_name
        columns = self._columns(table_name)
        for row in self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source, table_name))}
            WHERE STARTS_WITH(datarepo_row_id, {batch_prefix!r})
        '''):
            ref = EntityReference(entity_type=table_name, entity_id=row['datarepo_row_id'])
            yield ref, row

    def _bundle_entity(self, bundle_fqid: TDRAnvilBundleFQID) -> KeyReference:
        source = bundle_fqid.source
        bundle_uuid = bundle_fqid.uuid
        entity_id = uuids.change_version(bundle_uuid,
                                         self.bundle_uuid_version,
                                         self.datarepo_row_uuid_version)
        table_name = bundle_fqid.table_name
        pk_column = table_name.removeprefix('anvil_') + '_id'
        bundle_entity = one(self._run_sql(f'''
            SELECT {pk_column}
            FROM {backtick(self._full_table_name(source.spec, table_name))}
            WHERE datarepo_row_id = '{entity_id}'
        '''))[pk_column]
        bundle_entity = KeyReference(key=bundle_entity, entity_type=table_name)
        log.info('Bundle UUID %r resolved to primary key %r in table %r',
                 bundle_uuid, bundle_entity.key, table_name)
        return bundle_entity

    def _consolidate_by_type(self, entities: Keys) -> MutableKeysByType:
        result = {
            table['name']: set()
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
            self._upstream_from_files(source, entities['anvil_file']),
            self._upstream_from_biosamples(source, entities['anvil_biosample']),
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
            self._diagnoses_from_donors(source, entities['anvil_donor'])
        )

    def _follow_downstream(self,
                           source: TDRSourceSpec,
                           entities: KeysByType
                           ) -> KeyLinks:
        return set.union(
            self._downstream_from_biosamples(source, entities['anvil_biosample']),
            self._downstream_from_files(source, entities['anvil_file'])
        )

    def _upstream_from_biosamples(self,
                                  source: TDRSourceSpec,
                                  biosample_ids: AbstractSet[Key]
                                  ) -> KeyLinks:
        if biosample_ids:
            rows = self._run_sql(f'''
                SELECT b.biosample_id, b.donor_id, b.part_of_dataset_id
                FROM {backtick(self._full_table_name(source, 'anvil_biosample'))} AS b
                WHERE b.biosample_id IN ({', '.join(map(repr, biosample_ids))})
            ''')
            result: KeyLinks = set()
            for row in rows:
                downstream_ref = KeyReference(entity_type='anvil_biosample',
                                              key=row['biosample_id'])
                result.add(KeyLink(outputs={downstream_ref},
                                   inputs={KeyReference(entity_type='anvil_dataset',
                                                        key=one(row['part_of_dataset_id']))}))
                for donor_id in row['donor_id']:
                    result.add(KeyLink(outputs={downstream_ref},
                                       inputs={KeyReference(entity_type='anvil_donor',
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
                  SELECT f.file_id FROM {backtick(self._full_table_name(source, 'anvil_file'))} AS f
                  WHERE f.file_id IN ({', '.join(map(repr, file_ids))})
                )
                SELECT
                      f.file_id AS generated_file_id,
                      'anvil_alignmentactivity' AS activity_table,
                      ama.alignmentactivity_id AS activity_id,
                      ama.used_file_id AS uses_file_id,
                      [] AS uses_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'anvil_alignmentactivity'))} AS ama
                    ON f.file_id IN UNNEST(ama.generated_file_id)
                UNION ALL SELECT
                      f.file_id,
                      'anvil_assayactivity',
                      aya.assayactivity_id,
                      [],
                      aya.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'anvil_assayactivity'))} AS aya
                    ON f.file_id IN UNNEST(aya.generated_file_id)
                UNION ALL SELECT
                      f.file_id,
                      'anvil_sequencingactivity',
                      sqa.sequencingactivity_id,
                      [],
                      sqa.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'anvil_sequencingactivity'))} AS sqa
                    ON f.file_id IN UNNEST(sqa.generated_file_id)
                UNION ALL SELECT
                    f.file_id,
                    'anvil_variantcallingactivity',
                    vca.variantcallingactivity_id,
                    vca.used_file_id,
                    []
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'anvil_variantcallingactivity'))} AS vca
                    ON f.file_id IN UNNEST(vca.generated_file_id)
                UNION ALL SELECT
                    f.file_id,
                    'anvil_activity',
                    a.activity_id,
                    a.used_file_id,
                    a.used_biosample_id,
                  FROM file AS f
                  JOIN {backtick(self._full_table_name(source, 'anvil_activity'))} AS a
                    ON f.file_id IN UNNEST(a.generated_file_id)
            ''')
            return {
                KeyLink(
                    activity=KeyReference(entity_type=row['activity_table'], key=row['activity_id']),
                    # The generated link is not a complete representation of the
                    # upstream activity because it does not include generated files
                    # that are not ancestors of the downstream file
                    outputs={KeyReference(entity_type='anvil_file', key=row['generated_file_id'])},
                    inputs={
                        KeyReference(entity_type=entity_type, key=key)
                        for entity_type, column in [('anvil_file', 'uses_file_id'),
                                                    ('anvil_biosample', 'uses_biosample_id')]
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
                FROM {backtick(self._full_table_name(source, 'anvil_diagnosis'))} as dgn
                WHERE dgn.donor_id IN ({', '.join(map(repr, donor_ids))})
            ''')
            return {
                KeyLink(inputs={KeyReference(key=row['diagnosis_id'], entity_type='anvil_diagnosis')},
                        outputs={KeyReference(key=row['donor_id'], entity_type='anvil_donor')},
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
                        'anvil_sequencingactivity' as activity_table,
                        sqa.used_biosample_id,
                        sqa.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'anvil_sequencingactivity'))} AS sqa
                    UNION ALL
                    SELECT
                        aya.assayactivity_id,
                        'anvil_assayactivity',
                        aya.used_biosample_id,
                        aya.generated_file_id,
                    FROM {backtick(self._full_table_name(source, 'anvil_assayactivity'))} AS aya
                    UNION ALL
                    SELECT
                        a.activity_id,
                        'anvil_activity',
                        a.used_biosample_id,
                        a.generated_file_id,
                    FROM {backtick(self._full_table_name(source, 'anvil_activity'))} AS a
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
                KeyLink(inputs={KeyReference(key=row['biosample_id'], entity_type='anvil_biosample')},
                        outputs={
                            KeyReference(key=output_id, entity_type='anvil_file')
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
                        'anvil_alignmentactivity' AS activity_table,
                        ala.used_file_id,
                        ala.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'anvil_alignmentactivity'))} AS ala
                    UNION ALL SELECT
                        vca.variantcallingactivity_id,
                        'anvil_variantcallingactivity',
                        vca.used_file_id,
                        vca.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'anvil_variantcallingactivity'))} AS vca
                    UNION ALL SELECT
                        a.activity_id,
                        'anvil_activity',
                        a.used_file_id,
                        a.generated_file_id
                    FROM {backtick(self._full_table_name(source, 'anvil_activity'))} AS a
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
                KeyLink(inputs={KeyReference(key=row['used_file_id'], entity_type='anvil_file')},
                        outputs={
                            KeyReference(key=file_id, entity_type='anvil_file')
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
            columns = self._columns(entity_type)
            table_name = self._full_table_name(source, entity_type)
            pk_column = entity_type.removeprefix('anvil_') + '_id'
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

    def _columns(self, table_name: str) -> set[str]:
        try:
            columns = self._schema_columns[table_name]
        except KeyError:
            return {'*'}
        else:
            columns = set(columns)
            columns.add('datarepo_row_id')
            return columns

    def _list_tables(self, source: TDRSourceSpec) -> AbstractSet[str]:
        return set(self.tdr.list_tables(source)) - {
            'datarepo_row_ids'
        }
