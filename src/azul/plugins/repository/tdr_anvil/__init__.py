from collections import (
    defaultdict,
)
from collections.abc import (
    Set,
)
import datetime
from enum import (
    Enum,
)
from functools import (
    cache,
)
import itertools
import logging
from operator import (
    itemgetter,
)
import re
from typing import (
    Callable,
    ClassVar,
    Iterable,
    Mapping,
    Self,
)
import uuid

import attrs
from more_itertools import (
    one,
)

from azul import (
    config,
)
from azul.drs import (
    DRSURI,
)
from azul.indexer import (
    BundleUUID,
    BundleVersion,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.bigquery import (
    BigQueryRow,
    backtick,
)
from azul.lib.collections import (
    singleton,
)
from azul.lib.time import (
    format_dcp2_datetime,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
    not_none,
    optional,
)
from azul.lib.uuids import (
    change_version,
)
from azul.plugins.metadata.anvil import (
    AnvilFile,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    EntityLink,
    Key,
    KeyLink,
    KeyReference,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schemas,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRPlugin,
)
from azul.source import (
    Prefix,
)
from azul.terra import (
    TDRSourceRef,
    TDRSourceSpec,
)

log = logging.getLogger(__name__)

#: The version of the AnVIL schema that this module was written against
#:
anvil_schema = anvil_schemas[6]

#: AnVIL snapshots are not versioned, so every AnVIL bundle has this version,
#: and so does every entity in one
#:
fixed_version: BundleVersion = format_dcp2_datetime(
    datetime.datetime(year=2022, month=6, day=1, hour=0,
                      tzinfo=datetime.timezone.utc))

Keys = Set[KeyReference]
MutableKeys = set[KeyReference]
KeysByType = dict[EntityType, Set[Key]]
MutableKeysByType = dict[EntityType, set[Key]]
KeyLinks = set[KeyLink]


@attrs.frozen(kw_only=True)
class SnapshotName:
    """
    The name of an AnVIL snapshot, broken into its components.
    """

    #: Most snapshots spell the prefix `ANVIL`, a few outliers use `AnVIL`
    #:
    prefix: str

    #: How the name spells the dataset.
    #:
    dataset: str

    #: The date the dataset was created, as `YYYYMMDD`
    #:
    dataset_date: str

    #: The version of the AnVIL schema the snapshot was ingested under
    #:
    schema_version: int = attrs.field(converter=int)

    #: The time the snapshot was created, as `YYYYMMDDhhmm`
    #:
    snapshot_time: str

    # The names of the attributes above must match those of the capture groups
    _pattern: ClassVar[re.Pattern] = re.compile(r'(?P<prefix>(?i:ANVIL))'
                                                r'_(?P<dataset>\w+)'
                                                r'_(?P<dataset_date>\d{8})'
                                                r'_ANV(?P<schema_version>\d+)'
                                                r'_(?P<snapshot_time>\d{12})',
                                                re.ASCII)

    @classmethod
    @cache
    def parse(cls, name: str) -> Self:
        """
        >>> SnapshotName.parse('ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330')
        ... # doctest: +NORMALIZE_WHITESPACE
        SnapshotName(prefix='ANVIL',
                     dataset='CMG_UWash_GRU',
                     dataset_date='20240301',
                     schema_version=5,
                     snapshot_time='202403040330')

        >>> SnapshotName.parse('AnVIL_GTEx_V8_hg38_20230419_ANV5_202304202007').prefix
        'AnVIL'

        >>> SnapshotName.parse('ANVIL_1000G_2019_Dev_20230609_ANV_202306121732')
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R('Snapshot name does not match the expected convention',
        'ANVIL_1000G_2019_Dev_20230609_ANV_202306121732')
        """
        match = cls._pattern.fullmatch(name)
        assert match is not None, R(
            'Snapshot name does not match the expected convention', name)
        self = cls(**match.groupdict())
        assert name == str(self), R('Snapshot name is not canonical', name)
        return self

    def __str__(self) -> str:
        """
        The inverse of :py:meth:`parse`.

        >>> name = 'ANVIL_CMG_UWash_GRU_20240301_ANV5_202403040330'
        >>> name == str(SnapshotName.parse(name))
        True
        """
        return '_'.join([
            self.prefix,
            self.dataset,
            self.dataset_date,
            f'ANV{self.schema_version}',
            self.snapshot_time
        ])


#: The namespace of the UUIDs identifying AnVIL entities
#:
_entity_id_namespace = uuid.UUID('06d615ea-1a34-41c0-8780-135ebaead1c7')


def _entity_id(source: TDRSourceSpec, table_name: str, key: Key) -> EntityID:
    """
    The ID of the entity with the given primary key, in the given table of the
    given snapshot.

    AnVIL primary keys are only unique per table and snapshot. Qualifying the
    key with the table and the dataset makes the resulting ID globally unique
    while also maximizing stability across releases of that dataset. Previously
    this plugin used the datarepo_row_id to identify entities, which was
    globally unique but would change with every new snapshot for any given
    dataset.

    The letter case of a dataset name has been observed to vary between
    releases, so it needs to be normalized to lower case.
    """
    dataset = SnapshotName.parse(source.name).dataset.lower()
    name = ':'.join([dataset, table_name, key])
    return str(uuid.uuid5(_entity_id_namespace, name))


#: The namespace of the UUIDs identifying batches of AnVIL rows
#:
_batch_id_namespace = uuid.UUID('b8b3ac80-e035-4904-8b02-2d04f9e9a369')


def _batch_id(source: TDRSourceSpec, table_name: str, batch_prefix: str) -> str:
    """
    The ID of the batch of rows whose primary key, or, for non-schema tables,
    whose row ID starts with the given prefix, in the given table of the given
    snapshot. The UUID of non-batched bundles is derived from the entity ID of
    their singular bundle entity. Batched bundles have no such entity, so the
    batch ID is used instead.
    """
    name = f'{source}:{table_name}:{batch_prefix}'
    return str(uuid.uuid5(_batch_id_namespace, name))


class BundleType(Enum):
    """
    Unlike HCA, AnVIL has no inherent notion of a "bundle". Its data model is
    strictly relational: each row in a table represents an entity, each entity
    has a primary key, and entities reference each other via foreign keys.
    During indexing, we dynamically construct bundles by querying each table in
    the snapshot. This class enumerates the tables that require special
    strategies for listing and fetching their bundles.

    An orphan is defined as an AnVIL entity that does not appear in any of
    Azul's `/index/{entity_type}`. Bundles *can* contain orphans, but they will
    only ever manifest as replicas in our index. A *local orphan* is an entity
    in a bundle that is not referenced anywhere in that bundle's links. Local
    orphans may or may not be true/global orphans (because they may still be
    references in *other* bundles' links), but all global orphans are always
    local orphans. Bundles only contain local orphans from the table that
    matches the bundle's `table_name` attribute.
    """

    #: The BigQuery table associated with this bundle type. If this is None for
    #: an enum member, it must appear last and serves as a catch-all for all
    #: tables not listed on other members.
    #:
    table_name: str | None

    #: Whether this bundle contains batches of rows from the table associated
    #: with this bundle type, or just a single row.
    #:
    is_batched: bool

    #: Primary bundles are defined by a biosample entity, termed the *bundle
    #: entity*. Each primary bundle includes all of the bundle entity's
    #: descendants and all of those entities' ancestors. Descendants and
    #: ancestors are discovered by iteratively following foreign keys.
    #: Biosamples were chosen to act as the bundle entities for primary bundles
    #: based on a desirable balance between the size and number of the resulting
    #: bundles as well as the degree of overlap between them. The implementation
    #: of the graph traversal is tightly coupled to this choice, and switching
    #: to a different bundle entity type would require re-implementing much of
    #: the Plugin code. Primary bundles consist of at least one biosample (the
    #: bundle entity), exactly one dataset entity, and zero or more other
    #: entities of assorted types. Primary bundles never contain local orphans
    #: because they are bijective to rows in the biosample table.
    #:
    primary = ('anvil_biosample', False)

    #: Supplementary bundles consist of batches of file entities, which may
    #: include supplementary files. Supplementary files lack any foreign keys
    #: that would associate them with any other entity. Each supplementary
    #: bundle also includes a dataset entity, and we create synthetic links
    #: between the supplementary files and the dataset. Without these links, the
    #: relationship between these files and their parent dataset would not be
    #: properly represented in the service response. Supplementary files
    #: therefore are never local or global orphans.
    #:
    #: Normal (non-supplementary) files are not linked to the dataset and thus
    #: are local orphans within these bundles. This is because these files may
    #: also appear in primary bundles. If they do, then those bundles will
    #: contribute them to the index alongside all of their linked entities. If
    #: they don't, then they are global orphans. In either case, it would be
    #: pointless for a supplementary bundle to emit contributions for them,
    #: hence we treat them as orphans.
    #:
    supplementary = ('anvil_file', True)

    #: A placeholder entry to indicate that we don't create any bundles for the
    #: rows of the anvil_dataset table. Instead, the dataset entity is added
    #: explicitly to each primary and replica bundle.
    #:
    no_bundle = ('anvil_dataset', False)

    #: All other tables are represented by replica bundles. Replica bundles
    #: consist of a batch of rows from an arbitrary BigQuery table, which may or
    #: may not be described by the AnVIL schema, and the snapshot's dataset
    #: entity. Replica bundles contain no links and thus all of their entities
    #: are local orphans.
    #:
    replica = (None, True)

    def __init__(self, table_name: str | None, is_batched: bool) -> None:
        super().__init__()
        self.table_name = table_name
        self.is_batched = is_batched

    @classmethod
    def for_table(cls, table_name: str) -> BundleType:
        for self in cls:
            if self.table_name == table_name:
                return self
        return cls.replica


@attrs.frozen(kw_only=True, eq=False, init=False)
class TDRAnvilBundleFQID(TDRBundleFQID):
    table_name: str
    batch_prefix: str | None

    #: The primary key of the bundle entity. A bundle's UUID is derived from
    #: this key, and that derivation can't be inverted, so the key has to
    #: travel with the FQID for the bundle to be fetched. Batched bundles have
    #: no bundle entity, and therefore no key.
    #:
    primary_key: Key | None

    #: The version of the UUIDs we derive with `uuid5`
    #:
    _derived_uuid_version: ClassVar[int] = 5

    #: The version of bundle UUIDs, which distinguishes them from the entity
    #: and batch IDs they are derived from
    #:
    _bundle_uuid_version: ClassVar[int] = 10

    def __init__(self,
                 *,
                 uuid: BundleUUID | None = None,
                 version: BundleVersion = fixed_version,
                 source: TDRSourceRef,
                 table_name: str,
                 batch_prefix: str | None = None,
                 primary_key: Key | None = None
                 ) -> None:
        """
        Construct an AnVIL bundle FQID. The `uuid` parameter can be omitted,
        in which case it will be derived from the other arguments. If it is
        passed, it must be consistent with the other arguments. Similarly, the
        `version` parameter can be omitted. If it is passed, it must be equal
        to `fixed_version`.

        Either `batch_prefix` or `primary_key` must be given and the choice must
        match `table_name`: batched tables require the former, all other tables
        the latter.
        """
        if batch_prefix is None:
            derived_uuid = _entity_id(source.spec, table_name, not_none(primary_key))
        else:
            derived_uuid = _batch_id(source.spec, table_name, batch_prefix)
        derived_uuid = change_version(derived_uuid,
                                      self._derived_uuid_version,
                                      self._bundle_uuid_version)
        assert uuid is None or uuid == derived_uuid, R(
            'Bundle UUID disagrees with the attributes it is derived from',
            uuid, derived_uuid)
        assert version == fixed_version, R(
            'AnVIL bundles all have the same version',
            version, fixed_version)
        self.__attrs_init__(uuid=derived_uuid,
                            version=version,
                            source=source,
                            table_name=table_name,
                            batch_prefix=batch_prefix,
                            primary_key=primary_key)

    def __attrs_post_init__(self):
        should_be_batched = BundleType.for_table(self.table_name).is_batched
        is_batched = self.is_batched
        assert is_batched == should_be_batched, self
        assert is_batched == (self.primary_key is None), self
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
        target = self.orphans if is_orphan else self.entities
        # The assertion below detects duplicate primary keys within a table, for
        # all but one table. Partitioning, batching and graph traversal are all
        # keyed on the primary key, so two rows sharing a primary key always end
        # up in the same bundle and trip this assertion, causing an obvious
        # failure during indexing. Note that this assertion does not detect
        # duplicates in `anvil_dataset`; those are detected by `_get_dataset`.
        assert entity not in self.entities and entity not in self.orphans, R(
            'Two rows are identified by the same entity reference',
            entity, self.fqid)
        metadata = dict(row,
                        version=version)
        if entity.entity_type == 'anvil_file':
            drs_uri = row['file_ref']
            # Validate URI syntax
            DRSURI.parse(drs_uri)
            metadata.update(drs_uri=drs_uri)
        target[entity] = metadata

    def add_links(self, links: Iterable[EntityLink]):
        self.links.update(links)
        # Merge links that share the same (non-null) activity
        groups_by_activity: dict[EntityReference, set[EntityLink]] = defaultdict(set)
        for link in self.links:
            if link.activity is not None:
                groups_by_activity[link.activity].add(link)
        for activity, group in groups_by_activity.items():
            if len(group) > 1:
                self.links -= group
                merged_link = EntityLink(
                    inputs=frozenset.union(*[link.inputs for link in group]),
                    activity=activity,
                    outputs=frozenset.union(*[link.outputs for link in group])
                )
                self.links.add(merged_link)


class Plugin(TDRPlugin[TDRAnvilBundle, TDRAnvilBundleFQID]):

    def _entity_ref(self,
                    source: TDRSourceSpec,
                    table_name: str,
                    row: BigQueryRow
                    ) -> EntityReference:
        """
        A reference to the entity represented by the given row of the given
        table of the given snapshot.
        """
        pk_column = self._pk_column(source, table_name)
        if pk_column is None:
            # The schema doesn't describe this table, so it declares no primary
            # key, leaving nothing stable to derive an ID from. Entities from
            # such tables only ever occur as replicas.
            entity_id = row['datarepo_row_id']
        else:
            entity_id = _entity_id(source, table_name, row[pk_column])
        return EntityReference(entity_type=table_name, entity_id=entity_id)

    def count_files(self, source: TDRSourceRef) -> int:
        prefix = '' if source.prefix is None else source.prefix.common
        assert prefix == prefix.lower(), source
        query = f'''
        SELECT COUNT(*) AS count
        FROM {backtick(self._full_table_name(source.spec, 'anvil_file'))}
        WHERE STARTS_WITH(LOWER(file_md5sum), {prefix!r})
        '''
        return one(self._run_sql(query))['count']

    def count_bundles(self, source: TDRSourceRef) -> int:
        prefix = '' if source.prefix is None else source.prefix.common
        assert prefix == prefix.lower(), source
        table_name = BundleType.primary.table_name
        pk_column = not_none(self._pk_column(source.spec, table_name))
        primary_count = one(self._run_sql(f'''
            SELECT COUNT(*) AS count
            FROM {backtick(self._full_table_name(source.spec, table_name))}
            WHERE STARTS_WITH(LOWER({pk_column}), {prefix!r})
        '''))['count']
        sizes_by_table = self._batch_tables(source.spec, prefix)
        batched_count = sum(batch_size for (_, batch_size) in sizes_by_table.values())
        return primary_count + batched_count

    def list_bundles(self,
                     source: TDRSourceRef,
                     prefix: str
                     ) -> list[TDRAnvilBundleFQID]:
        self._assert_source(source)
        self._assert_partition(source, prefix)
        assert prefix == prefix.lower(), prefix
        bundles = []
        spec = source.spec
        table_name = BundleType.primary.table_name
        pk_column = not_none(self._pk_column(spec, table_name))
        for row in self._run_sql(f'''
            SELECT {pk_column}
            FROM {backtick(self._full_table_name(spec, table_name))}
            WHERE STARTS_WITH(LOWER({pk_column}), {prefix!r})
        '''):
            bundle_fqid = TDRAnvilBundleFQID(source=source,
                                             table_name=table_name,
                                             primary_key=row[pk_column])
            bundles.append(bundle_fqid)
        prefix_lengths_by_table = self._batch_tables(source.spec, prefix)
        for table_name, (batch_prefix_length, _) in prefix_lengths_by_table.items():
            batch_prefixes = Prefix(common=prefix,
                                    partition=batch_prefix_length - len(prefix)).partition_prefixes()
            for batch_prefix in batch_prefixes:
                bundle_fqid = TDRAnvilBundleFQID(source=source,
                                                 table_name=table_name,
                                                 batch_prefix=batch_prefix)
                bundles.append(bundle_fqid)
        return bundles

    def list_files(self, source: TDRSourceRef, prefix: str) -> list[AnvilFile]:
        self._assert_source(source)
        self._assert_partition(source, prefix)
        batch = self._get_batch(source.spec,
                                'anvil_file',
                                prefix,
                                key_column=self._column_from_64_to_hex('file_md5sum'))

        def missing_md5(row: BigQueryRow) -> bool:
            missing = row['file_md5sum'] is None
            if missing:
                # FIXME: Files from 1000G snapshot in anvildev can't be mirrored
                #        https://github.com/DataBiosphere/azul/issues/7634
                assert source.spec.name == 'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732', R(
                    'File lacks MD5 digest', source, dict(row))
            return missing

        return [
            AnvilFile(uuid=ref.entity_id,
                      name=row['file_name'],
                      version=fixed_version,
                      size=row['file_size'],
                      md5=row['file_md5sum'],
                      drs_uri=row['file_ref'],
                      source=source)
            for ref, row in batch
            if not missing_md5(row)
        ]

    def _emulate_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        if bundle_fqid.table_name == BundleType.primary.table_name:
            log.info('Bundle %r is a primary bundle', bundle_fqid.uuid)
            return self._primary_bundle(bundle_fqid)
        elif bundle_fqid.table_name == BundleType.supplementary.table_name:
            log.info('Bundle %r is a supplementary bundle', bundle_fqid.uuid)
            return self._supplementary_bundle(bundle_fqid)
        else:
            log.info('Bundle %r is a replica bundle', bundle_fqid.uuid)
            return self._replica_bundle(bundle_fqid)

    def _batch_tables(self,
                      source: TDRSourceSpec,
                      prefix: str,
                      ) -> dict[str, tuple[int, int]]:
        """
        Find a batch prefix length that yields as close to 256 rows per batch
        as possible for each table within the specified partition. The result's
        keys are table names and its values are tuples where the first element
        is the prefix length (*including* the partition prefix) and the second
        element is the resulting number of batches. Tables are only included in
        the result if they are non-empty and are used to produce batched bundle
        formats (i.e. replica and supplementary).

        Because the partitions of a table do not contain exactly the same number
        of bundles, calculating the batch size statistics for the entire table
        at once produces a different result than performing the same calculation
        for any individual partition. We expect the inconsistencies to average
        out across partitions so that `count_bundles` and `list_bundles` give
        consistent results as long the partition size is substantially larger
        than the batch size.

        This method relies on BigQuery's `AVG` function, which is
        nondeterministic for floating-point return values. The probability that
        this affects this method's return value is very small, but nonzero.
        https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions#avg
        """
        assert prefix == prefix.lower(), prefix
        max_length = 4

        def repeat(fmt):
            return ', '.join(fmt.format(i=i) for i in range(1, max_length + 1))

        def is_batched(table_name):
            return BundleType.for_table(table_name).is_batched

        target_size = 256
        prefix_len = len(prefix)
        table_names = self.tdr.list_tables(source)
        # This table is present in all snapshots. It is large and contains no
        # useful metadata, so we skip indexing replicas from it.
        table_names.discard('datarepo_row_ids')
        # The dataset entity is already included as an orphan in every other
        # replica bundle, so a dedicated anvil_dataset batch would be redundant.
        table_names.discard(BundleType.no_bundle.table_name)
        table_names = sorted(filter(is_batched, table_names))
        batch_columns_by_table = {
            table_name: self._batch_column(source, table_name)
            for table_name in table_names
        }
        log.info('Calculating batch prefix lengths for partition %r of %d tables '
                 'in source %s', prefix, len(table_names), source)
        # The extraneous outer 'SELECT *' works around a bug in BigQuery emulator
        # FIXME: BigQuery Emulator rejects valid query
        #        https://github.com/DataBiosphere/azul/issues/6704
        query = ' UNION ALL '.join(f'''(
            SELECT * FROM (
                SELECT
                    {table_name!r} AS table_name,
                    {prefix_len} + LENGTH(CONCAT(
                        {repeat('IFNULL(p{i}, "")')}
                    )) AS batch_prefix_length,
                    AVG(num_rows) AS average_batch_size,
                    COUNT(*) AS num_batches
                FROM (
                    SELECT
                        {repeat(f'LOWER(SUBSTR({batch_column}, {prefix_len} + {{i}}, 1)) AS p{{i}}')},
                        COUNT(*) AS num_rows
                    FROM {backtick(self._full_table_name(source, table_name))}
                    WHERE STARTS_WITH(LOWER({batch_column}), {prefix!r})
                    GROUP BY ROLLUP ({repeat('p{i}')})
                )
                GROUP BY batch_prefix_length
                ORDER BY ABS({target_size} - average_batch_size), batch_prefix_length
                LIMIT 1
            )
        )''' for table_name, batch_column in batch_columns_by_table.items())

        def result(row):
            table_name = row['table_name']
            prefix_length = row['batch_prefix_length']
            average_size = row['average_batch_size']
            num_batches = row['num_batches']
            log.info('Selected batch prefix length %d for table %r (average '
                     'batch size %.1f, num batches %d)',
                     prefix_length, table_name, average_size, num_batches)
            return table_name, (prefix_length, num_batches)

        return dict(map(result, self._run_sql(query)))

    def _primary_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert not bundle_fqid.is_batched, bundle_fqid
        source = bundle_fqid.source
        bundle_entity = KeyReference(key=not_none(bundle_fqid.primary_key),
                                     entity_type=bundle_fqid.table_name)

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
            log.debug('Found %r linked entities via method %s',
                      len(keys) - n, method.__name__)

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
            pk_column = not_none(self._pk_column(source.spec, entity_type))
            rows = self._retrieve_entities(source.spec, entity_type, typed_keys)
            if entity_type == 'anvil_dataset':
                for row in rows:
                    self._augment_dataset_with_duos(row, source)
            if entity_type == 'anvil_donor':
                # We expect that the foreign key `part_of_dataset_id` is
                # redundant for biosamples and donors. To simplify our queries,
                # we do not follow the latter during the graph traversal.
                # Here, we validate our expectation. Note that the key is an
                # array for biosamples, but not for donors.
                dataset_id: Key = one(keys_by_type['anvil_dataset'])
                for row in rows:
                    donor_dataset_id = row['part_of_dataset_id']
                    assert donor_dataset_id == dataset_id, R(
                        'Conflicting keys', donor_dataset_id, dataset_id)
            for row in sorted(rows, key=itemgetter(pk_column)):
                key = KeyReference(key=row[pk_column], entity_type=entity_type)
                entity = self._entity_ref(source.spec, entity_type, row)
                entities_by_key[key] = entity
                result.add_entity(entity, fixed_version, row)
        result.add_links(link.to_entity_link(entities_by_key) for link in links)
        return result

    def _supplementary_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert bundle_fqid.is_batched, bundle_fqid
        result = TDRAnvilBundle(fqid=bundle_fqid)
        linked_file_refs = set()
        for file_ref, file_row in self._get_bundle_batch(bundle_fqid):
            is_supplementary = file_row['is_supplementary']
            result.add_entity(file_ref,
                              fixed_version,
                              dict(file_row),
                              is_orphan=not is_supplementary)
            if is_supplementary:
                linked_file_refs.add(file_ref)
        dataset_ref, dataset_row = self._get_dataset(bundle_fqid.source)
        result.add_entity(dataset_ref, fixed_version, dataset_row)
        # Avoid inserting "degenerate" links with an empty list of outputs, i.e.
        # in case of an empty batch (as is common on `anvilbox`). Such links
        # would be harmless in production, but would complicate the bundle
        # canning integration test.
        if linked_file_refs:
            result.add_links([
                EntityLink(inputs=singleton(dataset_ref),
                           outputs=frozenset(linked_file_refs))
            ])
        return result

    def _replica_bundle(self, bundle_fqid: TDRAnvilBundleFQID) -> TDRAnvilBundle:
        assert bundle_fqid.is_batched, bundle_fqid
        result = TDRAnvilBundle(fqid=bundle_fqid)
        batch = self._get_bundle_batch(bundle_fqid)
        dataset = self._get_dataset(bundle_fqid.source)
        for ref, row in itertools.chain([dataset], batch):
            result.add_entity(ref, fixed_version, dict(row), is_orphan=True)
        return result

    def _get_dataset(self, source: TDRSourceRef) -> tuple[EntityReference, MutableJSON]:
        table_name = 'anvil_dataset'
        columns = self._columns(source.spec, table_name)
        row = dict(one(self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source.spec, table_name))}
        ''')))
        # Check that the dataset name encoded in the snapshot name matches the
        # dataset title from the anvil_dataset table row within that snapshot.
        # See :func:`_entity_id` for why the comparison is case-insensitive.
        name = SnapshotName.parse(source.spec.name)
        title = row['title']
        expected_title = f'{name.prefix}_{name.dataset}'
        assert optional(str.lower, title) == expected_title.lower(), R(
            'Dataset title does not match the snapshot name', title, expected_title)
        ref = self._entity_ref(source.spec, table_name, row)
        self._augment_dataset_with_duos(row, source)
        return ref, row

    def _augment_dataset_with_duos(self,
                                   row: MutableJSON,
                                   source: TDRSourceRef
                                   ) -> None:
        if config.duos_service_url is not None:
            duos_id, duos_info = self.tdr.get_duos(source)
            if duos_id is not None:
                row['duos_id'] = duos_id
                row['description'] = duos_info.get('studyDescription')
                return
        row['duos_id'] = None
        row['description'] = None

    def _get_batch(self,
                   source: TDRSourceSpec,
                   table_name: str,
                   batch_prefix: str,
                   *,
                   key_column: str
                   ) -> Iterable[tuple[EntityReference, BigQueryRow]]:
        columns = self._columns(source, table_name)
        assert not any(map(str.isupper, batch_prefix)), source
        for row in self._run_sql(f'''
            SELECT {', '.join(sorted(columns))}
            FROM {backtick(self._full_table_name(source, table_name))}
            WHERE STARTS_WITH(LOWER({key_column}), {batch_prefix!r})
        '''):
            ref = self._entity_ref(source, table_name, row)
            yield ref, row

    def _get_bundle_batch(self,
                          bundle_fqid: TDRAnvilBundleFQID
                          ) -> Iterable[tuple[EntityReference, BigQueryRow]]:
        source = bundle_fqid.source.spec
        table_name = bundle_fqid.table_name
        return self._get_batch(source,
                               table_name,
                               bundle_fqid.batch_prefix,
                               key_column=self._batch_column(source, table_name))

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
                                  biosample_ids: Set[Key]
                                  ) -> KeyLinks:
        if biosample_ids:
            rows = self._run_sql(f'''
                SELECT b.biosample_id, b.donor_id, b.part_of_dataset_id
                FROM {backtick(self._full_table_name(source, 'anvil_biosample'))} AS b
                WHERE b.biosample_id IN ({', '.join(map(repr, biosample_ids))})
            ''')
            result: KeyLinks = set()
            for row in rows:
                outputs = singleton(KeyReference(entity_type='anvil_biosample',
                                                 key=row['biosample_id']))
                inputs = singleton(KeyReference(entity_type='anvil_dataset',
                                                key=one(row['part_of_dataset_id'])))
                result.add(KeyLink(outputs=outputs, inputs=inputs))
                for donor_id in row['donor_id']:
                    inputs = singleton(KeyReference(entity_type='anvil_donor',
                                                    key=donor_id))
                    result.add(KeyLink(outputs=outputs, inputs=inputs))
            return result
        else:
            return set()

    def _upstream_from_files(self,
                             source: TDRSourceSpec,
                             file_ids: Set[Key]
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
                    activity=KeyReference(entity_type=row['activity_table'],
                                          key=row['activity_id']),
                    # The generated link is not a complete representation of the
                    # upstream activity because it does not include generated files
                    # that are not ancestors of the downstream file
                    outputs=singleton(
                        KeyReference(entity_type='anvil_file',
                                     key=row['generated_file_id'])),
                    inputs=frozenset(
                        KeyReference(entity_type=entity_type,
                                     key=key)
                        for entity_type, column in [
                            ('anvil_file', 'uses_file_id'),
                            ('anvil_biosample', 'uses_biosample_id')
                        ]
                        for key in row[column]
                    )
                )
                for row in rows
            }
        else:
            return set()

    def _diagnoses_from_donors(self,
                               source: TDRSourceSpec,
                               donor_ids: Set[Key]
                               ) -> KeyLinks:
        if donor_ids:
            rows = self._run_sql(f'''
                SELECT dgn.donor_id, dgn.diagnosis_id
                FROM {backtick(self._full_table_name(source, 'anvil_diagnosis'))} as dgn
                WHERE dgn.donor_id IN ({', '.join(map(repr, donor_ids))})
            ''')
            return {
                KeyLink(
                    inputs=singleton(
                        KeyReference(key=row['diagnosis_id'],
                                     entity_type='anvil_diagnosis')),
                    outputs=singleton(
                        KeyReference(key=row['donor_id'],
                                     entity_type='anvil_donor')),
                    activity=None)
                for row in rows
            }
        else:
            return set()

    def _downstream_from_biosamples(self,
                                    source: TDRSourceSpec,
                                    biosample_ids: Set[Key],
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
                KeyLink(
                    inputs=singleton(
                        KeyReference(key=row['biosample_id'],
                                     entity_type='anvil_biosample')
                    ),
                    outputs=frozenset(
                        KeyReference(key=output_id,
                                     entity_type='anvil_file')
                        for output_id in row['generated_file_id']
                    ),
                    activity=KeyReference(key=row['activity_id'],
                                          entity_type=row['activity_table']))
                for row in rows
            }
        else:
            return set()

    def _downstream_from_files(self,
                               source: TDRSourceSpec,
                               file_ids: Set[Key]
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
                KeyLink(
                    inputs=singleton(
                        KeyReference(key=row['used_file_id'],
                                     entity_type='anvil_file')),
                    outputs=frozenset(
                        KeyReference(key=file_id,
                                     entity_type='anvil_file')
                        for file_id in row['generated_file_id']
                    ),
                    activity=KeyReference(key=row['activity_id'],
                                          entity_type=row['activity_table']))
                for row in rows
            }
        else:
            return set()

    def _retrieve_entities(self,
                           source: TDRSourceSpec,
                           entity_type: EntityType,
                           keys: Set[Key],
                           ) -> MutableJSONs:
        if keys:
            columns = self._columns(source, entity_type)
            table_name = self._full_table_name(source, entity_type)
            pk_column = not_none(self._pk_column(source, entity_type))
            assert pk_column in columns, entity_type
            log.debug('Retrieving %i entities of type %r ...', len(keys), entity_type)
            rows = self._run_sql(f'''
                SELECT {', '.join(sorted(columns))}
                FROM {backtick(table_name)}
                WHERE {pk_column} IN ({', '.join(map(repr, keys))})
            ''')

            rows = [
                {
                    k: self.format_version(v) if isinstance(v, datetime.datetime) else v
                    for k, v in row.items()
                }
                for row in rows
            ]
            log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
            missing = keys - {row[pk_column] for row in rows}
            assert not missing, R(
                f'Found only {len(rows)} out of {len(keys)} expected rows in {table_name}. '
                f'Missing entities: {missing}')
            return rows
        else:
            return []

    def validate_source_spec(self, source_spec: TDRSourceSpec) -> None:
        self._schema_version(source_spec)

    def _schema_version(self, source: TDRSourceSpec) -> int:
        """
        The version of the AnVIL schema the given snapshot was ingested under.
        A snapshot's tables have the columns of that version, not those of the
        most recent version, so the two must not be confused when selecting
        columns from a snapshot.
        """
        version = SnapshotName.parse(source.name).schema_version
        assert version in anvil_schemas, R(
            'Snapshot was ingested under an untracked schema version',
            source.name, version, sorted(anvil_schemas))
        return version

    @cached_property
    def _schema_columns_by_version_and_table(self) -> Mapping[int, Mapping[str, Set[str]]]:
        return {
            version: self._schema_columns_by_table(schema)
            for version, schema in anvil_schemas.items()
        }

    def _schema_columns_by_table(self, schema: JSON) -> Mapping[str, Set[str]]:
        columns_by_table = {}
        for table in schema['tables']:
            table_name = table['name']
            column_names = {column['name'] for column in table['columns']}
            column_names.add('datarepo_row_id')
            if table_name == 'anvil_file':
                column = 'file_md5sum'
                column_names.remove(column)
                column_names.add(f'{self._column_from_64_to_hex(column)} AS {column}')
            columns_by_table[table_name] = column_names
        return columns_by_table

    @cached_property
    def _pk_columns_by_schema_version_and_table(self) -> Mapping[int, Mapping[str, str]]:
        return {
            schema_version: {
                table['name']: one(table['primaryKey'])
                for table in schema['tables']
            }
            for schema_version, schema in anvil_schemas.items()
        }

    def _pk_column(self, source: TDRSourceSpec, table_name: str) -> str | None:
        """
        The name of the column holding the primary key of the given table, as
        declared by the schema the given snapshot was ingested under, or None
        for tables the schema doesn't describe.
        """
        version = self._schema_version(source)
        return self._pk_columns_by_schema_version_and_table[version].get(table_name)

    def _batch_column(self, source: TDRSourceSpec, table_name: str) -> str:
        """
        The name of the column whose value's prefix assigns a row of the given
        batched table to a batch. Tables the schema doesn't describe declare no
        primary key, so their rows are batched by their row ID instead.
        """
        return self._pk_column(source, table_name) or 'datarepo_row_id'

    def _columns(self, source: TDRSourceSpec, table_name: str) -> Set[str]:
        version = self._schema_version(source)
        columns_by_table = self._schema_columns_by_version_and_table[version]
        # Include all columns for replicas of non-schema tables
        return columns_by_table.get(table_name, {'*'})

    def _column_from_64_to_hex(self, column: str) -> str:
        return f'TO_HEX(FROM_BASE64({column}))'
