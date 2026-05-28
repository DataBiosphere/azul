from abc import (
    ABC,
)
from collections import (
    defaultdict,
)
import json
from operator import (
    itemgetter,
)
from typing import (
    Iterable,
    Type,
    cast,
)
from unittest.mock import (
    Mock,
    PropertyMock,
    patch,
)

from furl import (
    furl,
)
from more_itertools import (
    one,
)
from urllib3 import (
    HTTPResponse,
)

from azul import (
    config,
)
from azul.indexer.cache_service import (
    UrlCacheService,
)
from azul.indexer.document import (
    DocumentType,
    EntityReference,
)
from azul.lib import (
    R,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schemas,
)
from azul.plugins.repository import (
    tdr_anvil,
)
from azul.plugins.repository.tdr_anvil import (
    TDRAnvilBundle,
    _entity_id,
    fixed_version,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
)
from azul_test_case import (
    TDRTestCase,
)
from indexer import (
    AnvilCannedBundleTestCase,
    IndexerTestCase,
)
from indexer.test_tdr import (
    TDRPluginTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class DUOSTestCase(TDRTestCase, ABC):

    def _mock_normal_duos(self):
        for p in self._duos_patches(self.normal_tdr_response,
                                    self.normal_duos_response):
            self.addPatch(p)

    @property
    def normal_tdr_response(self) -> MutableJSON:
        return {
            'name': self.source.ref.spec.name,
            'duosFirecloudGroup': {'duosId': 'DUOS-000000'}
        }

    @property
    def normal_duos_response(self) -> MutableJSON:
        return {
            'consentGroups': [{'datasetIdentifier': 'DUOS-000000'}],
            'studyDescription': 'Study description from DUOS'
        }

    def _duos_patches(self,
                      tdr_response: JSON,
                      duos_response: JSON | None = None
                      ) -> Iterable[patch]:
        tdr_mock = Mock(spec=HTTPResponse, status=200,
                        data=json.dumps(tdr_response))
        duos_host = 'mock_duos.lan'
        mock_url = PropertyMock(return_value=furl(f'https://{duos_host}'))
        mock_cache = Mock(spec=UrlCacheService)
        if duos_response is None:
            duos_mock = None
        else:
            duos_mock = Mock(spec=HTTPResponse, status=200,
                             data=json.dumps(duos_response))

        def get_url(url, *_args, **_kwargs):
            # The snapshot metadata and the DUOS dataset registration are
            # fetched through the same cache, and are told apart by their host
            return duos_mock if url.host == duos_host else tdr_mock

        mock_cache.get_url.side_effect = get_url
        patches = [
            patch.object(type(config), 'duos_service_url', new=mock_url),
            patch.object(TDRClient, '_url_cache', new=mock_cache),
        ]
        return patches


class AnvilIndexerTestCase(AnvilCannedBundleTestCase, IndexerTestCase):
    pass


class TestAnvilIndexer(AnvilIndexerTestCase,
                       TDRPluginTestCase[tdr_anvil.Plugin],
                       DUOSTestCase):

    @classmethod
    def _plugin_cls(cls) -> Type[tdr_anvil.Plugin]:
        return tdr_anvil.Plugin

    def test_indexing(self):
        self.maxDiff = None
        bundle = self.primary_bundle()
        canned_hits = self._load_canned_result(bundle)
        for enable_replicas in True, False:
            with patch.object(target=type(config),
                              attribute='enable_replicas',
                              new_callable=PropertyMock,
                              return_value=enable_replicas):
                with self.subTest(enable_replicas=enable_replicas):
                    if enable_replicas:
                        expected_hits = canned_hits
                    else:
                        expected_hits = [
                            h
                            for h in canned_hits
                            if self._parse_index_name(h)[1] is not DocumentType.replica
                        ]
                    self.index_service.create_indices(self.catalog)
                    try:
                        self._index_canned_bundle(bundle)
                        hits = self._get_all_hits()
                        hits.sort(key=itemgetter('_id'))
                        self.assertElasticEqual(expected_hits, hits)
                    finally:
                        self._purge_indices()

    def test_list_and_fetch_bundles(self):
        self._mock_normal_duos()
        source_ref = self.source.ref
        self._make_mock_tables(source_ref)
        canned_bundle_fqids = [
            self.primary_bundle(),
            self.supplementary_bundle(),
            self.replica_bundle(),
        ]
        expected_bundle_fqids = sorted(canned_bundle_fqids + [
            # Replica bundles for the AnVIL schema tables, which we don't can
            self.bundle_fqid(table_name='anvil_activity'),
            self.bundle_fqid(table_name='anvil_alignmentactivity'),
            self.bundle_fqid(table_name='anvil_assayactivity'),
            self.bundle_fqid(table_name='anvil_diagnosis'),
            self.bundle_fqid(table_name='anvil_donor'),
            self.bundle_fqid(table_name='anvil_sequencingactivity'),
            self.bundle_fqid(table_name='anvil_variantcallingactivity')
        ])
        plugin = self.plugin
        bundle_fqids = sorted(plugin.list_bundles(source_ref, ''))
        self.assertEqual(expected_bundle_fqids, bundle_fqids)
        for bundle_fqid in bundle_fqids:
            with self.subTest(bundle_fqid=bundle_fqid):
                bundle = plugin.fetch_bundle(bundle_fqid)
                assert isinstance(bundle, TDRAnvilBundle)
                if bundle_fqid in canned_bundle_fqids:
                    canned_bundle = self._load_canned_bundle(bundle_fqid)
                    assert isinstance(canned_bundle, TDRAnvilBundle)
                    self.assertEqual(canned_bundle.fqid, bundle.fqid)
                    self.assertEqual(canned_bundle.entities, bundle.entities)
                    self.assertEqual(canned_bundle.links, bundle.links)
                    self.assertEqual(canned_bundle.orphans, bundle.orphans)

    def test_absent_duos_id(self):
        source_ref = self.source.ref
        self._make_mock_tables(source_ref)
        cases = {
            'Absent duosFirecloudGroup':
                {'name': self.source.ref.spec.name},
            'Empty duosFirecloudGroup':
                {
                    'name': self.source.ref.spec.name,
                    'duosFirecloudGroup': {}
                },
            'Null duosId':
                {
                    'name': self.source.ref.spec.name,
                    'duosFirecloudGroup': {'duosId': None}
                },
        }
        for sub_test, tdr_response in cases.items():
            with self.subTest(sub_test):
                with self.stacked_patches(self._duos_patches(tdr_response)):
                    bundle = self.plugin.fetch_bundle(self.primary_bundle())
                    self.assertIsInstance(bundle, TDRAnvilBundle)
                    dataset_ref = one(
                        ref for ref in bundle.entities
                        if ref.entity_type == 'anvil_dataset'
                    )
                    metadata = bundle.entities[dataset_ref]
                    self.assertIsNone(metadata['duos_id'])
                    self.assertIsNone(metadata['description'])

    def test_schema_version(self):
        plugin = self.plugin
        for name, expected in [
            ('ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732', 5),
            ('ANVIL_1000G_high_coverage_2019_20230517_ANV6_202607071430', 6),
            # Some snapshots spell the prefix `AnVIL`
            ('AnVIL_1000G_2019_Dev_20230609_ANV5_202306121732', 5)
        ]:
            with self.subTest(name=name):
                self.assertEqual(expected, plugin._schema_version(self._spec(name)))
        for name in [
            # No version at all
            'anvil_snapshot',
            # A version we don't track, and therefore can't select columns for
            'ANVIL_1000G_2019_Dev_20230609_ANV4_202306121732',
            # No dataset name
            'ANVIL_20230609_ANV5_202306121732',
            # Truncated date, version and time
            'ANVIL_1000G_2019_Dev_2023060_ANV5_202306121732',
            'ANVIL_1000G_2019_Dev_20230609_ANV_202306121732',
            'ANVIL_1000G_2019_Dev_20230609_ANV5_20230612173',
            # Trailing and leading garbage
            'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732_v2',
            'restored_ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732',
            # An atlas whose snapshots don't encode a schema version at all
            'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20241205_dcp45'
        ]:
            with self.subTest(name=name):
                with self.assertRaises(AssertionError):
                    plugin._schema_version(self._spec(name))

    def test_entity_id(self):
        key = 'f9d40cf6-37b8-22f3-ce35-0dc614d2452b'
        spec = self._spec('ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958')
        entity_id = _entity_id(spec, 'anvil_biosample', key)
        self.assertEqual('6f8461fd-0e84-524e-9488-7614e45e2eec', entity_id)
        # A later release of the same dataset yields the same ID, even if it
        # spells the dataset differently, or was ingested under a different
        # schema version, or the dataset was created anew …
        for name in [
            'ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV6_202502201850',
            'ANVIL_cmg_uwash_ds_bdis_20250206_ANV6_202502201850',
            'AnVIL_CMG_UWash_DS_BDIS_20230418_ANV5_202304201958'
        ]:
            with self.subTest(name=name):
                self.assertEqual(entity_id,
                                 _entity_id(self._spec(name), 'anvil_biosample', key))
        # … while another dataset, or another table, does not. Primary keys are
        # only unique within a table of a snapshot, so without these
        # qualifiers, entities would share an ID.
        for other_spec, other_table in [
            (self._spec('ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932'), 'anvil_biosample'),
            (spec, 'anvil_donor')
        ]:
            with self.subTest(spec=str(other_spec), table=other_table):
                self.assertNotEqual(entity_id,
                                    _entity_id(other_spec, other_table, key))

    def test_duplicate_entity(self):
        """
        Test the detection of duplicate primary keys.
        """
        for bundle in [
            self._load_canned_bundle(self.primary_bundle()),
            self._load_canned_bundle(self.replica_bundle())
        ]:
            listed = bundle.entities or bundle.orphans
            entity, row = next(iter(listed.items()))
            for is_orphan in [False, True]:
                with self.subTest(bundle=bundle.uuid, is_orphan=is_orphan):
                    with self.assertRaises(AssertionError) as cm:
                        bundle.add_entity(entity, fixed_version, row, is_orphan=is_orphan)
                    self.assertTrue(R.caused(cm.exception))

    def test_absent_file_path(self):
        """
        Test that the `file_path` column, which version 6 of the schema added,
        is null in rows from a snapshot that was ingested under version 5.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        entity, row = next(
            (entity, row)
            for entity, row in bundle.entities.items()
            if entity.entity_type == 'anvil_file'
        )
        self.assertIsNotNone(row['file_path'])
        del row['file_path']
        bundle = TDRAnvilBundle(fqid=bundle.fqid)
        bundle.add_entity(entity, fixed_version, row)
        self.assertIsNone(bundle.entities[entity]['file_path'])

    def test_pk_column(self):
        plugin = self.plugin
        for version, schema in anvil_schemas.items():
            spec = self._spec(f'ANVIL_1000G_2019_Dev_20230609_ANV{version}_202306121732')
            for table in schema['tables']:
                table_name = table['name']
                with self.subTest(version=version, table=table_name):
                    # Every table the schema describes declares exactly one
                    # primary key, and names it after the table. The plugin
                    # used to hard-code that convention instead of reading the
                    # declaration, so this pins the two together.
                    self.assertEqual(table_name.removeprefix('anvil_') + '_id',
                                     plugin._pk_column(spec, table_name))
            # Tables absent from the schema declare no primary key, so their
            # rows are identified, partitioned and batched by their row ID
            with self.subTest(version=version, table='anvil_unknown'):
                self.assertIsNone(plugin._pk_column(spec, 'anvil_unknown'))
                self.assertEqual('datarepo_row_id',
                                 plugin._batch_column(spec, 'anvil_unknown'))

    def test_columns_per_schema_version(self):
        plugin = self.plugin
        # Version 6 added this column to the anvil_file table, so selecting it
        # from a snapshot ingested under version 5 would fail
        column = 'file_path'
        for version, expected in [(5, False), (6, True)]:
            with self.subTest(version=version):
                name = f'ANVIL_1000G_2019_Dev_20230609_ANV{version}_202306121732'
                columns = plugin._columns(self._spec(name), 'anvil_file')
                self.assertEqual(expected, column in columns)
                # Columns common to both versions are selected either way
                self.assertIn('file_name', columns)
        # Tables absent from the schema are replicated in their entirety
        name = 'ANVIL_1000G_2019_Dev_20230609_ANV6_202306121732'
        self.assertEqual({'*'}, plugin._columns(self._spec(name), 'anvil_unknown'))

    def _spec(self, name: str) -> TDRSourceSpec:
        return TDRSourceSpec.parse(f'tdr:bigquery:gcp:test_anvil_project:{name}')

    def test_reject_duplicate_file_names(self):
        bundle_fqid = self.primary_bundle()
        source_ref = self.source.ref
        canned_file = self._load_canned_file_version(uuid=source_ref.id,
                                                     version=None,
                                                     extension='tables.tdr')
        # Create a bundle with duplicated file names
        file_name = 'dup-file-name-test.txt'
        file_rows = canned_file['tables']['anvil_file']['rows']
        file_rows[0]['file_name'] = file_name
        file_rows[1]['file_name'] = file_name
        for name, table in canned_file['tables'].items():
            self._make_mock_table(source_ref.spec, name, table['rows'], table.get('schema'))
        with self.assertRaises(AssertionError) as cm:
            self.plugin.fetch_bundle(bundle_fqid)
        self.assertTrue(R.caused(cm.exception))
        expected = ('Bundle contains duplicate file names', bundle_fqid, [file_name])
        self.assertEqual(expected, one(cm.exception.args).args)


class TestAnvilIndexerWithIndexesSetUp(AnvilIndexerTestCase):
    """
    Conveniently sets up (tears down) indices before (after) each test.
    """

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self):
        super().tearDown()
        self._purge_indices()

    def test_dataset_description(self):
        dataset_ref = EntityReference(entity_type='anvil_dataset',
                                      entity_id='208441e2-cf28-584e-a0ac-7553c7118412')
        bundle_fqid = self.primary_bundle()
        bundle = cast(TDRAnvilBundle, self._load_canned_bundle(bundle_fqid))
        bundle.links.clear()
        bundle.entities = {dataset_ref: bundle.entities[dataset_ref]}
        self._index_bundle(bundle, delete=False)

        hits = self._get_all_hits()
        doc_counts: dict[DocumentType, int] = defaultdict(int)
        for hit in hits:
            qualifier, doc_type = self._parse_index_name(hit)
            if qualifier == 'bundles':
                continue
            elif qualifier in {'datasets', 'replica'}:
                doc_counts[doc_type] += 1
                if qualifier == 'datasets' and doc_type is DocumentType.aggregate:
                    self.assertEqual(1, hit['_source']['num_contributions'])
                    contents = one(hit['_source']['contents']['datasets'])
                    self.assertEqual(dataset_ref.entity_id, contents['document_id'])
                    self.assertEqual(['phs000693'], contents['registered_identifier'])
                    self.assertEqual('Study description from DUOS', contents['description'])
                    self.assertEqual('DUOS-000000', contents['duos_id'])
                    self.assertEqual('52ee7665-7033-63f2-a8d9-ce8e32666739', contents['dataset_id'])
            else:
                self.fail(qualifier)
        self.assertDictEqual(doc_counts, {
            DocumentType.aggregate: 1,
            DocumentType.contribution: 1,
            **({DocumentType.replica: 1} if config.enable_replicas else {})
        })

    def test_orphans(self):
        bundle = self._index_canned_bundle(self.replica_bundle())
        assert isinstance(bundle, TDRAnvilBundle)
        dataset_entity_id = one(
            ref.entity_id
            for ref in bundle.orphans
            if ref.entity_type == 'anvil_dataset'
        )
        expected = bundle.orphans if config.enable_replicas else {}
        actual = {}
        hits = self._get_all_hits()
        for hit in hits:
            qualifier, doc_type = self._parse_index_name(hit)
            self.assertEqual(DocumentType.replica, doc_type)
            source = hit['_source']
            self.assertEqual(source['hub_ids'], [dataset_entity_id])
            ref = EntityReference(entity_type=source['replica_type'],
                                  entity_id=source['entity_id'])
            actual[ref] = source['contents']
        self.assertEqual(expected, actual)
