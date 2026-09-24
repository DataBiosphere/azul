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
from azul.lib.types import (
    JSON,
    MutableJSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.repository import (
    tdr_anvil,
)
from azul.plugins.repository.tdr_anvil import (
    TDRAnvilBundle,
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
            self.bundle_fqid(uuid='2a65294f-9cdf-a455-87e9-666bd8477725',
                             table_name='anvil_activity'),
            self.bundle_fqid(uuid='5f896854-75a9-a0be-ac10-cf4cb2ef2efd',
                             table_name='anvil_alignmentactivity'),
            self.bundle_fqid(uuid='8ec98aad-7d3e-ab63-841e-2e6c40e766eb',
                             table_name='anvil_assayactivity'),
            self.bundle_fqid(uuid='477175ff-777e-a5fc-985f-572ab137d934',
                             table_name='anvil_diagnosis'),
            self.bundle_fqid(uuid='2e84f5a3-ccfe-aae7-aab0-0a5cbd78b685',
                             table_name='anvil_donor'),
            self.bundle_fqid(uuid='713f9fe8-1736-aa50-8355-d71fadaca2e1',
                             table_name='anvil_sequencingactivity'),
            self.bundle_fqid(uuid='f01e33f5-71a4-a90e-9ccb-8c0f31ae3214',
                             table_name='anvil_variantcallingactivity')
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
                                      entity_id='2370f948-2783-4eb6-afea-e022897f4dcf')
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
