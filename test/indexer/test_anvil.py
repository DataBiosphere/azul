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
        mock_url = PropertyMock(return_value=furl('https://mock_duos.lan'))
        mock_cache = Mock(spec=UrlCacheService)
        if duos_response is not None:
            duos_mock = Mock(spec=HTTPResponse, status=200,
                             data=json.dumps(duos_response))
            mock_cache.get_url.return_value = duos_mock
        patches = [
            patch.object(type(config), 'duos_service_url', new=mock_url),
            patch.object(TDRClient, '_request', return_value=tdr_mock),
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
            self.bundle_fqid(uuid='59337757-a45e-af00-9238-34997372f696',
                             table_name='anvil_activity'),
            self.bundle_fqid(uuid='1df1df09-3e18-adfa-80e2-515e71a5d3e7',
                             table_name='anvil_alignmentactivity'),
            self.bundle_fqid(uuid='a70d523a-da59-aabe-8a4b-bae394df4253',
                             table_name='anvil_assayactivity'),
            self.bundle_fqid(uuid='0ad2c320-4383-a5b4-87c4-cbc66527837f',
                             table_name='anvil_diagnosis'),
            self.bundle_fqid(uuid='dfd8ff3d-de2c-a44d-bebe-f540f62a92da',
                             table_name='anvil_donor'),
            self.bundle_fqid(uuid='4ca2319c-abe0-a583-b7a5-d3c8ad2114a0',
                             table_name='anvil_sequencingactivity'),
            self.bundle_fqid(uuid='b47b0e77-7e5a-ab0e-917b-a7096bb7297a',
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
