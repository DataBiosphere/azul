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
from azul.indexer.document import (
    DocumentType,
    EntityReference,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins.repository import (
    tdr_anvil,
)
from azul.plugins.repository.tdr_anvil import (
    BundleType,
    TDRAnvilBundle,
    TDRAnvilBundleFQID,
)
from azul.terra import (
    TDRClient,
)
from azul.types import (
    JSONs,
    MutableJSONs,
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


class DUOSTestCase(TDRTestCase, ABC):

    def _mock_normal_duos(self):
        for p in self._duos_patches(self.normal_response_bodies):
            self.addPatch(p)

    @property
    def normal_response_bodies(self) -> MutableJSONs:
        duos_id = 'DUOS-000000'
        return [
            # TDR's /snapshots/{snapshot_id} response:
            {
                'name': self.source.spec.name,
                'duosFirecloudGroup': {'duosId': duos_id}
            },
            # DUOS' /dataset/registration/{duos_id}:
            {
                'consentGroups': [{'datasetIdentifier': duos_id}],
                'studyDescription': 'Study description from DUOS'
            }
        ]

    def _duos_patches(self, bodies: JSONs) -> Iterable[patch]:
        responses = [
            Mock(spec=HTTPResponse, status=200, data=json.dumps(body))
            for body in bodies
        ]
        mock_url = PropertyMock(return_value=furl('https://mock_duos.lan'))
        patches = [
            patch.object(type(config), 'duos_service_url', new=mock_url),
            patch.object(TDRClient, '_request', side_effect=responses)
        ]
        return patches


class AnvilIndexerTestCase(AnvilCannedBundleTestCase, IndexerTestCase):

    @classmethod
    def primary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068')

    @classmethod
    def supplementary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='c2711e94-9966-a0ef-88be-88caf3e8a29b',
                               table_name=BundleType.supplementary.value)

    @classmethod
    def duos_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='2370f948-2783-aeb6-afea-e022897f4dcf',
                               table_name=BundleType.duos.value)

    @classmethod
    def replica_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='6b35f59c-d33d-abf7-9ba0-c7b3a0ca82f3',
                               table_name='non_schema_orphan_table')


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
        source_ref = self.source
        self._make_mock_tdr_tables(source_ref)
        canned_bundle_fqids = [
            self.primary_bundle(),
            self.supplementary_bundle(),
            self.duos_bundle(),
            self.replica_bundle(),
        ]
        expected_bundle_fqids = sorted(canned_bundle_fqids + [
            # Replica bundles for the AnVIL schema tables, which we don't can
            self.bundle_fqid(uuid='9461293c-447c-a75f-a9ee-a544b106cba3',
                             table_name='anvil_activity'),
            self.bundle_fqid(uuid='115cedcf-2b4b-a8ab-ae6f-178e2362dc60',
                             table_name='anvil_alignmentactivity'),
            self.bundle_fqid(uuid='9998900d-4481-aeb5-8a0f-4e485d26412d',
                             table_name='anvil_assayactivity'),
            self.bundle_fqid(uuid='50eaf222-be04-af62-aac4-a21dad96a734',
                             table_name='anvil_diagnosis'),
            self.bundle_fqid(uuid='a3ef24e4-5739-a2ee-ba59-4a2dc24c0bfe',
                             table_name='anvil_donor'),
            self.bundle_fqid(uuid='eeaae015-86da-a018-bc4c-2aec42aa88a2',
                             table_name='anvil_sequencingactivity'),
            self.bundle_fqid(uuid='6aec5e41-3a08-a86e-9f29-07092145ebdb',
                             table_name='anvil_variantcallingactivity')
        ])
        plugin = self.plugin_for_source_spec(source_ref.spec)
        bundle_fqids = sorted(plugin.list_bundles(source_ref, ''))
        self.assertEqual(expected_bundle_fqids, bundle_fqids)
        for bundle_fqid in canned_bundle_fqids:
            with self.subTest(bundle_fqid=bundle_fqid):
                canned_bundle = self._load_canned_bundle(bundle_fqid)
                assert isinstance(canned_bundle, TDRAnvilBundle)
                bundle = plugin.fetch_bundle(bundle_fqid)
                assert isinstance(bundle, TDRAnvilBundle)
                self.assertEqual(canned_bundle.fqid, bundle.fqid)
                self.assertEqual(canned_bundle.entities, bundle.entities)
                self.assertEqual(canned_bundle.links, bundle.links)
                self.assertEqual(canned_bundle.orphans, bundle.orphans)

    def test_absent_duos_id(self):
        source_ref = self.source
        self._make_mock_tdr_tables(source_ref)
        cases = {
            'Absent duosFirecloudGroup': [
                {'name': self.source.spec.name}
            ],
            'Empty duosFirecloudGroup': [
                {
                    'name': self.source.spec.name,
                    'duosFirecloudGroup': {}
                }
            ],
            'Null duosId': [
                {
                    'name': self.source.spec.name,
                    'duosFirecloudGroup': {'duosId': None}
                }
            ]
        }
        for sub_test, response_bodies in cases.items():
            with self.subTest(sub_test):
                with self.stacked_patches(self._duos_patches(response_bodies)):
                    plugin = self.plugin_for_source_spec(source_ref.spec)
                    bundle = plugin.fetch_bundle(self.duos_bundle())
                    self.assertIsInstance(bundle, TDRAnvilBundle)
                    self.assertEqual({}, bundle.entities)
                    self.assertEqual(1, len(bundle.orphans))


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
        bundles = [self.primary_bundle(), self.duos_bundle()]
        for bundle_fqid in bundles:
            bundle = cast(TDRAnvilBundle, self._load_canned_bundle(bundle_fqid))
            # To simplify the test, we drop all entities from the bundles
            # except for the dataset
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
                    self.assertEqual(2, hit['_source']['num_contributions'])
                    self.assertEqual(sorted(b.uuid for b in bundles),
                                     sorted(b['uuid'] for b in hit['_source']['bundles']))
                    contents = one(hit['_source']['contents']['datasets'])
                    # These fields are populated only in the primary bundle
                    self.assertEqual(dataset_ref.entity_id, contents['document_id'])
                    self.assertEqual(['phs000693'], contents['registered_identifier'])
                    # These fields are populated only in the DUOS bundle
                    self.assertEqual('Study description from DUOS', contents['description'])
                    self.assertEqual('DUOS-000000', contents['duos_id'])
                    # This field is present in both bundles
                    self.assertEqual('52ee7665-7033-63f2-a8d9-ce8e32666739', contents['dataset_id'])
            else:
                self.fail(qualifier)
        self.assertDictEqual(doc_counts, {
            DocumentType.aggregate: 1,
            DocumentType.contribution: 2,
            **({DocumentType.replica: 2} if config.enable_replicas else {})
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
