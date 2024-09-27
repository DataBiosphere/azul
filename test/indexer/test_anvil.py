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

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._patch_duos()

    mock_duos_url = furl('https:://mock_duos.lan')

    duos_id = 'foo'
    duos_description = 'Study description from DUOS'

    @classmethod
    def _patch_duos(cls) -> None:
        cls.addClassPatch(patch.object(type(config),
                                       'duos_service_url',
                                       new=PropertyMock(return_value=cls.mock_duos_url)))
        cls.addClassPatch(patch.object(TDRClient,
                                       '_request',
                                       side_effect=[
                                           Mock(spec=HTTPResponse, status=200, data=json.dumps({
                                               'name': cls.source.spec.name,
                                               'duosFirecloudGroup': {
                                                   'duosId': cls.duos_id
                                               }
                                           })),
                                           Mock(spec=HTTPResponse, status=200, data=json.dumps({
                                               'studyDescription': cls.duos_description
                                           }))
                                       ]))


class AnvilIndexerTestCase(AnvilCannedBundleTestCase, IndexerTestCase):

    @classmethod
    def bundle_fqid(cls,
                    *,
                    uuid,
                    version=None,
                    table_name=BundleType.primary.value
                    ) -> TDRAnvilBundleFQID:
        assert version is None, 'All AnVIL bundles should use the same version'
        return TDRAnvilBundleFQID(source=cls.source,
                                  uuid=uuid,
                                  version=cls.version,
                                  table_name=table_name)

    @classmethod
    def primary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068')

    @classmethod
    def supplementary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='6b0f6c0f-5d80-a242-accb-840921351cd5',
                               table_name=BundleType.supplementary.value)

    @classmethod
    def duos_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='2370f948-2783-aeb6-afea-e022897f4dcf',
                               table_name=BundleType.duos.value)


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
                        self.index_service.delete_indices(self.catalog)

    def test_list_and_fetch_bundles(self):
        source_ref = self.source
        self._make_mock_tdr_tables(source_ref)
        expected_bundle_fqids = sorted([
            self.primary_bundle(),
            self.supplementary_bundle(),
            self.duos_bundle()
        ])
        plugin = self.plugin_for_source_spec(source_ref.spec)
        bundle_fqids = sorted(plugin.list_bundles(source_ref, ''))
        self.assertEqual(expected_bundle_fqids, bundle_fqids)
        for bundle_fqid in bundle_fqids:
            with self.subTest(bundle_fqid=bundle_fqid):
                canned_bundle = self._load_canned_bundle(bundle_fqid)
                assert isinstance(canned_bundle, TDRAnvilBundle)
                bundle = plugin.fetch_bundle(bundle_fqid)
                assert isinstance(bundle, TDRAnvilBundle)
                self.assertEqual(canned_bundle.fqid, bundle.fqid)
                self.assertEqual(canned_bundle.entities, bundle.entities)
                self.assertEqual(canned_bundle.links, bundle.links)


class TestAnvilIndexerWithIndexesSetUp(AnvilIndexerTestCase):
    """
    Conveniently sets up (tears down) indices before (after) each test.
    """

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self):
        super().tearDown()
        self.index_service.delete_indices(self.catalog)

    def test_dataset_description(self):
        dataset_ref = EntityReference(entity_type='dataset',
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
                    # This field is populated only in the DUOS bundle
                    self.assertEqual('Study description from DUOS', contents['description'])
            else:
                self.fail(qualifier)
        self.assertDictEqual(doc_counts, {
            DocumentType.aggregate: 1,
            DocumentType.contribution: 2,
            # No replica is emitted for the primary dataset because we dropped
            # the files (hubs) from its bundle above
            **({DocumentType.replica: 1} if config.enable_replicas else {})
        })
