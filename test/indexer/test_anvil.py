from collections import (
    defaultdict,
)
from operator import (
    itemgetter,
)
from typing import (
    cast,
)
import unittest

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.indexer.document import (
    DocumentType,
    EntityReference,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins.repository.tdr_anvil import (
    BundleEntityType,
    TDRAnvilBundle,
    TDRAnvilBundleFQID,
)
from indexer import (
    IndexerTestCase,
)
from indexer.test_tdr import (
    TDRAnvilPluginTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class AnvilIndexerTestCase(IndexerTestCase, TDRAnvilPluginTestCase):

    @classmethod
    def bundle_fqid(cls,
                    *,
                    uuid,
                    version,
                    entity_type=BundleEntityType.primary
                    ) -> TDRAnvilBundleFQID:
        return TDRAnvilBundleFQID(source=cls.source,
                                  uuid=uuid,
                                  version=version,
                                  entity_type=entity_type)

    @classmethod
    def bundles(cls) -> list[TDRAnvilBundleFQID]:
        return [
            cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068',
                            version='')
        ]

    @property
    def bundle(self) -> TDRAnvilBundleFQID:
        return one(self.bundles())

    @classmethod
    def catalog_config(cls) -> dict[CatalogName, config.Catalog]:
        return {
            cls.catalog: config.Catalog(name=cls.catalog,
                                        atlas='anvil',
                                        internal=False,
                                        plugins=dict(metadata=config.Catalog.Plugin(name='anvil'),
                                                     repository=config.Catalog.Plugin(name='tdr_anvil')),
                                        sources={str(cls.source.spec)})
        }


class TestAnvilIndexer(AnvilIndexerTestCase):

    def test_indexing(self):
        self.maxDiff = None
        expected_hits = self._load_canned_result(self.bundle)
        self.index_service.create_indices(self.catalog)
        try:
            self._index_canned_bundle(self.bundle)
            hits = self._get_all_hits()
            hits.sort(key=itemgetter('_id'))
            self.assertEqual(expected_hits, hits)
        finally:
            self.index_service.delete_indices(self.catalog)


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
        dataset_bundle = self.bundle_fqid(uuid='2370f948-2783-aeb6-afea-e022897f4dcf',
                                          version=self.bundle.version,
                                          entity_type=BundleEntityType.duos)

        bundles = [self.bundle, dataset_bundle]
        for bundle_fqid in bundles:
            bundle = cast(TDRAnvilBundle, self._load_canned_bundle(bundle_fqid))
            bundle.links.clear()
            bundle.entities = {dataset_ref: bundle.entities[dataset_ref]}
            self._index_bundle(bundle, delete=False)

        hits = self._get_all_hits()
        doc_counts: dict[DocumentType, int] = defaultdict(int)
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            if entity_type == 'bundles':
                continue
            elif entity_type == 'datasets':
                doc_counts[doc_type] += 1
                if doc_type is DocumentType.aggregate:
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
                self.fail(entity_type)
        self.assertEqual(1, doc_counts[DocumentType.aggregate])
        self.assertEqual(2, doc_counts[DocumentType.contribution])

    # FIXME: Enable test after the issue with TinyQuery `WITH` has been resolved
    #        https://github.com/DataBiosphere/azul/issues/5046
    @unittest.skip('TinyQuery does not support the WITH clause')
    def test_fetch_bundle(self):
        canned_bundle = self._load_canned_bundle(self.bundle)
        assert isinstance(canned_bundle, TDRAnvilBundle)
        self._make_mock_tdr_tables(self.bundle)
        plugin = self.plugin_for_source_spec(canned_bundle.fqid.source.spec)
        bundle = plugin.fetch_bundle(self.bundle)
        assert isinstance(bundle, TDRAnvilBundle)
        self.assertEqual(canned_bundle.fqid, bundle.fqid)
        self.assertEqual(canned_bundle.entities, bundle.entities)
        self.assertEqual(canned_bundle.links, bundle.links)
