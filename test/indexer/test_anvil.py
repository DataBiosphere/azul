from operator import (
    itemgetter,
)
import unittest

from azul import (
    CatalogName,
    config,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from indexer import (
    IndexerTestCase,
)
from indexer.test_tdr import (
    TDRAnvilPluginTestCase,
)


class TestAnvil(IndexerTestCase, TDRAnvilPluginTestCase):
    bundle_fqid = SourcedBundleFQID(uuid='9b78f049-83fc-a99b-83bc-8d40e34265cd',
                                    version='',
                                    source=TDRAnvilPluginTestCase.source)

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    @classmethod
    def catalog_config(cls) -> dict[CatalogName, config.Catalog]:
        return {
            cls.catalog: config.Catalog(name=cls.catalog,
                                        atlas='anvil',
                                        internal=False,
                                        plugins=dict(metadata=config.Catalog.Plugin(name='anvil'),
                                                     repository=config.Catalog.Plugin(name='tdr_anvil')),
                                        sources={TDRAnvilPluginTestCase.source})
        }

    def test_indexing(self):
        self.maxDiff = None
        self._index_canned_bundle(self.bundle_fqid)
        hits = self._get_all_hits()
        hits.sort(key=itemgetter('_id'))
        expected_hits = self._load_canned_result(self.bundle_fqid)
        self.assertEqual(expected_hits, hits)

    @unittest.skip('TinyQuery does not support the WITH clause')
    def test_fetch_bundle(self):
        canned_bundle = self._load_canned_bundle(self.bundle_fqid)
        self._make_mock_tdr_tables(self.bundle_fqid)
        plugin = self.plugin_for_source_spec(canned_bundle.fqid.source.spec)
        bundle = plugin.fetch_bundle(self.bundle_fqid)
        self.assertEqual(canned_bundle.fqid, bundle.fqid)
        self.assertEqual(canned_bundle.manifest, bundle.manifest)
        self.assertEqual(canned_bundle.metadata_files, bundle.metadata_files)
