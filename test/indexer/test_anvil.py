from operator import (
    itemgetter,
)
import unittest

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.logging import (
    configure_test_logging,
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
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068',
                            version='')
        ]

    @property
    def bundle(self) -> SourcedBundleFQID:
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

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    def test_indexing(self):
        self.maxDiff = None
        self._index_canned_bundle(self.bundle)
        hits = self._get_all_hits()
        hits.sort(key=itemgetter('_id'))
        expected_hits = self._load_canned_result(self.bundle)
        self.assertEqual(expected_hits, hits)

    @unittest.skip('TinyQuery does not support the WITH clause')
    def test_fetch_bundle(self):
        canned_bundle = self._load_canned_bundle(self.bundle)
        self._make_mock_tdr_tables(self.bundle)
        plugin = self.plugin_for_source_spec(canned_bundle.fqid.source.spec)
        bundle = plugin.fetch_bundle(self.bundle)
        self.assertEqual(canned_bundle.fqid, bundle.fqid)
        self.assertEqual(canned_bundle.manifest, bundle.manifest)
        self.assertEqual(canned_bundle.metadata_files, bundle.metadata_files)
