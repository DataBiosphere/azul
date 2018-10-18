from itertools import chain
import logging
import unittest
from datetime import datetime

from azul import config, eventually
from azul.es import ESClientFactory
from indexer.test_hca_indexer import IndexerTestCase

module_logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestDataExtractorTestCase(IndexerTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.production_test_bundles = [("17a3d288-01a0-464a-9599-7375fda3353d", "2018-03-28T151023.074974Z"),
                                       ("2a87dc5c-0c3c-4d91-a348-5d784ab48b92", "2018-03-29T104041.822717Z"),
                                       ("4afbb0ea-81ad-49dc-9b12-9f77f4f50be8", "2018-03-29T090403.442059Z"),
                                       ("aee55415-d128-4b30-9644-e6b2742fa32b", "2018-03-29T152812.404846Z"),
                                       ("b0850e79-5544-49fe-b54d-e29b9fc3f61f", "2018-03-29T090340.934358Z"),
                                       ("c94a43f9-257f-4cd0-b2fe-eaf6d5d37d18", "2018-03-29T090343.782253Z")]
        cls.test_same_ids_different_bundles = [("b2216048-7eaa-45f4-8077-5a3fb4204953", "2018-03-29T142048.835519Z"),
                                               ("ddb8f660-1160-4f6c-9ce4-c25664ac62c9", "2018-03-29T142057.907086Z")]
        cls.test_duplicate_bundle = ("8543d32f-4c01-48d5-a79f-1c5439659da3", "2018-03-29T143828.884167Z")
        cls.es_client = ESClientFactory.get()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_hca_extraction(self):
        for bundle_pack in self.production_test_bundles:
            self._mock_index(bundle_pack)

        @eventually(5.0, 0.5)
        def _assert_number_of_files():
            total_files = self.es_client.count(
                index=config.es_index_name('files'), doc_type='doc')
            self.assertEqual(776, total_files["count"])
            total_specimens = self.es_client.count(
                index=config.es_index_name('specimens'), doc_type='doc')
            self.assertEqual(129, total_specimens["count"])
            total_projects = self.es_client.count(
                index=config.es_index_name('projects'), doc_type='doc')
            self.assertEqual(3, total_projects["count"])

        _assert_number_of_files()

    # When two processes point at a file (this is the case for most files in production)
    # there is a bug where the files index contains duplicate dictionaries for the file.
    #
    def test_no_duplicate_files_in_specimen(self):
        self._mock_index(self.test_duplicate_bundle)
        for aggregate in True, False:
            with self.subTest(aggregate=aggregate):
                result = self.es_client.get(index=config.es_index_name('specimens', aggregate=aggregate),
                                            id='b3623b88-c369-46c9-a2e9-a16042d2c589')
                if aggregate:
                    file_ids = [chain(f['uuid'] for f in result['_source']['contents']['files'])]
                else:
                    file_ids = [f['uuid']
                                for bundle in result['_source']['bundles']
                                for f in bundle['contents']['files']]
                self.assertEqual(len(file_ids), len(set(file_ids)))


if __name__ == "__main__":
    unittest.main()
