import logging
import unittest
from datetime import datetime

from azul import config, eventually
from indexer.test_hca_indexer import IndexerTestCase

module_logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestDataExtractorTestCase(IndexerTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_bundles = {
            "https://dss.dev.data.humancellatlas.org/v1": [],
            "https://dss.integration.data.humancellatlas.org/v1": [("23e25ba4-094c-40ff-80b3-12861961a244", "2018-04-12T112557.587946Z")],
            "https://dss.staging.data.humancellatlas.org/v1": [],
            "https://dss.data.humancellatlas.org/v1": [("17a3d288-01a0-464a-9599-7375fda3353d", "2018-03-28T151023.074974Z"),
                                                       ("2a87dc5c-0c3c-4d91-a348-5d784ab48b92", "2018-03-29T104041.822717Z"),
                                                       ("4afbb0ea-81ad-49dc-9b12-9f77f4f50be8", "2018-03-29T090403.442059Z"),
                                                       ("aee55415-d128-4b30-9644-e6b2742fa32b", "2018-03-29T152812.404846Z"),
                                                       ("b0850e79-5544-49fe-b54d-e29b9fc3f61f", "2018-03-29T090340.934358Z"),
                                                       ("c94a43f9-257f-4cd0-b2fe-eaf6d5d37d18", "2018-03-29T090343.782253Z")]
        }
        cls.test_same_ids_different_bundles = {
            "https://dss.dev.data.humancellatlas.org/v1": [],
            "https://dss.integration.data.humancellatlas.org/v1": [],
            "https://dss.staging.data.humancellatlas.org/v1": [],
            "https://dss.data.humancellatlas.org/v1": [("b2216048-7eaa-45f4-8077-5a3fb4204953", "2018-03-29T142048.835519Z"),
                                                       ("ddb8f660-1160-4f6c-9ce4-c25664ac62c9", "2018-03-29T142057.907086Z")]
        }
        cls.test_duplicates_bundles = {
            "https://dss.dev.data.humancellatlas.org/v1": [],
            "https://dss.integration.data.humancellatlas.org/v1": [],
            "https://dss.staging.data.humancellatlas.org/v1": [],
            "https://dss.data.humancellatlas.org/v1": [("8543d32f-4c01-48d5-a79f-1c5439659da3", "2018-03-29T143828.884167Z")]
        }
        cls.es_client = cls.index_properties.elastic_search_client

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    @unittest.skip('https://github.com/DataBiosphere/azul/issues/207')
    def test_hca_extraction(self):
        # Index the test bundles
        for bundle_uuid, bundle_version in self.test_bundles[config.dss_endpoint]:
            module_logger.info("Start computation %s",
                               datetime.now().isoformat(timespec='microseconds'))
            bundle_pack = (bundle_uuid, bundle_version)
            data_pack = self._get_data_files(bundle_uuid)
            self._mock_index(bundle_pack)
            module_logger.info("Indexing operation finished for %s. Check values in ElasticSearch",
                               bundle_uuid + bundle_version)
            module_logger.info("End computation %s",
                               datetime.now().isoformat(timespec='microseconds'))
        # Check values in ElasticSearch

        @eventually(5.0, 0.5)
        def _assert_number_of_files():
            total_files = self.es_client.count(index="browser_files_dev", doc_type="doc")
            self.assertEqual(776, total_files["count"])
            total_specimens = self.es_client.count(index="browser_specimens_dev", doc_type="doc")
            self.assertEqual(129, total_specimens["count"])

        _assert_number_of_files()

    # When two processes point at a file (this is the case for most files in production)
    # there is a bug where the files index contains duplicate dictionaries for the file.
    def test_no_duplicate_files_in_specimen(self):
        bundle_uuid, bundle_version = self.test_duplicates_bundles[config.dss_endpoint][0]
        bundle_pack = (bundle_uuid, bundle_version)
        data_pack = self._get_data_files(bundle_uuid)
        module_logger.info("Start computation %s",
                           datetime.now().isoformat(timespec='microseconds'))
        self._mock_index(bundle_pack)
        module_logger.info("Indexing operation finished for %s. Check values in ElasticSearch",
                           bundle_uuid + bundle_version)
        module_logger.info("End computation %s",
                           datetime.now().isoformat(timespec='microseconds'))
        # Check values in ElasticSearch
        results = self.es_client.get(index="browser_specimens_dev",
                                     id="b3623b88-c369-46c9-a2e9-a16042d2c589")
        file_ids = [f["uuid"] for f in
                    results["_source"]["bundles"][0]["contents"]["files"]]
        self.assertEqual(len(file_ids), len(set(file_ids)))


if __name__ == "__main__":
    unittest.main()
