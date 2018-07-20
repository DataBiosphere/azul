import logging
import unittest
from datetime import datetime
from typing import Mapping, Any
from uuid import uuid4

from azul import eventually
from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from azul.project.hca.metadata_api import Bundle

logging.basicConfig(level=logging.INFO)
module_logger = logging.getLogger(__name__)


def make_fake_notification(uuid: str, version: str) -> Mapping[str, Any]:
    hca_simulated_event = {
        "query": {
            "match_all": {}
        },
        "subscription_id": str(uuid4()),
        "transaction_id": str(uuid4()),
        "match": {
            "bundle_uuid": uuid,
            "bundle_version": version
        }
    }
    return hca_simulated_event


class TestDataExtractor(unittest.TestCase):

    test_bundles = {
        "dev": [],
        "integration": [("23e25ba4-094c-40ff-80b3-12861961a244", "2018-04-12T112557.587946Z")],
        "staging": [],
        "production": [("17a3d288-01a0-464a-9599-7375fda3353d", "2018-03-28T151023.074974Z"),
                       ("2a87dc5c-0c3c-4d91-a348-5d784ab48b92", "2018-03-29T104041.822717Z"),
                       ("4afbb0ea-81ad-49dc-9b12-9f77f4f50be8", "2018-03-29T090403.442059Z"),
                       ("aee55415-d128-4b30-9644-e6b2742fa32b", "2018-03-29T152812.404846Z"),
                       ("b0850e79-5544-49fe-b54d-e29b9fc3f61f", "2018-03-29T090340.934358Z"),
                       ("c94a43f9-257f-4cd0-b2fe-eaf6d5d37d18", "2018-03-29T090343.782253Z")]
    }

    test_same_ids_different_bundles = {
        "dev": [],
        "integration": [],
        "staging": [],
        "production": [("b2216048-7eaa-45f4-8077-5a3fb4204953", "2018-03-29T142048.835519Z"),
                       ("ddb8f660-1160-4f6c-9ce4-c25664ac62c9", "2018-03-29T142057.907086Z")]
    }

    # Integration Bundle
    hca_simulated_event = {
        "query": {
            "match_all": {}
        },
        "subscription_id": str(uuid4()),
        "transaction_id": str(uuid4()),
        "match": {
            "bundle_uuid": "23e25ba4-094c-40ff-80b3-12861961a244",
            "bundle_version": "2018-04-12T112557.587946Z"
        }
    }

    def test_hca_extraction(self):
        # Trigger the indexing operation
        dss_url = "https://dss.data.humancellatlas.org/v1"
        index_properties = IndexProperties(dss_url, es_endpoint=("localhost", 9200))
        hca_indexer = Indexer(index_properties)
        # Index the test bundles
        for bundle_uuid, bundle_version in self.test_bundles["production"]:
            fake_event = make_fake_notification(bundle_uuid, bundle_version)
            module_logger.info("Start computation %s",
                               datetime.now().isoformat(timespec='microseconds'))
            hca_indexer.index(fake_event)
            module_logger.info("Indexing operation finished for %s. Check values in ElasticSearch",
                               bundle_uuid+bundle_version)
            module_logger.info("End computation %s",
                               datetime.now().isoformat(timespec='microseconds'))
        # Check values in ElasticSearch

        @eventually(5.0, 0.5)
        def _assert_number_of_files():
            es_client = index_properties.elastic_search_client
            total_files = es_client.count(index="browser_files_dev", doc_type="doc")
            self.assertEqual(776, total_files["count"])
            total_specimens = es_client.count(index="browser_specimens_dev", doc_type="doc")
            self.assertEqual(129, total_specimens["count"])

        _assert_number_of_files()

    def test_accessor_class(self):
        from azul.downloader import MetadataDownloader
        from azul.dss_bundle import DSSBundle
        bundle_uuid = "b2216048-7eaa-45f4-8077-5a3fb4204953"
        bundle_version = "2018-03-29T142048.835519Z"
        fake_event = make_fake_notification(bundle_uuid, bundle_version)
        metadata_downloader = MetadataDownloader("https://dss.data.humancellatlas.org/v1")
        metadata, manifest = metadata_downloader.extract_bundle(fake_event)
        dss_bundle = DSSBundle(uuid=bundle_uuid,
                               version=bundle_version,
                               manifest=manifest,
                               metadata_files=metadata)
        reconstructed_bundle = Bundle(dss_bundle)
        print(reconstructed_bundle.project)
        sequencing_inputs = {si.biomaterial_id for si in reconstructed_bundle.sequencing_input}
        specimens = {sp.biomaterial_id for sp in reconstructed_bundle.specimens}
        self.assertEqual({"22011_1#268"}, sequencing_inputs)
        self.assertEqual({"1139_T"}, specimens)

    def test_no_duplicate_files_in_specimen(self):
        # Trigger the indexing operation
        dss_url = "https://dss.data.humancellatlas.org/v1"
        index_properties = IndexProperties(dss_url, es_endpoint=("localhost", 9200))
        hca_indexer = Indexer(index_properties)
        fake_event = make_fake_notification("8543d32f-4c01-48d5-a79f-1c5439659da3",
                                            "2018-03-29T143828.884167Z")
        module_logger.info("Start computation %s",
                           datetime.now().isoformat(timespec='microseconds'))
        hca_indexer.index(fake_event)
        module_logger.info("Indexing operation finished for %s. Check values in ElasticSearch",
                           "8543d32f-4c01-48d5-a79f-1c5439659da3" + "2018-03-29T143828.884167Z")
        module_logger.info("End computation %s",
                           datetime.now().isoformat(timespec='microseconds'))
        # Check values in ElasticSearch
        es_client = index_properties.elastic_search_client
        results = es_client.get(index="browser_specimens_dev",
                                id="b3623b88-c369-46c9-a2e9-a16042d2c589")
        file_ids = [f["uuid"] for f in
                    results["_source"]["bundles"][0]["contents"]["files"]]
        self.assertTrue(len(file_ids) == len(set(file_ids)))


if __name__ == "__main__":
    unittest.main()
