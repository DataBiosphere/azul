import unittest
from project.hca.indexer import Indexer
from project.hca.config import IndexProperties
from typing import Mapping, Any
from uuid import uuid4


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
            hca_indexer.index(fake_event)
            print("Indexing operation finished for {}. Check values in ElasticSearch".format(bundle_uuid+bundle_version))
        # Check values in ElasticSearch
        es_client = index_properties.elastic_search_client
        for entity_index in index_properties.index_names:
            results = es_client.search(index=entity_index,
                                       doc_type="doc",
                                       size=100)
            print(results)
        self.assertEqual("pass", "pass")


if __name__ == "__main__":
    unittest.main()
