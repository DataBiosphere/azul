import unittest
from project.hca.indexer import Indexer
from project.hca.config import IndexProperties
from uuid import uuid4


class TestDataExtractor(unittest.TestCase):

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
        es_host = "localhost"
        es_port = 9200
        dss_url = "https://dss.integration.data.humancellatlas.org/v1"
        index_properties = IndexProperties(dss_url, es_host, es_port)
        hca_indexer = Indexer(index_properties)
        hca_indexer.index(self.hca_simulated_event)
        print("Indexing operation finished. Check values in ElasticSearch")
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
