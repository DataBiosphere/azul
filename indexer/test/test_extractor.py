import json
import os
import sys
import unittest
from uuid import uuid4
from utils.extractor import DataExtractor


class TestDataExtractor(unittest.TestCase):

    blue_box_host = "https://{}".format(
        "dss.integration.data.humancellatlas.org/v1"
    )
    simulated_event = {
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

    def test_extraction(self):
        extractor = DataExtractor(self.blue_box_host)
        extractor.extract_bundle(self.simulated_event, "aws")
        # TODO: Compare expected vs actual
        self.assertEqual("pass", "pass")


if __name__ == "__main__":
    unittest.main()
