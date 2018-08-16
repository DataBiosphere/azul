# -*- coding: utf-8 -*-
"""
Suite for unit testing indexer.py
"""

from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
import time
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch

from azul import eventually
from indexer import IndexerTestCase

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHCAIndexer(IndexerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.old_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-29T152812.404846Z")
        cls.new_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-30T152812.404846Z")
        cls.specimens = [("9dec1bd6-ced8-448a-8e45-1fc7846d8995", "2018-03-29T154319.834528Z"),
                         ("56a338fe-7554-4b5d-96a2-7df127a7640b", "2018-03-29T153507.198365Z")]

    @eventually(5.0, 0.5)
    def _get_es_results(self, assert_func):
        es_results = []
        for entity_index in self.index_properties.index_names:
            results = self.es_client.search(index=entity_index,
                                            doc_type="doc",
                                            size=100)
            if "project" not in entity_index:
                for result_dict in results["hits"]["hits"]:
                    es_results.append(result_dict)

        assert_func(es_results)
        return es_results

    def tearDown(self):
        for index_name in self.index_properties.index_names:
            self.es_client.indices.delete(index=index_name, ignore=[400, 404])

    def test_index_correctness(self):
        """
        Index a bundle and check that the index contains the correct attributes
        """
        self._mock_index(self.new_bundle)

        def check_bundle_correctness(es_results):
            for result_dict in es_results:
                result_uuid = result_dict["_source"]["bundles"][0]["uuid"]
                result_version = result_dict["_source"]["bundles"][0]["version"]
                result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.new_bundle[0], result_uuid)
                self.assertEqual(self.new_bundle[1], result_version)
                self.assertEqual("Aardvark Ailment", result_contents["project"]["project"])
                self.assertIn("John Denver", result_contents["project"]["laboratory"])
                self.assertIn("Lorem ipsum", result_contents["specimens"][0]["species"])

        self._get_es_results(check_bundle_correctness)

    def test_update_with_newer_version(self):
        """
        Updating a bundle with a future version should overwrite the old version.
        """
        self._mock_index(self.old_bundle)

        def check_old_submission(es_results):
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.old_bundle[1], old_result_version)
                self.assertEqual("Mouse Melanoma", old_result_contents["project"]["project"])
                self.assertIn("Sarah Teichmann", old_result_contents["project"]["laboratory"])
                self.assertIn("Mus musculus", old_result_contents["specimens"][0]["species"])

        old_results = self._get_es_results(check_old_submission)

        self._mock_index(self.new_bundle)

        def check_updated_submission(old_results_list, new_results_list):
            for old_result_dict, new_result_dict in list(zip(old_results_list, new_results_list)):
                old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

                new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
                new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

                self.assertNotEqual(old_result_version, new_result_version)
                self.assertNotEqual(old_result_contents["project"]["project"],
                                    new_result_contents["project"]["project"])
                self.assertNotEqual(old_result_contents["project"]["laboratory"],
                                    new_result_contents["project"]["laboratory"])
                self.assertNotEqual(old_result_contents["specimens"][0]["species"],
                                    new_result_contents["specimens"][0]["species"])

        self._get_es_results(partial(check_updated_submission, old_results))

    def test_old_version_overwrite(self):
        """
        An attempt to overwrite a newer version of a bundle with an older version should fail.
        """
        self._mock_index(self.new_bundle)

        def check_new_submission(es_results):
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.new_bundle[1], old_result_version)
                self.assertEqual("Aardvark Ailment", old_result_contents["project"]["project"])
                self.assertIn("John Denver", old_result_contents["project"]["laboratory"])
                self.assertIn("Lorem ipsum", old_result_contents["specimens"][0]["species"])

        old_results = self._get_es_results(check_new_submission)

        self._mock_index(self.old_bundle)

        def check_for_overwrite(old_results_list, new_results_list):
            for old_result_dict, new_result_dict in list(zip(old_results_list, new_results_list)):
                old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

                new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
                new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(old_result_version, new_result_version)
                self.assertEqual(old_result_contents["project"]["project"],
                                 new_result_contents["project"]["project"])
                self.assertEqual(old_result_contents["project"]["laboratory"],
                                 new_result_contents["project"]["laboratory"])
                self.assertEqual(old_result_contents["specimens"][0]["species"],
                                 new_result_contents["specimens"][0]["species"])

        self._get_es_results(partial(check_for_overwrite, old_results))

    def test_concurrent_specimen_submissions(self):
        """
        We submit two different bundles for the same specimen. What happens?
        """
        unmocked_mget = Elasticsearch.mget

        def mocked_mget(self, body):
            mget_return = unmocked_mget(self, body=body)
            # both threads sleep after reading to force conflict while writing
            time.sleep(0.5)
            return mget_return

        with patch.object(Elasticsearch, 'mget', new=mocked_mget):
            with self.assertLogs(level='WARNING') as cm:
                with ThreadPoolExecutor(max_workers=len(self.specimens)) as executor:
                    thread_results = executor.map(self._mock_index, self.specimens)
                    self.assertIsNotNone(thread_results)
                    self.assertTrue(all(r is None for r in thread_results))
                self.assertIsNotNone(cm.records)

                num_hits = sum(1 for log_msg in cm.output
                               if "There was a conflict with document" in log_msg)
                self.assertEqual(1, num_hits)

        def check_specimen_merge(es_results):
            specimen_uuids = []
            file_uuids = []
            for result_dict in es_results:
                result_contents = result_dict["_source"]["bundles"][0]["contents"]
                if "files" in result_dict["_index"]:
                    for file_dict in result_contents["files"]:
                        file_uuids.append(file_dict["uuid"])
                elif "specimens" in result_dict["_index"]:
                    # Each bundle in specimen list contains two files
                    self.assertEqual(2, len(result_contents["files"]))
                    specimen_uuids.append(result_dict["_id"])
                else:
                    continue

            for spec_uuid, _ in self.specimens:
                spec_metadata, _ = self._get_data_files(spec_uuid)
                for biomaterials_dict in spec_metadata["biomaterial.json"]["biomaterials"]:
                    if "specimen_from_organism" in biomaterials_dict["content"]["describedBy"]:
                        self.assertIn(biomaterials_dict["hca_ingest"]["document_id"], specimen_uuids)
                for file_dict in spec_metadata["file.json"]["files"]:
                    self.assertIn(file_dict["hca_ingest"]["document_id"], file_uuids)

        self._get_es_results(check_specimen_merge)


if __name__ == "__main__":
    unittest.main()
