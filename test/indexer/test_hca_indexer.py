# -*- coding: utf-8 -*-
"""
Suite for unit testing indexer.py
"""

from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
import logging
import os
import time
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch

from azul import config, eventually
from azul.json_freeze import freeze
from indexer import IndexerTestCase

from azul.transformer import ElasticSearchDocument

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
            if entity_index != config.es_index_name("projects"):
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
        self._mock_index(self.old_bundle)

        def check_bundle_correctness(es_results):
            self.assertGreater(len(es_results), 0)
            data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
            for result_dict in es_results:
                index_name = result_dict["_index"]
                index_id = result_dict["_id"]
                expected_ids = set()
                entity_type = config.entity_type_for_es_index(index_name)
                path = os.path.join(data_prefix, f'aee55415-d128-4b30-9644-e6b2742fa32b.{entity_type}.results.json')
                with open(path, 'r') as fp:
                    expected_dict = json.load(fp)
                    self.assertGreater(len(expected_dict["hits"]["hits"]), 0)
                    for expected_hit in expected_dict["hits"]["hits"]:
                        expected_ids.add(expected_hit["_id"])
                        if index_id == expected_hit["_id"]:
                            self.assertEqual(freeze(expected_hit["_source"]), freeze(result_dict["_source"]))
                self.assertIn(index_id, expected_ids)

        self._get_es_results(check_bundle_correctness)

    def test_delete_correctness(self):
        """
        Delete a bundle and check that the index contains the appropriate flags
        """
        data_pack = self._get_data_files(self.new_bundle[0], self.new_bundle[1], updated=True)

        self._mock_delete(self.new_bundle, data_pack)

        def check_bundle_delete_correctness(es_results):
            self.assertGreater(len(es_results), 0)
            for result_dict in es_results:
                result_doc = ElasticSearchDocument.from_index(result_dict)
                self.assertEqual(result_doc.bundles[0].uuid, self.new_bundle[0])
                self.assertEqual(result_doc.bundles[0].version, self.new_bundle[1])
                self.assertEqual(result_doc.bundles[0].contents, {'deleted': True})
                self.assertTrue(result_doc.bundles[0].deleted)

        self._get_es_results(check_bundle_delete_correctness)


    def test_update_with_newer_version(self):
        """
        Updating a bundle with a future version should overwrite the old version.
        """
        self._mock_index(self.old_bundle)

        def check_old_submission(es_results):
            self.assertGreater(len(es_results), 0)
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.old_bundle[1], old_result_version)
                self.assertEqual("Mouse Melanoma", old_result_contents["project"]["project_shortname"])
                self.assertIn("Sarah Teichmann", old_result_contents["project"]["laboratory"])
                self.assertIn("Mus musculus", old_result_contents["specimens"][0]["genus_species"])

        old_results = self._get_es_results(check_old_submission)

        self._mock_index(self.new_bundle, updated=True)

        def check_updated_submission(old_results_list, new_results_list):
            for old_result_dict, new_result_dict in list(zip(old_results_list, new_results_list)):
                old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

                new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
                new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

                self.assertNotEqual(old_result_version, new_result_version)
                self.assertNotEqual(old_result_contents["project"]["project_shortname"],
                                    new_result_contents["project"]["project_shortname"])
                self.assertNotEqual(old_result_contents["project"]["laboratory"],
                                    new_result_contents["project"]["laboratory"])
                self.assertNotEqual(old_result_contents["specimens"][0]["genus_species"],
                                    new_result_contents["specimens"][0]["genus_species"])

        self._get_es_results(partial(check_updated_submission, old_results))

    def test_old_version_overwrite(self):
        """
        An attempt to overwrite a newer version of a bundle with an older version should fail.
        """
        self._mock_index(self.new_bundle, updated=True)

        def check_new_submission(es_results):
            self.assertGreater(len(es_results), 0)
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.new_bundle[1], old_result_version)
                self.assertEqual("Aardvark Ailment", old_result_contents["project"]["project_shortname"])
                self.assertIn("John Denver", old_result_contents["project"]["laboratory"])
                self.assertIn("Lorem ipsum", old_result_contents["specimens"][0]["genus_species"])

        old_results = self._get_es_results(check_new_submission)

        self._mock_index(self.old_bundle)

        def check_for_overwrite(old_results_list, new_results_list):
            for old_result_dict, new_result_dict in list(zip(old_results_list, new_results_list)):
                old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

                new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
                new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(old_result_version, new_result_version)
                self.assertEqual(old_result_contents["project"]["project_shortname"],
                                 new_result_contents["project"]["project_shortname"])
                self.assertEqual(old_result_contents["project"]["laboratory"],
                                 new_result_contents["project"]["laboratory"])
                self.assertEqual(old_result_contents["specimens"][0]["genus_species"],
                                 new_result_contents["specimens"][0]["genus_species"])

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
            file_doc_ids = set()
            self.assertEqual(len(es_results), 5)
            for result_dict in es_results:
                self.assertEqual(result_dict["_id"], result_dict["_source"]["entity_id"])
                if result_dict["_index"] == config.es_index_name("files"):
                    # files assumes one bundle per result
                    self.assertEqual(len(result_dict["_source"]["bundles"]), 1)
                    result_contents = result_dict["_source"]["bundles"][0]["contents"]
                    self.assertEqual(1, len(result_contents["files"]))
                    file_doc_ids.add(result_contents["files"][0]["uuid"])
                elif result_dict["_index"] == config.es_index_name("specimens"):
                    self.assertEqual(len(result_dict["_source"]["bundles"]), 2)
                    for bundle in result_dict["_source"]["bundles"]:
                        result_contents = bundle["contents"]
                        # Each bundle in specimen list contains two files
                        self.assertEqual(2, len(result_contents["files"]))
                else:
                    continue

            self.assertEqual(len(file_doc_ids), 4)
            for spec_uuid, spec_version in self.specimens:
                _, _, spec_metadata = self._get_data_files(spec_uuid, spec_version)
                for file_dict in spec_metadata["file.json"]["files"]:
                    self.assertIn(file_dict["hca_ingest"]["document_id"], file_doc_ids)

        self._get_es_results(check_specimen_merge)


if __name__ == "__main__":
    unittest.main()
