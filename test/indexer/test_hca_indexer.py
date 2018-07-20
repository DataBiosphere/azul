# -*- coding: utf-8 -*-
"""
Suite for unit testing indexer.py
"""

import docker
import json
import logging
import time
import unittest

from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from functools import partial
from typing import Mapping, Any
from unittest.mock import patch
from uuid import uuid4

from azul.project.hca.indexer import Indexer
from azul.project.hca.config import IndexProperties
from azul.downloader import MetadataDownloader
from azul import eventually

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHCAIndexer(unittest.TestCase):
    @staticmethod
    def _get_data_files(metadata_file, data_file):
        data_prefix = "data/"

        with open(data_prefix + metadata_file, 'r') as infile:
            metadata = json.loads(infile.read())

        with open(data_prefix + data_file, 'r') as infile:
            data = json.loads(infile.read())

        return metadata, data

    def _get_data_by_uuid(self, uuid):
        return self._get_data_files(uuid + '.metadata', uuid + '.data')

    @staticmethod
    def _make_fake_notification(uuid: str, version: str) -> Mapping[str, Any]:
        return {
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

    def _mock_index(self, test_bundles, data_pack):
        bundle_uuid, bundle_version = test_bundles
        metadata, manifest = data_pack
        fake_event = self._make_fake_notification(bundle_uuid, bundle_version)

        with patch.object(MetadataDownloader, 'extract_bundle') as mock_method:
            mock_method.return_value = metadata, manifest
            self.hca_indexer.index(fake_event)

    @eventually(5.0, 0.5)
    def _get_es_results(self, assert_func):
        es_results = []
        for entity_index in self.index_names:
            results = self.es_client.search(index=entity_index,
                                            doc_type="doc",
                                            size=100)
            if "project" not in entity_index:
                for result_dict in results["hits"]["hits"]:
                    es_results.append(result_dict)

        assert_func(es_results)
        return es_results

    @classmethod
    def setUpClass(cls):
        cls.old_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-29T152812.404846Z")
        cls.new_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-30T152812.404846Z")
        cls.spec1_bundle = ("9dec1bd6-ced8-448a-8e45-1fc7846d8995",
                            "2018-03-29T154319.834528Z")
        cls.spec2_bundle = ("56a338fe-7554-4b5d-96a2-7df127a7640b",
                            "2018-03-29T153507.198365Z")

        docker_client = docker.from_env()
        api_container_port = '9200/tcp'
        cls.container_obj = docker_client.containers.run("docker.elastic.co/elasticsearch/elasticsearch:5.5.3",
                                                         detach=True,
                                                         ports={api_container_port: ('127.0.0.1', None)},
                                                         environment=["xpack.security.enabled=false",
                                                                      "discovery.type=single-node"])
        container_info = docker_client.api.inspect_container(cls.container_obj.name)
        container_ports = container_info['NetworkSettings']['Ports']
        container_port = container_ports[api_container_port][0]
        host_port, host_ip = int(container_port['HostPort']), container_port['HostIp']

        index_properties = IndexProperties(dss_url="https://rtfbvgfncgfjkolpcgfcdg.fcdgf/gibberish",
                                           es_endpoint=(host_ip, host_port))
        cls.es_client = index_properties.elastic_search_client

        # try wait here for the elasticsearch container
        patched_log_level = logging.WARNING if logger.getEffectiveLevel() <= logging.DEBUG else logging.ERROR
        with patch.object(logging.getLogger('elasticsearch'), 'level', new=patched_log_level):
            while not cls.es_client.ping():
                logger.info('Could not ping Elasticsearch. Retrying ...')
                time.sleep(1)
        logger.info('Elasticsearch appears to be up.')

        cls.index_names = index_properties.index_names
        cls.hca_indexer = Indexer(index_properties)

    @classmethod
    def tearDownClass(cls):
        cls.container_obj.kill()

    def tearDown(self):
        for index_name in self.index_names:
            self.es_client.indices.delete(index=index_name, ignore=[400, 404])

    def test_index_correctness(self):
        """
        Index a bundle and check that the index contains the correct attributes
        """
        data_pack = self._get_data_files('updated.metadata',
                                         'updated.data')

        self._mock_index(self.new_bundle, data_pack)

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
        old_data = self._get_data_by_uuid(self.old_bundle[0])
        new_data = self._get_data_files('updated.metadata',
                                        'updated.data')

        self._mock_index(self.old_bundle, old_data)

        def check_old_submission(es_results):
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.old_bundle[1], old_result_version)
                self.assertEqual("Mouse Melanoma", old_result_contents["project"]["project"])
                self.assertIn("Sarah Teichmann", old_result_contents["project"]["laboratory"])
                self.assertIn("Mus musculus", old_result_contents["specimens"][0]["species"])

        old_results = self._get_es_results(check_old_submission)

        self._mock_index(self.new_bundle, new_data)

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
        old_data = self._get_data_by_uuid(self.old_bundle[0])
        new_data = self._get_data_files('updated.metadata',
                                        'updated.data')

        self._mock_index(self.new_bundle, new_data)

        def check_new_submission(es_results):
            for result_dict in es_results:
                old_result_version = result_dict["_source"]["bundles"][0]["version"]
                old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

                self.assertEqual(self.new_bundle[1], old_result_version)
                self.assertEqual("Aardvark Ailment", old_result_contents["project"]["project"])
                self.assertIn("John Denver", old_result_contents["project"]["laboratory"])
                self.assertIn("Lorem ipsum", old_result_contents["specimens"][0]["species"])

        old_results = self._get_es_results(check_new_submission)

        self._mock_index(self.old_bundle, old_data)

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
        spec1_pack = self._get_data_by_uuid(self.spec1_bundle[0])
        spec2_pack = self._get_data_by_uuid(self.spec2_bundle[0])
        specimen_list = [(self.spec1_bundle, spec1_pack),
                         (self.spec2_bundle, spec2_pack)]

        unmocked_mget = Elasticsearch.mget

        def mocked_mget(self, body):
            mget_return = unmocked_mget(self, body=body)
            # both threads sleep after reading to force conflict while writing
            time.sleep(0.5)
            return mget_return

        def help_index(specimen_tuple):
            self._mock_index(specimen_tuple[0], specimen_tuple[1])

        with patch.object(Elasticsearch, 'mget', new=mocked_mget):
            with self.assertLogs(level='WARNING') as cm:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    thread_results = executor.map(help_index, specimen_list)
                    self.assertNotEqual(None, thread_results)
                    self.assertTrue(all(r is None for r in thread_results))
                self.assertNotEqual(None, cm.records)

                num_hits = 0
                for log_msg in cm.output:
                    if "There was a conflict with document" in log_msg:
                        num_hits += 1
                self.assertEqual(3, num_hits)

        def check_specimen_merge(es_results):
            specimen_uuids = []
            file_uuids = []
            for result_dict in es_results:
                result_contents = result_dict["_source"]["bundles"][0]["contents"]
                if "files" in result_dict["_index"]:
                    for file_dict in result_contents["files"]:
                        file_uuids.append(file_dict["uuid"])
                elif "specimens" in result_dict["_index"]:
                    self.assertLess(1, len(result_contents["files"]))
                    for file_dict in result_contents["files"]:
                        specimen_uuids.append(file_dict["uuid"])
                else:
                    continue

            self.assertEqual(len(file_uuids), len(specimen_uuids))
            for uuid in specimen_uuids:
                self.assertIn(uuid, file_uuids)

        self._get_es_results(check_specimen_merge)


if __name__ == "__main__":
    unittest.main()
