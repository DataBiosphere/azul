import boto3
import copy
from datetime import datetime
import logging
import os
import random
import requests
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib
import uuid

from azul import config
from azul.project.hca import Plugin
from azul.types import JSON
from scripts.subscribe import subscribe
from scripts.reindex import Reindexer


it_logger = logging.Logger('integrated-test-logger')
it_logger.setLevel(logging.INFO)
it_strm_hndlr = logging.StreamHandler(sys.stdout)
it_logger.addHandler(it_strm_hndlr)


class IntegrationTest(unittest.TestCase):
    bundle_id_prefix = None
    service_base_url = 'https://' + config.api_lambda_domain('service')
    indexer_base_url = 'https://' + config.api_lambda_domain('indexer')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        pass

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        pass

    def generate_query(self) -> JSON:
        self.bundle_id_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(2)])
        test_dss_query = copy.deepcopy(Plugin().dss_subscription_query())
        prefix_section = {
                            "prefix": {
                                "uuid": self.bundle_id_prefix
                            }
                         }
        test_dss_query['query']['bool']['must'].append(prefix_section)
        return test_dss_query

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def check_endpoint_is_working(self, url: str):
        url = self.service_base_url + url
        response = requests.get(url)
        self.assertTrue(200, response.status_code)

    def wait_for_sqs_queue_to_empty(self, sqs_names: list, timeout: int = 480):
        queue_list = [boto3.resource('sqs').get_queue_by_name(QueueName=q) for q in sqs_names]

        start_time = datetime.now()
        while True:
            total_message_count = 0
            for queue_name, queue in zip(sqs_names, queue_list):
                message_subcounts = tuple(int(queue.attributes['ApproximateNumberOfMessages' + suffix])
                                          for suffix in ('', 'NotVisible', 'Delayed'))
                queue.reload()
                it_logger.info(f'{queue_name: <30}| Available:{message_subcounts[0]:>4}, '
                               f'In Flight: {message_subcounts[1]:>4}, Delayed: {message_subcounts[2]:>4}, '
                               f'Subtotal: {sum(message_subcounts):>4}')
                total_message_count += sum(message_subcounts)
            it_logger.info(f'Total Messages: {total_message_count}\n')

            queue_wait_time_elapsed = (datetime.now() - start_time).seconds
            if queue_wait_time_elapsed > timeout:
                it_logger.warning(f'Timed out waiting for the queues to empty'
                                  f' after {queue_wait_time_elapsed}s.')
                break
            elif 0 >= total_message_count:
                it_logger.warning('Queue emptied successfully.\n')
                break

            time.sleep(5)

        for queue in queue_list:
            queue.meta.client._endpoint.http_session.close()

    def check_bundles_are_indexed(self, test_name: str, entity: str, indexed_bundle_ids: list):
        max_num_of_retries = 5
        delay_between_retries = 5
        queue_check_delay = 10
        page_size = 100

        bundle_list_size = len(indexed_bundle_ids)
        it_logger.info(f'Test Integration Test Name: {test_name}')
        it_logger.info(f'Search Prefix: {self.bundle_id_prefix}')
        it_logger.info(f'Entity Type: {entity}')
        it_logger.info(f'Expected Bundle Count: {bundle_list_size}')
        expected_bundle_ids_str = '\n'.join(sorted(indexed_bundle_ids))
        it_logger.info(f"\nExpected Bundle IDs:\n{expected_bundle_ids_str}\n")

        filter_dict = urllib.parse.quote('{"file":{"project":{"is":["' + test_name + '"]}}}')
        base_url = self.service_base_url + f'/repository/{entity}?filters={filter_dict}&order=desc&sort=entryId'
        url = base_url + f'&size={page_size}&order=desc'

        it_logger.info('Waiting for the queues to empty...\n')
        time.sleep(queue_check_delay)
        self.wait_for_sqs_queue_to_empty([config.notify_queue_name, config.document_queue_name])

        it_logger.info('Testing service endpoints...')
        response = requests.get(url)
        retries = 0
        found_bundle_ids = set()
        ids_with_prefix = None
        while retries < max_num_of_retries:
            response = requests.get(url)
            response_json = response.json()
            hit_list = response_json.get('hits', [])
            found_bundle_ids.update({f"{file_metadata['bundleUuid']}.{file_metadata['bundleVersion']}"
                                     for bundle in hit_list for file_metadata in bundle.get('bundles', [])})
            ids_with_prefix = [bundle_id for bundle_id in found_bundle_ids
                               if bundle_id.startswith(self.bundle_id_prefix)]
            search_after = response_json['pagination']['search_after']
            search_after_uid = response_json['pagination']['search_after_uid']
            total_entities = response_json['pagination']['total']

            it_logger.info(f'Bundles Found: {len(ids_with_prefix)}/{bundle_list_size}, Hits on Page: {len(hit_list)},'
                           f' Total: {total_entities}, Attempt: {retries+1}/{max_num_of_retries}, URL: {url}')

            if search_after is None and search_after_uid is None:
                if len(ids_with_prefix) == bundle_list_size:
                    break
                else:
                    time.sleep(delay_between_retries)
                    retries += 1
                    url = base_url + f'&size={page_size}'
            else:
                url = base_url + f'&size={page_size}&search_after={search_after}&search_after_uid={search_after_uid}'

        it_logger.info(f'\nActual Count: {len(ids_with_prefix)}')

        self.assertListEqual(sorted(ids_with_prefix), sorted(indexed_bundle_ids))
        for hit in response.json()['hits']:
            projects = [project for project in hit['projects']]
            self.assertTrue(any([test_name in project['projectShortname'] for project in projects]),
                            f'There was a problem during indexing.'
                            f'Bundle(s) ({",".join([bundle["bundleUuid"] for bundle in hit["bundles"]])})'
                            f' has been indexed without the debug project name. Contains {projects}')

    def test_webservice_and_indexer(self):
        dss_client = config.dss_client()
        sm = boto3.client('secretsmanager')
        creds = sm.get_secret_value(SecretId=config.google_service_account('indexer'))
        with tempfile.NamedTemporaryFile(mode='w+') as f:
            f.write(creds['SecretString'])
            f.flush()
            with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=f.name):
                subscribe(True, dss_client)
        query = self.generate_query()
        test_uuid = str(uuid.uuid4())
        test_name = f'integration-test_{test_uuid}_{self.bundle_id_prefix}'
        test_reindexer = Reindexer(indexer_url=self.indexer_base_url,
                                   test_name=test_name,
                                   es_query=query)

        if False:
            it_logger.info('Deleting all indices...\n')
            test_reindexer.delete_all_indices()

        it_logger.info(f'Starting test using test name, {test_name}...\n')
        it_logger.info('Creating indices and reindexing...\n')
        test_reindexer.reindex()
        self.check_bundles_are_indexed(test_name, 'files', test_reindexer.selected_bundle_ids)
        self.check_endpoint_is_working('/')
        self.check_endpoint_is_working('/health')
        self.check_endpoint_is_working('/version')
        self.check_endpoint_is_working('/repository/summary/files')
        self.check_endpoint_is_working('/repository/summary/specimens')
        self.check_endpoint_is_working('/repository/files/order')
        self.check_endpoint_is_working('/repository/files/export')
        subscribe(False, dss_client)
