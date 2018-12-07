import boto3
import copy
import logging
from more_itertools import one
import random
import requests
import time
from typing import List, Set
import unittest
import urllib
import uuid

from azul import config
from azul.project.hca import Plugin
from scripts.subscribe import subscribe
from scripts.reindex import Reindexer


logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class IntegrationTest(unittest.TestCase):
    bundle_id_prefix = None
    service_base_url = config.service_endpoint()
    indexer_base_url = config.indexer_endpoint()
    prefix_length = 2
    sqs_queue_empty_timeout = 4500/(8 ^ (prefix_length-1)) if prefix_length <= 3 else 60

    def setUp(self):
        super().setUp()
        self.bundle_id_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(2)])
        plugin = Plugin().load()
        plugin.dss_subscription_query()
        test_dss_query = copy.deepcopy(plugin.dss_subscription_query())
        prefix_section = {
            "prefix": {
                "uuid": self.bundle_id_prefix
            }
        }
        test_dss_query['query']['bool']['must'].append(prefix_section)
        self.dss_query = test_dss_query

    def check_endpoint_is_working(self, url: str):
        url = self.service_base_url + url
        response = requests.get(url)
        response.raise_for_status()

    def wait_for_sqs_queue_to_empty(self, queue_names: List[str], timeout: int = sqs_queue_empty_timeout):
        queues = [{'queue': boto3.resource('sqs').get_queue_by_name(QueueName=queue_name), 'name': queue_name}
                  for queue_name in queue_names]

        start_time = time.time()
        while True:
            total_message_count = 0
            for queue in queues:
                num_available, num_inflight, num_delayed = tuple(int(
                    queue['queue'].attributes['ApproximateNumberOfMessages' + suffix])
                                                                 for suffix in ('', 'NotVisible', 'Delayed'))
                queue['queue'].reload()
                message_sum = sum((num_available, num_inflight, num_delayed))
                queue_name = queue['name']
                queue_status_log = f'{queue_name: <30}| Available:{num_available:>4}, In Flight: {num_inflight:>4},' \
                                   f' Delayed: {num_delayed:>4}, Subtotal: {message_sum:>4}'
                logger.info(queue_status_log)
                total_message_count += message_sum
            logger.info('Total Messages: %r', total_message_count)

            queue_wait_time_elapsed = (time.time() - start_time)
            if queue_wait_time_elapsed > timeout:
                logger.warning('Timed out waiting for the queues to empty after %s(s).', queue_wait_time_elapsed)
                break
            elif 0 >= total_message_count:
                logger.info('Queue emptied successfully.')
                break

            time.sleep(5)

        # Hack that removes the ResourceWarning that is caused by an unclosed SQS session
        for queue in queues:
            queue['queue'].meta.client._endpoint.http_session.close()

    def check_bundles_are_indexed(self, test_name: str, entity_type: str, indexed_bundle_ids: Set[str]):
        max_num_of_retries = 5
        delay_between_retries = 5
        queue_check_delay = 10
        page_size = 100

        num_bundles = len(indexed_bundle_ids)
        logger.info('Starting integration test %s', test_name)
        logger.info('Search Prefix: %s', self.bundle_id_prefix)
        logger.info('Entity Type: %s', entity_type)
        logger.info('Expected %s bundle(s)',  num_bundles)
        expected_bundle_ids_str = '\n'.join(sorted(indexed_bundle_ids))
        logger.debug('Expecting Bundle IDs:')
        logger.debug(expected_bundle_ids_str)
        filters = '{"file":{"project":{"is":["' + test_name + '"]}}}'
        url = base_url = self.service_base_url + '/repository/' + entity_type + '?' + urllib.parse.urlencode(
            {'filters': filters, 'order': 'desc', 'sort': 'entryId', 'size': page_size})

        logger.info('Waiting for the queues to empty...')
        time.sleep(queue_check_delay)
        self.wait_for_sqs_queue_to_empty([config.notify_queue_name, config.document_queue_name])

        logger.info('Testing service endpoints...')
        response = requests.get(url)
        retries = 0
        found_bundle_ids = set()
        ids_with_prefix = None
        while retries < max_num_of_retries:
            response = requests.get(url)
            response_json = response.json()
            hits = response_json.get('hits', [])
            found_bundle_ids.update({f"{entity['bundleUuid']}.{entity['bundleVersion']}"
                                     for bundle in hits for entity in bundle.get('bundles', [])})
            ids_with_prefix = {bundle_id for bundle_id in found_bundle_ids
                               if bundle_id.startswith(self.bundle_id_prefix)}
            search_after = response_json['pagination']['search_after']
            search_after_uid = response_json['pagination']['search_after_uid']
            total_entities = response_json['pagination']['total']

            service_search_log = f'Bundles Found: {len(ids_with_prefix)}/{num_bundles},' \
                                 f' Hits on Page: {len(hits)}, Total: {total_entities}, Attempt:' \
                                 f' {retries+1}/{max_num_of_retries}, URL: {url}'
            logger.info(service_search_log)

            if search_after is None and search_after_uid is None:
                if ids_with_prefix == indexed_bundle_ids:
                    logger.info('Found all bundles.')
                    break
                else:
                    time.sleep(delay_between_retries)
                    retries += 1
            else:
                url = base_url + f'&size={page_size}&search_after={search_after}&search_after_uid={search_after_uid}'

        logger.info('Actual Count: %s', len(ids_with_prefix))

        self.assertEqual(ids_with_prefix, indexed_bundle_ids)
        for hit in response.json()['hits']:
            project = one(hit['projects'])
            bundle_fqids = [f'{bundle["bundleUuid"]}.{bundle["bundleVersion"]}' for bundle in hit["bundles"]]
            self.assertTrue(test_name in project['projectShortname'],
                            f'There was a problem during indexing an {entity_type} entity with the id,'
                            f' {hit["entryId"]}. Bundle(s) ({",".join(bundle_fqids)})'
                            f' has been indexed without the debug project name. Contains {project}')

    def test_webservice_and_indexer(self):
        subscribe(True)
        query = self.dss_query
        test_uuid = str(uuid.uuid4())
        test_name = f'integration-test_{test_uuid}_{self.bundle_id_prefix}'
        test_reindexer = Reindexer(indexer_url=self.indexer_base_url,
                                   test_name=test_name,
                                   es_query=query)

        if False:
            logger.info('Deleting all indices...')
            test_reindexer.delete_all_indices()

        logger.info('Starting test using test name, %s...', test_name)
        logger.info('Creating indices and reindexing...')
        selected_bundle_ids = test_reindexer.reindex()
        self.check_bundles_are_indexed(test_name, 'files', set(selected_bundle_ids))
        self.check_endpoint_is_working('/')
        self.check_endpoint_is_working('/health')
        self.check_endpoint_is_working('/version')
        self.check_endpoint_is_working('/repository/summary')
        self.check_endpoint_is_working('/repository/files/order')
        self.check_endpoint_is_working('/repository/files/export')
        subscribe(False)
