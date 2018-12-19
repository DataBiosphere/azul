import boto3
import copy
import logging
from more_itertools import one
import random
import requests
import time
from typing import List, Set, Dict
import unittest
import urllib
import uuid

from azul import config
from azul.plugin import Plugin
from scripts.subscribe import subscribe
from scripts.reindex import Reindexer


logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class IntegrationTest(unittest.TestCase):
    bundle_uuid_prefix = None
    prefix_length = 3

    def setUp(self):
        super().setUp()
        self.bundle_uuid_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(self.prefix_length)])
        plugin = Plugin.load()
        test_dss_query = copy.deepcopy(plugin.dss_subscription_query())
        prefix_section = {
            "prefix": {
                "uuid": self.bundle_uuid_prefix
            }
        }
        test_dss_query['query']['bool']['must'].append(prefix_section)
        self.dss_query = test_dss_query

    def tearDown(self):
        subscribe(False)

    def test_webservice_and_indexer(self):
        subscribe(True)
        query = self.dss_query
        test_uuid = str(uuid.uuid4())
        test_name = f'integration-test_{test_uuid}_{self.bundle_uuid_prefix}'
        test_reindexer = Reindexer(indexer_url=config.indexer_endpoint(),
                                   test_name=test_name,
                                   es_query=query)

        if False:
            logger.info('Deleting all indices ...')
            test_reindexer.delete_all_indices()

        logger.info('Starting test using test name, %s ...', test_name)
        logger.info('Creating indices and reindexing ...')
        selected_bundle_fqids = test_reindexer.reindex()
        self.num_of_bundles = len(selected_bundle_fqids)
        self.check_bundles_are_indexed(test_name, 'files', set(selected_bundle_fqids))
        self.check_endpoint_is_working('/')
        self.check_endpoint_is_working('/health')
        self.check_endpoint_is_working('/version')
        self.check_endpoint_is_working('/repository/summary')
        self.check_endpoint_is_working('/repository/files/order')
        self.check_endpoint_is_working('/repository/files/export')

    def check_endpoint_is_working(self, url: str):
        url = config.service_endpoint() + url
        response = requests.get(url)
        response.raise_for_status()

    def get_number_of_messages(self, queues: Dict[str, boto3.resources.base.ServiceResource]):
        total_message_count = 0
        for queue_name, queue in queues.items():
            attributes = tuple('ApproximateNumberOfMessages' + suffix for suffix in ('', 'NotVisible', 'Delayed'))
            num_messages = tuple(int(queue.attributes[attribute]) for attribute in attributes)
            num_available, num_inflight, num_delayed = num_messages
            queue.reload()
            message_sum = sum((num_available, num_inflight, num_delayed))
            logger.info('Queue %s has %i messages, %i available, %i in flight and %i delayed',
                        queue_name, message_sum, num_available, num_inflight, num_delayed)
            total_message_count += message_sum
        logger.info('Total # of messages: %i', total_message_count)
        return total_message_count

    @property
    def default_queue_empty_timeout(self):
        return max(self.num_of_bundles * 30, 60)

    def wait_for_queues_to_empty(self, queue_names: List[str], timeout: int = None):
        empty_queue_timeout = timeout or self.default_queue_empty_timeout
        queues = {queue_name: boto3.resource('sqs').get_queue_by_name(QueueName=queue_name)
                  for queue_name in queue_names}
        populating_timeout = 60
        populating_start_time = time.time()

        while True:
            total_message_count = self.get_number_of_messages(queues)
            queue_wait_time_elapsed = (time.time() - populating_start_time)
            if queue_wait_time_elapsed > populating_timeout:
                logger.error('The queue(s) are still empty')
                return
            elif 0 < total_message_count:
                logger.info('The queue(s) have messages.')
                break
            time.sleep(5)

        emptying_start_time = time.time()
        while True:
            total_message_count = self.get_number_of_messages(queues)
            queue_wait_time_elapsed = (time.time() - emptying_start_time)
            if queue_wait_time_elapsed > empty_queue_timeout:
                self.fail(f'Timed out waiting for the queues to empty after {queue_wait_time_elapsed}')
            elif 0 >= total_message_count:
                logger.info('Queue emptied successfully.')
                break
            time.sleep(5)

        # Hack that removes the ResourceWarning that is caused by an unclosed SQS session
        for queue in queues.values():
            queue.meta.client._endpoint.http_session.close()

    def check_bundles_are_indexed(self, test_name: str, entity_type: str, indexed_bundle_fqids: Set[str]):
        max_retries = 5
        delay_between_retries = 5
        page_size = 100

        num_bundles = len(indexed_bundle_fqids)
        logger.info('Starting integration test %s with the prefix %s for the entity type %s. Expected %i bundle(s).',
                    test_name, self.bundle_uuid_prefix, entity_type, num_bundles)
        logger.debug('Expected bundles %s ', sorted(indexed_bundle_fqids))
        filters = '{"file":{"project":{"is":["' + test_name + '"]}}}'
        url = base_url = config.service_endpoint() + '/repository/' + entity_type + '?' + urllib.parse.urlencode(
            {'filters': filters, 'order': 'desc', 'sort': 'entryId', 'size': page_size})
        logger.info('Waiting for the queues to empty ...')
        self.wait_for_queues_to_empty([config.notify_queue_name, config.document_queue_name])
        logger.info('Checking if bundles are referenced by the service response ...')
        retries = 0
        found_bundle_fqids = set()

        while True:
            response_json = requests.get(url).json()
            hits = response_json.get('hits', [])
            found_bundle_fqids.update(f"{entity['bundleUuid']}.{entity['bundleVersion']}"
                                      for bundle in hits for entity in bundle.get('bundles', [])
                                      if entity['bundleUuid'].startswith(self.bundle_uuid_prefix))
            search_after = response_json['pagination']['search_after']
            search_after_uid = response_json['pagination']['search_after_uid']
            total_entities = response_json['pagination']['total']
            logger.info('Found %i/%i bundles on try #%i/%i. There are %i total hits. Current page has %i hits in %s.',
                        len(found_bundle_fqids), num_bundles, retries+1, max_retries, total_entities, len(hits), url)

            if search_after is None:
                assert search_after_uid is None
                if indexed_bundle_fqids == found_bundle_fqids:
                    logger.info('Found all bundles.')
                    break
                elif retries >= max_retries:
                    logger.error('Unable to find all the bundles. Retried too many (%i) times.', retries)
                    break
                else:
                    time.sleep(delay_between_retries)
                    retries += 1
            else:
                assert search_after_uid is not None
                url = base_url + '&' + urllib.parse.urlencode({'search_after': search_after,
                                                               'search_after_uid': search_after_uid})

        logger.info('Actual bundle count is %i.', len(found_bundle_fqids))
        self.assertEqual(found_bundle_fqids, indexed_bundle_fqids)
        for hit in response_json['hits']:
            project = one(hit['projects'])
            bundle_fqids = [bundle['bundleUuid'] + '.' + bundle['bundleVersion'] for bundle in hit['bundles']]

            self.assertTrue(test_name in project['projectShortname'],
                            f'There was a problem during indexing an {entity_type} entity'
                            f' {hit["entryId"]}. Bundle(s) ({",".join(bundle_fqids)})'
                            f' have been indexed without the debug project name. Contains {project}')
