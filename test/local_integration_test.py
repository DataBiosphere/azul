from collections import deque
import logging
import random

from furl import furl
import requests
import time
from typing import Any, Mapping, Set, Optional
import unittest
import urllib
from urllib.parse import urlencode
import uuid
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile
import os
import csv
from more_itertools import one, first

from azul import config
from azul.decorators import memoized_property
from azul.azulclient import AzulClient
from azul.deployment import aws
from azul.requests import requests_session

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class IntegrationTest(unittest.TestCase):
    bundle_uuid_prefix = None
    prefix_length = 3

    @memoized_property
    def dss(self):
        return config.dss_client()

    def setUp(self):
        super().setUp()
        self.set_lambda_test_mode(True)
        self.bundle_uuid_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(self.prefix_length)])

    def tearDown(self):
        self.set_lambda_test_mode(False)
        super().tearDown()

    def test_webservice_and_indexer(self):
        if config.deployment_stage != 'prod':
            test_uuid = str(uuid.uuid4())
            test_name = f'integration-test_{test_uuid}_{self.bundle_uuid_prefix}'
            logger.info('Starting test using test name, %s ...', test_name)

            azul_client = AzulClient(indexer_url=config.indexer_endpoint(),
                                     test_name=test_name,
                                     prefix=self.bundle_uuid_prefix)
            logger.info('Creating indices and reindexing ...')
            selected_bundle_fqids = azul_client.reindex()
            self.num_bundles = len(selected_bundle_fqids)
            self.check_bundles_are_indexed(test_name, 'files', set(selected_bundle_fqids))

        self.check_endpoint_is_working(config.indexer_endpoint(), '/health')
        self.check_endpoint_is_working(config.service_endpoint(), '/')
        self.check_endpoint_is_working(config.service_endpoint(), '/health')
        self.check_endpoint_is_working(config.service_endpoint(), '/version')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/summary')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/files/order')

        if config.dss_query_prefix:
            # only subset of indexed metadata, use unrestricted filter
            manifest_filter = {"file": {}}
        else:
            manifest_filter = {"file": {"organ": {"is": ["brain"]}, "fileFormat": {"is": ["bai"]}}}

        for format_, validator in [
            (None, self.check_manifest),
            ('tsv', self.check_manifest),
            ('bdbag', self.check_bdbag)
        ]:
            for path in '/repository/files/export', '/manifest/files':
                with self.subTest(format=format_, filter=manifest_filter, path=path):
                    query = {'filters': manifest_filter}
                    if format_ is not None:
                        query['format'] = format_
                    response = self.check_endpoint_is_working(config.service_endpoint(), path, query)
                    validator(response)

    def set_lambda_test_mode(self, mode: bool):
        client = aws.lambda_
        indexer_lambda_config = client.get_function_configuration(FunctionName=config.indexer_name)
        environment = indexer_lambda_config['Environment']
        environment['Variables']['TEST_MODE'] = '1' if mode else '0'
        client.update_function_configuration(FunctionName=config.indexer_name, Environment=environment)

    @memoized_property
    def requests(self) -> requests.Session:
        return requests_session()

    def check_endpoint_is_working(self, endpoint: str, path: str, query: Optional[Mapping[str, str]] = None) -> bytes:
        url = furl(endpoint)
        url.path.add(path)
        if query is not None:
            url.query.set({k: str(v) for k, v in query.items()})
        logger.info('Requesting %s', url)
        response = self.requests.get(url.url)
        response.raise_for_status()
        return response.content

    def check_manifest(self, response: bytes):
        """Assert that manifest contains at least one row of metadata."""
        wrapper = TextIOWrapper(BytesIO(response))
        num_rows = len(list(csv.reader(wrapper, delimiter='\t')))
        self.assertTrue(num_rows > 1)
        logger.info(f'Manifest contains {num_rows} rows.')

    def check_bdbag(self, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            tsv_files = {filename: os.path.join(data_path, filename) for filename in ['participants.tsv', 'samples.tsv']}
            for file_name, file_path in tsv_files.items():
                with zip_fh.open(file_path) as bytesfile:
                    text = TextIOWrapper(bytesfile, encoding='utf-8')
                    num_rows = len(list(csv.reader(text, delimiter='\t')))
                    self.assertTrue(num_rows > 1)
                    logger.info(f'BDBag file {file_name} contains {num_rows} rows.')

    def get_number_of_messages(self, queues: Mapping[str, Any]):
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

    def get_queue_empty_timeout(self, num_bundles: int):
        return max(num_bundles * 30, 60)

    def wait_for_queue_level(self, empty: bool = True, timeout: int = 60):
        queue_names = [config.notify_queue_name, config.document_queue_name]
        queues = {queue_name: aws.sqs_resource.get_queue_by_name(QueueName=queue_name)
                  for queue_name in queue_names}
        wait_start_time = time.time()
        queue_size_history = deque(maxlen=10)

        while True:
            total_message_count = self.get_number_of_messages(queues)
            queue_wait_time_elapsed = (time.time() - wait_start_time)
            queue_size_history.append(total_message_count)
            cumulative_queue_size = sum(queue_size_history)
            if queue_wait_time_elapsed > timeout:
                logger.error('The queue(s) are NOT at the desired level.')
                return
            elif (cumulative_queue_size == 0) == empty:
                logger.info('The queue(s) at the desired level.')
                break
            else:
                logger.info('The most recently sampled queue sizes are %r.', queue_size_history)
            time.sleep(5)

        # Hack that removes the ResourceWarning that is caused by an unclosed SQS session
        for queue in queues.values():
            queue.meta.client._endpoint.http_session.close()

    def check_bundles_are_indexed(self, test_name: str, entity_type: str, indexed_bundle_fqids: Set[str]):
        service_check_timeout = 600
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
        self.wait_for_queue_level(empty=False)
        self.wait_for_queue_level(timeout=self.get_queue_empty_timeout(self.num_bundles))
        logger.info('Checking if bundles are referenced by the service response ...')
        retries = 0
        found_bundle_fqids = set()
        deadline = time.time() + service_check_timeout

        while True:
            response_json = self.requests.get(url).json()
            hits = response_json.get('hits', [])
            found_bundle_fqids.update(f"{entity['bundleUuid']}.{entity['bundleVersion']}"
                                      for bundle in hits for entity in bundle.get('bundles', [])
                                      if entity['bundleUuid'].startswith(self.bundle_uuid_prefix))
            search_after = response_json['pagination']['search_after']
            search_after_uid = response_json['pagination']['search_after_uid']
            total_entities = response_json['pagination']['total']
            logger.info('Found %i/%i bundles on try #%i. There are %i files with the project name.'
                        ' Current page has %i hits in %s.', len(found_bundle_fqids), num_bundles, retries + 1,
                        total_entities, len(hits), url)

            if search_after is None:
                assert search_after_uid is None
                if indexed_bundle_fqids == found_bundle_fqids:
                    logger.info('Found all bundles.')
                    break
                elif time.time() > deadline:
                    logger.error('Unable to find all the bundles in under %i seconds.', service_check_timeout)
                    break
                else:
                    time.sleep(delay_between_retries)
                    retries += 1
            else:
                assert search_after_uid is not None
                url = base_url + '&' + urlencode(dict(search_after=search_after,
                                                      search_after_uid=search_after_uid))

        logger.info('Actual bundle count is %i.', len(found_bundle_fqids))
        self.assertEqual(found_bundle_fqids, indexed_bundle_fqids)
        for hit in response_json['hits']:
            project = one(hit['projects'])
            bundle_fqids = [bundle['bundleUuid'] + '.' + bundle['bundleVersion'] for bundle in hit['bundles']]
            self.assertTrue(test_name in project['projectShortname'],
                            f'There was a problem during indexing an {entity_type} entity'
                            f' {hit["entryId"]}. Bundle(s) ({",".join(bundle_fqids)})'
                            f' have been indexed without the debug project name. Contains {project}')
