from unittest import mock
import boto3
from collections import deque
import logging
import random

from furl import furl
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
import requests
import time
from typing import Any, Mapping, Set, Optional, IO
import unittest
import urllib
from urllib.parse import urlencode
import uuid
import os
import csv
import gzip
import json
from more_itertools import one, first
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

from azul import config
from azul.decorators import memoized_property
from azul.azulclient import AzulClient
from azul.dss import patch_client_for_direct_file_access
from azul.requests import requests_session
from azul import drs

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class IntegrationTest(unittest.TestCase):
    bundle_uuid_prefix = None
    prefix_length = 3

    def setUp(self):
        super().setUp()
        self.set_lambda_test_mode(True)
        self.bundle_uuid_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(self.prefix_length)])

    def tearDown(self):
        self.set_lambda_test_mode(False)
        super().tearDown()

    def test_webservice_and_indexer(self):
        if config.deployment_stage != 'prod':
            test_name = self._test_indexing()
            self._test_manifest(test_name)
            self._test_drs(test_name)
        self._test_other_endpoints()

    def _test_indexing(self) -> str:
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
        return test_name

    def _test_other_endpoints(self):
        self.check_endpoint_is_working(config.indexer_endpoint(), '/health')
        self.check_endpoint_is_working(config.service_endpoint(), '/')
        self.check_endpoint_is_working(config.service_endpoint(), '/health')
        self.check_endpoint_is_working(config.service_endpoint(), '/version')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/summary')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/files/order')

    def _test_manifest(self, test_name: str):
        manifest_filter = {"file": {"project": {"is": [test_name]}}}
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

    def _test_drs(self, test_name):
        filters = {"file": {"project": {"is": [test_name]}, "fileFormat": {"is": ["fastq.gz", "fastq"]}}}
        response = self.check_endpoint_is_working(endpoint=config.service_endpoint(),
                                                  path='/repository/files',
                                                  query={
                                                      'filters': filters,
                                                      'size': 1,
                                                      'order': 'asc',
                                                      'sort': 'fileSize'
                                                  })
        hits = json.loads(response)
        file_uuid = one(one(hits['hits'])['files'])['uuid']
        drs_endpoint = drs.drs_http_object_path(file_uuid)
        self.download_file_from_drs_response(self.check_endpoint_is_working(config.service_endpoint(), drs_endpoint))

    def set_lambda_test_mode(self, mode: bool):
        client = boto3.client('lambda')
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
        self._check_manifest(BytesIO(response), 'bundle_uuid')

    def check_bdbag(self, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            file_path = os.path.join(data_path, 'samples.tsv')
            with zip_fh.open(file_path) as file:
                self._check_manifest(file, 'entity:bundle_id')

    def _check_manifest(self, file: IO[bytes], uuid_field_name: str):
        text = TextIOWrapper(file)
        reader = csv.DictReader(text, delimiter='\t')
        rows = list(reader)
        logger.info(f'Manifest contains {len(rows)} rows.')
        self.assertGreater(len(rows), 0)
        self.assertIn(uuid_field_name, reader.fieldnames)
        bundle_uuid = rows[0][uuid_field_name]
        self.assertEqual(bundle_uuid, str(uuid.UUID(bundle_uuid)))

    def download_file_from_drs_response(self, response: bytes):
        json_data = json.loads(response)['data_object']
        file_url = first(json_data['urls'])['url']
        file_name = json_data['name']
        response = self.check_endpoint_is_working(file_url, '')
        # Check signature of FASTQ file.
        with gzip.open(BytesIO(response)) as buf:
            fastq = buf.read()
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        logger.info(f'Unzipped file {file_name} and verified it to be a FASTQ file.')
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

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
        queues = {queue_name: boto3.resource('sqs').get_queue_by_name(QueueName=queue_name)
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

    @classmethod
    def _get_file_uuid(cls, response: bytes) -> str:
        """Returns file UUID of first FASTQ file."""



class DSSIntegrationTest(unittest.TestCase):

    def test_patched_dss_client(self):
        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "term": {
                                "admin_deleted": True
                            }
                        }
                    ],
                    "must": [
                        {
                            "exists": {
                                "field": "files.project_json"
                            }
                        },
                        {
                            "range": {
                                "manifest.version": {
                                    "gte": "2019-04-01"
                                }
                            }
                        }

                    ]
                }
            }
        }
        self.maxDiff = None
        for patch in False, True:
            for replica in 'aws', 'gcp':
                with self.subTest(patch=patch, replica=replica):
                    dss_client = config.dss_client()
                    if patch:
                        patch_client_for_direct_file_access(dss_client)
                    response = dss_client.post_search(es_query=query, replica=replica, per_page=10)
                    bundle_uuid, _, bundle_version = response['results'][0]['bundle_fqid'].partition('.')
                    with mock.patch('azul.dss.logger') as log:
                        _, manifest, metadata = download_bundle_metadata(client=dss_client,
                                                                         replica=replica,
                                                                         uuid=bundle_uuid,
                                                                         version=bundle_version,
                                                                         num_workers=config.num_dss_workers)
                        self.assertGreater(len(metadata), 0)
                        self.assertGreater(set(f['name'] for f in manifest), set(metadata.keys()))
                        if patch:
                            if replica == 'aws':
                                # Extract the log method name and the first two words of log message logged
                                actual = [(m, ' '.join(a[0].split()[:2])) for m, a, k in log.mock_calls]
                                expected = [('debug', 'Loading file'), ('debug', 'Loading blob')] * len(metadata)
                                self.assertSequenceEqual(sorted(actual), sorted(expected))
                            else:
                                self.assertListEqual(log.mock_calls, [mock.call.warning(mock.ANY)] * len(metadata))
                        else:
                            self.assertListEqual(log.mock_calls, [])
