import unittest
from collections import deque
from contextlib import contextmanager
import csv
import gzip
from io import BytesIO, TextIOWrapper
import json
import logging
import os
import random
import re
import time
from typing import Any, IO, Mapping, Optional
from unittest import mock
from urllib.parse import urlencode
import uuid
from zipfile import ZipFile

import boto3
from furl import furl
from hca.util import SwaggerAPIException
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from more_itertools import first, one
from openapi_spec_validator import validate_spec
import requests
from requests import HTTPError

from azul import config, drs
from azul.azulclient import AzulClient
from azul.decorators import memoized_property
from azul.dss import MiniDSS, patch_client_for_direct_access
from azul.logging import configure_test_logging
from azul.requests import requests_session
from azul import drs
from azul_test_case import AlwaysTearDownTestCase

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(logger)


class IntegrationTest(AlwaysTearDownTestCase):
    """
    The integration tests work by first setting the lambdas in test mode (this happens in setUp). This
    Sets some environment variable in the indexer lambda that change its behavior slightly, allowing
    it to process modified test notifications differently.

    Next, the test queries Azul for all of the bundles that match a certain prefix. In
    AzulClient.reindex() these bundle are used to construct fake notifications. These fake notifications
    Have an extra information embedded. They include:
     - the name of the integration test run,
     - a new UUID for the project, and
     - a new unique bundle UUID.
    When the bundle is processed by the indexer lambda the original bundle uuid is used to fetch the
    metadata, etc., but these new fields are what's actually stored in Azul. This effectively creates a
    copy of the bundles, but each copy with a new and different bundle UUID and all copies sharing a
    synthesized project with a different project UUID. Note that unlike real projects, the shared project
    will contain a mix of metadata graph shapes since the bundles that contribute to it are selected
    randomly.

    The UUIDs for these new bundles are used within the integration tests to assert that everything is
    indexed. Health endpoints etc. are also checked.

    Finally, the tests send deletion notifications for these new bundle UUIDs which should remove all
    trace of the integration test from the index.
    """
    prefix_length = 1 if config.dss_deployment_stage == 'integration' else 3

    def setUp(self):
        super().setUp()
        self.bundle_uuid_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(self.prefix_length)])
        self.expected_fqids = set()
        self.test_notifications = set()
        self.num_bundles = 0
        self.azul_client = AzulClient(indexer_url=config.indexer_endpoint(),
                                      prefix=self.bundle_uuid_prefix)
        self.test_uuid = str(uuid.uuid4())
        self.test_name = f'integration-test_{self.test_uuid}_{self.bundle_uuid_prefix}'
        self.num_bundles = 0
        self.set_lambda_test_mode(True)

    def tearDown(self):
        self.set_lambda_test_mode(False)
        self.delete_bundles()
        # Delete again to test duplicate deletion notifications
        self.delete_bundles(duplicates=True)
        super().tearDown()

    def test_webservice_and_indexer(self):
        if config.deployment_stage != 'prod':
            self._test_indexing()
            self._test_manifest()
            self._test_drs()
        self._test_other_endpoints()

    def _test_indexing(self):
        logger.info('Starting test using test name, %s ...', self.test_name)
        azul_client = AzulClient(indexer_url=config.indexer_endpoint(),
                                 prefix=self.bundle_uuid_prefix)
        logger.info('Creating indices and reindexing ...')
        self.test_notifications, self.expected_fqids = azul_client.test_notifications(self.test_name, self.test_uuid)
        azul_client._index(self.test_notifications)
        # Index some again to test that we can handle duplicate notifications. Note: choices are with replacement
        azul_client._index(random.choices(self.test_notifications, k=len(self.test_notifications) // 2))
        self.num_bundles = len(self.expected_fqids)
        self.check_bundles_are_indexed(self.test_name, 'files')

    def _test_other_endpoints(self):
        for health_key in ('',  # default keys for lambda
                           '/',  # all keys
                           '/basic',
                           '/elasticsearch',
                           '/queues',
                           '/progress',
                           '/api_endpoints',
                           '/other_lambdas'):
            for endpoint in config.service_endpoint(), config.indexer_endpoint():
                self.check_endpoint_is_working(endpoint, '/health' + health_key)
        self.check_endpoint_is_working(config.service_endpoint(), '/')
        self.check_endpoint_is_working(config.service_endpoint(), '/openapi')
        self.check_endpoint_is_working(config.service_endpoint(), '/version')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/summary')
        self.check_endpoint_is_working(config.service_endpoint(), '/repository/files/order')

    def _test_manifest(self):
        manifest_filter = json.dumps({'project': {'is': [self.test_name]}})
        for format_, validator in [
            (None, self.check_manifest),
            ('tsv', self.check_manifest),
            ('compact', self.check_manifest),
            ('full', self.check_manifest),
            ('bdbag', self.check_terra_bdbag),
            ('terra.bdbag', self.check_terra_bdbag)
        ]:
            with self.subTest(format=format_, filter=manifest_filter):
                query = {'filters': manifest_filter}
                if format_ is not None:
                    query['format'] = format_
                response = self.check_endpoint_is_working(config.service_endpoint(), '/manifest/files', query)
                validator(response)

    def _test_drs(self):
        filters = json.dumps({'project': {'is': [self.test_name]}, 'fileFormat': {'is': ['fastq.gz', 'fastq']}})
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

    def delete_bundles(self, duplicates=False):
        if duplicates:
            # Note: random.choices is with replacement (so the same choice may be made several times
            notifications = random.choices(self.test_notifications, k=len(self.test_notifications) // 2)
        else:
            notifications = self.test_notifications
        for n in notifications:
            try:
                self.azul_client.delete_notification(n)
            except HTTPError as e:
                logger.warning('Deletion for notification %s failed. Possibly it was never indexed.', n, exc_info=e)
        self.wait_for_queue_level(empty=False)
        self.wait_for_queue_level(timeout=self.get_queue_empty_timeout(self.num_bundles))
        self.assertTrue(self.project_removed_from_azul(),
                        f"Project '{self.test_name}' was not fully removed from index within 5 min. of deletion")

    def set_lambda_test_mode(self, mode: bool):
        client = boto3.client('lambda')
        indexer_lambda_config = client.get_function_configuration(FunctionName=config.indexer_name)
        environment = indexer_lambda_config['Environment']
        environment['Variables']['AZUL_TEST_MODE'] = '1' if mode else '0'
        client.update_function_configuration(FunctionName=config.indexer_name, Environment=environment)

    @memoized_property
    def requests(self) -> requests.Session:
        return requests_session()

    def check_endpoint_is_working(self, endpoint: str, path: str, query: Optional[Mapping[str, str]] = None) -> bytes:
        url = furl(endpoint)
        url.path.add(path)
        query = query or {}
        logger.info('Requesting %s?%s', url.url, urlencode(query))
        response = self.requests.get(url.url, params=query)
        response.raise_for_status()
        return response.content

    def check_manifest(self, response: bytes):
        self._check_manifest(BytesIO(response), 'bundle_uuid')

    def check_terra_bdbag(self, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            file_path = os.path.join(data_path, 'participants.tsv')
            with zip_fh.open(file_path) as file:
                self._check_manifest(file, 'bundle_uuid')

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
                logger.info('The queue(s) are at the desired level.')
                break
            else:
                logger.info('The most recently sampled queue sizes are %r.', queue_size_history)
            time.sleep(5)

        # Hack that removes the ResourceWarning that is caused by an unclosed SQS session
        for queue in queues.values():
            queue.meta.client._endpoint.http_session.close()

    def check_bundles_are_indexed(self, test_name: str, entity_type: str):
        service_check_timeout = 600
        delay_between_retries = 5
        indexed_fqids = set()

        num_bundles = len(self.expected_fqids)
        logger.info('Starting integration test %s with the prefix %s for the entity type %s. Expected %i bundle(s).',
                    test_name, self.bundle_uuid_prefix, entity_type, num_bundles)
        logger.debug('Expected bundles %s ', sorted(self.expected_fqids))
        logger.info('Waiting for the queues to empty ...')
        self.wait_for_queue_level(empty=False)
        self.wait_for_queue_level(timeout=self.get_queue_empty_timeout(self.num_bundles))
        logger.info('Checking if bundles are referenced by the service response ...')
        retries = 0
        deadline = time.time() + service_check_timeout

        while True:
            hits = self._get_entities_by_project(entity_type, test_name)
            indexed_fqids.update({
                f"{entity['bundleUuid']}.{entity['bundleVersion']}"
                for hit in hits
                for entity in hit.get('bundles', [])
                if f"{entity['bundleUuid']}.{entity['bundleVersion']}" in self.expected_fqids
            })
            logger.info('Found %i/%i bundles on try #%i. There are %i files with the project name.',
                        len(indexed_fqids), num_bundles, retries + 1, len(hits))

            if indexed_fqids == self.expected_fqids:
                logger.info('Found all bundles.')
                break
            elif time.time() > deadline:
                logger.error('Unable to find all the bundles in under %i seconds.', service_check_timeout)
                break
            else:
                time.sleep(delay_between_retries)
                retries += 1

        logger.info('Actual bundle count is %i.', len(indexed_fqids))
        self.assertEqual(indexed_fqids, self.expected_fqids)
        for hit in hits:
            project = one(hit['projects'])
            bundle_fqids = [bundle['bundleUuid'] + '.' + bundle['bundleVersion'] for bundle in hit['bundles']]
            self.assertTrue(test_name in project['projectShortname'],
                            f'There was a problem during indexing an {entity_type} entity'
                            f' {hit["entryId"]}. Bundle(s) ({",".join(bundle_fqids)})'
                            f' have been indexed without the debug project name. Contains {project}')

    def project_removed_from_azul(self):
        results_empty = [len(self._get_entities_by_project(entity, self.test_name)) == 0
                         for entity in ['files', 'projects', 'samples', 'bundles']]
        logger.info("Project removed from index files: {}, projects: {}, "
                    "specimens: {}, bundles: {}".format(*results_empty))
        return all(results_empty)

    def _get_entities_by_project(self, entity_type, project_short_name):
        """
        Returns all entities of a given type in a given project.
        """
        filters = json.dumps({'project': {'is': [project_short_name]}})
        entities = []
        size = 100
        params = dict(filters=filters, size=str(size))
        while True:
            url = f'{config.service_endpoint()}/repository/{entity_type}'
            response = self.requests.get(url, params=params)
            response.raise_for_status()
            body = response.json()
            hits = body['hits']
            entities.extend(hits)
            pagination = body['pagination']
            search_after = pagination['search_after']
            if search_after is None:
                break
            params['search_after'] = search_after
            params['search_after_uid'] = pagination['search_after_uid']
        return entities


class OpenAPIIntegrationTest(unittest.TestCase):

    def test_openapi(self):
        service = config.service_endpoint()
        response = requests.get(service + '/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'text/html')
        self.assertGreater(len(response.content), 0)
        # validate OpenAPI spec
        response = requests.get(service + '/openapi')
        response.raise_for_status()
        spec = response.json()
        validate_spec(spec)


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
                if patch:
                    with self._failing_s3_get_object():
                        dss_client = config.dss_client()
                        patch_client_for_direct_access(dss_client)
                        self._test_patched_client(patch, query, dss_client, replica, fallback=True)
                    dss_client = config.dss_client()
                    patch_client_for_direct_access(dss_client)
                    self._test_patched_client(patch, query, dss_client, replica, fallback=False)
                else:
                    dss_client = config.dss_client()
                    self._test_patched_client(patch, query, dss_client, replica, fallback=False)

    class SpecialError(Exception):
        pass

    @contextmanager
    def _failing_s3_get_object(self):
        with mock.patch('boto3.client') as mock_client:
            mock_s3 = mock.MagicMock()
            mock_s3.get_object.side_effect = self.SpecialError()
            mock_client.return_value = mock_s3
            yield

    def _test_patched_client(self, patch, query, dss_client, replica, fallback):
        with self.subTest(patch=patch, replica=replica, fallback=fallback):
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
                for f in manifest:
                    self.assertIn('s3_etag', f)
                # Extract the log method name and the first three words of log message logged
                # Note that the PyCharm debugger will call certain dunder methods on the variable, leading to failed
                actual = [(m, ' '.join(re.split(r'[\s,]', a[0])[:3])) for m, a, k in log.mock_calls]
                if patch:
                    if replica == 'aws':
                        if fallback:
                            expected = [
                                           ('debug', 'Loading bundle %s'),
                                           ('debug', 'Loading object %s'),
                                           ('warning', 'Error accessing bundle'),
                                           ('warning', 'Failed getting bundle')
                                       ] + [
                                           ('debug', 'Loading file %s'),
                                           ('debug', 'Loading object %s'),
                                           ('warning', 'Error accessing file'),
                                           ('warning', 'Failed getting file')
                                       ] * len(metadata)
                        else:
                            expected = [
                                           ('debug', 'Loading bundle %s'),
                                           ('debug', 'Loading object %s')
                                       ] + [
                                           ('debug', 'Loading file %s'),
                                           ('debug', 'Loading object %s'),  # file
                                           ('debug', 'Loading object %s')  # blob
                                       ] * len(metadata)

                    else:
                        # On `gcp` the precondition check fails right away, preventing any attempts of direct access
                        expected = [
                                       ('warning', 'Failed getting bundle')
                                   ] + [
                                       ('warning', 'Failed getting file')
                                   ] * len(metadata)
                else:
                    expected = []
                self.assertSequenceEqual(sorted(expected), sorted(actual))

    def test_get_file_fail(self):
        for patch in True, False:
            with self.subTest(path=patch):
                dss_client = config.dss_client()
                if patch:
                    patch_client_for_direct_access(dss_client)
                with self.assertRaises(SwaggerAPIException) as e:
                    dss_client.get_file(uuid='acafefed-beef-4bad-babe-feedfa11afe1',
                                        version='2018-11-19T232756.056947Z',
                                        replica='aws')
                self.assertEqual(e.exception.reason, 'not_found')

    def test_mini_dss_failures(self):
        uuid = 'acafefed-beef-4bad-babe-feedfa11afe1'
        version = '2018-11-19T232756.056947Z'
        with self._failing_s3_get_object():
            mini_dss = MiniDSS()
            with self.assertRaises(self.SpecialError):
                mini_dss._get_file_object(uuid, version)
            with self.assertRaises(KeyError):
                mini_dss._get_blob_key({})
            with self.assertRaises(self.SpecialError):
                mini_dss._get_blob('/blobs/foo', {'content-type': 'application/json'})
            with self.assertRaises(self.SpecialError):
                mini_dss.get_bundle(uuid, version, 'aws')
            with self.assertRaises(self.SpecialError):
                mini_dss.get_file(uuid, version, 'aws')
            with self.assertRaises(self.SpecialError):
                mini_dss.get_native_file_url(uuid, version, 'aws')
