from concurrent.futures.thread import ThreadPoolExecutor
import csv
import gzip
from io import (
    BytesIO,
    TextIOWrapper,
)
import json
import logging
import os
import random
import re
import sys
import time
from typing import (
    IO,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    cast,
)
import unittest
from unittest import mock
import uuid
from zipfile import ZipFile

from boltons.cacheutils import cachedproperty
import boto3
from furl import furl
from hca.util import SwaggerAPIException
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata
from more_itertools import (
    first,
    one,
)
from openapi_spec_validator import validate_spec
import requests

from azul import (
    config,
    drs,
)
from azul.azulclient import (
    AzulClient,
    AzulClientNotificationError,
    FQID,
)
import azul.dss
from azul.drs import (
    drs_http_object_path,
    Client,
)
from azul.logging import configure_test_logging
from azul.portal_service import PortalService
from azul.queues import Queues
from azul.requests import requests_session_with_retry_after
from azul.types import (
    JSON,
)
from azul_test_case import AlwaysTearDownTestCase

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class IntegrationTest(AlwaysTearDownTestCase):
    """
    The integration test first kicks the indexer into test mode by setting some environment variables in the indexer
    Lambda, instrumenting the production code in the indexer, causing it to process incoming notifications slightly
    differently. In test mode, the indexer rejects organic notifications originating from the DSS, causing them to be
    retried later. Only notifications sent by the integration test will be handled during test mode. When handling a
    test notification, the indexer loads the real bundle referenced by the notification and rewrites the metadata
    files in the bundle as if the bundle had a different UUID, a current version and belonged to a virtual test
    project.

    Next, the integration test then queries DSS for all of bundles matching a certain randomly chosen UUID prefix and
    sends a notification for each of these bundles. This and the indexer instrumentation effectively create a copy of
    the bundles in the prefix, but each copy with a new and different bundle FQID and all copies sharing a virtual
    project with a new and different project UUID. Note that unlike real projects, the virtual project will contain a
    mix of metadata graph shapes since the bundles that contribute to it are selected randomly.

    The UUIDs for these new bundles are tracked within the integration test and are used to assert that all bundles
    are indexed.

    Finally, the tests sends deletion notifications for these test bundles. The test then asserts that this removed
    every trace of the test bundles from the index.

    The metadata structure created by the instrumented production code is as follows:

    ┌──────────────────────┐
    │     Test Project     │          ┌────────────────┐
    │     title = test     │          │    Project     │
    │   document_id = b    │          │  title = foo   │
    │(copy of project with │          │document_id = a │
    │   document_id = a)   │          └────────────────┘
    └──────────────────────┘                   │
                │                              │
                │                     ┌────────┴────────┐
                │                     │                 │
                ▼                     │                 │
     ┌─────────────────────┐          ▼                 ▼
     │     Test Bundle     │  ┌──────────────┐  ┌───────────────┐
     │      uuid = e       │  │    Bundle    │  │    Bundle     │
     │    version = 100    │  │   uuid = c   │  │   uuid = d    │
     │(copy of bundle with │  │ version = 1  │  │  version = 2  │
     │      uuid = 3)      │  └──────────────┘  └───────────────┘
     └─────────────────────┘          ▲                 ▲
                ▲                     │                 │
                │                     │                 │
                │              ┌────────────┐           │
                │              │    File    │           │
                └──────────────│  uuid = f  │───────────┘
                               └────────────┘

    However, it is important to note that because the selection of test bundles is random, so will be the selection
    of project_0.json files in those bundles. Since each original project is mapped to the same test project UUID,
    the test project will have contributions from a diverse set of file project_0.json files.

    The same applies to other types of entities that are shared between bundles.
    """
    prefix_length = 2
    max_bundles = 64

    def setUp(self):
        super().setUp()
        self.bundle_uuid_prefix = ''.join([str(random.choice('abcdef0123456789')) for _ in range(self.prefix_length)])
        self.expected_fqids = set()
        self.test_notifications = set()
        self.num_bundles = 0
        self.azul_client = AzulClient(indexer_url=config.indexer_endpoint(),
                                      prefix=self.bundle_uuid_prefix)
        self.queues = Queues()
        self.test_uuid = str(uuid.uuid4())
        self.test_name = f'integration-test_{self.test_uuid}_{self.bundle_uuid_prefix}'
        self.num_bundles = 0
        self._set_test_mode(True)

    def tearDown(self):
        self._set_test_mode(False)
        super().tearDown()

    def test(self):
        if config.deployment_stage != 'prod':
            try:
                self._test_indexing()
                self._test_manifest()
                if config.dss_direct_access:
                    self._test_dos_and_drs()
            finally:
                self._delete_bundles_twice()
                self.assertTrue(self._project_removed_from_index())
        self._test_other_endpoints()

    def _test_indexing(self):
        log.info('Starting test using test name, %s ...', self.test_name)
        azul_client = self.azul_client
        self.test_notifications, self.expected_fqids = self._test_notifications(test_name=self.test_name,
                                                                                test_uuid=self.test_uuid,
                                                                                max_bundles=self.max_bundles)
        self.num_bundles = len(self.expected_fqids)
        self.queues.wait_for_queue_level(empty=True, num_bundles=self.num_bundles)
        azul_client._index(self.test_notifications)
        # Index some bundles again to test that we handle duplicate additions.
        # Note: random.choices() may pick the same element multiple times so
        # some notifications will end up being sent three or more times.
        azul_client._index(random.choices(self.test_notifications, k=len(self.test_notifications) // 2))
        self._check_bundles_are_indexed(self.test_name, 'files')

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
                self._check_endpoint(endpoint, '/health' + health_key)
        self._check_endpoint(config.service_endpoint(), '/')
        self._check_endpoint(config.service_endpoint(), '/openapi')
        self._check_endpoint(config.service_endpoint(), '/version')
        self._check_endpoint(config.service_endpoint(), '/repository/summary')
        self._check_endpoint(config.service_endpoint(), '/repository/files/order')

    def _test_manifest(self):
        manifest_filter = json.dumps({'project': {'is': [self.test_name]}})
        for format_, validator in [
            (None, self._check_manifest),
            ('compact', self._check_manifest),
            ('full', self._check_manifest),
            ('terra.bdbag', self._check_terra_bdbag)
        ]:
            with self.subTest(format=format_, filter=manifest_filter):
                query = {'filters': manifest_filter}
                if format_ is not None:
                    query['format'] = format_
                start_request = time.time()
                response = self._check_endpoint(config.service_endpoint(), '/manifest/files', query)
                if format_ == 'full':
                    log.info('First `full` request took %s to execute.', time.time() - start_request)
                    start_request = time.time()
                    self._check_endpoint(config.service_endpoint(), '/manifest/files', query)
                    log.info('Second `full` request took %s to execute.', time.time() - start_request)
                    start_request = time.time()
                    self._check_endpoint(config.service_endpoint(), '/manifest/files', query)
                    log.info('Third `full` request took %s to execute.', time.time() - start_request)
                validator(response)

    def _test_dos_and_drs(self):
        filters = json.dumps({'project': {'is': [self.test_name]}, 'fileFormat': {'is': ['fastq.gz', 'fastq']}})
        response = self._check_endpoint(endpoint=config.service_endpoint(),
                                        path='/repository/files',
                                        query={
                                            'filters': filters,
                                            'size': 1,
                                            'order': 'asc',
                                            'sort': 'fileSize'
                                        })
        hits = json.loads(response)
        file_uuid = one(one(hits['hits'])['files'])['uuid']
        self._download_with_dos(file_uuid)
        self._download_with_drs(file_uuid)

    def _delete_bundles_twice(self):
        self._delete_bundles(self.test_notifications)
        # Delete some bundles again to test that we handle duplicate deletions.
        # Note: random.choices() may pick the same element multiple times so
        # some notifications will end up being sent three or more times.
        notifications = random.choices(self.test_notifications, k=len(self.test_notifications) // 2)
        self._delete_bundles(notifications)

    def _delete_bundles(self, notifications):
        if notifications:
            self.azul_client.delete_notification(notifications)
        self.queues.wait_for_queue_level(empty=False, num_bundles=self.num_bundles)
        self.queues.wait_for_queue_level(empty=True, num_bundles=self.num_bundles)

    def _set_test_mode(self, mode: bool):
        client = boto3.client('lambda')
        function_name = config.indexer_name
        indexer_lambda_config = client.get_function_configuration(FunctionName=function_name)
        environment = indexer_lambda_config['Environment']
        env_var_name = 'AZUL_TEST_MODE'
        env_var_value = '1' if mode else '0'
        environment['Variables'][env_var_name] = env_var_value
        log.info('Setting environment variable %s to "%s" on function %s',
                 env_var_name, env_var_value, function_name)
        client.update_function_configuration(FunctionName=function_name, Environment=environment)

    @cachedproperty
    def _requests(self) -> requests.Session:
        return requests_session_with_retry_after()

    def _check_endpoint(self, endpoint: str, path: str, query: Optional[Mapping[str, str]] = None) -> bytes:
        url = furl(endpoint)
        url.path.add(path)
        if query is not None:
            url.query.set({k: str(v) for k, v in query.items()})
        log.info('Requesting %s', url)
        response = self._requests.get(url.url)
        response.raise_for_status()
        return response.content

    def _check_manifest(self, response: bytes):
        self.__check_manifest(BytesIO(response), 'bundle_uuid')

    def _check_terra_bdbag(self, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            file_path = os.path.join(data_path, 'participants.tsv')
            with zip_fh.open(file_path) as file:
                self.__check_manifest(file, 'bundle_uuid')

    def __check_manifest(self, file: IO[bytes], uuid_field_name: str):
        text = TextIOWrapper(file)
        reader = csv.DictReader(text, delimiter='\t')
        rows = list(reader)
        log.info(f'Manifest contains {len(rows)} rows.')
        self.assertGreater(len(rows), 0)
        self.assertIn(uuid_field_name, reader.fieldnames)
        bundle_uuid = rows[0][uuid_field_name]
        self.assertEqual(bundle_uuid, str(uuid.UUID(bundle_uuid)))

    def _download_with_drs(self, file_uuid: str):
        base_url = config.service_endpoint() + drs_http_object_path('')
        client = Client(base_url)
        response = client.get_object(file_uuid)
        self._validate_fastq_response_content(file_uuid, response.content)
        log.info('Successfully downloaded file %s with DRS', file_uuid)

    def _download_with_dos(self, file_uuid: str):
        dos_endpoint = drs.dos_http_object_path(file_uuid)
        response = self._check_endpoint(config.service_endpoint(), dos_endpoint)
        json_data = json.loads(response)['data_object']
        file_url = first(json_data['urls'])['url']
        response = self._check_endpoint(file_url, '')
        self._validate_fastq_response_content(file_uuid, response)
        log.info('Successfully downloaded file %s with DOS', file_uuid)

    def _validate_fastq_response_content(self, file_uuid, response):
        # Check signature of FASTQ file.
        with gzip.open(BytesIO(response)) as buf:
            fastq = buf.read()
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        log.info(f'Unzipped file {file_uuid} and verified it to be a FASTQ file.')
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

    def _test_notifications(self, test_name: str, test_uuid: str, max_bundles: int) -> Tuple[List[JSON], Set[FQID]]:
        bundle_fqids = self.azul_client.list_dss_bundles()
        bundle_fqids = self._prune_test_bundles(bundle_fqids, max_bundles)
        notifications = []
        test_bundle_fqids = set()
        for bundle_fqid in bundle_fqids:
            new_bundle_uuid = str(uuid.uuid4())
            # Using a new version helps ensure that any aggregation will choose
            # the contribution from the test bundle over that by any other
            # bundle not in the test set.
            # Failing to do this before caused https://github.com/DataBiosphere/azul/issues/1174
            new_bundle_version = azul.dss.new_version()
            test_bundle_fqids.add((new_bundle_uuid, new_bundle_version))
            notification = self.azul_client.synthesize_notification(bundle_fqid,
                                                                    test_bundle_uuid=new_bundle_uuid,
                                                                    test_bundle_version=new_bundle_version,
                                                                    test_name=test_name,
                                                                    test_uuid=test_uuid)
            notifications.append(notification)
        return notifications, test_bundle_fqids

    def _prune_test_bundles(self, bundle_fqids, max_bundles):
        filtered_bundle_fqids = []
        seed = random.randint(0, sys.maxsize)
        log.info('Selecting %i out of %i candidate bundle(s) with random seed %i.',
                 max_bundles, len(bundle_fqids), seed)
        random_ = random.Random(x=seed)
        bundle_fqids = random_.sample(bundle_fqids, len(bundle_fqids))
        for bundle_uuid, bundle_version in bundle_fqids:
            if len(filtered_bundle_fqids) < max_bundles:
                if self.azul_client.bundle_has_project_json(bundle_uuid, bundle_version):
                    filtered_bundle_fqids.append((bundle_uuid, bundle_version))
            else:
                break
        return filtered_bundle_fqids

    def _check_bundles_are_indexed(self, test_name: str, entity_type: str):
        service_check_timeout = 600
        delay_between_retries = 5
        indexed_fqids = set()

        num_bundles = len(self.expected_fqids)
        log.info('Starting integration test %s with the prefix %s for the entity type %s. Expected %i bundle(s).',
                 test_name, self.bundle_uuid_prefix, entity_type, num_bundles)
        log.debug('Expected bundles %s ', sorted(self.expected_fqids))
        self.queues.wait_for_queue_level(empty=False, num_bundles=self.num_bundles)
        self.queues.wait_for_queue_level(empty=True, num_bundles=self.num_bundles)
        log.info('Checking if bundles are referenced by the service response ...')
        retries = 0
        deadline = time.time() + service_check_timeout

        while True:
            hits = self._get_entities_by_project(entity_type, test_name)
            indexed_fqids.update(
                (entity['bundleUuid'], entity['bundleVersion'])
                for hit in hits
                for entity in hit.get('bundles', [])
                if (entity['bundleUuid'], entity['bundleVersion']) in self.expected_fqids
            )
            log.info('Found %i/%i bundles on try #%i. There are %i files with the project name.',
                     len(indexed_fqids), num_bundles, retries + 1, len(hits))

            if indexed_fqids == self.expected_fqids:
                log.info('Found all bundles.')
                break
            elif time.time() > deadline:
                log.error('Unable to find all the bundles in under %i seconds.', service_check_timeout)
                break
            else:
                time.sleep(delay_between_retries)
                retries += 1

        log.info('Actual bundle count is %i.', len(indexed_fqids))
        self.assertEqual(indexed_fqids, self.expected_fqids)
        for hit in hits:
            project = one(hit['projects'])
            bundle_fqids = [bundle['bundleUuid'] + '.' + bundle['bundleVersion'] for bundle in hit['bundles']]
            self.assertTrue(test_name in project['projectShortname'],
                            f'There was a problem during indexing an {entity_type} entity'
                            f' {hit["entryId"]}. Bundle(s) ({",".join(bundle_fqids)})'
                            f' have been indexed without the debug project name. Contains {project}')

    def _project_removed_from_index(self):
        results_empty = [len(self._get_entities_by_project(entity, self.test_name)) == 0
                         for entity in ['files', 'projects', 'samples', 'bundles']]
        log.info('Project removed from index files: %d, projects: %d, '
                 'specimens: %d, bundles: %d', *results_empty)
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
            response = self._requests.get(url, params=params)
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

    def test_azul_client_error_handling(self):
        invalid_notification = {}
        notifications = [invalid_notification]
        self.assertRaises(AzulClientNotificationError, self.azul_client._index, notifications)

    @unittest.skipIf(config.is_main_deployment, 'Test would pollute portal DB')
    def test_concurrent_portal_db_crud(self):
        """
        Use multithreading to simulate multiple users simultaneously modifying
        the portals database.
        """

        # Currently takes about 50 seconds and creates a 25 kb db file.
        n_threads = 10
        n_tasks = n_threads * 10
        n_ops = 5
        portal_service = PortalService()

        entry_format = 'task={};op={}'

        def run(thread_count):
            for op_count in range(n_ops):
                mock_entry = cast(JSON, {
                    "portal_id": "foo",
                    "integrations": [
                        {
                            "integration_id": "bar",
                            "entity_type": "project",
                            "integration_type": "get",
                            "entity_ids": ["baz"]
                        }
                    ],
                    "mock-count": entry_format.format(thread_count, op_count)
                })
                portal_service._crud(lambda db: list(db) + [mock_entry])

        old_db = portal_service.read()

        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [executor.submit(run, i) for i in range(n_tasks)]

        self.assertTrue(all(f.result() is None for f in futures))

        new_db = portal_service.read()

        old_entries = [portal for portal in new_db if 'mock-count' not in portal]
        self.assertEqual(old_entries, old_db)
        mock_counts = [portal['mock-count'] for portal in new_db if 'mock-count' in portal]
        self.assertEqual(len(mock_counts), len(set(mock_counts)))
        self.assertEqual(set(mock_counts), {entry_format.format(i, j) for i in range(n_tasks) for j in range(n_ops)})

        # Reset to pre-test state.
        portal_service.overwrite(old_db)


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
        for direct in {config.dss_direct_access, False}:
            for replica in 'aws', 'gcp':
                if direct:
                    with self._failing_s3_get_object():
                        dss_client = azul.dss.direct_access_client()
                        self._test_dss_client(direct, query, dss_client, replica, fallback=True)
                    dss_client = azul.dss.direct_access_client()
                    self._test_dss_client(direct, query, dss_client, replica, fallback=False)
                else:
                    dss_client = azul.dss.client()
                    self._test_dss_client(direct, query, dss_client, replica, fallback=False)

    class SpecialError(Exception):
        pass

    def _failing_s3_get_object(self):
        def make_mock(**kwargs):
            original = kwargs['spec']

            def mock_boto3_client(service, *args, **kwargs):
                if service == 's3':
                    mock_s3 = mock.MagicMock()
                    mock_s3.get_object.side_effect = self.SpecialError()
                    return mock_s3
                else:
                    return original(service, *args, **kwargs)

            return mock_boto3_client

        return mock.patch('azul.deployment.aws.client', spec=True, new_callable=make_mock)

    def _test_dss_client(self, direct, query, dss_client, replica, fallback):
        with self.subTest(direct=direct, replica=replica, fallback=fallback):
            response = dss_client.post_search(es_query=query, replica=replica, per_page=10)
            bundle_uuid, _, bundle_version = response['results'][0]['bundle_fqid'].partition('.')
            with mock.patch('azul.dss.logger') as captured_log:
                _, manifest, metadata = download_bundle_metadata(client=dss_client,
                                                                 replica=replica,
                                                                 uuid=bundle_uuid,
                                                                 version=bundle_version,
                                                                 num_workers=config.num_dss_workers)
            log.info('Captured log calls: %r', captured_log.mock_calls)
            self.assertGreater(len(metadata), 0)
            self.assertGreater(set(f['name'] for f in manifest), set(metadata.keys()))
            for f in manifest:
                self.assertIn('s3_etag', f)
            # Extract the log method name and the first three words of log
            # message logged. Note that the PyCharm debugger will call
            # certain dunder methods on the variable, leading to failed
            # assertions.
            actual = [(m, ' '.join(re.split(r'[\s,]', a[0])[:3])) for m, a, k in captured_log.mock_calls]
            if direct:
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
        for direct in {config.dss_direct_access, False}:
            with self.subTest(direct=direct):
                dss_client = azul.dss.direct_access_client() if direct else azul.dss.client()
                with self.assertRaises(SwaggerAPIException) as e:
                    dss_client.get_file(uuid='acafefed-beef-4bad-babe-feedfa11afe1',
                                        version='2018-11-19T232756.056947Z',
                                        replica='aws')
                self.assertEqual(e.exception.reason, 'not_found')

    def test_mini_dss_failures(self):
        uuid = 'acafefed-beef-4bad-babe-feedfa11afe1'
        version = '2018-11-19T232756.056947Z'
        with self._failing_s3_get_object():
            mini_dss = azul.dss.MiniDSS(config.dss_endpoint)
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
