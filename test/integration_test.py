from abc import (
    ABCMeta,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
import csv
from functools import (
    lru_cache,
)
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
import threading
import time
from typing import (
    AbstractSet,
    Any,
    Dict,
    IO,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    cast,
)
import unittest
from unittest import (
    mock,
)
import uuid
from zipfile import (
    ZipFile,
)

import attr
import chalice.cli
from furl import (
    furl,
)
from google.cloud import (
    storage,
)
from google.oauth2 import (
    service_account,
)
from hca.dss import (
    DSSClient,
)
from hca.util import (
    SwaggerAPIException,
)
from humancellatlas.data.metadata.helpers.dss import (
    download_bundle_metadata,
)
from more_itertools import (
    first,
    one,
)
from openapi_spec_validator import (
    validate_spec,
)
import requests

from azul import (
    CatalogName,
    cached_property,
    config,
    drs,
)
from azul.azulclient import (
    AzulClient,
    AzulClientNotificationError,
)
from azul.drs import (
    AccessMethod,
    DRSClient,
)
import azul.dss
from azul.es import (
    ESClientFactory,
)
from azul.indexer import (
    BundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
)
from azul.plugins.repository import (
    dss,
)
from azul.portal_service import (
    PortalService,
)
from azul.requests import (
    requests_session_with_retry_after,
)
from azul.types import (
    JSON,
)
from azul_test_case import (
    AlwaysTearDownTestCase,
    AzulTestCase,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class IntegrationTestCase(AzulTestCase, metaclass=ABCMeta):
    bundle_uuid_prefix: str = ''

    @cached_property
    def azul_client(self):
        return AzulClient(prefix=self.bundle_uuid_prefix)


class IndexingIntegrationTest(IntegrationTestCase, AlwaysTearDownTestCase):
    prefix_length = 2
    max_bundles = 64
    min_timeout = 20 * 60

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.bundle_uuid_prefix = ''.join([
            str(random.choice('abcdef0123456789'))
            for _ in range(cls.prefix_length)
        ])

    def setUp(self) -> None:
        super().setUp()
        self.pruning_seed = random.randint(0, sys.maxsize)

    def test(self):

        @attr.s(auto_attribs=True, kw_only=True)
        class Catalog:
            name: CatalogName
            notifications: Mapping[BundleFQID, JSON]

            @property
            def num_bundles(self):
                return len(self.notifications)

            @property
            def bundle_fqids(self) -> AbstractSet[BundleFQID]:
                return self.notifications.keys()

            def notifications_with_duplicates(self) -> List[JSON]:
                num_duplicates = self.num_bundles // 2
                notifications = list(self.notifications.values())
                # Index some bundles again to test that we handle duplicate additions.
                # Note: random.choices() may pick the same element multiple times so
                # some notifications will end up being sent three or more times.
                notifications.extend(random.choices(notifications, k=num_duplicates))
                return notifications

        def _wait_for_indexer():
            num_bundles = sum(catalog.num_bundles for catalog in catalogs)
            self.azul_client.wait_for_indexer(num_expected_bundles=num_bundles,
                                              min_timeout=self.min_timeout)

        # For faster modify-deploy-test cycles, set `delete` to False and run
        # test once. Then also set `index` to False. Subsequent runs will use
        # catalogs from first run. Don't commit changes to these two lines.
        index = True
        delete = True

        if index:
            self._reset_indexer()

        catalogs: List[Catalog] = [
            Catalog(name=catalog, notifications=self._prepare_notifications(catalog) if index else {})
            for catalog in config.integration_test_catalogs
        ]

        if index:
            for catalog in catalogs:
                log.info('Starting integration test for catalog %r with %i bundles from prefix %r.',
                         catalog, catalog.num_bundles, self.bundle_uuid_prefix)
                self.azul_client.index(catalog=catalog.name,
                                       notifications=catalog.notifications_with_duplicates())
            _wait_for_indexer()
            for catalog in catalogs:
                self._assert_catalog_complete(catalog=catalog.name,
                                              entity_type='files',
                                              bundle_fqids=catalog.bundle_fqids)
        for catalog in catalogs:
            self._test_manifest(catalog.name)
            self._test_dos_and_drs(catalog.name)
            self._test_repository_files(catalog.name)

        if index and delete:
            for catalog in catalogs:
                self.azul_client.index(catalog=catalog.name,
                                       notifications=catalog.notifications_with_duplicates(),
                                       delete=True)
            _wait_for_indexer()
            for catalog in catalogs:
                self._assert_catalog_empty(catalog.name)

        self._test_other_endpoints()

    def _reset_indexer(self):
        # While it's OK to erase the integration test catalog, the queues are
        # shared by all catalogs and we can't afford to trash them in a stable
        # deployment like production.
        self.azul_client.reset_indexer(catalogs=config.integration_test_catalogs,
                                       # Can't purge the queues in stable deployment as
                                       # they may contain work for non-IT catalogs.
                                       purge_queues=not config.is_stable_deployment(),
                                       delete_indices=True,
                                       create_indices=True)

    def _test_other_endpoints(self):
        service_paths = (
            '/',
            '/openapi',
            '/version',
            '/index/summary',
            '/index/files/order',
        )
        service_routes = (
            (config.service_endpoint(), path)
            for path in service_paths
        )
        health_endpoints = (
            config.service_endpoint(),
            config.indexer_endpoint()
        )
        health_paths = (
            '',  # default keys for lambda
            '/',  # all keys
            '/basic',
            '/elasticsearch',
            '/queues',
            '/progress',
            '/api_endpoints',
            '/other_lambdas'
        )
        health_routes = (
            (endpoint, '/health' + path)
            for endpoint in health_endpoints
            for path in health_paths
        )
        for endpoint, path in (*service_routes, *health_routes):
            with self.subTest('other_endpoints', endpoint=endpoint, path=path):
                self._check_endpoint(endpoint, path)

    def _test_manifest(self, catalog: CatalogName):
        for format_, validator, attempts in [
            (None, self._check_manifest, 1),
            ('compact', self._check_manifest, 1),
            ('full', self._check_manifest, 3),
            ('terra.bdbag', self._check_terra_bdbag, 1)
        ]:
            with self.subTest('manifest', format=format_, attempts=attempts):
                assert attempts > 0
                params = dict(catalog=catalog)
                if format_ is not None:
                    params['format'] = format_
                for attempt in range(attempts):
                    start = time.time()
                    response = self._check_endpoint(config.service_endpoint(), '/manifest/files', params)
                    log.info('Request %i/%i took %.3fs to execute.', attempt + 1, attempts, time.time() - start)
                    validator(catalog, response)

    @lru_cache(maxsize=None)
    def _get_one_file_uuid(self, catalog: CatalogName) -> str:
        filters = {'fileFormat': {'is': ['fastq.gz', 'fastq']}}
        response = self._check_endpoint(endpoint=config.service_endpoint(),
                                        path='/index/files',
                                        query=dict(catalog=catalog,
                                                   filters=json.dumps(filters),
                                                   size=1,
                                                   order='asc',
                                                   sort='fileSize'))
        hits = json.loads(response)
        return one(one(hits['hits'])['files'])['uuid']

    def _test_dos_and_drs(self, catalog: CatalogName):
        repository_plugin = self.azul_client.repository_plugin(catalog)
        if isinstance(repository_plugin, dss.Plugin) and config.dss_direct_access:
            file_uuid = self._get_one_file_uuid(catalog)
            self._test_dos(catalog, file_uuid)
            self._test_drs(repository_plugin.drs_client(), file_uuid)

    @cached_property
    def _requests(self) -> requests.Session:
        return requests_session_with_retry_after()

    def _check_endpoint(self,
                        endpoint: str,
                        path: str,
                        query: Optional[Mapping[str, Any]] = None) -> bytes:
        query = {} if query is None else {k: str(v) for k, v in query.items()}
        url = furl(endpoint, path=path, query=query)
        return self._get_url_content(url.url)

    def _get_url_content(self, url: str) -> bytes:
        return self._get_url(url).content

    def _get_url(self, url: str, allow_redirects=True) -> requests.Response:
        log.info('GET %s', url)
        response = self._requests.get(url, allow_redirects=allow_redirects)
        expected_statuses = (200,) if allow_redirects else (200, 301, 302)
        self._assertResponseStatus(response, expected_statuses)
        return response

    def _assertResponseStatus(self,
                              response: requests.Response,
                              expected_statuses: Tuple[int, ...] = (200,)):
        self.assertIn(response.status_code,
                      expected_statuses,
                      (response.reason, response.content))

    def _check_manifest(self, _catalog: CatalogName, response: bytes):
        self.__check_manifest(BytesIO(response), 'bundle_uuid')

    def _check_terra_bdbag(self, catalog: CatalogName, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            file_path = os.path.join(data_path, 'participants.tsv')
            with zip_fh.open(file_path) as file:
                rows = self.__check_manifest(file, 'bundle_uuid')
                for row in rows:
                    # Terra doesn't allow colons in this column, but they may
                    # exist in versions indexed by TDR
                    self.assertNotIn(':', row['entity:participant_id'])

        suffix = '__file_drs_uri'
        header, *rows = rows
        prefixes = [
            c[:-len(suffix)]
            for c in header.keys()
            if c.endswith(suffix)
        ]
        size, drs_uri, name = min(
            (
                int(row[prefix + '__file_size']),
                row[prefix + suffix],
                row[prefix + '__file_name'],
            )
            for row in rows
            for prefix in prefixes
            if row[prefix + suffix]
        )
        log.info('Resolving %r (%r) from catalog %r (%i bytes)',
                 drs_uri, name, catalog, size)
        plugin = self.azul_client.repository_plugin(catalog)
        drs_client = plugin.drs_client()
        access = drs_client.get_object(drs_uri, access_method=AccessMethod.https)
        self.assertIsNone(access.headers)
        self.assertEqual('https', furl(access.url).scheme)
        # Try HEAD first because it's more efficient, fall back to GET if the
        # DRS implementations prohibits it, like Azul's DRS proxy of DSS.
        for method in ('HEAD', 'GET'):
            log.info('%s %s', method, access.url)
            # For DSS, any HTTP client should do but for TDR we need to use an
            # authenticated client. TDR does return a Bearer token in the `headers`
            # part of the DRS response but we know that this token is the same as
            # the one we're making the DRS request with.
            response = drs_client.http_client.request(method, access.url)
            if response.status != 403:
                break
        self.assertEqual(200, response.status, response.data)
        self.assertEqual(size, int(response.headers['Content-Length']))

    def __check_manifest(self, file: IO[bytes], uuid_field_name: str) -> List[Mapping[str, str]]:
        text = TextIOWrapper(file)
        reader = csv.DictReader(text, delimiter='\t')
        rows = list(reader)
        log.info(f'Manifest contains {len(rows)} rows.')
        self.assertGreater(len(rows), 0)
        self.assertIn(uuid_field_name, reader.fieldnames)
        bundle_uuid = rows[0][uuid_field_name]
        self.assertEqual(bundle_uuid, str(uuid.UUID(bundle_uuid)))
        return rows

    def _test_repository_files(self, catalog: str):
        file_uuid = self._get_one_file_uuid(catalog)
        response = self._check_endpoint(endpoint=config.service_endpoint(),
                                        path=f'/fetch/repository/files/{file_uuid}',
                                        query=dict(catalog=catalog))
        response = json.loads(response)

        while response['Status'] != 302:
            self.assertEqual(301, response['Status'])
            response = self._get_url(response['Location']).json()

        content = self._get_url_content(response['Location'])
        self._validate_fastq_content(content)

    def _test_drs(self, drs: DRSClient, file_uuid: str):
        for access_method in AccessMethod:
            with self.subTest('drs', access_method=AccessMethod.https):
                log.info('Resolving file %r with DRS using %r', file_uuid, access_method)
                drs_uri = f'drs://{config.api_lambda_domain("service")}/{file_uuid}'
                access = drs.get_object(drs_uri, access_method=access_method)
                self.assertIsNone(access.headers)
                if access.method is AccessMethod.https:
                    content = self._get_url_content(access.url)
                elif access.method is AccessMethod.gs:
                    content = self._get_gs_url_content(access.url)
                else:
                    self.fail(access_method)
                self._validate_fastq_content(content)

    def _test_dos(self, catalog: CatalogName, file_uuid: str):
        with self.subTest('dos'):
            log.info('Resolving file %s with DOS', file_uuid)
            response = self._check_endpoint(config.service_endpoint(),
                                            path=drs.dos_object_url_path(file_uuid),
                                            query=dict(catalog=catalog))
            json_data = json.loads(response)['data_object']
            file_url = first(json_data['urls'])['url']
            while True:
                response = self._get_url(file_url, allow_redirects=False)
                # We handle redirects ourselves so we can log each request
                if response.status_code in (301, 302):
                    file_url = response.headers['Location']
                    try:
                        retry_after = response.headers['Retry-After']
                    except KeyError:
                        pass
                    else:
                        time.sleep(int(retry_after))
                else:
                    break
            self._assertResponseStatus(response)
            self._validate_fastq_content(response.content)

    def _get_gs_url_content(self, url: str) -> bytes:
        self.assertTrue(url.startswith('gs://'))
        path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        credentials = service_account.Credentials.from_service_account_file(path)
        storage_client = storage.Client(credentials=credentials)
        content = BytesIO()
        storage_client.download_blob_to_file(url, content)
        return content.getvalue()

    def _validate_fastq_content(self, content: bytes):
        # Check signature of FASTQ file.
        with gzip.open(BytesIO(content)) as buf:
            fastq = buf.read(1024 * 1024)
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

    def _prepare_notifications(self, catalog: CatalogName) -> Dict[BundleFQID, JSON]:
        bundle_fqids = self.azul_client.list_bundles(catalog)
        bundle_fqids = self._prune_test_bundles(catalog, bundle_fqids, self.max_bundles)
        return {
            bundle_fqid: self.azul_client.synthesize_notification(catalog, bundle_fqid)
            for bundle_fqid in bundle_fqids
        }

    def _prune_test_bundles(self,
                            catalog: CatalogName,
                            bundle_fqids: Sequence[BundleFQID],
                            max_bundles: int
                            ) -> List[BundleFQID]:
        seed = self.pruning_seed
        log.info('Selecting %i bundles with projects, out of %i candidates, using random seed %i.',
                 max_bundles, len(bundle_fqids), seed)
        random_ = random.Random(x=seed)
        # The same seed should give same random order so we need to have a
        # deterministic order in the input list.
        bundle_fqids = sorted(bundle_fqids)
        random_.shuffle(bundle_fqids)
        # Pick bundles off of the randomly ordered input until we have the
        # desired number of bundles with project metadata.
        filtered_bundle_fqids = []
        for bundle_fqid in bundle_fqids:
            if len(filtered_bundle_fqids) < max_bundles:
                if self.azul_client.bundle_has_project_json(catalog, bundle_fqid):
                    filtered_bundle_fqids.append(bundle_fqid)
            else:
                break
        return filtered_bundle_fqids

    def _assert_catalog_complete(self,
                                 catalog: CatalogName,
                                 entity_type: str,
                                 bundle_fqids: AbstractSet[BundleFQID]) -> None:
        with self.subTest('catalog_complete', catalog=catalog):
            expected_fqids = set(self.azul_client.filter_obsolete_bundle_versions(bundle_fqids))
            obsolete_fqids = bundle_fqids - expected_fqids
            if obsolete_fqids:
                log.debug('Ignoring obsolete bundle versions %r', obsolete_fqids)
            num_bundles = len(expected_fqids)
            timeout = 600
            indexed_fqids = set()
            log.debug('Expecting bundles %s ', sorted(expected_fqids))
            retries = 0
            deadline = time.time() + timeout
            while True:
                hits = self._get_entities(catalog, entity_type)
                indexed_fqids.update(
                    BundleFQID(bundle['bundleUuid'], bundle['bundleVersion'])
                    for hit in hits
                    for bundle in hit.get('bundles', [])
                )
                log.info('Detected %i of %i bundles in %i hits for entity type %s on try #%i.',
                         len(indexed_fqids), num_bundles, len(hits), entity_type, retries)
                if len(indexed_fqids) == num_bundles:
                    log.info('Found the expected %i bundles.', num_bundles)
                    break
                elif len(indexed_fqids) > num_bundles:
                    log.error('Found %i bundles, more than the expected %i.',
                              len(indexed_fqids), num_bundles)
                    break
                elif time.time() > deadline:
                    log.error('Only found %i of %i bundles in under %i seconds.',
                              len(indexed_fqids), num_bundles, timeout)
                    break
                else:
                    retries += 1
                    time.sleep(5)
            self.assertSetEqual(indexed_fqids, expected_fqids)

    entity_types = ['files', 'projects', 'samples', 'bundles']

    def _assert_catalog_empty(self, catalog: CatalogName):
        with self.subTest('catalog_empty', catalog=catalog):
            hit_counts = {
                entity_type: len(self._get_entities(catalog, entity_type))
                for entity_type in self.entity_types
            }
            log.info('Hit counts are %r', hit_counts)
            self.assertEqual(0, sum(hit_counts.values()))

    def _get_entities(self, catalog: CatalogName, entity_type):
        entities = []
        size = 100
        params = dict(catalog=catalog,
                      size=str(size))
        url = furl(url=config.service_endpoint(),
                   path=('index', entity_type),
                   query_params=params
                   ).url
        while True:
            response = self._get_url(url)
            body = response.json()
            hits = body['hits']
            entities.extend(hits)
            url = body['pagination']['next']
            if url is None:
                break

        return entities

    def _assert_indices_exist(self, catalog: CatalogName):
        """
        Aside from checking that all indices exist this method also asserts
        that we can instantiate a local ES client pointing at a real, remote
        ES domain.
        """
        es_client = ESClientFactory.get()
        service = IndexService()
        for index_name in service.index_names(catalog):
            self.assertTrue(es_client.indices.exists(index_name))


class AzulClientIntegrationTest(IntegrationTestCase):

    def test_azul_client_error_handling(self):
        invalid_notification = {}
        notifications = [invalid_notification]
        self.assertRaises(AzulClientNotificationError,
                          self.azul_client.index,
                          first(config.integration_test_catalogs),
                          notifications)


class PortalRegistrationIntegrationTest(IntegrationTestCase):

    @unittest.skipIf(config.is_main_deployment(), 'Test would pollute portal DB')
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


class OpenAPIIntegrationTest(AzulTestCase):

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


class DSSIntegrationTest(AzulTestCase):

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

    def _test_dss_client(self, direct: bool, query: JSON, dss_client: DSSClient, replica: str, fallback: bool):
        with self.subTest(direct=direct, replica=replica, fallback=fallback):
            response = dss_client.post_search(es_query=query, replica=replica, per_page=10)
            bundle_uuid, _, bundle_version = response['results'][0]['bundle_fqid'].partition('.')
            with mock.patch('azul.dss.logger') as captured_log:
                _, manifest, metadata = download_bundle_metadata(client=dss_client,
                                                                 replica=replica,
                                                                 uuid=bundle_uuid,
                                                                 version=bundle_version,
                                                                 num_workers=config.num_repo_workers)
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


class AzulChaliceLocalIntegrationTest(AzulTestCase):
    url = furl(scheme='http', host='127.0.0.1', port=8000)
    server = None
    server_thread = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        app_module = load_app_module('service')
        app_dir = os.path.dirname(app_module.__file__)
        factory = chalice.cli.factory.CLIFactory(app_dir)
        config = factory.create_config_obj()
        cls.server = factory.create_local_server(app_obj=app_module.app,
                                                 config=config,
                                                 host=cls.url.host,
                                                 port=cls.url.port)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server_thread.join()
        super().tearDownClass()

    def test_local_chalice_health_endpoint(self):
        url = self.url.copy().set(path='health').url
        response = requests.get(url)
        self.assertEqual(200, response.status_code)

    catalog = first(config.integration_test_catalogs.keys())

    def test_local_chalice_index_endpoints(self):
        url = self.url.copy().set(path='index/files',
                                  query=dict(catalog=self.catalog)).url
        response = requests.get(url)
        self.assertEqual(200, response.status_code)

    def test_local_filtered_index_endpoints(self):
        filters = {'genusSpecies': {'is': ['Homo sapiens']}}
        url = self.url.copy().set(path='index/files',
                                  query=dict(filters=json.dumps(filters),
                                             catalog=self.catalog)).url
        response = requests.get(url)
        self.assertEqual(200, response.status_code)
