from abc import (
    ABCMeta,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
from contextlib import (
    contextmanager,
)
import csv
import gzip
from io import (
    BytesIO,
    TextIOWrapper,
)
from itertools import (
    chain,
)
import json
import logging
import os
from random import (
    Random,
    randint,
)
import re
import sys
import tempfile
import threading
import time
from typing import (
    AbstractSet,
    Any,
    Callable,
    ContextManager,
    Dict,
    IO,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Union,
)
import unittest
from unittest import (
    mock,
)
from unittest.mock import (
    PropertyMock,
)
import uuid
from zipfile import (
    ZipFile,
)

import attr
import chalice.cli
import fastavro
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
    grouper,
    one,
)
from openapi_spec_validator import (
    validate_spec,
)
import requests
import urllib3

from azul import (
    CatalogName,
    cache,
    cached_property,
    config,
    drs,
)
from azul.auth import (
    OAuth2,
)
from azul.azulclient import (
    AzulClient,
    AzulClientNotificationError,
)
from azul.drs import (
    AccessMethod,
)
import azul.dss
from azul.es import (
    ESClientFactory,
)
from azul.http import (
    RetryAfter301,
    http_client,
)
from azul.indexer import (
    BundleFQID,
    SourcedBundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.iterators import (
    reservoir_sample,
)
from azul.json_freeze import (
    freeze,
)
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
    load_script,
)
from azul.plugins.repository.tdr import (
    TDRSourceRef,
)
from azul.portal_service import (
    PortalService,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul.vendored.frozendict import (
    frozendict,
)
from azul_test_case import (
    AlwaysTearDownTestCase,
    AzulTestCase,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)
    for catalog in config.integration_test_catalogs:
        IndexService().create_indices(catalog)


class SupportsRead(Protocol):

    def read(self, amount: int) -> bytes: ...


class IntegrationTestCase(AzulTestCase, metaclass=ABCMeta):

    @cached_property
    def azul_client(self):
        return AzulClient()

    def setUp(self) -> None:
        super().setUp()
        # All random operations should be made using this seed so that test
        # results are deterministically reproducible
        self.random_seed = randint(0, sys.maxsize)
        self.random = Random(self.random_seed)
        log.info('Using random seed %r', self.random_seed)

    @cached_property
    def _tdr_client(self) -> TDRClient:
        return TDRClient.with_service_account_credentials()

    @cached_property
    def _public_tdr_client(self) -> TDRClient:
        return TDRClient.with_public_service_account_credentials()

    @cached_property
    def managed_access_sources_by_catalog(self) -> Dict[CatalogName,
                                                        Set[TDRSourceRef]]:
        public_sources = self._public_tdr_client.snapshot_names_by_id()
        all_sources = self._tdr_client.snapshot_names_by_id()
        configured_sources = {
            catalog: [TDRSourceSpec.parse(source) for source in config.sources(catalog)]
            for catalog in config.integration_test_catalogs
            if config.is_tdr_enabled(catalog)
        }
        managed_access_sources = {catalog: set() for catalog in config.catalogs}
        for catalog, specs in configured_sources.items():
            for spec in specs:
                source_id = one(id for id, name in all_sources.items() if name == spec.name)
                if source_id not in public_sources:
                    ref = TDRSourceRef(id=source_id, spec=spec.effective)
                    managed_access_sources[catalog].add(ref)
        return managed_access_sources

    def _list_partition_bundles(self,
                                catalog: CatalogName,
                                source: str
                                ) -> List[SourcedBundleFQID]:
        """
        Randomly select a partition of bundles from the specified source, check that
        it isn't empty, and return the FQIDs of the bundles in that partition.
        """
        source = self.azul_client.repository_plugin(catalog).resolve_source(source)
        partition_prefixes = list(source.spec.prefix.effective.partition_prefixes())
        prefix = self.random.choice(partition_prefixes)
        fqids = self.azul_client.list_bundles(catalog, source, prefix)
        self.assertGreater(len(fqids), 0,
                           f'Partition {prefix!r} of source {source.spec} is empty')
        return fqids

    def _list_bundles(self,
                      catalog: CatalogName,
                      max_bundles: int
                      ) -> List[SourcedBundleFQID]:
        sources = sorted(self.azul_client.catalog_sources(catalog))
        bundle_fqids = chain.from_iterable(
            self._list_partition_bundles(catalog, source)
            for source in sources
        )
        log.info('Randomly selecting %i bundles from catalog %s.', max_bundles, catalog)
        # This exhausts the iterator to ensure that a partition is checked for
        # every source.
        bundle_fqids = reservoir_sample(max_bundles, bundle_fqids, random=self.random)

        if len(bundle_fqids) >= max_bundles:
            log.warning('Not enough bundles in catalog %r. The test may fail.', catalog)

        return bundle_fqids


class IndexingIntegrationTest(IntegrationTestCase, AlwaysTearDownTestCase):
    max_bundles = 64
    num_fastq_bytes = 1024 * 1024

    def setUp(self) -> None:
        super().setUp()
        self._http = http_client()

    @contextmanager
    def subTest(self, msg: Any = None, **params: Any):
        log.info('Beginning sub-test [%s] %r', msg, params)
        with super().subTest(msg, **params):
            try:
                yield
            except BaseException:
                log.info('Failed sub-test [%s] %r', msg, params)
                raise
            else:
                log.info('Successful sub-test [%s] %r', msg, params)

    def test_catalog_listing(self):
        response = self._check_endpoint(config.service_endpoint(), '/index/catalogs')
        response = json.loads(response)
        self.assertEqual(config.default_catalog, response['default_catalog'])
        self.assertIn(config.default_catalog, response['catalogs'])
        # Test the classification of catalogs as internal or not, other
        # response properties are covered by unit tests.
        expected = {
            catalog.name: catalog.internal
            for catalog in config.catalogs.values()
        }
        actual = {
            catalog_name: catalog['internal']
            for catalog_name, catalog in response['catalogs'].items()
        }
        self.assertEqual(expected, actual)

    def test_snapshot_listing(self):
        """
        Test with a small page size to be sure paging works
        """
        page_size = 5
        with mock.patch.object(TDRClient, 'page_size', page_size):
            paged_snapshots = self._public_tdr_client.snapshot_names_by_id()
        self.assertGreater(len(paged_snapshots), page_size)
        snapshots = self._public_tdr_client.snapshot_names_by_id()
        self.assertEqual(snapshots, paged_snapshots)

    def test_indexing(self):

        @attr.s(auto_attribs=True, kw_only=True)
        class Catalog:
            name: CatalogName
            notifications: Mapping[SourcedBundleFQID, JSON]
            random: Random = self.random

            @property
            def num_bundles(self):
                return len(self.notifications)

            @property
            def bundle_fqids(self) -> AbstractSet[SourcedBundleFQID]:
                return self.notifications.keys()

            def notifications_with_duplicates(self) -> List[JSON]:
                num_duplicates = self.num_bundles // 2
                notifications = list(self.notifications.values())
                # Index some bundles again to test that we handle duplicate additions.
                # Note: random.choices() may pick the same element multiple times so
                # some notifications will end up being sent three or more times.
                notifications.extend(self.random.choices(notifications, k=num_duplicates))
                return notifications

        def _wait_for_indexer():
            self.azul_client.wait_for_indexer()

        # For faster modify-deploy-test cycles, set `delete` to False and run
        # test once. Then also set `index` to False. Subsequent runs will use
        # catalogs from first run. Don't commit changes to these two lines.
        index = True
        delete = True

        if index:
            self._reset_indexer()

        catalogs: List[Catalog] = [
            Catalog(name=catalog,
                    notifications=self._prepare_notifications(catalog) if index else {})
            for catalog in config.integration_test_catalogs
        ]

        if index:
            for catalog in catalogs:
                self.azul_client.index(catalog=catalog.name,
                                       notifications=catalog.notifications_with_duplicates())
            _wait_for_indexer()
            for catalog in catalogs:
                self._assert_catalog_complete(catalog=catalog.name,
                                              entity_type='files',
                                              bundle_fqids=catalog.bundle_fqids)
                self._test_managed_access(catalog=catalog.name,
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
            with self._service_account_credentials:
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
            ('terra.bdbag', self._check_terra_bdbag, 1),
            ('terra.pfb', self._check_terra_pfb, 1),
            ('curl', self._check_curl_manifest, 1),
        ]:
            with self.subTest('manifest',
                              catalog=catalog,
                              format=format_,
                              attempts=attempts):
                assert attempts > 0
                params = dict(catalog=catalog)
                if format_ is not None:
                    params['format'] = format_
                for attempt in range(attempts):
                    start = time.time()
                    response = self._check_endpoint(config.service_endpoint(), '/manifest/files', params)
                    log.info('Request %i/%i took %.3fs to execute.', attempt + 1, attempts, time.time() - start)
                    validator(catalog, response)

    @cache
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
        if config.is_dss_enabled(catalog) and config.dss_direct_access:
            file_uuid = self._get_one_file_uuid(catalog)
            self._test_dos(catalog, file_uuid)
            self._test_drs(catalog, file_uuid)

    @property
    def _service_account_credentials(self) -> ContextManager:
        return self._authorization_context(self._tdr_client)

    @property
    def _public_service_account_credentials(self) -> ContextManager:
        return self._authorization_context(self._public_tdr_client)

    @contextmanager
    def _authorization_context(self, tdr: TDRClient) -> ContextManager:
        old_http = self._http
        try:
            self._http = tdr._http_client
            yield
        finally:
            self._http = old_http

    def _check_endpoint(self,
                        endpoint: str,
                        path: str,
                        query: Optional[Mapping[str, Any]] = None) -> bytes:
        query = {} if query is None else {k: str(v) for k, v in query.items()}
        url = furl(endpoint, path=path, query=query)
        return self._get_url_content(str(url))

    def _get_url_json(self, url: str) -> JSON:
        return json.loads(self._get_url_content(url))

    def _get_url_content(self, url: str) -> bytes:
        return self._get_url(url).data

    # FIXME: Accept furl instance parameter instead of URL string
    #        https://github.com/DataBiosphere/azul/issues/3398
    def _get_url(self,
                 url: str,
                 allow_redirects: bool = True,
                 stream: bool = False
                 ) -> urllib3.HTTPResponse:
        retry = RetryAfter301(total=30, redirect=30 if allow_redirects else 0)
        response = self._get_url_unchecked(url,
                                           retries=retry,
                                           preload_content=not stream)
        expected_statuses = (200,) if allow_redirects else (200, 301, 302)
        self._assertResponseStatus(response, expected_statuses)
        return response

    def _get_url_unchecked(self,
                           url: str,
                           *,
                           retries: Optional[Union[urllib3.util.retry.Retry, bool, int]] = None,
                           redirect: bool = True,
                           preload_content: bool = True) -> urllib3.HTTPResponse:
        log.info('GET %s ...', url)
        response = self._http.request('GET',
                                      url,
                                      retries=retries,
                                      redirect=redirect,
                                      preload_content=preload_content)
        assert isinstance(response, urllib3.HTTPResponse)
        log.info('... -> %i', response.status)
        return response

    def _assertResponseStatus(self,
                              response: urllib3.HTTPResponse,
                              expected_statuses: Tuple[int, ...] = (200,)):
        # Using assert to avoid tampering with response content prematurely
        # (in case the response is streamed)
        assert response.status in expected_statuses, (
            response.status,
            response.reason,
            (
                response.data[:1204]
                if response.isclosed() else
                next(response.stream(amt=1024))
            )
        )

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
        prefixes = [
            c[:-len(suffix)]
            for c in rows[0].keys()
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
        # FIXME: Remove this authentication once managed access controls are
        #        applied to the manifest endpoints
        #        https://github.com/DataBiosphere/azul/issues/3302
        drs_client = plugin.drs_client(OAuth2(self._tdr_client.credentials.token))
        access = drs_client.get_object(drs_uri, access_method=AccessMethod.gs)
        # TDR quirkily uses the GS access method to provide both a GS access URL
        # *and* an access ID that produces an HTTPS signed URL
        #
        # https://github.com/ga4gh/data-repository-service-schemas/issues/360
        # https://github.com/ga4gh/data-repository-service-schemas/issues/361
        self.assertIsNone(access.headers)
        self.assertEqual('https', furl(access.url).scheme)
        # Try HEAD first because it's more efficient, fall back to GET if the
        # DRS implementations prohibits it, like Azul's DRS proxy of DSS.
        for method in ('HEAD', 'GET'):
            log.info('%s %s', method, access.url)
            # The signed access URL shouldn't require any authentication
            response = self._http.request(method, access.url)
            if response.status != 403:
                break
        self.assertEqual(200, response.status, response.data)
        self.assertEqual(size, int(response.headers['Content-Length']))

    def _check_terra_pfb(self, _: CatalogName, response: bytes):
        reader = fastavro.reader(BytesIO(response))
        for record in reader:
            fastavro.validate(record, reader.writer_schema)
            object_schema = one(f for f in reader.writer_schema['fields']
                                if f['name'] == 'object')
            entity_schema = one(e for e in object_schema['type']
                                if e['name'] == record['name'])
            fields = entity_schema['fields']
            rows_present = set(record['object'].keys())
            rows_expected = set(f['name'] for f in fields)
            self.assertEqual(rows_present, rows_expected)

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

    def _check_curl_manifest(self, _catalog: CatalogName, response: bytes):
        text = TextIOWrapper(BytesIO(response))
        # Skip over empty lines and curl configurations to count and verify that
        # all the remaining lines are pairs of 'url=' and 'output=' lines.
        lines = (
            line for line in text
            if not line == '\n' and not line.startswith('--')
        )
        num_files = 0
        for url, output in grouper(lines, 2):
            num_files += 1
            self.assertTrue(url.startswith('url='))
            self.assertTrue(output.startswith('output='))
        log.info(f'Manifest contains {num_files} files.')
        self.assertGreater(num_files, 0)

    def _test_repository_files(self, catalog: str):
        with self.subTest('repository_files', catalog=catalog):
            file_uuid = self._get_one_file_uuid(catalog)
            response = self._check_endpoint(endpoint=config.service_endpoint(),
                                            path=f'/fetch/repository/files/{file_uuid}',
                                            query=dict(catalog=catalog))
            response = json.loads(response)

            while response['Status'] != 302:
                self.assertEqual(301, response['Status'])
                response = self._get_url_json(response['Location'])

            response = self._get_url(response['Location'], stream=True)
            self._validate_fastq_response(response)

    def _validate_fastq_response(self, response: urllib3.HTTPResponse):
        """
        Note: The response object must have been obtained with stream=True
        """
        try:
            self._validate_fastq_content(response)
        finally:
            response.close()

    def _test_drs(self, catalog: CatalogName, file_uuid: str):
        repository_plugin = self.azul_client.repository_plugin(catalog)
        drs = repository_plugin.drs_client()
        for access_method in AccessMethod:
            with self.subTest('drs', catalog=catalog, access_method=AccessMethod.https):
                log.info('Resolving file %r with DRS using %r', file_uuid, access_method)
                drs_uri = f'drs://{config.api_lambda_domain("service")}/{file_uuid}'
                access = drs.get_object(drs_uri, access_method=access_method)
                self.assertIsNone(access.headers)
                if access.method is AccessMethod.https:
                    response = self._get_url(access.url, stream=True)
                    self._validate_fastq_response(response)
                elif access.method is AccessMethod.gs:
                    content = self._get_gs_url_content(access.url, size=self.num_fastq_bytes)
                    self._validate_fastq_content(content)
                else:
                    self.fail(access_method)

    def _test_dos(self, catalog: CatalogName, file_uuid: str):
        with self.subTest('dos', catalog=catalog):
            log.info('Resolving file %s with DOS', file_uuid)
            response = self._check_endpoint(config.service_endpoint(),
                                            path=drs.dos_object_url_path(file_uuid),
                                            query=dict(catalog=catalog))
            json_data = json.loads(response)['data_object']
            file_url = first(json_data['urls'])['url']
            while True:
                with self._get_url(file_url, allow_redirects=False, stream=True) as response:
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
            self._validate_fastq_response(response)

    def _get_gs_url_content(self, url: str, size: Optional[int] = None) -> BytesIO:
        self.assertTrue(url.startswith('gs://'))
        path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        credentials = service_account.Credentials.from_service_account_file(path)
        storage_client = storage.Client(credentials=credentials)
        content = BytesIO()
        storage_client.download_blob_to_file(url, content, start=0, end=size)
        return content

    def _validate_fastq_content(self, content: SupportsRead):
        # Check signature of FASTQ file.
        with gzip.open(content) as buf:
            fastq = buf.read(self.num_fastq_bytes)
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

    def _prepare_notifications(self, catalog: CatalogName) -> Dict[BundleFQID, JSON]:
        bundle_fqids = self._list_bundles(catalog, max_bundles=self.max_bundles)
        log.info('Preparing notifications for catalog %r.', catalog)
        return {
            bundle_fqid: self.azul_client.synthesize_notification(bundle_fqid)
            for bundle_fqid in bundle_fqids
        }

    def _assert_catalog_complete(self,
                                 catalog: CatalogName,
                                 entity_type: str,
                                 bundle_fqids: AbstractSet[SourcedBundleFQID]) -> None:
        fqid_by_uuid: Mapping[str, SourcedBundleFQID] = {
            fqid.uuid: fqid for fqid in bundle_fqids
        }
        self.assertEqual(len(bundle_fqids), len(fqid_by_uuid))
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
                with self._service_account_credentials:
                    hits = self._get_entities(catalog, entity_type)
                indexed_fqids.update(
                    # FIXME: We should use the source from the index rather than
                    #        looking it up from the expectation.
                    #        https://github.com/DataBiosphere/azul/issues/2625
                    fqid_by_uuid[bundle['bundleUuid']]
                    for hit in hits
                    for bundle in hit.get('bundles', ())
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
        for entity_type in self.entity_types:
            with self.subTest('catalog_empty',
                              catalog=catalog,
                              entity_type=entity_type):
                hits = self._get_entities(catalog, entity_type)
                self.assertEqual([], [hit['entryId'] for hit in hits])

    def _get_entities(self, catalog: CatalogName, entity_type, filters: Optional[JSON] = None):
        entities = []
        size = 100
        params = dict(catalog=catalog,
                      size=str(size),
                      filters=json.dumps(filters if filters else {}))
        url = str(furl(url=config.service_endpoint(),
                       path=('index', entity_type),
                       query_params=params))
        while True:
            body = self._get_url_json(url)
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

    def _list_sources(self, catalog: CatalogName) -> Set[frozendict]:
        url = str(furl(config.service_endpoint(),
                       path='/repository/sources',
                       query={'catalog': catalog}))
        response = self._get_url_json(url)
        sources = freeze(response['sources'])
        assert isinstance(sources, tuple)
        return set(sources)

    def _list_bundles(self,
                      catalog: CatalogName,
                      max_bundles: int
                      ) -> List[SourcedBundleFQID]:
        """
        Ensures that at least one bundle is included for every managed access
        source in the catalog.
        """
        sources = self.azul_client.catalog_sources(catalog)
        managed_access_sources = self.managed_access_sources_by_catalog[catalog]
        managed_access_sources = {str(ref.spec) for ref in managed_access_sources}
        self.assertIsSubset(managed_access_sources, sources)
        num_bundles = max_bundles - len(managed_access_sources)
        bundle_fqids = super()._list_bundles(catalog, num_bundles)
        managed_access_bundle_fqids = [
            self.random.choice(
                self.azul_client.list_bundles(catalog, source, prefix='')
            )
            for source in managed_access_sources
        ]
        bundle_fqids.extend(managed_access_bundle_fqids)
        assert managed_access_sources <= {
            str(fqid.source.spec)
            for fqid in bundle_fqids
        }
        return bundle_fqids

    def _test_managed_access(self,
                             catalog: CatalogName,
                             bundle_fqids: AbstractSet[SourcedBundleFQID]):
        with self.subTest('managed_access'):
            indexed_source_ids = {fqid.source.id for fqid in bundle_fqids}
            managed_access_sources = self.managed_access_sources_by_catalog[catalog]
            managed_access_source_ids = {source.id for source in managed_access_sources}
            self.assertIsSubset(managed_access_source_ids, indexed_source_ids)

            def list_source_ids():
                return {source['sourceId'] for source in self._list_sources(catalog)}

            # Uses the indexer service account credentials, which should have
            # access to all sources.
            with self._service_account_credentials:
                self.assertIsSubset(indexed_source_ids, list_source_ids())
            with self._public_service_account_credentials:
                public_source_ids = list_source_ids()
            self.assertEqual(set(), list_source_ids() & managed_access_source_ids)
            self.assertEqual(public_source_ids, list_source_ids())

            def _source_ids_from_hits(hits: JSONs) -> Set[str]:
                return {one(bundle['sources'])['sourceId'] for bundle in hits}

            hits = self._get_entities(catalog, 'bundles')
            hit_source_ids = _source_ids_from_hits(hits)
            self.assertEqual(hit_source_ids & managed_access_source_ids, set())

            source_filter = {'sourceId': {'is': list(managed_access_source_ids)}}
            hits = self._get_entities(catalog, 'bundles', filters=source_filter)
            self.assertEqual(hits, [])

            with self._service_account_credentials:
                hits = self._get_entities(catalog, 'bundles', filters=source_filter)
            hit_source_ids = _source_ids_from_hits(hits)
            self.assertEqual(hit_source_ids, managed_access_source_ids)
            managed_access_files = {
                file['url']
                for bundle in hits
                for file in bundle['files']
            }
            if managed_access_source_ids:
                file_url = first(managed_access_files)
                response = self._get_url_unchecked(file_url, redirect=False)
                self.assertEqual(response.status, 404)
                with self._service_account_credentials:
                    response = self._get_url_unchecked(file_url, redirect=False)
                    self.assertIn(response.status, (301, 302))
            elif config.deployment_stage in ('dev', 'sandbox'):
                managed_access_catalog = 'it2'
                assert managed_access_catalog in config.integration_test_catalogs
                self.assertNotEqual(catalog, managed_access_catalog)

            summary_url = furl(config.service_endpoint(),
                               path='/index/summary',
                               args={'catalog': catalog}).url

            def _get_summary_file_count() -> int:
                return self._get_url_json(summary_url)['fileCount']

            public_summary_file_count = _get_summary_file_count()
            with self._service_account_credentials:
                auth_summary_file_count = _get_summary_file_count()
            self.assertEqual(auth_summary_file_count,
                             public_summary_file_count + len(managed_access_files))


class AzulClientIntegrationTest(IntegrationTestCase):

    def test_azul_client_error_handling(self):
        invalid_notification = {}
        notifications = [invalid_notification]
        self.assertRaises(AzulClientNotificationError,
                          self.azul_client.index,
                          first(config.integration_test_catalogs),
                          notifications)


class PortalTestCase(IntegrationTestCase):

    @cached_property
    def portal_service(self) -> PortalService:
        return PortalService()


class PortalExpirationIntegrationTest(PortalTestCase):

    def test_expiration_tagging(self):
        # This will upload the default DB if it is missing
        self.portal_service.read()
        s3_client = self.portal_service.client
        response = s3_client.get_object_tagging(Bucket=self.portal_service.bucket,
                                                Key=self.portal_service.object_key)
        tags = [(tag['Key'], tag['Value']) for tag in response['TagSet']]
        self.assertIn(self.portal_service._expiration_tag, tags)


@unittest.skipIf(config.is_main_deployment(), 'Test would pollute portal DB')
class PortalRegistrationIntegrationTest(PortalTestCase, AlwaysTearDownTestCase):

    @property
    def expected_db(self) -> JSONs:
        return self.portal_service.default_db

    def setUp(self) -> None:
        self.old_db = self.portal_service.read()
        self.portal_service.overwrite(self.expected_db)

    def test_concurrent_portal_db_crud(self):
        """
        Use multithreading to simulate multiple users simultaneously modifying
        the portals database.
        """

        n_threads = 4
        n_tasks = n_threads * 5
        n_ops = 5

        entry_format = 'task={};op={}'

        running = True

        def run(thread_count):
            for op_count in range(n_ops):
                if not running:
                    break
                mock_entry = {
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
                }
                self.portal_service._crud(lambda db: [*db, mock_entry])

        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [executor.submit(run, i) for i in range(n_tasks)]
            try:
                self.assertTrue(all(f.result() is None for f in futures))
            finally:
                running = False

        new_db = self.portal_service.read()

        old_entries = [portal for portal in new_db if 'mock-count' not in portal]
        self.assertEqual(old_entries, self.expected_db)
        mock_counts = [portal['mock-count'] for portal in new_db if 'mock-count' in portal]
        self.assertEqual(len(mock_counts), len(set(mock_counts)))
        self.assertEqual(set(mock_counts), {
            entry_format.format(i, j)
            for i in range(n_tasks) for j in range(n_ops)
        })

    def tearDown(self) -> None:
        self.portal_service.overwrite(self.old_db)


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


@unittest.skipIf(config.dss_endpoint is None,
                 'DSS endpoint is not configured')
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
        cls.server_thread = threading.Thread(target=cls.server.server.serve_forever)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server_thread.join()
        super().tearDownClass()

    def test_local_chalice(self):
        response = requests.get(str(self.url))
        self.assertEqual(200, response.status_code)

    def test_local_chalice_health_endpoint(self):
        url = str(self.url.copy().set(path='health'))
        response = requests.get(url)
        self.assertEqual(200, response.status_code)

    catalog = first(config.integration_test_catalogs)

    def test_local_chalice_index_endpoints(self):
        url = str(self.url.copy().set(path='index/files',
                                      query=dict(catalog=self.catalog)))
        response = requests.get(url)
        self.assertEqual(200, response.status_code)

    def test_local_filtered_index_endpoints(self):
        filters = {'genusSpecies': {'is': ['Homo sapiens']}}
        url = str(self.url.copy().set(path='index/files',
                                      query=dict(filters=json.dumps(filters),
                                                 catalog=self.catalog)))
        response = requests.get(url)
        self.assertEqual(200, response.status_code)


class CanBundleScriptIntegrationTest(IntegrationTestCase):

    def _test_catalog(self, catalog: config.Catalog):
        fqid = self.bundle_fqid(catalog.name)
        log.info('Canning bundle %r from catalog %r', fqid, catalog.name)
        with tempfile.TemporaryDirectory() as d:
            self._can_bundle(source=str(fqid.source.spec),
                             uuid=fqid.uuid,
                             version=fqid.version,
                             output_dir=d)

            def file_name(part):
                return f'{fqid.uuid}.{part}.json'

            manifest_file_name = file_name('manifest')
            metadata_file_name = file_name('metadata')
            expected_files = sorted([manifest_file_name, metadata_file_name])
            generated_files = sorted(os.listdir(d))
            self.assertListEqual(generated_files, expected_files)

            with open(f'{d}/{manifest_file_name}') as f:
                manifest = json.load(f)
            with open(f'{d}/{metadata_file_name}') as f:
                metadata = json.load(f)

            self.assertIsInstance(manifest, list)
            self.assertIsInstance(metadata, dict)

            manifest_files = sorted(e['name'] for e in manifest if e['indexed'])
            metadata_files = sorted(metadata.keys())

            if catalog.plugins['repository'].name == 'canned':
                # FIXME: Manifest entry not generated for links.json by
                #        StagingArea.get_bundle
                #        https://github.com/DataBiosphere/hca-metadata-api/issues/52
                assert 'links.json' not in manifest_files
                metadata_files.remove('links.json')

            self.assertListEqual(manifest_files, metadata_files)

    def test_can_bundle_configured_catalogs(self):
        for catalog_name, catalog in config.catalogs.items():
            if catalog.is_integration_test_catalog:
                with self.subTest(catalog=catalog.name,
                                  repository=catalog.plugins['repository']):
                    self._test_catalog(catalog)

    def test_can_bundle_canned_repository(self):
        mock_catalog = config.Catalog(name='canned-it',
                                      atlas='hca',
                                      internal=True,
                                      plugins={
                                          'metadata': config.Catalog.Plugin(name='hca'),
                                          'repository': config.Catalog.Plugin(name='canned'),
                                      },
                                      sources={
                                          'https://github.com/HumanCellAtlas/schema-test-data/tree/master/tests:/0'
                                      })

        with mock.patch.object(azul.Config,
                               'catalogs',
                               new=PropertyMock(return_value={
                                   mock_catalog.name: mock_catalog
                               })):
            self._test_catalog(mock_catalog)

    def bundle_fqid(self, catalog: CatalogName) -> SourcedBundleFQID:
        bundle_fqids = self._list_bundles(catalog, max_bundles=1)
        return one(bundle_fqids)

    def _can_bundle(self,
                    source: str,
                    uuid: str,
                    version: str,
                    output_dir: str
                    ) -> None:
        args = [
            '--source', source,
            '--uuid', uuid,
            '--version', version,
            '--output-dir', output_dir
        ]
        return self._can_bundle_main(args)

    @cached_property
    def _can_bundle_main(self) -> Callable[[Sequence[str]], None]:
        can_bundle = load_script('can_bundle')
        return can_bundle.main
