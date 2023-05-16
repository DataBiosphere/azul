from abc import (
    ABCMeta,
)
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Set,
)
from concurrent.futures.thread import (
    ThreadPoolExecutor,
)
from contextlib import (
    AbstractContextManager,
    contextmanager,
)
import csv
import gzip
from io import (
    BytesIO,
    TextIOWrapper,
)
from itertools import (
    starmap,
)
import json
import os
from pathlib import (
    PurePath,
)
from random import (
    Random,
    randint,
)
import sys
import tempfile
import threading
import time
from typing import (
    Any,
    Callable,
    IO,
    Optional,
    Protocol,
    Union,
    cast,
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
    Config,
    RequirementError,
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
from azul.es import (
    ESClientFactory,
)
from azul.http import (
    RetryAfter301,
    http_client,
)
from azul.indexer import (
    SourceRef,
    SourcedBundleFQID,
)
from azul.indexer.index_service import (
    IndexExistsAndDiffersException,
    IndexService,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.modules import (
    load_app_module,
    load_script,
)
from azul.plugins import (
    MetadataPlugin,
    RepositoryPlugin,
)
from azul.plugins.repository.tdr import (
    TDRSourceRef,
)
from azul.plugins.repository.tdr_anvil import (
    BundleEntityType,
)
from azul.portal_service import (
    PortalService,
)
from azul.service.manifest_service import (
    ManifestFormat,
    ManifestGenerator,
)
from azul.strings import (
    pluralize,
)
from azul.terra import (
    ServiceAccountCredentialsProvider,
    TDRClient,
    TDRSourceSpec,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul_test_case import (
    AlwaysTearDownTestCase,
    AzulTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)
    for catalog in config.integration_test_catalogs:
        try:
            IndexService().create_indices(catalog)
        except IndexExistsAndDiffersException:
            log.debug('Properties of the catalog %s have changed, the catalog '
                      'will be deleted and recreated', catalog)
            IndexService().delete_indices(catalog)
            IndexService().create_indices(catalog)


class ReadableFileObject(Protocol):

    def read(self, amount: int) -> bytes: ...

    def seek(self, amount: int) -> Any: ...


class IntegrationTestCase(AzulTestCase, metaclass=ABCMeta):
    min_bundles = 32

    @cached_property
    def azul_client(self):
        return AzulClient()

    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return self.azul_client.repository_plugin(catalog)

    @cache
    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return MetadataPlugin.load(catalog).create()

    def setUp(self) -> None:
        super().setUp()
        # All random operations should be made using this seed so that test
        # results are deterministically reproducible
        self.random_seed = randint(0, sys.maxsize)
        self.random = Random(self.random_seed)
        log.info('Using random seed %r', self.random_seed)

    @cached_property
    def _tdr_client(self) -> TDRClient:
        return TDRClient.for_indexer()

    @cached_property
    def _public_tdr_client(self) -> TDRClient:
        return TDRClient.for_anonymous_user()

    @cached_property
    def _unregistered_tdr_client(self) -> TDRClient:
        tdr = TDRClient(
            credentials_provider=ServiceAccountCredentialsProvider(
                service_account=config.ServiceAccount.unregistered
            )
        )
        email = tdr.credentials.service_account_email
        self.assertFalse(tdr.is_registered(),
                         f'The "unregistered" service account ({email!r}) has '
                         f'been registered')
        # The unregistered service account should not have access to any sources
        with self.assertRaises(RequirementError) as cm:
            tdr.snapshot_names_by_id()
        msg = one(cm.exception.args)
        expected_msg_prefix = f'The service account (SA) {email!r} is not authorized'
        self.assertEqual(expected_msg_prefix, msg[:len(expected_msg_prefix)])
        return tdr

    @cached_property
    def managed_access_sources_by_catalog(self) -> dict[CatalogName,
                                                        set[TDRSourceRef]]:
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
                    ref = TDRSourceRef(id=source_id, spec=spec)
                    managed_access_sources[catalog].add(ref)
        return managed_access_sources

    def _list_partition_bundles(self,
                                catalog: CatalogName,
                                source: str
                                ) -> tuple[SourceRef, str, list[SourcedBundleFQID]]:
        """
        Randomly select a partition of bundles from the specified source, check that
        it isn't empty, and return the FQIDs of the bundles in that partition.
        """
        plugin = self.azul_client.repository_plugin(catalog)
        source = plugin.resolve_source(source)
        prefix = source.spec.prefix
        partition_prefixes = list(prefix.partition_prefixes())
        partition_prefix = self.random.choice(partition_prefixes)
        effective_prefix = prefix.common + partition_prefix
        fqids = self.azul_client.list_bundles(catalog, source, partition_prefix)
        bundle_count = len(fqids)
        partition = f'Partition {effective_prefix!r} of source {source.spec}'
        if not config.is_sandbox_or_personal_deployment:
            # For sources that use partitioning, 512 is the desired partition
            # size. In practice, we observe the reindex succeeding with sizes
            # >700 without the partition size becoming a limiting factor. From
            # this we project 1024 as a reasonable upper bound to enforce.
            upper = 1024
            if effective_prefix:
                lower = 512 // 16
                if len(fqids) < lower:
                    # If bundle UUIDs were uniformly distributed by prefix, we
                    # could directly assert a minimum partition size, but since
                    # they're not, a given partition may fail to exceed this
                    # minimum even with an optimal choice of partition prefix
                    # length.
                    log.warning('With %i bundles, %s is too small', len(fqids), partition)
                    counts = plugin.list_partitions(source)
                    if counts is None:
                        lower = 1
                    else:
                        # For plugins that support efficiently counting bundles
                        # across all partitions simultaneously, we can check
                        # whether the chosen partition is an outlier by
                        # determining the *average* partition size.
                        bundle_count = sum(counts.values()) / len(counts)
            else:
                # Sources too small to be split into more than one partition may
                # have as few as one bundle in total
                lower = 1
        else:
            # The sandbox and personal deployments typically don't use
            # partitioning and the desired number of bundles per source is
            # 16 +/- 50%, i.e. 8 to 24. However, no choice of common prefix can
            # satisfy this range for snapshots with `n` bundles where `n < 8` or
            # `24 < n <= 112`. The choice of upper bound here is somewhat
            # arbitrary.
            upper = 64
            lower = 1

        self.assertLessEqual(bundle_count, upper, partition + ' is too large')
        self.assertGreaterEqual(bundle_count, lower, partition + ' is too small')

        return source, partition_prefix, fqids

    def _list_bundles(self,
                      catalog: CatalogName,
                      *,
                      min_bundles: int,
                      check_all: bool,
                      public_1st: bool
                      ) -> Iterator[tuple[SourceRef, str, list[SourcedBundleFQID]]]:
        total_bundles = 0
        sources = sorted(config.sources(catalog))
        self.random.shuffle(sources)
        if public_1st:
            managed_access_sources = frozenset(
                map(str, self.managed_access_sources_by_catalog[catalog])
            )
            index = first(
                i for i, source in enumerate(sources)
                if source not in managed_access_sources
            )
            sources[0], sources[index] = sources[index], sources[0]
        # This iteration prefers sources occurring first, so we shuffle them
        # above to neutralize the bias.
        for source in sources:
            source, prefix, new_fqids = self._list_partition_bundles(catalog, source)
            if total_bundles < min_bundles:
                total_bundles += len(new_fqids)
                yield source, prefix, new_fqids
            # If `check_all` is True, keep looping to verify the size of a
            # partition for all sources
            elif not check_all:
                break
        if total_bundles < min_bundles:
            log.warning('Checked all sources and found only %d bundles instead of the '
                        'expected minimum %d', total_bundles, min_bundles)

    def _list_managed_access_bundles(self,
                                     catalog: CatalogName
                                     ) -> Iterator[tuple[SourceRef, str, list[SourcedBundleFQID]]]:
        sources = self.azul_client.catalog_sources(catalog)
        # We need at least one managed_access bundle per IT. To index them with
        # remote_reindex and avoid collateral bundles, we use as specific a
        # prefix as possible.
        for source in self.managed_access_sources_by_catalog[catalog]:
            assert str(source.spec) in sources
            bundle_fqid = self.random.choice(
                self.azul_client.list_bundles(catalog, source, prefix='')
            )
            # FIXME: We shouldn't need to include the common prefix
            #        https://github.com/DataBiosphere/azul/issues/3579
            common = source.spec.prefix.common
            prefix = bundle_fqid.uuid[len(common):8]
            assert prefix != '', prefix
            new_fqids = self.azul_client.list_bundles(catalog, source, prefix)
            yield source, prefix, new_fqids


class IndexingIntegrationTest(IntegrationTestCase, AlwaysTearDownTestCase):
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
        response = self._check_endpoint('/index/catalogs')
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
        snapshots = self._public_tdr_client.snapshot_names_by_id()
        self.assertEqual(snapshots, paged_snapshots)

    def test_indexing(self):

        @attr.s(auto_attribs=True, kw_only=True)
        class Catalog:
            name: CatalogName
            bundles: set[SourcedBundleFQID]
            notifications: list[JSON]
            random: Random = self.random

        def _wait_for_indexer():
            self.azul_client.wait_for_indexer()

        # For faster modify-deploy-test cycles, set `delete` to False and run
        # test once. Then also set `index` to False. Subsequent runs will use
        # catalogs from first run. Don't commit changes to these two lines.
        index = True
        delete = True

        if index:
            self._reset_indexer()

        catalogs: list[Catalog] = []
        for catalog in config.integration_test_catalogs:
            if index:
                notifications, fqids = self._prepare_notifications(catalog)
            else:
                notifications, fqids = [], set()
            catalogs.append(Catalog(name=catalog,
                                    bundles=fqids,
                                    notifications=notifications))

        if index:
            for catalog in catalogs:
                self.azul_client.queue_notifications(catalog.notifications)
            _wait_for_indexer()
            for catalog in catalogs:
                self._assert_catalog_complete(catalog=catalog.name,
                                              bundle_fqids=catalog.bundles)
                self._test_single_entity_response(catalog=catalog.name)

        for catalog in catalogs:
            self._test_manifest(catalog.name)
            self._test_dos_and_drs(catalog.name)
            self._test_repository_files(catalog.name)
            if index:
                bundle_fqids = catalog.bundles
            else:
                bundle_fqids = self._list_indexed_bundles(catalog.name)
            self._test_managed_access(catalog=catalog.name, bundle_fqids=bundle_fqids)

        if index and delete:
            # FIXME: Test delete notifications
            #        https://github.com/DataBiosphere/azul/issues/3548
            if False:
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
        catalog = config.default_catalog
        if config.is_hca_enabled(catalog):
            bundle_index, project_index = 'bundles', 'projects'
        elif config.is_anvil_enabled(catalog):
            bundle_index = pluralize(BundleEntityType.primary.value)
            project_index = 'datasets'
        else:
            assert False, catalog
        service_paths = {
            '/': None,
            '/openapi': None,
            '/version': None,
            '/index/summary': None,
            f'/index/{bundle_index}': {
                'filters': json.dumps(self._fastq_filter(catalog))
            },
            f'/index/{project_index}': {'size': 25}
        }
        service_routes = (
            (config.service_endpoint, path, args)
            for path, args in service_paths.items()
        )
        health_endpoints = (
            config.service_endpoint,
            config.indexer_endpoint
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
            (endpoint, '/health' + path, None)
            for endpoint in health_endpoints
            for path in health_paths
        )
        for endpoint, path, args in [*service_routes, *health_routes]:
            with self.subTest('other_endpoints', endpoint=endpoint, path=path, args=args):
                self._check_endpoint(path, args=args, endpoint=endpoint)

    def _test_manifest(self, catalog: CatalogName):
        supported_formats = self.metadata_plugin(catalog).manifest_formats
        assert supported_formats
        validators = {
            ManifestFormat.compact: self._check_manifest,
            ManifestFormat.terra_bdbag: self._check_terra_bdbag,
            ManifestFormat.terra_pfb: self._check_terra_pfb,
            ManifestFormat.curl: self._check_curl_manifest
        }
        for format_ in [None, *supported_formats]:
            with self.subTest('manifest',
                              catalog=catalog,
                              format=format_):
                args = dict(catalog=catalog)
                if format_ is None:
                    validator = validators[first(supported_formats)]
                else:
                    validator = validators[format_]
                    args['format'] = format_.value
                start = time.time()
                response = self._check_endpoint('/manifest/files', args=args)
                log.info('Request took %.3fs to execute.', time.time() - start)
                validator(catalog, response)

    def _get_one_inner_file(self, catalog: CatalogName) -> JSON:
        outer_file = self._get_one_outer_file(catalog)
        inner_files: JSONs = outer_file['files']
        return one(inner_files)

    @cache
    def _get_one_outer_file(self, catalog: CatalogName) -> JSON:
        # Try to filter for an easy-to-parse format to verify its contents
        if config.is_hca_enabled(catalog):
            file_size_facet = 'fileSize'
        elif config.is_anvil_enabled(catalog):
            file_size_facet = 'files.file_size'
        else:
            assert False, catalog
        for filters in [self._fastq_filter(catalog), {}]:
            response = self._check_endpoint(path='/index/files',
                                            args=dict(catalog=catalog,
                                                      filters=json.dumps(filters),
                                                      size=1,
                                                      order='asc',
                                                      sort=file_size_facet))
            hits = json.loads(response)['hits']
            if hits:
                break
        else:
            self.fail('No files found')
        return one(hits)

    def _fastq_filter(self, catalog: CatalogName) -> JSON:
        if config.is_hca_enabled(catalog):
            facet = 'fileFormat'
        elif config.is_anvil_enabled(catalog):
            facet = 'files.file_format'
        else:
            assert False, catalog
        return {facet: {'is': ['fastq', 'fastq.gz']}}

    def _test_dos_and_drs(self, catalog: CatalogName):
        if config.is_dss_enabled(catalog) and config.dss_direct_access:
            file = self._get_one_inner_file(catalog)
            file_uuid, file_ext = file['uuid'], self._file_ext(file)
            self._test_dos(catalog, file_uuid, file_ext)
            self._test_drs(catalog, file_uuid, file_ext)

    @property
    def _service_account_credentials(self) -> AbstractContextManager:
        return self._authorization_context(self._tdr_client)

    @property
    def _public_service_account_credentials(self) -> AbstractContextManager:
        return self._authorization_context(self._public_tdr_client)

    @property
    def _unregistered_service_account_credentials(self) -> AbstractContextManager:
        return self._authorization_context(self._unregistered_tdr_client)

    @contextmanager
    def _authorization_context(self, tdr: TDRClient) -> AbstractContextManager:
        old_http = self._http
        try:
            self._http = tdr._http_client
            yield
        finally:
            self._http = old_http

    def _check_endpoint(self,
                        path: str,
                        *,
                        args: Optional[Mapping[str, Any]] = None,
                        endpoint: Optional[furl] = None
                        ) -> bytes:
        if endpoint is None:
            endpoint = config.service_endpoint
        args = {} if args is None else {k: str(v) for k, v in args.items()}
        url = furl(url=endpoint, path=path, args=args)
        return self._get_url_content(url)

    def _get_url_json(self, url: furl) -> JSON:
        return json.loads(self._get_url_content(url))

    def _get_url_content(self, url: furl) -> bytes:
        return self._get_url(url).data

    def _get_url(self,
                 url: Union[furl, str],
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
                           url: Union[furl, str],
                           *,
                           retries: Optional[Union[urllib3.util.retry.Retry, bool, int]] = None,
                           redirect: bool = True,
                           preload_content: bool = True) -> urllib3.HTTPResponse:
        url = str(url)
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
                              expected_statuses: tuple[int, ...] = (200,)):
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
        drs_client = plugin.drs_client()
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

    def _check_terra_pfb(self, _catalog: CatalogName, response: bytes):
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

    def _read_manifest(self, file: IO[bytes]) -> csv.DictReader:
        text = TextIOWrapper(file)
        return csv.DictReader(text, delimiter='\t')

    def __check_manifest(self,
                         file: IO[bytes],
                         uuid_field_name: str
                         ) -> list[Mapping[str, str]]:
        reader = self._read_manifest(file)
        rows = list(reader)
        log.info(f'Manifest contains {len(rows)} rows.')
        self.assertGreater(len(rows), 0)
        self.assertIn(uuid_field_name, reader.fieldnames)
        bundle_uuids = rows[0][uuid_field_name].split(ManifestGenerator.column_joiner)
        self.assertGreater(len(bundle_uuids), 0)
        for bundle_uuid in bundle_uuids:
            self.assertEqual(bundle_uuid, str(uuid.UUID(bundle_uuid)))
        return rows

    def _check_curl_manifest(self, _catalog: CatalogName, response: bytes):
        text = TextIOWrapper(BytesIO(response))
        # Skip over empty lines, comments and curl configurations to count and
        # verify that all the remaining lines are pairs of 'url=' and 'output='
        # lines.
        lines = (
            line
            for line in text
            if not (line == '\n' or line.startswith('--') or line.startswith('#'))
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
            file = self._get_one_inner_file(catalog)
            file_uuid, file_version = file['uuid'], file['version']
            endpoint_url = config.service_endpoint
            file_url = endpoint_url.set(path=f'/fetch/repository/files/{file_uuid}',
                                        args=dict(catalog=catalog,
                                                  version=file_version))
            response = self._get_url_unchecked(file_url)
            response = json.loads(response.data)
            # Phantom files lack DRS URIs and cannot be downloaded
            if response.get('Code') == 'NotFoundError':
                self.assertEqual(response['Message'],
                                 f'File {file_uuid!r} with version {file_version!r} '
                                 f'was found in catalog {catalog!r}, however no download is currently available')
            else:
                while response['Status'] != 302:
                    self.assertEqual(301, response['Status'])
                    response = self._get_url_json(furl(response['Location']))

                response = self._get_url(response['Location'], stream=True)
                self._validate_file_response(response, self._file_ext(file))

    def _file_ext(self, file: JSON) -> str:
        # We believe that the file extension is a more reliable indicator than
        # the `format` metadata field. Note that this method preserves multipart
        # extensions and includes the leading '.', so the extension of
        # "foo.fastq.gz" is ".fastq.gz" instead of "gz"
        suffixes = PurePath(file['name']).suffixes
        return ''.join(suffixes).lower()

    def _validate_file_content(self, content: ReadableFileObject, file_ext: str):
        if file_ext == '.fastq':
            self._validate_fastq_content(content)
        elif file_ext == '.fastq.gz':
            with gzip.open(content) as buf:
                self._validate_fastq_content(buf)
        else:
            self.assertEqual(1, len(content.read(1)))

    def _validate_file_response(self, response: urllib3.HTTPResponse, file_ext: str):
        """
        Note: The response object must have been obtained with stream=True
        """
        try:
            self._validate_file_content(response, file_ext)
        finally:
            response.close()

    def _test_drs(self, catalog: CatalogName, file_uuid: str, file_ext: str):
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
                    self._validate_file_response(response, file_ext)
                elif access.method is AccessMethod.gs:
                    content = self._get_gs_url_content(access.url, size=self.num_fastq_bytes)
                    self._validate_file_content(content, file_ext)
                else:
                    self.fail(access_method)

    def _test_dos(self, catalog: CatalogName, file_uuid: str, file_ext: str):
        with self.subTest('dos', catalog=catalog):
            log.info('Resolving file %s with DOS', file_uuid)
            response = self._check_endpoint(path=drs.dos_object_url_path(file_uuid),
                                            args=dict(catalog=catalog))
            json_data = json.loads(response)['data_object']
            file_url = first(json_data['urls'])['url']
            while True:
                with self._get_url(file_url, allow_redirects=False, stream=True) as response:
                    # We handle redirects ourselves so we can log each request
                    if response.status in (301, 302):
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
            self._validate_file_content(response, file_ext)

    def _get_gs_url_content(self,
                            url: Union[furl, str],
                            size: Optional[int] = None
                            ) -> BytesIO:
        url = str(url)
        self.assertTrue(url.startswith('gs://'))
        path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        credentials = service_account.Credentials.from_service_account_file(path)
        storage_client = storage.Client(credentials=credentials)
        content = BytesIO()
        storage_client.download_blob_to_file(url, content, start=0, end=size)
        return content

    def _validate_fastq_content(self, content: ReadableFileObject):
        # Check signature of FASTQ file.
        fastq = content.read(self.num_fastq_bytes)
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

    def _prepare_notifications(self, catalog: CatalogName) -> tuple[JSONs,
                                                                    set[SourcedBundleFQID]]:
        bundle_fqids = set()
        notifications = []

        def update(source: SourceRef,
                   prefix: str,
                   partition_bundle_fqids: Iterable[SourcedBundleFQID]):
            bundle_fqids.update(partition_bundle_fqids)
            notifications.append(self.azul_client.reindex_message(catalog,
                                                                  source,
                                                                  prefix))

        list(starmap(update, self._list_managed_access_bundles(catalog)))
        num_bundles = max(self.min_bundles - len(bundle_fqids), 1)
        log.info('Selected %d bundles to satisfy managed access coverage; '
                 'selecting at least %d more', len(bundle_fqids), num_bundles)
        # _list_bundles selects both public and managed access sources at random.
        # If we don't index at least one public source, every request would need
        # service account credentials and we couldn't compare the responses for
        # public and managed access data. `public_1st` ensures that at least
        # one of the sources will be public because sources are indexed starting
        # with the first one yielded by the iteration.
        list(starmap(update, self._list_bundles(catalog,
                                                min_bundles=num_bundles,
                                                check_all=True,
                                                public_1st=True)))

        # Index some bundles again to test that we handle duplicate additions.
        # Note: random.choices() may pick the same element multiple times so
        # some notifications may end up being sent three or more times.
        num_duplicates = len(bundle_fqids) // 2
        duplicate_bundles = [
            self.azul_client.bundle_message(catalog, bundle)
            for bundle in self.random.choices(sorted(bundle_fqids), k=num_duplicates)
        ]
        notifications.extend(duplicate_bundles)
        return notifications, bundle_fqids

    def _get_indexed_bundles(self,
                             catalog: CatalogName,
                             ) -> set[SourcedBundleFQID]:
        indexed_fqids = set()
        if config.is_hca_enabled(catalog):
            entity_type = 'files'
        elif config.is_anvil_enabled(catalog):
            # While the files index does exist for AnVIL, it's possible
            # for a bundle entity not to contain any files and
            # thus be absent from the files response. The only entity
            # type that is linked to both primary and supplementary
            # bundles is datasets.
            entity_type = 'datasets'
        else:
            assert False, catalog
        with self._service_account_credentials:
            hits = self._get_entities(catalog, entity_type)
        for hit in hits:
            source = one(hit['sources'])
            for bundle in hit.get('bundles', ()):
                bundle_fqid = dict(
                    source=dict(id=source['sourceId'], spec=source['sourceSpec']),
                    uuid=bundle['bundleUuid'],
                    version=bundle['bundleVersion']
                )
                if config.is_anvil_enabled(catalog):
                    for file in hit['files']:
                        is_supplementary = file['is_supplementary']
                        if isinstance(is_supplementary, list):
                            is_supplementary = one(is_supplementary)
                        if is_supplementary:
                            bundle_fqid['entity_type'] = BundleEntityType.supplementary.value
                            break
                    else:
                        bundle_fqid['entity_type'] = BundleEntityType.primary.value
                bundle_fqid = self.repository_plugin(catalog).resolve_bundle(bundle_fqid)
                indexed_fqids.add(bundle_fqid)
        return indexed_fqids

    def _assert_catalog_complete(self,
                                 catalog: CatalogName,
                                 bundle_fqids: Set[SourcedBundleFQID]
                                 ) -> None:
        with self.subTest('catalog_complete', catalog=catalog):
            expected_fqids = set(self.azul_client.filter_obsolete_bundle_versions(bundle_fqids))
            obsolete_fqids = bundle_fqids - expected_fqids
            if obsolete_fqids:
                log.debug('Ignoring obsolete bundle versions %r', obsolete_fqids)
            num_bundles = len(expected_fqids)
            timeout = 600
            log.debug('Expecting bundles %s ', sorted(expected_fqids))
            retries = 0
            deadline = time.time() + timeout
            while True:
                indexed_fqids = self._get_indexed_bundles(catalog)
                log.info('Detected %i of %i bundles on try #%i.',
                         len(indexed_fqids), num_bundles, retries)
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

    def _test_single_entity_response(self,
                                     catalog: CatalogName
                                     ) -> None:
        entity_type = 'files'
        with self.subTest('single_entity', entity_type=entity_type, catalog=catalog):
            entity_id = self._get_one_outer_file(catalog)['entryId']
            url = config.service_endpoint.set(path=('index', entity_type, entity_id),
                                              args=dict(catalog=catalog))
            hit = self._get_url_json(url)
            self.assertEqual(entity_id, hit['entryId'])

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
        url = config.service_endpoint.set(path=('index', entity_type),
                                          query_params=params)
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

    def _list_indexed_bundles(self, catalog: CatalogName) -> set[SourcedBundleFQID]:
        bundle_fqids = set()
        plugin = self.azul_client.repository_plugin(catalog)
        with self._service_account_credentials:
            for hit in self._get_entities(catalog, 'bundles'):
                bundle = one(hit['bundles'])
                source = one(hit['sources'])
                # FIXME: Encapsulate source JSON representations
                #        https://github.com/databiosphere/azul/issues/3889
                source = plugin.source_from_json({
                    'id': source['sourceId'],
                    'spec': source['sourceSpec']
                })
                bundle_fqids.add(SourcedBundleFQID(uuid=bundle['bundleUuid'],
                                                   version=bundle['bundleVersion'],
                                                   source=source))
        return bundle_fqids

    def _test_managed_access(self,
                             catalog: CatalogName,
                             bundle_fqids: Set[SourcedBundleFQID]
                             ) -> None:
        with self.subTest('managed_access'):
            indexed_source_ids = {fqid.source.id for fqid in bundle_fqids}
            managed_access_sources = self.managed_access_sources_by_catalog[catalog]
            managed_access_source_ids = {source.id for source in managed_access_sources}
            self.assertIsSubset(managed_access_source_ids, indexed_source_ids)

            if not managed_access_sources:
                if config.deployment_stage in ('dev', 'sandbox'):
                    # There should always be at least one managed-access source
                    # indexed and tested on the default catalog for these deployments
                    self.assertNotEqual(catalog, config.it_catalog_for(config.default_catalog))
                self.skipTest(f'No managed access sources found in catalog {catalog!r}')

            with self.subTest('managed_access_indices'):
                bundles = self._test_managed_access_indices(catalog, managed_access_source_ids)
                with self.subTest('managed_access_repository_files'):
                    files = self._test_managed_access_repository_files(bundles)
                    with self.subTest('managed_access_summary'):
                        self._test_managed_access_summary(catalog, files)
                with self.subTest('managed_access_repository_sources'):
                    public_source_ids = self._test_managed_access_repository_sources(catalog,
                                                                                     indexed_source_ids,
                                                                                     managed_access_source_ids)
                    with self.subTest('managed_access_manifest'):
                        self._test_managed_access_manifest(catalog,
                                                           bundles,
                                                           first(public_source_ids & indexed_source_ids))

    def _test_managed_access_repository_sources(self,
                                                catalog: CatalogName,
                                                indexed_source_ids: Set[str],
                                                managed_access_source_ids: Set[str]
                                                ) -> set[str]:
        """
        Test the managed access controls for the /repository/sources endpoint
        :return: the set of public sources
        """
        url = config.service_endpoint.set(path='/repository/sources',
                                          query={'catalog': catalog})

        def list_source_ids() -> set[str]:
            response = self._get_url_json(url)
            return {source['sourceId'] for source in cast(JSONs, response['sources'])}

        with self._service_account_credentials:
            self.assertIsSubset(indexed_source_ids, list_source_ids())
        with self._public_service_account_credentials:
            public_source_ids = list_source_ids()
        with self._unregistered_service_account_credentials:
            self.assertEqual(public_source_ids, list_source_ids())
        with self._authorization_context(TDRClient.for_registered_user(OAuth2('foo'))):
            self.assertEqual(401, self._get_url_unchecked(url).status)
        self.assertEqual(set(), list_source_ids() & managed_access_source_ids)
        self.assertEqual(public_source_ids, list_source_ids())
        return public_source_ids

    def _test_managed_access_indices(self,
                                     catalog: CatalogName,
                                     managed_access_source_ids: Set[str]
                                     ) -> JSONs:
        """
        Test the managed access controls for the /index/bundles and
        /index/projects endpoints
        :return: hits for the managed access bundles
        """

        def source_id_from_hit(hit: JSON) -> str:
            return one(hit['sources'])['sourceId']

        hits = self._get_entities(catalog, 'projects')
        sources_found = set()
        for hit in hits:
            source_id = source_id_from_hit(hit)
            sources_found.add(source_id)
            self.assertEqual(source_id not in managed_access_source_ids,
                             one(hit['projects'])['accessible'])
        self.assertIsSubset(managed_access_source_ids, sources_found)

        hits = self._get_entities(catalog, 'bundles')
        hit_source_ids = set(map(source_id_from_hit, hits))
        self.assertEqual(set(), hit_source_ids & managed_access_source_ids)

        source_filter = {'sourceId': {'is': list(managed_access_source_ids)}}
        url = config.service_endpoint.set(path='/index/bundles',
                                          args={'filters': json.dumps(source_filter)})
        response = self._get_url_unchecked(url)
        self.assertEqual(403 if managed_access_source_ids else 200, response.status)

        with self._service_account_credentials:
            hits = self._get_entities(catalog, 'bundles', filters=source_filter)
        hit_source_ids = set(map(source_id_from_hit, hits))
        self.assertEqual(managed_access_source_ids, hit_source_ids)
        return hits

    def _test_managed_access_repository_files(self, bundles: JSONs) -> set[str]:
        """
        Test the managed access controls for the /repository/files endpoint
        :return: download URLs for the managed access files
        """
        managed_access_file_urls = {
            file['url']
            for bundle in bundles
            for file in cast(JSONs, bundle['files'])
        }
        file_url = furl(first(managed_access_file_urls))
        response = self._get_url_unchecked(file_url, redirect=False)
        self.assertEqual(404, response.status)
        with self._service_account_credentials:
            response = self._get_url_unchecked(file_url, redirect=False)
            self.assertIn(response.status, (301, 302))
        return managed_access_file_urls

    def _test_managed_access_summary(self,
                                     catalog: CatalogName,
                                     managed_access_files: Set[str]
                                     ) -> None:
        """
        Test the managed access controls for the /index/summary endpoint
        """
        params = {'catalog': catalog}
        summary_url = config.service_endpoint.set(path='/index/summary', args=params)

        def _get_summary_file_count() -> int:
            return self._get_url_json(summary_url)['fileCount']

        public_summary_file_count = _get_summary_file_count()
        with self._service_account_credentials:
            auth_summary_file_count = _get_summary_file_count()
        self.assertEqual(auth_summary_file_count,
                         public_summary_file_count + len(managed_access_files))

    def _test_managed_access_manifest(self,
                                      catalog: CatalogName,
                                      bundles: JSONs,
                                      source_id: str
                                      ) -> None:
        """
        Test the managed access controls for the /manifest/files endpoint and
        the cURL manifest file download
        """
        endpoint = config.service_endpoint
        managed_access_bundles = [
            bundle['entryId']
            for bundle in bundles
            if len(bundle['sources']) == 1
        ]
        filters = {'sourceId': {'is': [source_id]}}
        params = {'size': 1, 'catalog': catalog, 'filters': json.dumps(filters)}
        bundles_url = furl(url=endpoint, path='index/bundles', args=params)
        response = self._get_url_json(bundles_url)
        public_bundle = one(response['hits'])['entryId']
        self.assertNotIn(public_bundle, managed_access_bundles)

        filters = {'bundleUuid': {'is': [public_bundle, *managed_access_bundles]}}
        params = {'catalog': catalog, 'filters': json.dumps(filters)}
        manifest_url = furl(url=endpoint, path='/manifest/files', args=params)

        def assert_manifest(expected_bundles):
            manifest_rows = self._read_manifest(BytesIO(
                self._get_url_content(manifest_url)
            ))
            all_found_bundles = set()
            for row in manifest_rows:
                row_bundles = set(row['bundle_uuid'].split(ManifestGenerator.column_joiner))
                # It's possible for one file to be present in multiple
                # bundles (e.g. due to stitching), so each row may include
                # additional bundles besides those included in the filters.
                # However, we still shouldn't observe any files that don't
                # occur in *any* of the expected bundles.
                found_bundles = row_bundles & expected_bundles
                self.assertNotEqual(set(), found_bundles)
                all_found_bundles.update(found_bundles)
            self.assertEqual(expected_bundles, all_found_bundles)

        # With authorized credentials, all bundles included in the filters
        # should be represented in the manifest
        with self._service_account_credentials:
            assert_manifest({public_bundle, *managed_access_bundles})

        # Without credentials, only the public bundle should be represented
        assert_manifest({public_bundle})

        # Create a single-file curl manifest and verify that the OAuth2
        # token is present on the command line
        managed_access_files: JSONs = self.random.choice(bundles)['files']
        managed_access_file_id = self.random.choice(managed_access_files)['uuid']
        manifest_url.set(args={
            'catalog': catalog,
            'filters': json.dumps({'fileId': {'is': [managed_access_file_id]}}),
            'format': 'curl'
        })
        while True:
            with self._service_account_credentials:
                response = self._get_url_unchecked(manifest_url, redirect=False)
            if response.status == 302:
                break
            else:
                self.assertEqual(response.status, 301)
                time.sleep(int(response.headers['Retry-After']))
                manifest_url.url = response.headers['Location']
        token = self._tdr_client.credentials.token
        expected_auth_header = bytes(f'Authorization: Bearer {token}', 'UTF8')
        command_lines = list(filter(None, response.data.split(b'\n')))[1::2]
        for command_line in command_lines:
            self.assertIn(expected_auth_header, command_line)


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


# FIXME: Re-enable when SlowDown error can be avoided
#        https://github.com/DataBiosphere/azul/issues/4285
@unittest.skip('Test disabled. FIXME #4285')
@unittest.skipUnless(config.is_sandbox_or_personal_deployment,
                     'Test would pollute portal DB')
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
        for component, url in [
            ('service', config.service_endpoint),
            ('indexer', config.indexer_endpoint)
        ]:
            with self.subTest(component=component):
                url.set(path='/')
                response = requests.get(str(url))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.headers['content-type'], 'text/html')
                self.assertGreater(len(response.content), 0)
                # validate OpenAPI spec
                url.set(path='/openapi')
                response = requests.get(str(url))
                response.raise_for_status()
                spec = response.json()
                validate_spec(spec)


class AzulChaliceLocalIntegrationTest(AzulTestCase):
    url = furl(scheme='http', host='127.0.0.1', port=8000)
    server = None
    server_thread = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        app_module = load_app_module('service', unit_test=True)
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
        if config.is_hca_enabled(self.catalog):
            species_facet = 'genusSpecies'
        elif config.is_anvil_enabled(self.catalog):
            species_facet = 'donors.organism_type'
        else:
            assert False, self.catalog
        filters = {species_facet: {'is': ['Homo sapiens']}}
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
        with mock.patch.object(Config,
                               'catalogs',
                               new=PropertyMock(return_value={
                                   mock_catalog.name: mock_catalog
                               })):
            self._test_catalog(mock_catalog)

    def bundle_fqid(self, catalog: CatalogName) -> SourcedBundleFQID:
        source, prefix, bundle_fqids = next(self._list_bundles(catalog,
                                                               min_bundles=1,
                                                               check_all=False,
                                                               public_1st=False))
        return self.random.choice(bundle_fqids)

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


class SwaggerResourceIntegrationTest(AzulTestCase):

    def test(self):
        http = http_client()
        for component, base_url in [
            ('service', config.service_endpoint),
            ('indexer', config.indexer_endpoint)
        ]:
            for file, expected_status in [
                ('swagger-ui.css', 200),
                ('does-not-exist', 404),
                ('../environ.json', 403),
                ('../does-not-exist', 403),
                # Normally the next two paths would return a 400, however the
                # WAF rule group CommonRuleSet now catches and blocks these
                ('..%2Fenviron.json', 403),
                ('..%2Fdoes-not-exist', 403),
            ]:
                with self.subTest(component=component, file=file):
                    response = http.request('GET', str(base_url / 'static' / file))
                    self.assertEqual(expected_status, response.status)
