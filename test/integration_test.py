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
    contextmanager,
)
import csv
import gzip
from io import (
    BytesIO,
    TextIOWrapper,
)
import itertools
from itertools import (
    count,
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
    ContextManager,
    IO,
    Optional,
    Protocol,
    TypedDict,
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
from chalice import (
    UnauthorizedError,
)
import chalice.cli
import elasticsearch
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
import urllib3.request

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
from azul.chalice import (
    AzulChaliceApp,
)
from azul.drs import (
    AccessMethod,
)
from azul.es import (
    ESClientFactory,
)
from azul.http import (
    http_client,
)
from azul.indexer import (
    SourceJSON,
    SourceRef,
    SourcedBundleFQID,
    SourcedBundleFQIDJSON,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.indexer.index_service import (
    IndexExistsAndDiffersException,
    IndexService,
)
from azul.json_freeze import (
    freeze,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.modules import (
    load_app_module,
    load_script,
)
from azul.oauth2 import (
    OAuth2Client,
)
from azul.plugins import (
    MetadataPlugin,
    RepositoryPlugin,
)
from azul.plugins.metadata.anvil.bundle import (
    Link,
)
from azul.plugins.repository.tdr_anvil import (
    BundleType,
    TDRAnvilBundleFQID,
    TDRAnvilBundleFQIDJSON,
)
from azul.portal_service import (
    PortalService,
)
from azul.service.async_manifest_service import (
    Token,
)
from azul.service.manifest_service import (
    ManifestFormat,
    ManifestGenerator,
)
from azul.terra import (
    ServiceAccountCredentialsProvider,
    TDRClient,
    TDRSourceRef,
    TDRSourceSpec,
    UserCredentialsProvider,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSONs,
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


class FileInnerEntity(TypedDict):
    uuid: str
    version: str
    name: str
    size: int


GET = 'GET'
HEAD = 'HEAD'
PUT = 'PUT'
POST = 'POST'


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
    def managed_access_sources_by_catalog(self
                                          ) -> dict[CatalogName, set[TDRSourceRef]]:
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

    def _list_partitions(self,
                         catalog: CatalogName,
                         *,
                         min_bundles: int,
                         public_1st: bool
                         ) -> Iterator[tuple[SourceRef, str, list[SourcedBundleFQID]]]:
        """
        Iterate through the sources in the given catalog and yield partitions of
        bundle FQIDs until a desired minimum number of bundles are found. For
        each emitted source, every partition is included, even if it's empty.
        """
        total_bundles = 0
        sources = sorted(config.sources(catalog))
        self.random.shuffle(sources)
        if public_1st:
            managed_access_sources = frozenset(
                str(source.spec)
                for source in self.managed_access_sources_by_catalog[catalog]
            )
            index = first(
                i
                for i, source in enumerate(sources)
                if source not in managed_access_sources
            )
            sources[0], sources[index] = sources[index], sources[0]
        plugin = self.azul_client.repository_plugin(catalog)
        # This iteration prefers sources occurring first, so we shuffle them
        # above to neutralize the bias.
        for source in sources:
            source = plugin.resolve_source(source)
            source = plugin.partition_source(catalog, source)
            for prefix in source.spec.prefix.partition_prefixes():
                new_fqids = self.azul_client.list_bundles(catalog, source, prefix)
                total_bundles += len(new_fqids)
                yield source, prefix, new_fqids
            # We postpone this check until after we've yielded all partitions in
            # the current source to ensure test coverage for handling multiple
            # partitions per source
            if total_bundles >= min_bundles:
                break
        else:
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
            source = self.repository_plugin(catalog).partition_source(catalog, source)
            bundle_fqids = sorted(
                bundle_fqid
                for bundle_fqid in self.azul_client.list_bundles(catalog, source, prefix='')
                if not (
                    # DUOS bundles are too sparse to fulfill the managed access tests
                    config.is_anvil_enabled(catalog)
                    and cast(TDRAnvilBundleFQID, bundle_fqid).table_name is BundleType.duos
                )
            )
            bundle_fqid = self.random.choice(bundle_fqids)
            prefix = bundle_fqid.uuid[:8]
            new_fqids = self.azul_client.list_bundles(catalog, source, prefix)
            yield source, prefix, new_fqids


class IndexingIntegrationTest(IntegrationTestCase, AlwaysTearDownTestCase):
    """
    An integration test case that tests indexing of public and managed-access
    metadata from a random selection of bundles, and the expected effects on the
    service API. This is our main integration test case.
    """

    #: A vanilla urllib3 HTTP client without authentication or any of the
    #: special retry behaviour that we employ for Terra services. Note that
    #: IT-specific retries are configured explicitly for each request, no matter
    #: which client is used, in the :py:meth:`_get_url_unchecked` method.
    #:
    _plain_http: urllib3.request.RequestMethods

    #: Depending on the authorization context, this is either the same client as
    #: the one refered to by the attribute above, or a client that sends an
    #: access token — whose access token also depends on the context. Note that
    #: IT-specific retries are configured explicitly for each request, no matter
    #: which client is used, in the :py:meth:`_get_url_unchecked` method.
    #:
    _http: urllib3.request.RequestMethods

    def setUp(self) -> None:
        super().setUp()
        self._plain_http = http_client(log)
        self._http = self._plain_http

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
        response = self._check_endpoint(GET, '/index/catalogs')
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
            self._test_manifest_tagging_race(catalog.name)
            self._test_dos_and_drs(catalog.name)
            self._test_repository_files(catalog.name)
            if index:
                bundle_fqids = catalog.bundles
            else:
                with self._service_account_credentials:
                    bundle_fqids = self._get_indexed_bundles(catalog.name)
            self._test_managed_access(catalog=catalog.name, bundle_fqids=bundle_fqids)

        if index and delete:
            # FIXME: Test delete notifications
            #        https://github.com/DataBiosphere/azul/issues/3548
            # noinspection PyUnreachableCode
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
                                       purge_queues=not config.deployment.is_stable,
                                       delete_indices=True,
                                       create_indices=True)

    def _test_other_endpoints(self):
        catalog = config.default_catalog
        if config.is_hca_enabled(catalog):
            bundle_index, project_index = 'bundles', 'projects'
        elif config.is_anvil_enabled(catalog):
            bundle_index, project_index = 'biosamples', 'datasets'
        else:
            assert False, catalog
        service_paths = {
            '/': None,
            '/openapi': None,
            # the version endpoint is tested separately
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
                self._check_endpoint(GET, path, args=args, endpoint=endpoint)

    def _test_manifest(self, catalog: CatalogName):
        supported_formats = self.metadata_plugin(catalog).manifest_formats
        assert supported_formats
        for format in [None, *supported_formats]:
            filters = self._manifest_filters(catalog)
            first_fetch = bool(self.random.getrandbits(1))
            for fetch in [first_fetch, not first_fetch]:
                with self.subTest('manifest', catalog=catalog, format=format, fetch=fetch):
                    args = dict(catalog=catalog, filters=json.dumps(filters))
                    if format is None:
                        format = first(supported_formats)
                    else:
                        args['format'] = format.value

                    # Wrap self._get_url to collect all HTTP responses
                    _get_url = self._get_url
                    responses = list()

                    def get_url(*args, **kwargs):
                        response = _get_url(*args, **kwargs)
                        responses.append(response)
                        return response

                    with mock.patch.object(self, '_get_url', new=get_url):

                        # Make multiple identical concurrent requests to test
                        # the idempotence of manifest generation, and its
                        # resilience against DOS attacks.

                        def worker(_):
                            response = self._check_endpoint(PUT, '/manifest/files', args=args, fetch=fetch)
                            self._manifest_validators[format](catalog, response)

                        num_workers = 3
                        with ThreadPoolExecutor(max_workers=num_workers) as tpe:
                            results = list(tpe.map(worker, range(num_workers)))

                    self.assertEqual([None] * num_workers, results)
                    execution_ids = self._manifest_execution_ids(responses, fetch=fetch)
                    # The second iteration of the inner-most loop re-requests
                    # the manifest with only `fetch` being different. In that
                    # case, the manifest will already be cached and no step
                    # function execution is expected to have been started.
                    expect_execution = fetch == first_fetch
                    self.assertEqual(1 if expect_execution else 0, len(execution_ids))

    def _manifest_filters(self, catalog: CatalogName) -> JSON:
        # IT catalogs with just one public source are always indexed completely
        # if that source contains less than the minimum number of bundles
        # required. So regardless of any randomness employed by this test,
        # manifests derived from these catalogs will always be based on the same
        # content hash. Since the resulting reuse of cached manifests interferes
        # with this test, we need another means of randomizing the manifest key:
        # a random but all-inclusive filter.
        tibi_byte = 1024 ** 4
        return {
            self._file_size_facet(catalog): {
                'within': [[0, tibi_byte + self.random.randint(0, tibi_byte)]]
            }
        }

    @cached_property
    def _manifest_validators(self) -> dict[ManifestFormat, Callable[[str, bytes], None]]:
        return {
            ManifestFormat.compact: self._check_compact_manifest,
            ManifestFormat.terra_bdbag: self._check_terra_bdbag_manifest,
            ManifestFormat.terra_pfb: self._check_terra_pfb_manifest,
            ManifestFormat.curl: self._check_curl_manifest,
            ManifestFormat.verbatim_jsonl: self._check_jsonl_manifest,
            ManifestFormat.verbatim_pfb: self._check_terra_pfb_manifest
        }

    def _manifest_formats(self, catalog: CatalogName) -> Sequence[ManifestFormat]:
        supported_formats = self.metadata_plugin(catalog).manifest_formats
        assert supported_formats
        return supported_formats

    def _test_manifest_tagging_race(self, catalog: CatalogName):
        supported_formats = self._manifest_formats(catalog)
        for format in [ManifestFormat.compact, ManifestFormat.curl]:
            if format in supported_formats:
                with self.subTest('manifest_tagging_race', catalog=catalog, format=format):
                    filters = self._manifest_filters(catalog)
                    manifest_url = config.service_endpoint.set(path='/manifest/files',
                                                               args=dict(catalog=catalog,
                                                                         filters=json.dumps(filters),
                                                                         format=format.value))
                    method = PUT
                    responses = []
                    while True:
                        response = self._get_url(method, manifest_url)
                        if response.status == 301:
                            responses.append(response)
                            # Request the same manifest without following the
                            # redirect in order to expose a potential race
                            # condition that causes an untagged manifest object.
                            # The race condition could happen when a step
                            # function execution has finished generating a
                            # manifest object but is still in the process of
                            # tagging it.
                            #
                            # The more often we make these requests, the more
                            # likely it is that we catch the execution in this
                            # racy state. However, we still have to throttle the
                            # requests in order to prevent tripping the WAF rate
                            # limit.
                            time.sleep(config.waf_rate_rule_period / config.waf_rate_rule_limit)
                        elif response.status == 302:
                            responses.append(response)
                            method, manifest_url = GET, furl(response.headers['Location'])
                        else:
                            assert response.status == 200, response
                            self._manifest_validators[format](catalog, response.data)
                            break

                execution_ids = self._manifest_execution_ids(responses, fetch=False)
                self.assertEqual(1, len(execution_ids))

    def _manifest_execution_ids(self,
                                responses: list[urllib3.HTTPResponse],
                                *,
                                fetch: bool
                                ) -> set[bytes]:
        urls: list[furl]
        if fetch:
            responses = [
                json.loads(r.data)
                for r in responses
                if r.status == 200 and r.headers['Content-Type'] == 'application/json'
            ]
            urls = [furl(r['Location']) for r in responses if r['Status'] == 301]
        else:
            urls = [furl(r.headers['Location']) for r in responses if r.status == 301]
        tokens = {Token.decode(url.path.segments[-1]) for url in urls}
        execution_ids = {token.execution_id for token in tokens}
        return execution_ids

    def _get_one_inner_file(self, catalog: CatalogName) -> tuple[JSON, FileInnerEntity]:
        outer_file = self._get_one_outer_file(catalog)
        inner_files: JSONs = outer_file['files']
        return outer_file, cast(FileInnerEntity, one(inner_files))

    @cache
    def _get_one_outer_file(self, catalog: CatalogName) -> JSON:
        # Try to filter for an easy-to-parse format to verify its contents
        file_size_facet = self._file_size_facet(catalog)
        for filters in [self._fastq_filter(catalog), {}]:
            response = self._check_endpoint(method=GET,
                                            path='/index/files',
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

    def _source_spec(self, catalog: CatalogName, entity: JSON) -> TDRSourceSpec:
        if config.is_hca_enabled(catalog):
            field = 'sourceSpec'
        elif config.is_anvil_enabled(catalog):
            field = 'source_spec'
        else:
            assert False, catalog
        return TDRSourceSpec.parse(one(entity['sources'])[field])

    def _file_size_facet(self, catalog: CatalogName) -> str:
        if config.is_hca_enabled(catalog):
            return 'fileSize'
        elif config.is_anvil_enabled(catalog):
            return 'files.file_size'
        else:
            assert False, catalog

    def _fastq_filter(self, catalog: CatalogName) -> JSON:
        if config.is_hca_enabled(catalog):
            facet = 'fileFormat'
            prefix = ''
        elif config.is_anvil_enabled(catalog):
            facet = 'files.file_format'
            prefix = '.'
        else:
            assert False, catalog
        return {facet: {'is': [f'{prefix}fastq', f'{prefix}fastq.gz']}}

    def _bundle_type(self, catalog: CatalogName) -> EntityType:
        if config.is_hca_enabled(catalog):
            return 'bundles'
        elif config.is_anvil_enabled(catalog):
            return 'biosamples'
        else:
            assert False, catalog

    def _project_type(self, catalog: CatalogName) -> EntityType:
        if config.is_hca_enabled(catalog):
            return 'projects'
        elif config.is_anvil_enabled(catalog):
            return 'datasets'
        else:
            assert False, catalog

    def _uuid_column_name(self, catalog: CatalogName) -> str:
        if config.is_hca_enabled(catalog):
            return 'bundle_uuid'
        elif config.is_anvil_enabled(catalog):
            return 'bundles.bundle_uuid'
        else:
            assert False, catalog

    def _test_dos_and_drs(self, catalog: CatalogName):
        if config.is_dss_enabled(catalog) and config.dss_direct_access:
            _, file = self._get_one_inner_file(catalog)
            self._test_dos(catalog, file)
            self._test_drs(catalog, file)

    @property
    def _service_account_credentials(self) -> ContextManager:
        client = self._service_account_oauth2_client
        return self._authorization_context(client)

    @cached_property
    def _service_account_oauth2_client(self):
        provider = self._tdr_client.credentials_provider
        return OAuth2Client(credentials_provider=provider)

    @property
    def _public_service_account_credentials(self) -> ContextManager:
        client = self._public_service_account_oauth2_client
        return self._authorization_context(client)

    @cached_property
    def _public_service_account_oauth2_client(self):
        provider = self._public_tdr_client.credentials_provider
        return OAuth2Client(credentials_provider=provider)

    @property
    def _unregistered_service_account_credentials(self) -> ContextManager:
        client = self._unregistered_service_account_oauth2_client
        return self._authorization_context(client)

    @cached_property
    def _unregistered_service_account_oauth2_client(self):
        provider = self._unregistered_tdr_client.credentials_provider
        return OAuth2Client(credentials_provider=provider)

    @contextmanager
    def _authorization_context(self, oauth2_client: OAuth2Client) -> ContextManager:
        old_http = self._http
        try:
            self._http = oauth2_client._http_client
            yield
        finally:
            self._http = old_http

    def _check_endpoint(self,
                        method: str,
                        path: str,
                        *,
                        args: Optional[Mapping[str, Any]] = None,
                        endpoint: Optional[furl] = None,
                        fetch: bool = False
                        ) -> bytes:
        if endpoint is None:
            endpoint = config.service_endpoint
        args = {} if args is None else {k: str(v) for k, v in args.items()}
        url = furl(url=endpoint, path=path, args=args)
        if fetch:
            url.path.segments.insert(0, 'fetch')
            while True:
                response = self._get_url(method, url)
                self.assertEqual(200, response.status)
                response = json.loads(response.data)
                status = response['Status']
                self.assertIn(status, {301, 302})
                method, url = GET, furl(response['Location'])
                retry_after = response.get('Retry-After')
                if retry_after is not None:
                    log.info('Sleeping %.3fs to honor Retry-After property', retry_after)
                    time.sleep(retry_after)
                if status == 302:
                    break
        return self._get_url_content(method, url)

    def _get_url_json(self, method: str, url: furl) -> JSON:
        return json.loads(self._get_url_content(method, url))

    def _get_url_content(self, method: str, url: furl) -> bytes:
        while True:
            response = self._get_url(method, url)
            if response.status in [301, 302]:
                retry_after = response.headers.get('Retry-After')
                if retry_after is not None:
                    retry_after = float(retry_after)
                    log.info('Sleeping %.3fs to honor Retry-After header', retry_after)
                    time.sleep(retry_after)
                url = furl(response.headers['Location'])
                method = GET
            else:
                return response.data

    def _get_url(self,
                 method: str,
                 url: furl,
                 stream: bool = False
                 ) -> urllib3.HTTPResponse:
        response = self._get_url_unchecked(method, url, stream=stream)
        self._assertResponseStatus(response, 200, 301, 302)
        return response

    #: Hosts that require an OAuth 2.0 bearer token via the Authorization header

    authenticating_hosts = {
        config.sam_service_url.host,
        config.tdr_service_url.host,
        config.indexer_endpoint.host,
        config.service_endpoint.host
    }

    def _get_url_unchecked(self,
                           method: str,
                           url: furl,
                           *,
                           stream: bool = False
                           ) -> urllib3.HTTPResponse:
        method, url, body, headers = self._hoist_parameters(method, url)
        # The type of client used will be evident from the logger name in the
        # log message. Authenticated requests will be logged by the azul.oauth2
        # module, plain ones will be logged by this module's logger.
        if url.host in self.authenticating_hosts:
            http = self._http
        else:
            http = self._plain_http
        url = str(url)
        response = http.request(method=method,
                                url=url,
                                body=body,
                                headers=headers,
                                timeout=float(config.api_gateway_lambda_timeout + 1),
                                retries=urllib3.Retry(total=5,
                                                      redirect=0,
                                                      status_forcelist={429, 500, 502, 503, 504}),
                                redirect=False,
                                preload_content=not stream)
        assert isinstance(response, urllib3.HTTPResponse)
        return response

    def _hoist_parameters(self,
                          method: str,
                          url: furl
                          ) -> tuple[str, furl, bytes | None, dict | None]:
        """
        Pass filters in the body of a POST if passing them in the URL of a GET
        makes the URL longer than what AWS allows for edge-optimized APIs.

        https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html
        """
        body: bytes | None = None
        headers: dict | None = None
        if method in {GET, PUT, POST} and url.netloc == config.service_endpoint.netloc:
            limit = 8192
            if len(str(url)) > limit:
                url = url.copy()
                filters = url.args.pop('filters')
                assert len(str(url)) <= limit, (url, limit)
                body = json.dumps({'filters': filters}).encode()
                headers = {'Content-Type': 'application/json'}
                if method == GET:
                    method = POST
        return method, url, body, headers

    def _assertResponseStatus(self,
                              response: urllib3.HTTPResponse,
                              expected_status: int,
                              /,
                              *expected_statuses: int):
        # Using assert to avoid tampering with response content prematurely
        # (in case the response is streamed)
        assert response.status in [expected_status, *expected_statuses], (
            response.status,
            response.reason,
            (
                response.data[:1204]
                if response.isclosed() else
                next(response.stream(amt=1024))
            )
        )

    def _check_compact_manifest(self, catalog: CatalogName, response: bytes):
        self.__check_csv_manifest(BytesIO(response), self._uuid_column_name(catalog))

    def _check_terra_bdbag_manifest(self, catalog: CatalogName, response: bytes):
        with ZipFile(BytesIO(response)) as zip_fh:
            data_path = os.path.join(os.path.dirname(first(zip_fh.namelist())), 'data')
            file_path = os.path.join(data_path, 'participants.tsv')
            with zip_fh.open(file_path) as file:
                rows = self.__check_csv_manifest(file, 'bundle_uuid')
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
        access_url = furl(access.url)
        self.assertEqual('https', access_url.scheme)
        # Try HEAD first because it's more efficient, fall back to GET if the
        # DRS implementations prohibits it, like Azul's DRS proxy of DSS.
        for method in [HEAD, GET]:
            response = self._get_url_unchecked(method, access_url)
            if response.status != 403:
                break
        self.assertEqual(200, response.status, response.data)
        self.assertEqual(size, int(response.headers['Content-Length']))

    def _check_terra_pfb_manifest(self, _catalog: CatalogName, response: bytes):
        # A PFB is an Avro Object Container File, i.e., a stream of Avro objects
        # preceded by a schema describing these objects. The internals of the
        # format are slightly more complicated and are described in
        #
        # https://avro.apache.org/docs/1.11.1/specification/#object-container-files
        #
        reader = fastavro.reader(BytesIO(response))
        # The schema is also an Avro object, specifically a Avro record which
        # FastAVRO exposes to us as a JSON object, i.e., a `dict` with string
        # keys
        record_schema = reader.writer_schema
        # Each object in a PFB is also of type 'record'
        self.assertEqual('record', record_schema['type'])
        # PFB calls the records *entities*. Unfortunately, the PFB standard is
        # afflicted with confusing terminology, so bear with us.
        self.assertEqual('Entity', record_schema['name'])
        # Each entity record has four fields: `id`, `name`, `object` and
        # `relations`. The `object` field holds the actual entity. The `name`
        # field, is a string denoting the type of entity. Entities records with
        # the same value in the `name` field are expected to contain entities of
        # the same shape. Here we extract the declaration of the `object` field
        # from the schema:
        object_field = one(f for f in record_schema['fields'] if f['name'] == 'object')
        # The different shapes, i.e., entity types are defined as members of a
        # union type, which manifests in Avro simply as an array of schemas.
        # Here we extract each union member and index it into a dictionary for
        # easy access by name.
        entity_types = {e['name']: e for e in object_field['type']}
        self.assertEqual(len(entity_types), len(object_field['type']))
        # The `id` field is a string uniquely identifying an entity among all
        # entities of the same shape, i.e., with the same value in the `name`
        # field of the containing record. The `relations` field holds references
        # to other entities, as an array of nested Avro records, each record
        # containing the `name` and `id` of the referenced entity.
        num_records = count()
        for record in reader:
            # Every record must follow the schema. Since each record's `object`
            # field contains an entity, the schema check therefore extends to
            # the various entity types.
            fastavro.validate(record, record_schema)
            if 0 == next(num_records):
                # PFB requires a special `Metadata` entity to occur first. It is
                # used to declare the relations between entity types, thereby
                # expressing additional constraints on the `relations` field.
                #
                # FIXME: We don't currently declare relations
                #        https://github.com/DataBiosphere/azul/issues/6066
                #
                # For now, we just check the `name` and the absence of an `id`.
                self.assertEqual('Metadata', record['name'])
                self.assertIsNone(record['id'])
            # The following is redundant given the schema validation above but
            # we'll leave it in for illustration.
            fields = entity_types[record['name']]['fields']
            fields_present = set(record['object'].keys())
            fields_expected = set(f['name'] for f in fields)
            self.assertEqual(fields_present, fields_expected)
        # We expect to observe the special `Metadata` entity record and at least
        # one additional entity record
        self.assertGreater(next(num_records), 1)

    def _read_csv_manifest(self, file: IO[bytes]) -> csv.DictReader:
        text = TextIOWrapper(file)
        return csv.DictReader(text, delimiter='\t')

    def __check_csv_manifest(self,
                             file: IO[bytes],
                             uuid_column_name: str
                             ) -> list[Mapping[str, str]]:
        reader = self._read_csv_manifest(file)
        rows = list(reader)
        log.info(f'Manifest contains {len(rows)} rows.')
        self.assertGreater(len(rows), 0)
        self.assertIn(uuid_column_name, reader.fieldnames)
        bundle_uuids = rows[0][uuid_column_name].split(ManifestGenerator.padded_joiner)
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

    def _check_jsonl_manifest(self, _catalog: CatalogName, response: bytes):
        text = TextIOWrapper(BytesIO(response))
        num_replicas = 0
        for line in text:
            json.loads(line)
            num_replicas += 1
        log.info('Manifest contains %d replicas', num_replicas)
        self.assertGreater(num_replicas, 0)

    def _test_repository_files(self, catalog: CatalogName):
        with self.subTest('repository_files', catalog=catalog):
            outer_file, inner_file = self._get_one_inner_file(catalog)
            source = self._source_spec(catalog, outer_file)
            file_uuid, file_version = inner_file['uuid'], inner_file['version']
            endpoint_url = config.service_endpoint
            file_url = endpoint_url.set(path=f'/fetch/repository/files/{file_uuid}',
                                        args=dict(catalog=catalog,
                                                  version=file_version))
            response = self._get_url_unchecked(GET, file_url)
            if response.status == 404:
                response = json.loads(response.data)
                # Phantom files lack DRS URIs and cannot be downloaded
                self.assertEqual('NotFoundError', response['Code'])
                self.assertEqual(response['Message'],
                                 f'File {file_uuid!r} with version {file_version!r} '
                                 f'was found in catalog {catalog!r}, '
                                 f'however no download is currently available')
            else:
                self.assertEqual(200, response.status)
                response = json.loads(response.data)
                while response['Status'] != 302:
                    self.assertEqual(301, response['Status'])
                    self.assertNotIn('Retry-After', response)
                    response = self._get_url_json(GET, furl(response['Location']))
                self.assertNotIn('Retry-After', response)
                response = self._get_url(GET, furl(response['Location']), stream=True)
                self._validate_file_response(response, source, inner_file)

    def _file_ext(self, file: FileInnerEntity) -> str:
        # We believe that the file extension is a more reliable indicator than
        # the `format` metadata field. Note that this method preserves multipart
        # extensions and includes the leading '.', so the extension of
        # "foo.fastq.gz" is ".fastq.gz" instead of "gz"
        suffixes = PurePath(file['name']).suffixes
        return ''.join(suffixes).lower()

    def _validate_file_content(self, content: ReadableFileObject, file: FileInnerEntity):
        file_ext = self._file_ext(file)
        if file_ext == '.fastq':
            self._validate_fastq_content(content)
        elif file_ext == '.fastq.gz':
            with gzip.open(content) as buf:
                self._validate_fastq_content(buf)
        else:
            self.assertEqual(1 if file['size'] > 0 else 0, len(content.read(1)))

    def _validate_file_response(self,
                                response: urllib3.HTTPResponse,
                                source: TDRSourceSpec,
                                file: FileInnerEntity):
        """
        Note: The response object must have been obtained with stream=True
        """
        try:
            if source.name == 'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732':
                # All files in this snapshot were truncated to zero bytes by the
                # Broad to save costs. The metadata is not a reliable indication
                # of these files' actual size.
                self.assertEqual(response.headers['Content-Length'], '0')
            else:
                self._validate_file_content(response, file)
        finally:
            response.close()

    def _test_drs(self, catalog: CatalogName, file: FileInnerEntity):
        repository_plugin = self.azul_client.repository_plugin(catalog)
        drs = repository_plugin.drs_client()
        for access_method in AccessMethod:
            with self.subTest('drs', catalog=catalog, access_method=AccessMethod.https):
                log.info('Resolving file %r with DRS using %r', file['uuid'], access_method)
                drs_uri = f'drs://{config.api_lambda_domain("service")}/{file["uuid"]}'
                access = drs.get_object(drs_uri, access_method=access_method)
                self.assertIsNone(access.headers)
                if access.method is AccessMethod.https:
                    response = self._get_url(GET, furl(access.url), stream=True)
                    self._validate_file_response(response, file)
                elif access.method is AccessMethod.gs:
                    content = self._get_gs_url_content(furl(access.url), size=self.num_fastq_bytes)
                    self._validate_file_content(content, file)
                else:
                    self.fail(access_method)

    def _test_dos(self, catalog: CatalogName, file: FileInnerEntity):
        with self.subTest('dos', catalog=catalog):
            log.info('Resolving file %s with DOS', file['uuid'])
            response = self._check_endpoint(method=GET,
                                            path=drs.dos_object_url_path(file['uuid']),
                                            args=dict(catalog=catalog))
            json_data = json.loads(response)['data_object']
            file_url = first(json_data['urls'])['url']
            while True:
                with self._get_url(method=GET,
                                   url=file_url,
                                   stream=True
                                   ) as response:
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
            self._assertResponseStatus(response, 200)
            self._validate_file_content(response, file)

    def _get_gs_url_content(self,
                            url: furl,
                            size: Optional[int] = None
                            ) -> BytesIO:
        self.assertEquals('gs', url.scheme)
        path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        credentials = service_account.Credentials.from_service_account_file(path)
        storage_client = storage.Client(credentials=credentials)
        content = BytesIO()
        storage_client.download_blob_to_file(str(url), content, start=0, end=size)
        return content

    num_fastq_bytes = 1024 * 1024

    def _validate_fastq_content(self, content: ReadableFileObject):
        # Check signature of FASTQ file.
        fastq = content.read(self.num_fastq_bytes)
        lines = fastq.splitlines()
        # Assert first character of first and third line of file (see https://en.wikipedia.org/wiki/FASTQ_format).
        self.assertTrue(lines[0].startswith(b'@'))
        self.assertTrue(lines[2].startswith(b'+'))

    def _prepare_notifications(self,
                               catalog: CatalogName
                               ) -> tuple[JSONs, set[SourcedBundleFQID]]:
        bundle_fqids: set[SourcedBundleFQID] = set()
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
        # _list_partitions selects both public and managed access sources at random.
        # If we don't index at least one public source, every request would need
        # service account credentials and we couldn't compare the responses for
        # public and managed access data. `public_1st` ensures that at least
        # one of the sources will be public because sources are indexed starting
        # with the first one yielded by the iteration.
        list(starmap(update, self._list_partitions(catalog,
                                                   min_bundles=num_bundles,
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
                             filters: Optional[JSON] = None
                             ) -> set[SourcedBundleFQID]:
        indexed_fqids = set()
        hits = self._get_entities(catalog, 'bundles', filters)
        special_fields = self.metadata_plugin(catalog).special_fields
        for hit in hits:
            source, bundle = one(hit['sources']), one(hit['bundles'])
            source = SourceJSON(id=source[special_fields.source_id],
                                spec=source[special_fields.source_spec])
            bundle_fqid = SourcedBundleFQIDJSON(uuid=bundle[special_fields.bundle_uuid],
                                                version=bundle[special_fields.bundle_version],
                                                source=source)
            if config.is_anvil_enabled(catalog):
                # Every primary bundle contains 1 or more biosamples, 1 dataset,
                # and 0 or more other entities. Biosamples only occur in primary
                # bundles.
                if len(hit['biosamples']) > 0:
                    table_name = BundleType.primary
                # Supplementary bundles contain only 1 file and 1 dataset.
                elif len(hit['files']) > 0:
                    table_name = BundleType.supplementary
                # DUOS bundles contain only 1 dataset.
                elif len(hit['datasets']) > 0:
                    table_name = BundleType.duos
                else:
                    assert False, hit
                bundle_fqid = cast(TDRAnvilBundleFQIDJSON, bundle_fqid)
                bundle_fqid['table_name'] = table_name.value
            bundle_fqid = self.repository_plugin(catalog).resolve_bundle(bundle_fqid)
            indexed_fqids.add(bundle_fqid)
        return indexed_fqids

    def _assert_catalog_complete(self,
                                 catalog: CatalogName,
                                 bundle_fqids: Set[SourcedBundleFQID]
                                 ) -> None:
        with self.subTest('catalog_complete', catalog=catalog):
            expected_fqids = bundle_fqids
            if not config.is_anvil_enabled(catalog):
                expected_fqids = set(self.azul_client.filter_obsolete_bundle_versions(expected_fqids))
                obsolete_fqids = bundle_fqids - expected_fqids
                if obsolete_fqids:
                    log.debug('Ignoring obsolete bundle versions %r', obsolete_fqids)
            num_bundles = len(expected_fqids)
            timeout = 600
            log.debug('Expecting bundles %s ', sorted(expected_fqids))
            retries = 0
            deadline = time.time() + timeout
            while True:
                with self._service_account_credentials:
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
            hit = self._get_url_json(GET, url)
            self.assertEqual(entity_id, hit['entryId'])

    entity_types = ['files', 'projects', 'samples', 'bundles']

    def _assert_catalog_empty(self, catalog: CatalogName):
        for entity_type in self.entity_types:
            with self.subTest('catalog_empty',
                              catalog=catalog,
                              entity_type=entity_type):
                hits = self._get_entities(catalog, entity_type)
                self.assertEqual([], [hit['entryId'] for hit in hits])

    def _get_entities(self,
                      catalog: CatalogName,
                      entity_type: EntityType,
                      filters: Optional[JSON] = None
                      ) -> MutableJSONs:
        entities = []
        size = 100
        params = dict(catalog=catalog,
                      size=str(size),
                      filters=json.dumps(filters if filters else {}))
        url = config.service_endpoint.set(path=('index', entity_type),
                                          query_params=params)
        while True:
            body = self._get_url_json(GET, url)
            hits = body['hits']
            entities.extend(hits)
            url = body['pagination']['next']
            if url is None:
                return entities
            else:
                url = furl(url)

    def _assert_indices_exist(self, catalog: CatalogName):
        """
        Aside from checking that all indices exist this method also asserts
        that we can instantiate a local ES client pointing at a real, remote
        ES domain.
        """
        es_client = ESClientFactory.get()
        service = IndexService()
        for index_name in service.index_names(catalog):
            self.assertTrue(es_client.indices.exists(index=str(index_name)))

    def _test_managed_access(self,
                             catalog: CatalogName,
                             bundle_fqids: Set[SourcedBundleFQID]
                             ) -> None:
        with self.subTest('managed_access', catalog=catalog):
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

            with self.subTest('managed_access_indices', catalog=catalog):
                self._test_managed_access_indices(catalog, managed_access_source_ids)
            with self.subTest('managed_access_repository_files', catalog=catalog):
                files = self._test_managed_access_repository_files(catalog, managed_access_source_ids)
                with self.subTest('managed_access_summary', catalog=catalog):
                    self._test_managed_access_summary(catalog, files)
                with self.subTest('managed_access_repository_sources', catalog=catalog):
                    public_source_ids = self._test_managed_access_repository_sources(catalog,
                                                                                     indexed_source_ids,
                                                                                     managed_access_source_ids)
                    with self.subTest('managed_access_manifest', catalog=catalog):
                        source_id = self.random.choice(sorted(public_source_ids & indexed_source_ids))
                        self._test_managed_access_manifest(catalog, files, source_id)

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
            response = self._get_url_json(GET, url)
            return {source['sourceId'] for source in cast(JSONs, response['sources'])}

        with self._service_account_credentials:
            self.assertIsSubset(indexed_source_ids, list_source_ids())
        with self._public_service_account_credentials:
            public_source_ids = list_source_ids()
        with self._unregistered_service_account_credentials:
            self.assertEqual(public_source_ids, list_source_ids())
        invalid_auth = OAuth2('foo')
        with self.assertRaises(UnauthorizedError):
            TDRClient.for_registered_user(invalid_auth)
        invalid_provider = UserCredentialsProvider(invalid_auth)
        invalid_client = OAuth2Client(credentials_provider=invalid_provider)
        with self._authorization_context(invalid_client):
            self.assertEqual(401, self._get_url_unchecked(GET, url).status)
        self.assertEqual(set(), list_source_ids() & managed_access_source_ids)
        self.assertEqual(public_source_ids, list_source_ids())
        return public_source_ids

    def _test_managed_access_indices(self,
                                     catalog: CatalogName,
                                     managed_access_source_ids: Set[str]
                                     ) -> JSONs:
        """
        Test the managed-access controls for the /index/bundles and
        /index/projects endpoints

        :return: hits for the managed-access bundles
        """

        special_fields = self.metadata_plugin(catalog).special_fields

        def source_id_from_hit(hit: JSON) -> str:
            sources: JSONs = hit['sources']
            return one(sources)[special_fields.source_id]

        bundle_type = self._bundle_type(catalog)
        project_type = self._project_type(catalog)

        unfiltered_hits = None
        for accessible in None, False, True:
            with self.subTest(accessible=accessible):
                filters = None if accessible is None else {
                    special_fields.accessible: {'is': [accessible]}
                }
                hits = self._get_entities(catalog, project_type, filters=filters)
                if accessible is None:
                    unfiltered_hits = hits
                accessible_sources, inaccessible_sources = set(), set()
                for hit in hits:
                    source_id = source_id_from_hit(hit)
                    source_accessible = source_id not in managed_access_source_ids
                    hit_accessible = one(hit[project_type])[special_fields.accessible]
                    self.assertEqual(source_accessible, hit_accessible, hit['entryId'])
                    if accessible is not None:
                        self.assertEqual(accessible, hit_accessible)
                    if source_accessible:
                        accessible_sources.add(source_id)
                    else:
                        inaccessible_sources.add(source_id)
                self.assertIsDisjoint(accessible_sources, inaccessible_sources)
                self.assertIsDisjoint(managed_access_source_ids, accessible_sources)
                self.assertEqual(set() if accessible else managed_access_source_ids,
                                 inaccessible_sources)
        self.assertIsNotNone(unfiltered_hits, 'Cannot recover from subtest failure')

        bundle_fqids = self._get_indexed_bundles(catalog)
        hit_source_ids = {fqid.source.id for fqid in bundle_fqids}
        self.assertEqual(set(), hit_source_ids & managed_access_source_ids)

        source_filter = {
            special_fields.source_id: {
                'is': list(managed_access_source_ids)
            }
        }
        params = {
            'filters': json.dumps(source_filter),
            'catalog': catalog
        }
        url = config.service_endpoint.set(path=('index', bundle_type), args=params)
        response = self._get_url_unchecked(GET, url)
        self.assertEqual(403 if managed_access_source_ids else 200, response.status)

        with self._service_account_credentials:
            bundle_fqids = self._get_indexed_bundles(catalog, filters=source_filter)
        hit_source_ids = {fqid.source.id for fqid in bundle_fqids}
        self.assertEqual(managed_access_source_ids, hit_source_ids)

        return unfiltered_hits

    def _test_managed_access_repository_files(self,
                                              catalog: CatalogName,
                                              managed_access_source_ids: set[str]
                                              ) -> JSONs:
        """
        Test the managed access controls for the /repository/files endpoint
        :return: Managed access file hits
        """
        special_fields = self.metadata_plugin(catalog).special_fields
        with self._service_account_credentials:
            files = self._get_entities(catalog, 'files', filters={
                special_fields.source_id: {
                    'is': list(managed_access_source_ids)
                }
            })
        managed_access_file_urls = {
            one(file['files'])['url']
            for file in files
        }
        file_url = furl(self.random.choice(sorted(managed_access_file_urls)))
        response = self._get_url_unchecked(GET, file_url)
        self.assertEqual(404, response.status)
        with self._service_account_credentials:
            response = self._get_url_unchecked(GET, file_url)
            self.assertIn(response.status, (301, 302))
        return files

    def _test_managed_access_summary(self,
                                     catalog: CatalogName,
                                     managed_access_files: JSONs
                                     ) -> None:
        """
        Test the managed access controls for the /index/summary endpoint
        """
        params = {'catalog': catalog}
        summary_url = config.service_endpoint.set(path='/index/summary', args=params)

        def _get_summary_file_count() -> int:
            return self._get_url_json(GET, summary_url)['fileCount']

        public_summary_file_count = _get_summary_file_count()
        with self._service_account_credentials:
            auth_summary_file_count = _get_summary_file_count()
        self.assertEqual(auth_summary_file_count,
                         public_summary_file_count + len(managed_access_files))

    def _test_managed_access_manifest(self,
                                      catalog: CatalogName,
                                      files: JSONs,
                                      source_id: str
                                      ) -> None:
        """
        Test the managed access controls for the /manifest/files endpoint and
        the cURL manifest file download
        """
        endpoint = config.service_endpoint

        metadata_plugin = self.metadata_plugin(catalog)
        special_fields = metadata_plugin.special_fields

        def bundle_uuids(hit: JSON) -> set[str]:
            return {
                bundle[special_fields.bundle_uuid]
                for bundle in hit['bundles']
            }

        managed_access_bundles = set.union(*(
            bundle_uuids(file)
            for file in files
            if len(file['sources']) == 1
        ))
        filters = {special_fields.source_id: {'is': [source_id]}}
        params = {'size': 1, 'catalog': catalog, 'filters': json.dumps(filters)}
        files_url = furl(url=endpoint, path='index/files', args=params)
        response = self._get_url_json(GET, files_url)
        public_bundle = self.random.choice(sorted(bundle_uuids(one(response['hits']))))
        self.assertNotIn(public_bundle, managed_access_bundles)
        all_bundles = {public_bundle, *managed_access_bundles}

        filters = {
            special_fields.bundle_uuid: {
                'is': list(all_bundles)
            }
        }
        params = {'catalog': catalog, 'filters': json.dumps(filters)}
        manifest_url = furl(url=endpoint, path='/manifest/files', args=params)

        def test_compact_manifest(expected_bundles):
            manifest = BytesIO(self._get_url_content(PUT, manifest_url))
            manifest_rows = self._read_csv_manifest(manifest)
            uuid_column_name = self._uuid_column_name(catalog)
            all_found_bundles = set()
            for row in manifest_rows:
                row_bundles = set(row[uuid_column_name].split(ManifestGenerator.padded_joiner))
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
            test_compact_manifest(all_bundles)

        # Without credentials, only the public bundle should be represented
        test_compact_manifest({public_bundle})

        def read_verbatim_jsonl_manifest(manifest: IO) -> set[JSON]:
            manifest_lines = manifest.readlines()
            manifest_content = {
                freeze(json.loads(replica))
                for replica in manifest_lines
            }
            self.assertEqual(len(manifest_lines), len(manifest_content))
            return manifest_content

        def read_verbatim_pfb_manifest(manifest: IO) -> set[str]:
            entities = list(fastavro.reader(manifest))
            manifest_content = {
                # We can't assert the full contents of each entity because the
                # schema changes depending on the filters used.
                # FIXME: Generate Avro schema from AnVIL schema
                #        https://github.com/DataBiosphere/azul/issues/6109
                entity['id']
                for entity in entities
                # The special "Metadata" entity is always present. Dropping it
                # from the result streamlines the set logic used in the
                # assertion below.
                if entity['name'] != 'Metadata'
            }
            return manifest_content

        def get_verbatim_manifest(format: ManifestFormat,
                                  bundles: Iterable[str],
                                  ) -> set:
            manifest_url = furl(url=endpoint, path='/manifest/files', args={
                'catalog': catalog,
                'format': format.value,
                'filters': json.dumps({special_fields.bundle_uuid: {'is': list(bundles)}})
            })
            content = BytesIO(self._get_url_content(PUT, manifest_url))
            return {
                ManifestFormat.verbatim_jsonl: read_verbatim_jsonl_manifest,
                ManifestFormat.verbatim_pfb: read_verbatim_pfb_manifest
            }[format](content)

        for format in ManifestFormat.verbatim_jsonl, ManifestFormat.verbatim_pfb:
            if format in metadata_plugin.manifest_formats:
                with self.subTest(format=format):
                    unauthorized = get_verbatim_manifest(format, all_bundles)
                    with self._service_account_credentials:
                        authorized = get_verbatim_manifest(format, all_bundles)
                        private_only = get_verbatim_manifest(format, managed_access_bundles)
                    self.assertSetEqual(private_only, authorized - unauthorized)

        if ManifestFormat.curl in metadata_plugin.manifest_formats:
            # Create a single-file curl manifest and verify that the OAuth2
            # token is present on the command line
            managed_access_file_id = one(self.random.choice(files)['files'])['uuid']
            filters = {'fileId': {'is': [managed_access_file_id]}}
            manifest_url.set(args=dict(catalog=catalog,
                                       filters=json.dumps(filters),
                                       format='curl'))
            method = PUT
            while True:
                with self._service_account_credentials:
                    response = self._get_url_unchecked(method, manifest_url)
                if response.status == 302:
                    break
                else:
                    self.assertEqual(response.status, 301)
                    time.sleep(float(response.headers['Retry-After']))
                    manifest_url = furl(response.headers['Location'])
                    method = GET
            token = self._tdr_client.credentials.token
            expected_auth_header = f'Authorization: Bearer {token}'.encode()
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
@unittest.skipUnless(config.deployment.is_sandbox_or_personal,
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
                    'portal_id': 'foo',
                    'integrations': [
                        {
                            'integration_id': 'bar',
                            'entity_type': 'project',
                            'integration_type': 'get',
                            'entity_ids': ['baz']
                        }
                    ],
                    'mock-count': entry_format.format(thread_count, op_count)
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
        self.assertEqual(200, response.status_code, response.content)

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
        self.assertEqual(200, response.status_code, response.content)


class CanBundleScriptIntegrationTest(IntegrationTestCase):

    def _test_catalog(self, catalog: config.Catalog):
        fqid = self.bundle_fqid(catalog.name)
        log.info('Canning bundle %r from catalog %r', fqid, catalog.name)
        with tempfile.TemporaryDirectory() as d:
            self._can_bundle(source=str(fqid.source.spec),
                             uuid=fqid.uuid,
                             version=fqid.version,
                             output_dir=d)
            generated_file = one(os.listdir(d))
            with open(os.path.join(d, generated_file)) as f:
                bundle_json = json.load(f)

            metadata_plugin_name = catalog.plugins['metadata'].name
            if metadata_plugin_name == 'hca':
                self.assertEqual({'manifest',
                                  'metadata',
                                  'links',
                                  'stitched'}, bundle_json.keys())
                manifest = bundle_json['manifest']
                metadata = bundle_json['metadata']
                links = bundle_json['links']
                stitched = bundle_json['stitched']
                self.assertIsInstance(manifest, dict)
                self.assertIsInstance(metadata, dict)
                self.assertIsInstance(links, dict)
                self.assertIsInstance(stitched, list)
                metadata_ids = {
                    EntityReference.parse(ref).entity_id
                    for ref in metadata.keys()
                }
                self.assertIsSubset(set(stitched), metadata_ids)
            elif metadata_plugin_name == 'anvil':
                self.assertEqual({'entities', 'links'}, bundle_json.keys())
                entities, links = bundle_json['entities'], bundle_json['links']
                self.assertIsInstance(entities, dict)
                self.assertIsInstance(links, list)
                entities = set(map(EntityReference.parse, entities.keys()))
                if len(entities) > 1:
                    linked_entities = frozenset().union(*(
                        Link.from_json(link).all_entities
                        for link in links
                    ))
                    self.assertEqual(entities, linked_entities)
                else:
                    self.assertEqual([], links)
            else:
                assert False, metadata_plugin_name

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
        # Skip through empty partitions
        bundle_fqids = itertools.chain.from_iterable(
            bundle_fqids
            for _, _, bundle_fqids in self._list_partitions(catalog,
                                                            min_bundles=1,
                                                            public_1st=False)
        )
        return self.random.choice(sorted(bundle_fqids))

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
        http = http_client(log)
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
                    response = http.request(GET, str(base_url / 'static' / file))
                    self.assertEqual(expected_status, response.status)


class DeployedVersionIntegrationTest(AzulTestCase):

    def test_version(self):
        local_status = config.git_status
        for component, endpoint in [
            ('service', config.service_endpoint),
            ('indexer', config.indexer_endpoint)
        ]:
            endpoint.set(path='/version')
            response = requests.get(str(endpoint))
            self.assertEqual(response.status_code, 200)
            lambda_status = response.json()['git']
            self.assertEqual(local_status, lambda_status)


class DisableAutomaticIndexCreationTest(IntegrationTestCase):

    def test(self):
        es = ESClientFactory.get()
        index_name = 'no-auto-create-' + self.random.randbytes(4).hex() + '-it'
        try:
            with self.assertRaises(elasticsearch.exceptions.NotFoundError) as cm:
                es.index(index=index_name, document={'foo': 'bar'})
            expected = ('no such index [' + index_name + ']')
            self.assertEqual(expected, cm.exception.args[2]['error']['reason'])
        finally:
            if es.indices.exists(index=index_name):
                es.indices.delete(index=[index_name])


class ResponseHeadersTest(AzulTestCase):

    def test_response_security_headers(self):
        test_cases = {
            '/': {'Cache-Control': 'public, max-age=0, must-revalidate'},
            '/static/swagger-ui.css': {'Cache-Control': 'public, max-age=86400'},
            '/openapi': {'Cache-Control': 'public, max-age=500'},
            '/oauth2_redirect': {'Cache-Control': 'no-store'},
            '/health/basic': {'Cache-Control': 'no-store'}
        }
        for endpoint in (config.service_endpoint, config.indexer_endpoint):
            for path, expected_headers in test_cases.items():
                with self.subTest(endpoint=endpoint, path=path):
                    if path == '/oauth2_redirect' and endpoint == config.indexer_endpoint:
                        pass  # no oauth2 endpoint on indexer Lambda
                    else:
                        response = requests.get(str(endpoint / path))
                        response.raise_for_status()
                        expected = AzulChaliceApp.security_headers() | expected_headers
                        # FIXME: Add a CSP header with a nonce value to text/html responses
                        #        https://github.com/DataBiosphere/azul-private/issues/6
                        if path in ['/', '/oauth2_redirect']:
                            del expected['Content-Security-Policy']
                        self.assertIsSubset(expected.items(), response.headers.items())

    def test_default_4xx_response_headers(self):
        for endpoint in (config.service_endpoint, config.indexer_endpoint):
            with self.subTest(endpoint=endpoint):
                response = requests.get(str(endpoint / 'does-not-exist'))
                self.assertEqual(403, response.status_code)
                self.assertIsSubset(AzulChaliceApp.security_headers().items(),
                                    response.headers.items())
