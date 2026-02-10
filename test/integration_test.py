from collections.abc import (
    Iterable,
    Mapping,
    Sequence,
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
import json
import os
from pathlib import (
    PurePath,
)
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
    Any,
    Callable,
    ContextManager,
    IO,
    Protocol,
    cast,
)
from unittest import (
    mock,
)
from unittest.mock import (
    PropertyMock,
)
import uuid

import attr
from chalice import (
    UnauthorizedError,
)
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
    only,
)
from openapi_spec_validator import (
    validate,
)
import opensearchpy
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
    false,
    mutable_furl,
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
from azul.collections import (
    alist,
    lookup,
)
from azul.csp import (
    CSP,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    AccessMethod,
)
from azul.es import (
    ESClientFactory,
)
from azul.http import (
    HttpClient,
    http_client,
)
from azul.indexer import (
    Prefix,
    SourceConfig,
    SourceRef,
    SourceSpec,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.indexer.index_service import (
    IndexExistsAndDiffersException,
    IndexService,
)
from azul.indexer.mirror_service import (
    BaseMirrorService,
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
    File,
    MetadataPlugin,
    RepositoryPlugin,
)
from azul.plugins.metadata.anvil.bundle import (
    EntityLink,
)
from azul.plugins.repository.tdr_anvil import (
    BundleType,
    TDRAnvilBundleFQID,
)
from azul.queues import (
    SQSMessage,
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
    MutableJSON,
    MutableJSONs,
)
from azul_test_case import (
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


GET = 'GET'
HEAD = 'HEAD'
PUT = 'PUT'
POST = 'POST'


class IntegrationTestCase(AzulTestCase):
    min_bundles = 32

    @cached_property
    def azul_client(self):
        return AzulClient()

    @property
    def index_queue_service(self):
        return self.azul_client.index_queue_service

    @property
    def index_repository_service(self):
        return self.azul_client.index_repository_service

    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return self.azul_client.repository_plugin(catalog)

    @cache
    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return MetadataPlugin.load(catalog).create()

    def setUp(self) -> None:
        super().setUp()
        pinned_seed = only(
            int(m.group(1))
            for flag in config.it_flags
            if (m := re.fullmatch(r'seed=(.*)', flag)) is not None
        )
        if pinned_seed is None:
            self.random_seed = randint(0, sys.maxsize)
            log.info('Using random seed %r', self.random_seed)
        else:
            self.random_seed = pinned_seed
            log.info('Using pinned seed %r', self.random_seed)
        # All random operations should be made using this seed so that test
        # results are deterministically reproducible
        self.random = Random(self.random_seed)

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
        msg = str(cm.exception)
        expected_msg_prefix = f'The service account (SA) {email!r} is not authorized'
        self.assertEqual(expected_msg_prefix, msg[:len(expected_msg_prefix)])
        return tdr

    @cached_property
    def managed_access_sources_by_catalog(self
                                          ) -> dict[CatalogName, set[TDRSourceRef]]:
        public_sources = self._public_tdr_client.snapshot_names_by_id()
        all_sources = self._tdr_client.snapshot_names_by_id()
        configured_sources = {
            catalog: self.repository_plugin(catalog).sources
            for catalog in config.integration_test_catalogs
            if config.is_tdr_enabled(catalog)
        }
        managed_access_sources = {catalog: set() for catalog in config.catalogs}
        for catalog, sources in configured_sources.items():
            for spec, _ in sources.items():
                source_id = one(id for id, name in all_sources.items() if name == spec.name)
                if source_id not in public_sources:
                    ref = TDRSourceRef(id=source_id, spec=spec, prefix=None)
                    managed_access_sources[catalog].add(ref)
        return managed_access_sources

    def _select_source(self,
                       catalog: CatalogName,
                       *,
                       public: bool | None = None,
                       mirror: bool = False,
                       ) -> tuple[SourceRef, SourceConfig] | None:
        """
        Choose an indexed source at random.

        :param catalog: The name of the catalog to select a source from.

        :param public: If none (as by default), allow the source to be either
                       public or non-public. If true, choose a public source, or
                       raise an `AssertionError` if the catalog contains no
                       public sources. If false, choose a non-public source, or
                       return `None` if the catalog contains no non-public
                       sources.

        :param mirror: If true, choose a source where the `no_mirror` flag is
                       not present, or return `None` if the catalog contains no
                       such source. If false, choose a source regardless of
                       whether this flag is present.
        """
        plugin = self.repository_plugin(catalog)
        sources = plugin.sources

        if public is None:
            ma_sources = set()
        else:
            ma_sources = {
                source.spec
                # This would raise a KeyError during the can bundle script test
                # due to it using a mock catalog, so we only evaluate it when
                # it's actually needed
                for source in self.managed_access_sources_by_catalog[catalog]
            }
            self.assertIsSubset(ma_sources, sources.keys())

        def _filter(source: tuple[SourceSpec, SourceConfig]) -> bool:
            if public is None:
                valid = True
            elif public is True:
                valid = source[0] not in ma_sources
            elif public is False:
                valid = source[0] in ma_sources
            else:
                assert False, public
            if mirror:
                valid &= source[1].mirror
            return valid

        sources = dict(filter(_filter, sources.items()))

        if len(sources) == 0:
            assert public is False, 'An IT catalog must contain at least one public source'
            return None
        else:
            source, cfg = self.random.choice(sorted(sources.items()))
            return plugin.resolve_source(source), cfg


class IndexingIntegrationTest(IntegrationTestCase):
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
    _plain_http: HttpClient

    #: Depending on the authorization context, this is either the same client as
    #: the one refered to by the attribute above, or a client that sends an
    #: access token — whose access token also depends on the context. Note that
    #: IT-specific retries are configured explicitly for each request, no matter
    #: which client is used, in the :py:meth:`_get_url_unchecked` method.
    #:
    _http: HttpClient

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
        Test with small page sizes to make sure paging works.
        """
        # Without a filter, the test takes so long that there's a real risk of
        # failure due to new snapshots being added mid-test.
        snapshot_filters_by_deployment = {
            'tempdev': 'anvil_',  # ~5 snapshots
            'anvildev': 'anvil_',  # ~5 snapshots
            'dev': 'hca_dev_5',  # ~10 snapshots
            'anvilprod': 'anvil_page_',  # ~13 snapshots
            'prod': '_dcp37'  # ~13 snapshots
        }
        filter = snapshot_filters_by_deployment[config.main_deployment_stage]
        for page_size in 1, 2:
            with self.subTest(page_size=page_size):
                with mock.patch.object(TDRClient, 'page_size', page_size):
                    paged_snapshots = self._tdr_client.snapshot_names_by_id(filter=filter)
                snapshots = self._tdr_client.snapshot_names_by_id(filter=filter)
                self.assertLess(len(snapshots), 20)
                # Show that multiple pages were fetched, via the pigeonhole
                # principle, and under the assumption that the TDR client
                # correctly implements paging, and doesn't, for example, ignore
                # the page size parameter. This test is designed to detect
                # problems in the server-side implementation of paging, or our
                # understanding of it. There is a unit test
                # (test_list_snapshots_paging) dedicated to ensuring that the
                # client-side implementation of paging is correct.
                self.assertGreater(len(paged_snapshots), page_size)
                self.assertEqual(snapshots, paged_snapshots)

    def test_indexing(self):

        @attr.s(auto_attribs=True, kw_only=True)
        class Catalog:
            name: CatalogName
            bundles: set[SourcedBundleFQID]
            notifications: list[SQSMessage]
            public_source: SourceRef
            ma_source: SourceRef | None

        flags = config.it_flags
        index, delete, mirror = [
            'no_' + flag not in flags
            for flag in ['index', 'delete', 'mirror']
        ]

        self._assert_queues_empty(config.indexer_fail_queue_names)
        if index:
            self._reset_indexer()
        else:
            log.warning('Will skip indexing due to overriding IT flag.')

        catalogs: list[Catalog] = []
        for catalog in config.integration_test_catalogs.values():
            if index:
                public_source, _ = self._select_source(
                    catalog.name,
                    public=True,
                    # If test_mirroring is run for the catalog, ensure that the
                    # source is not flagged as no_mirror so that we can test
                    # downloading a mirrored file
                    mirror=mirror and self._mirror_service(catalog.name).may_mirror()
                )
                ma_source = self._select_source(catalog.name, public=False)
                if ma_source is not None:
                    ma_source = ma_source[0]
                sources = alist(public_source, ma_source)
                notifications, fqids = self._prepare_notifications(catalog.name, sources)
            else:
                with self._service_account_credentials:
                    fqids = self._get_indexed_bundles(catalog.name)
                indexed_sources = {fqid.source for fqid in fqids}
                ma_sources = self.managed_access_sources_by_catalog[catalog.name]
                ma_source_ids = {s.id for s in ma_sources}
                public_source = one(s for s in indexed_sources if s.id not in ma_source_ids)
                ma_source = only(s for s in indexed_sources if s.id in ma_source_ids)
                notifications = []
            catalogs.append(Catalog(name=catalog.name,
                                    bundles=fqids,
                                    notifications=notifications,
                                    public_source=public_source,
                                    ma_source=ma_source))

        if index:
            service = self.index_queue_service
            for catalog in catalogs:
                service.queue_notifications(catalog.notifications)
            self.azul_client.wait_for_indexer()
            self._assert_queues_empty(config.indexer_fail_queue_names)
            for catalog in catalogs:
                self._assert_catalog_complete(catalog=catalog.name,
                                              bundle_fqids=catalog.bundles)
                self._test_single_entity_response(catalog=catalog.name)

        for catalog in catalogs:
            self._test_manifest(catalog.name)
            self._test_manifest_tagging_race(catalog.name)
            self._test_dos_and_drs(catalog.name)
            self._test_repository_files(catalog.name)
            self._test_managed_access(catalog=catalog.name,
                                      public_source=catalog.public_source,
                                      ma_source=catalog.ma_source)

        if mirror and config.enable_mirroring:
            self._test_mirroring(delete=delete)

        if index and delete:
            # FIXME: Test delete notifications
            #        https://github.com/DataBiosphere/azul/issues/3548
            if false():
                with self._service_account_credentials:
                    for catalog in catalogs:
                        self._assert_catalog_empty(catalog.name)
        else:
            log.warning('Will skip deletions due to overriding IT flag')

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
            '/openapi.json': None,
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
            execution_ids = set()
            coin_flip = bool(self.random.getrandbits(1))
            for i, fetch in enumerate([coin_flip, coin_flip, not coin_flip]):
                with self.subTest('manifest', catalog=catalog, format=format, i=i, fetch=fetch):
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

                    execution_ids.update(self._manifest_execution_ids(responses))
                    bucket, key = one(self._manifest_objects(responses))
                    if i == 0:
                        aws.s3.delete_object(Bucket=bucket, Key=key)
                        # One execution to generate the manifest
                        self.assertEqual(1, len(execution_ids))
                    elif i == 1:
                        # One more execution to re-generate the manifest
                        self.assertEqual(2, len(execution_ids))
                    elif i == 2:
                        # Only fetch mode changed, cached manifest will be used,
                        # and no additional executions are expectect
                        self.assertEqual(2, len(execution_ids))
                    else:
                        assert False

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
                            rate_limit = config.waf_rate_limit
                            time.sleep(rate_limit.period / rate_limit.value)
                        elif response.status == 302:
                            responses.append(response)
                            method, manifest_url = GET, furl(response.headers['Location'])
                        else:
                            assert response.status == 200, response
                            self._manifest_validators[format](catalog, response.data)
                            break

                    execution_ids = self._manifest_execution_ids(responses)
                    self.assertEqual(1, len(execution_ids))

    def _manifest_execution_ids(self,
                                responses: list[urllib3.HTTPResponse]
                                ) -> set[tuple[uuid.UUID, int]]:
        urls = self._manifest_urls(responses, status=301)
        tokens = {Token.decode(url.path.segments[-1]) for url in urls}
        execution_ids = {token.execution_id for token in tokens}
        return execution_ids

    def _manifest_objects(self,
                          responses: list[urllib3.HTTPResponse]
                          ) -> set[tuple[str, str]]:
        urls = self._manifest_urls(responses, status=302)
        return {
            (url.path.segments[0], '/'.join(url.path.segments[1:]))
            for url in urls
            if url.netloc == 's3.amazonaws.com' and url.scheme == 'https'
        }

    def _manifest_urls(self,
                       responses: list[urllib3.HTTPResponse],
                       *,
                       status: int
                       ) -> list[furl]:
        urls: list[furl] = []
        for response in responses:
            if response.status == 200:
                if response.headers['Content-Type'] == 'application/json':
                    body = json.loads(response.data)
                    if body['Status'] == status:
                        urls.append(furl(body['Location']))
            elif response.status == status:
                urls.append(furl(response.headers['Location']))
        return urls

    def _get_one_mirrorable_file(self,
                                 catalog: CatalogName
                                 ) -> tuple[File, SourceRef, JSON]:
        plugin = self.repository_plugin(catalog)
        with self._public_service_account_credentials:
            # This depends on the indexing test choosing a public source that
            # is not flagged as no_mirror
            outer_file, inner_file = self._get_one_inner_file(catalog)
        # Order matters here because sha256 is present in the file response for
        # AnVIL, but is always set to the empty string
        file_digest = lookup(inner_file, 'file_md5sum', 'sha256')
        source = one(outer_file['sources'])
        # In principle, we could use the entire digest here, but Prefix only
        # allows up to 8 chars because it can be used with UUIDs
        prefix = Prefix(common=file_digest[:8], partition=0)
        source = self._source_from_response(catalog, source)
        # FIXME: Avoid use of plugin, instantiate file from hit instead
        #        https://github.com/DataBiosphere/azul/issues/7615
        files = plugin.list_files(source.with_prefix(prefix), prefix=prefix.common)
        # Multiple files may have the same contents and therefore the same
        # digest. In AnVIL snapshot `CMG_Sample_1_20230225_ANV5_20251203111`,
        # *every* file has the same digest.
        file = first(file for file in files if file.digest.value == file_digest)
        return file, source, inner_file

    def _get_one_inner_file(self, catalog: CatalogName) -> tuple[JSON, JSON]:
        outer_file = self._get_one_outer_file(catalog)
        inner_files: JSONs = outer_file['files']
        inner_file = one(inner_files)
        return outer_file, inner_file

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

    def _source_spec(self, catalog: CatalogName, entity: JSON) -> SourceSpec:
        source = self._source_from_response(catalog, one(entity['sources']))
        return source.spec

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
            outer_file, inner_file = self._get_one_inner_file(catalog)
            source = self._source_spec(catalog, outer_file)
            self._test_dos(catalog, inner_file)
            self._test_drs(catalog, source, inner_file)

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
                        args: Mapping[str, Any] | None = None,
                        endpoint: furl | None = None,
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
        record_schema: MutableJSON = reader.writer_schema
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
        record_fqids = set()
        relations = set()
        for record in reader:
            # Every record must follow the schema. Since each record's `object`
            # field contains an entity, the schema check therefore extends to
            # the various entity types.
            fastavro.validate(record, record_schema)
            object = cast(MutableJSON, record['object'])
            record_id, record_name = record['id'], record['name']
            record_fqids.add((record_name, record_id))
            if len(record_fqids) == 1:
                # PFB requires a special `Metadata` entity to occur first. It is
                # used to declare the relations between entity types, thereby
                # expressing additional constraints on the `relations` field.
                self.assertEqual('Metadata', record_name)
                self.assertIsNone(record_id)
                nodes = cast(MutableJSONs, object['nodes'])
                for node in nodes:
                    for link in node['links']:
                        self.assertIn(link['dst'], entity_types)
            # The following is redundant given the schema validation above but
            # we'll leave it in for illustration.
            fields = entity_types[record_name]['fields']
            fields_present = set(object.keys())
            fields_expected = set(f['name'] for f in fields)
            self.assertEqual(fields_present, fields_expected)
            for relation in cast(MutableJSONs, record['relations']):
                relations.add((relation['dst_name'], relation['dst_id']))
        # We expect to observe the special `Metadata` entity record and at least
        # one additional entity record
        self.assertGreater(len(record_fqids), 1)
        # Terra will reject the handover if a relation references a record that
        # isn't present in the manifest
        self.assertIsSubset(relations, record_fqids)

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
            self.assertTrue(url.startswith('url='), url)
            self.assertTrue(output.startswith('output='), output)
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
            file_url = inner_file['azul_url']
            if file_url:
                source = self._source_spec(catalog, outer_file)
                self._test_file_download(source, inner_file)
            else:
                # Phantom files lack DRS URIs and cannot be downloaded
                self.assertIsNone(file_url, inner_file)
                self.assertEqual('lungmap', config.catalogs[catalog].atlas, inner_file)

    def _test_file_download(self, source: SourceSpec, file: JSON) -> mutable_furl | None:
        file_url = furl(file['azul_url'])
        # FIXME: Use _check_endpoint() instead
        #        https://github.com/DataBiosphere/azul/issues/7373
        self.assertEqual(file_url.path.segments[0], 'repository')
        file_url.path.segments.insert(0, 'fetch')
        response = self._get_url_unchecked(GET, file_url)
        if response.status == 401:
            msg = json.loads(response.data)['Message']
            prefix = 'Unexpected response from '
            self.assertEqual(prefix, msg[:len(prefix)])
            self.assertNotIn(str(config.tdr_service_url), msg)
            return None
        else:
            self.assertEqual(200, response.status)
            response = json.loads(response.data)
            while response['Status'] != 302:
                self.assertEqual(301, response['Status'])
                self.assertNotIn('Retry-After', response)
                response = self._get_url_json(GET, furl(response['Location']))
            self.assertNotIn('Retry-After', response)
            final_file_url = furl(response['Location'])
            response = self._get_url(GET, final_file_url, stream=True)
            self._validate_file_response(response, source, file)
            return final_file_url

    def _file_ext(self, file: JSON) -> str:
        # We believe that the file extension is a more reliable indicator than
        # the `format` metadata field. Note that this method preserves multipart
        # extensions and includes the leading '.', so the extension of
        # "foo.fastq.gz" is ".fastq.gz" instead of "gz"
        file_name = lookup(file, 'file_name', 'name')
        suffixes = PurePath(file_name).suffixes
        return ''.join(suffixes).lower()

    def _validate_file_content(self, content: ReadableFileObject, file: JSON):
        file_ext = self._file_ext(file)
        if file_ext == '.fastq':
            self._validate_fastq_content(content)
        elif file_ext == '.fastq.gz':
            with gzip.open(content) as buf:
                self._validate_fastq_content(buf)
        else:
            file_size = lookup(file, 'file_size', 'size')
            self.assertEqual(1 if file_size > 0 else 0, len(content.read(1)))

    def _validate_file_response(self,
                                response: urllib3.HTTPResponse,
                                source: SourceSpec,
                                file: JSON):
        """
        Note: The response object must have been obtained with stream=True
        """
        try:
            special = 'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732'
            if isinstance(source, TDRSourceSpec) and source.name == special:
                # All files in this snapshot were truncated to zero bytes by the
                # Broad to save costs. The metadata is not a reliable indication
                # of these files' actual size.
                self.assertEqual(response.headers['Content-Length'], '0')
            else:
                self._validate_file_content(response, file)
        finally:
            response.close()

    def _test_drs(self,
                  catalog: CatalogName,
                  source: SourceSpec,
                  file: JSON
                  ) -> None:
        repository_plugin = self.azul_client.repository_plugin(catalog)
        file_uuid = lookup(file, 'document_id', 'uuid')
        drs_uri = f'drs://{config.api_lambda_domain("service")}/{file_uuid}'
        drs_object = repository_plugin.drs_object(drs_uri)
        for access_method in AccessMethod:
            with self.subTest('drs', catalog=catalog, access_method=AccessMethod.https):
                log.info('Resolving file %r with DRS using %r', file_uuid, access_method)
                access = drs_object.get(access_method)
                self.assertIsNone(access.headers)
                if access.method is AccessMethod.https:
                    response = self._get_url(GET, furl(access.url), stream=True)
                    self._validate_file_response(response, source, file)
                elif access.method is AccessMethod.gs:
                    content = self._get_gs_url_content(furl(access.url), size=self.num_fastq_bytes)
                    self._validate_file_content(content, file)
                else:
                    self.fail(access_method)

    def _test_dos(self, catalog: CatalogName, file: JSON):
        with self.subTest('dos', catalog=catalog):
            file_uuid = lookup(file, 'document_id', 'uuid')
            log.info('Resolving file %s with DOS', file_uuid)
            response = self._check_endpoint(method=GET,
                                            path=drs.dos_object_url_path(file_uuid),
                                            args=dict(catalog=catalog))
            json_data = json.loads(response)['data_object']
            file_url = first(json_data['urls'])['azul_url']
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
                            size: int | None = None
                            ) -> BytesIO:
        self.assertEqual('gs', url.scheme)
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
                               catalog: CatalogName,
                               sources: Iterable[SourceRef]
                               ) -> tuple[list[SQSMessage], set[SourcedBundleFQID]]:
        plugin = self.repository_plugin(catalog)
        queue_service = self.index_queue_service
        repository_service = self.index_repository_service
        bundle_fqids, notifications = set(), []
        for source in sources:
            source = plugin.partition_source_for_indexing(catalog, source)
            # Some partitions may be empty, but we include them anyway to
            # ensure test coverage for handling multiple partitions per source
            for prefix in source.prefix.partition_prefixes():
                partition = repository_service.list_bundles(catalog, source, prefix)
                bundle_fqids.update(partition)
                message = queue_service.index_partition_message(catalog, source, prefix)
                notifications.append(message)
        # Index some bundles again to test that we handle duplicate additions.
        # Note: random.choices() may pick the same element multiple times so
        # some notifications may end up being sent three or more times.
        num_duplicates = len(bundle_fqids) // 2
        duplicate_bundles = [
            queue_service.index_bundle_message(catalog, bundle.to_json())
            for bundle in self.random.choices(sorted(bundle_fqids), k=num_duplicates)
        ]
        notifications.extend(duplicate_bundles)
        return notifications, bundle_fqids

    def _source_from_response(self, catalog: CatalogName, source_json: JSON) -> SourceRef:
        special_fields = self.metadata_plugin(catalog).special_fields
        source = dict(id=source_json[special_fields.source_id.name_in_hit],
                      spec=source_json[special_fields.source_spec.name_in_hit],
                      prefix=source_json[special_fields.source_prefix.name_in_hit])
        return self.repository_plugin(catalog).source_ref_cls.from_json(source)

    def _get_indexed_bundles(self,
                             catalog: CatalogName,
                             filters: JSON | None = None
                             ) -> set[SourcedBundleFQID]:
        indexed_fqids = set()
        hits = self._get_entities(catalog, 'bundles', filters)
        special_fields = self.metadata_plugin(catalog).special_fields
        bundle_uuid_field = special_fields.bundle_uuid.name_in_hit
        bundle_version_field = special_fields.bundle_version.name_in_hit
        for hit in hits:
            source, bundle = one(hit['sources']), one(hit['bundles'])
            source = self._source_from_response(catalog, source)
            bundle_fqid = SourcedBundleFQID(
                uuid=bundle[bundle_uuid_field],
                version=bundle[bundle_version_field],
                source=source
            )
            indexed_fqids.add(bundle_fqid)
        return indexed_fqids

    def _assert_catalog_complete(self,
                                 catalog: CatalogName,
                                 bundle_fqids: set[SourcedBundleFQID]
                                 ) -> None:
        with self.subTest('catalog_complete', catalog=catalog):
            expected_fqids = bundle_fqids
            if config.is_anvil_enabled(catalog):
                # Replica bundles do not add contributions to the index and
                # therefore do not appear anywhere in the service response
                # FIXME: Integration test does not assert that replica bundles are indexed
                #        https://github.com/DataBiosphere/azul/issues/6647
                replica_fqids = {
                    bundle_fqid
                    for bundle_fqid in expected_fqids
                    if cast(TDRAnvilBundleFQID, bundle_fqid).table_name not in (
                        BundleType.primary.value,
                        BundleType.supplementary.value,
                        BundleType.duos.value,
                    )
                }
                expected_fqids -= replica_fqids
                log.info('Ignoring replica bundles %r', replica_fqids)
            else:
                service = self.index_repository_service
                expected_fqids = set(service.filter_obsolete_bundle_versions(expected_fqids))
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

    def _assert_queues_empty(self, queue_names: list[str]) -> None:
        for queue_name in queue_names:
            self.assertTrue(self.azul_client.is_queue_empty(queue_name))

    def _get_entities(self,
                      catalog: CatalogName,
                      entity_type: EntityType,
                      filters: JSON | None = None
                      ) -> MutableJSONs:
        entities = []
        indices = self.metadata_plugin(catalog).exposed_indices
        size = min(100, indices[entity_type].max_page_size)
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
                             public_source: SourceRef,
                             ma_source: SourceRef | None,
                             ) -> None:
        with self.subTest('managed_access', catalog=catalog):
            if ma_source is None:
                if config.deployment_stage in ('dev', 'sandbox'):
                    # There should always be at least one managed-access source
                    # indexed and tested on the default catalog for these deployments
                    self.assertNotEqual(catalog, config.it_catalog_for(config.default_catalog))
                self.skipTest(f'No managed access sources found in catalog {catalog!r}')
            with self.subTest('managed_access_indices', catalog=catalog):
                self._test_managed_access_indices(catalog, public_source, ma_source)
            with self.subTest('managed_access_repository_files', catalog=catalog):
                files = self._test_managed_access_repository_files(catalog, ma_source)
                with self.subTest('managed_access_summary', catalog=catalog):
                    self._test_managed_access_summary(catalog, files)
                with self.subTest('managed_access_repository_sources', catalog=catalog):
                    self._test_managed_access_repository_sources(catalog,
                                                                 public_source,
                                                                 ma_source)
                with self.subTest('managed_access_manifest', catalog=catalog):
                    self._test_managed_access_manifest(catalog, files, public_source)

    def _test_managed_access_repository_sources(self,
                                                catalog: CatalogName,
                                                public_source: SourceRef,
                                                ma_source: SourceRef
                                                ) -> None:
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
            self.assertIsSubset({public_source.id, ma_source.id}, list_source_ids())
        with self._public_service_account_credentials:
            public_source_ids = list_source_ids()
            self.assertIn(public_source.id, public_source_ids)
            self.assertNotIn(ma_source.id, public_source_ids)
        with self._unregistered_service_account_credentials:
            self.assertEqual(public_source_ids, list_source_ids())
        self.assertEqual(public_source_ids, list_source_ids())
        invalid_auth = OAuth2('foo')
        with self.assertRaises(UnauthorizedError):
            TDRClient.for_registered_user(invalid_auth)
        invalid_provider = UserCredentialsProvider(invalid_auth)
        invalid_client = OAuth2Client(credentials_provider=invalid_provider)
        with self._authorization_context(invalid_client):
            self.assertEqual(401, self._get_url_unchecked(GET, url).status)

    def _test_managed_access_indices(self,
                                     catalog: CatalogName,
                                     public_source: SourceRef,
                                     ma_source: SourceRef
                                     ) -> JSONs:
        """
        Test the managed-access controls for the /index/bundles and
        /index/projects endpoints

        :return: hits for the managed-access bundles
        """

        special_fields = self.metadata_plugin(catalog).special_fields
        source_id_field = special_fields.source_id.name_in_hit
        accessible_field = special_fields.accessible.name_in_hit
        bundle_type = self._bundle_type(catalog)
        project_type = self._project_type(catalog)

        unfiltered_hits = None
        for accessible in None, False, True:
            with self.subTest(accessible=accessible):
                filters = None if accessible is None else {
                    special_fields.accessible.name: {'is': [accessible]}
                }
                hits = self._get_entities(catalog, project_type, filters=filters)
                if accessible is None:
                    unfiltered_hits = hits
                for hit in hits:
                    source_id = one(hit['sources'])[source_id_field]
                    source_accessible = {public_source.id: True, ma_source.id: False}[source_id]
                    hit_accessible = one(hit[project_type])[accessible_field]
                    self.assertEqual(source_accessible, hit_accessible, hit['entryId'])
                    if accessible is not None:
                        self.assertEqual(accessible, hit_accessible)
        self.assertIsNotNone(unfiltered_hits, 'Cannot recover from subtest failure')

        bundle_fqids = self._get_indexed_bundles(catalog)
        hit_source_ids = {fqid.source.id for fqid in bundle_fqids}
        self.assertEqual(hit_source_ids, {public_source.id})

        source_filter = {
            special_fields.source_id.name: {
                'is': [ma_source.id]
            }
        }
        params = {
            'filters': json.dumps(source_filter),
            'catalog': catalog
        }
        url = config.service_endpoint.set(path=('index', bundle_type), args=params)
        response = self._get_url_unchecked(GET, url)
        self.assertEqual(403, response.status)

        with self._service_account_credentials:
            bundle_fqids = self._get_indexed_bundles(catalog, filters=source_filter)
        hit_source_ids = {fqid.source.id for fqid in bundle_fqids}
        self.assertEqual({ma_source.id}, hit_source_ids)

        return unfiltered_hits

    def _test_managed_access_repository_files(self,
                                              catalog: CatalogName,
                                              ma_source: SourceRef
                                              ) -> JSONs:
        """
        Test the managed access controls for the /repository/files endpoint
        :return: Managed access file hits
        """
        special_fields = self.metadata_plugin(catalog).special_fields
        with self._service_account_credentials:
            files = self._get_entities(catalog, 'files', filters={
                special_fields.source_id.name: {
                    'is': [ma_source.id]
                }
            })
        inner_files = [one(file['files']) for file in files]
        for file in inner_files:
            self.assertIsNone(file['azul_mirror_uri'])
        managed_access_file_urls = {
            file['azul_url']
            for file in inner_files
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
                                      public_source: SourceRef
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
                bundle[special_fields.bundle_uuid.name_in_hit]
                for bundle in hit['bundles']
            }

        managed_access_bundles = set.union(*(
            bundle_uuids(file)
            for file in files
            if len(file['sources']) == 1
        ))
        filters = {special_fields.source_id.name: {'is': [public_source.id]}}
        params = {'size': 1, 'catalog': catalog, 'filters': json.dumps(filters)}
        files_url = furl(url=endpoint, path='index/files', args=params)
        response = self._get_url_json(GET, files_url)
        public_bundle = self.random.choice(sorted(bundle_uuids(one(response['hits']))))
        self.assertNotIn(public_bundle, managed_access_bundles)
        all_bundles = {public_bundle, *managed_access_bundles}

        filters = {
            special_fields.bundle_uuid.name: {
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
                'filters': json.dumps({special_fields.bundle_uuid.name: {'is': list(bundles)}})
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
            filters = {metadata_plugin.special_fields.file_uuid.name: {'is': [managed_access_file_id]}}
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

    def _mirror_service(self, catalog: CatalogName) -> BaseMirrorService:
        return self.azul_client.mirror_service(catalog)

    def _test_mirroring(self, *, delete: bool):
        with self.subTest('mirroring'):
            catalogs = [
                catalog.name
                for catalog in config.catalogs.values()
                if (
                    catalog.is_integration_test_catalog
                    and self._mirror_service(catalog.name).may_mirror()
                )
            ]
            sources_by_catalog = {
                catalog: [self._select_source(catalog, public=True, mirror=True)]
                for catalog in catalogs
            }

            def _delete():
                if delete:
                    # This potentially causes redundant ListObjects requests,
                    # since each IT catalog currently uses the same mirror
                    # prefix and bucket
                    for catalog in catalogs:
                        self._mirror_service(catalog=catalog).delete_it_files()

            self._assert_queues_empty([config.mirror_queue.name,
                                       config.mirror_queue.to_fail.name])
            _delete()

            indexed_files: dict[File, tuple[SourceRef, JSON]] = {}
            with self.subTest('mirror_sources_and_files'):
                for catalog, sources in sources_by_catalog.items():
                    mirror_service = self._mirror_service(catalog)
                    repository_file, source, file_response = self._get_one_mirrorable_file(catalog)
                    indexed_files[repository_file] = source, file_response
                    for _ in range(2):
                        mirror_service.mirror_sources(sources)
                        mirror_service.mirror_file(source, repository_file)
                        self.azul_client.wait_for_mirroring()
                        self._assert_queues_empty([config.mirror_queue.to_fail.name])

                with self.subTest('download_mirrored_files'):
                    for repository_file, (source, file_response) in indexed_files.items():
                        digest = repository_file.digest
                        expected_url = furl(scheme='https', host='s3.amazonaws.com', path=[
                            aws.mirror_bucket, '_it', 'file', f'{digest.value}.{digest.type}',
                        ])
                        actual_url = self._test_file_download(source.spec, file_response)
                        self.assertIsNotNone(actual_url)
                        actual_url.set(args=None)
                        self.assertEqual(expected_url, actual_url)
            _delete()


class AzulClientIntegrationTest(IntegrationTestCase):

    def test_azul_client_error_handling(self):
        invalid_notification = {}
        notifications = [invalid_notification]
        self.assertRaises(AzulClientNotificationError,
                          self.azul_client.index,
                          first(config.integration_test_catalogs),
                          notifications)


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
                url.set(path='/openapi.json')
                response = requests.get(str(url))
                response.raise_for_status()
                spec = response.json()
                validate(spec)


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
        url = str(self.url.copy().set(path='repository/sources',
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
            self._can_bundle(fqid, output_dir=d)
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
                self.assertEqual({'entities', 'links', 'orphans'}, bundle_json.keys())
                entities, links = bundle_json['entities'], bundle_json['links']
                self.assertIsInstance(entities, dict)
                self.assertIsInstance(links, list)
                entities = set(map(EntityReference.parse, entities.keys()))
                for link in map(EntityLink.from_json, links):
                    self.assertGreater(len(link.inputs), 0)
                    self.assertGreater(len(link.outputs), 0)
                    # Since we know the links' inputs and outputs are nonempty,
                    # this also validates that bundles containing only orphans
                    # contain no links.
                    self.assertIsSubset(link.all_entities, entities)
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
                                      mirror_limit=None,
                                      plugins={
                                          'metadata': config.Catalog.Plugin(name='hca'),
                                          'repository': config.Catalog.Plugin(name='canned'),
                                      },
                                      sources={
                                          'https://github.com/HumanCellAtlas/schema-test-data/tree/master/tests': {
                                              'mirror': False
                                          },
                                      })
        with mock.patch.object(Config,
                               'catalogs',
                               new=PropertyMock(return_value={
                                   mock_catalog.name: mock_catalog
                               })):
            self._test_catalog(mock_catalog)

    def bundle_fqid(self, catalog: CatalogName) -> SourcedBundleFQID:
        source, _ = self._select_source(catalog)
        # The plugin will raise an exception if the source lacks a prefix
        source = source.with_prefix(Prefix.of_everything)
        bundle_fqids = self.azul_client.index_repository_service.list_bundles(catalog, source, prefix='')
        return self.random.choice(sorted(bundle_fqids))

    def _can_bundle(self,
                    fqid: SourcedBundleFQID,
                    output_dir: str
                    ) -> None:
        args = [
            '--uuid', fqid.uuid,
            '--version', fqid.version,
            '--source', str(fqid.source.spec),
            *(
                [
                    '--table-name', fqid.table_name,
                    '--batch-prefix', 'null' if fqid.batch_prefix is None else fqid.batch_prefix,
                ]
                if isinstance(fqid, TDRAnvilBundleFQID) else
                []
            ),
            '--output-dir', output_dir,
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
                    response = http.request(GET, str(base_url / 'swagger' / file))
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
            with self.assertRaises(opensearchpy.exceptions.NotFoundError) as cm:
                es.index(index=index_name, body={'foo': 'bar'})
            expected = ('no such index [' + index_name + ']')
            self.assertEqual(expected, cm.exception.args[2]['error']['reason'])
        finally:
            if es.indices.exists(index=index_name):
                es.indices.delete(index=[index_name])


class ResponseHeadersTest(AzulTestCase):

    def test_response_security_headers(self):
        no_cache = 'no-store'
        short_cache = 'public, max-age=60, must-revalidate'
        long_cache = 'public, max-age=86400, must-revalidate'
        test_cases = {
            '/swagger/index.html': long_cache,
            '/swagger/swagger-initializer.js': short_cache,
            '/swagger/swagger-ui.css': long_cache,
            '/openapi.json': short_cache,
            '/health/basic': no_cache
        }
        for endpoint in (config.service_endpoint, config.indexer_endpoint):
            for path, cache_control in test_cases.items():
                with self.subTest(endpoint=endpoint, path=path):
                    response = requests.get(str(endpoint / path))
                    response.raise_for_status()
                    actual_csp = response.headers['Content-Security-Policy']
                    parsed_csp = CSP.parse(actual_csp)
                    parsed_csp.validate()
                    nonce = parsed_csp.nonce()
                    # Currently, we don't expect a CSP nonce in our endpoints.
                    self.assertIs(nonce, None)
                    expected_headers = {
                        # The fact that most headers are hard-coded in
                        # security_headers() gives us license to use that
                        # method here to compose the expected value, even
                        # though it constitutes code under test. There is
                        # not much that can break in that method, and even
                        # if one of the literals in it had an error, that
                        # error would likely be repeated in a literal here.
                        **AzulChaliceApp.security_headers(),
                        'Cache-Control': cache_control,
                        # The random nonce in the actual CSP makes it hard
                        # to compose an expected value for it. Instead, we
                        # parse and validate the actual CSP, then serialize
                        # it again and interpolate the result into the
                        # expected value.
                        'Content-Security-Policy': str(parsed_csp)
                    }
                    self.assertIsSubset(expected_headers.items(), response.headers.items())

    def test_default_4xx_response_headers(self):
        for endpoint in (config.service_endpoint, config.indexer_endpoint):
            with self.subTest(endpoint=endpoint):
                response = requests.get(str(endpoint / 'does-not-exist'))
                self.assertEqual(403, response.status_code)
                self.assertIsSubset(AzulChaliceApp.security_headers().items(),
                                    response.headers.items())
