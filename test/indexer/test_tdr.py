from abc import (
    ABCMeta,
    abstractmethod,
)
from io import (
    BytesIO,
)
import json
from operator import (
    attrgetter,
)
from typing import (
    Callable,
    ClassVar,
    Generic,
    Sequence,
    Type,
    TypeVar,
)
from unittest import (
    mock,
)
from unittest.mock import (
    Mock,
    patch,
)

import attr
from furl import (
    furl,
)
from google.api_core.client_options import (
    ClientOptions,
)
from google.auth.credentials import (
    AnonymousCredentials,
)
from google.cloud import (
    bigquery,
)
from more_itertools import (
    first,
    one,
    take,
)
import urllib3

from azul import (
    RequirementError,
    cache,
    config,
)
from azul.auth import (
    OAuth2,
)
from azul.bigquery import (
    BigQueryRow,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.oauth2 import (
    ScopedCredentials,
)
from azul.plugins.repository import (
    tdr_hca,
)
from azul.plugins.repository.tdr import (
    TDRPlugin,
)
from azul.plugins.repository.tdr_hca import (
    TDRBundleFQID,
    TDRHCABundle,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
    TerraClient,
    TerraCredentialsProvider,
)
from azul.types import (
    JSON,
    JSONs,
    reify,
)
from azul_test_case import (
    AzulUnitTestCase,
    TDRTestCase,
)
from docker_container_test_case import (
    DockerContainerTestCase,
)
from indexer import (
    CannedFileTestCase,
    DCP2CannedBundleTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class MockTDRClient(TDRClient):
    netloc: ClassVar[tuple[str, int] | None] = None

    def _bigquery(self, project: str) -> bigquery.Client:
        # noinspection PyArgumentList
        host, port = self.netloc
        options = ClientOptions(api_endpoint=f'http://{host}:{port}')
        # noinspection PyTypeChecker
        return bigquery.Client(project=project,
                               credentials=AnonymousCredentials(),
                               client_options=options)


@attr.s(frozen=True, auto_attribs=True)
class MockCredentials(AnonymousCredentials):
    project_id: str


@attr.s(frozen=True, auto_attribs=True)
class MockCredentialsProvider(TerraCredentialsProvider):
    project_id: str

    def insufficient_access(self, resource: str) -> Exception:
        pass

    def scoped_credentials(self) -> ScopedCredentials:
        # noinspection PyTypeChecker
        return MockCredentials(self.project_id)

    def oauth2_scopes(self) -> Sequence[str]:
        pass


class MockPlugin(TDRPlugin, metaclass=ABCMeta):
    netloc: str
    project_id: str

    @classmethod
    def _tdr(cls):
        credentials_provider = MockCredentialsProvider(cls.project_id)
        tdr = MockTDRClient(credentials_provider=credentials_provider)
        MockTDRClient.netloc = cls.netloc
        return tdr


TDR_PLUGIN = TypeVar('TDR_PLUGIN', bound=TDRPlugin)


class TDRPluginTestCase(TDRTestCase,
                        DockerContainerTestCase,
                        CannedFileTestCase,
                        Generic[TDR_PLUGIN]):

    @classmethod
    @abstractmethod
    def _plugin_cls(cls) -> Type[TDR_PLUGIN]:
        raise NotImplementedError

    @cache
    def plugin_for_source_spec(self, source_spec) -> TDR_PLUGIN:
        # noinspection PyAbstractClass
        class Plugin(MockPlugin, self._plugin_cls()):
            netloc = self.netloc
            project_id = self.source.spec.project

        return Plugin(sources={source_spec})

    netloc: tuple[str, int] | None = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.netloc = cls._create_container(image=config.docker_images['bigquery_emulator'],
                                           platform='linux/amd64',
                                           container_port=9050,
                                           command=[
                                               '--log-level=debug',
                                               '--port=9050',
                                               '--project=' + cls.source.spec.project,
                                               '--dataset=' + cls.source.spec.bq_name
                                           ])

    def _make_mock_tdr_tables(self,
                              bundle_fqid: SourcedBundleFQID) -> None:
        tables = self._load_canned_file_version(uuid=bundle_fqid.uuid,
                                                version=None,
                                                extension='tables.tdr')['tables']
        for table_name, table_rows in tables.items():
            self._make_mock_entity_table(bundle_fqid.source.spec,
                                         table_name,
                                         table_rows['rows'])

    def _make_mock_entity_table(self,
                                source: TDRSourceSpec,
                                table_name: str,
                                rows: JSONs) -> None:
        schema = self._bq_schema(rows[0])
        columns = {column.name for column in schema}
        json_type = reify(JSON)

        def dump_row(row: JSON) -> JSON:
            row_columns = row.keys()
            assert row_columns == columns, row_columns
            return {
                column_name: (
                    json.dumps(column_value)
                    if isinstance(column_value, json_type) else
                    column_value
                )
                for column_name, column_value in row.items()
            }

        plugin = self.plugin_for_source_spec(source)
        bq = plugin.tdr._bigquery(source.project)
        table_name = plugin._full_table_name(source, table_name)
        # https://youtrack.jetbrains.com/issue/PY-50178
        # noinspection PyTypeChecker
        table = bigquery.Table(table_name, schema)
        bq.create_table(table=table)
        self.addCleanup(bq.delete_table, table)
        bq.insert_rows(table=table, selected_fields=schema, rows=map(dump_row, rows))

    def _bq_schema(self, row: BigQueryRow) -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(name=k, field_type='TIMESTAMP' if k == 'version' else 'STRING')
            for k, v in row.items()
        ]


class TestTDRHCAPlugin(DCP2CannedBundleTestCase,
                       TDRPluginTestCase[tdr_hca.Plugin]):

    @classmethod
    def _plugin_cls(cls) -> Type[tdr_hca.Plugin]:
        return tdr_hca.Plugin

    bundle_fqid = SourcedBundleFQID(source=TDRPluginTestCase.source,
                                    uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                                    version='2019-09-24T09:35:06.958773Z')

    def test_list_bundles(self):
        source = self.source
        current_version = '2001-01-01T00:00:00.100001Z'
        links_ids = ['42-abc', '42-def', '42-ghi', '86-xyz']
        self._make_mock_entity_table(source=source.spec,
                                     table_name='links',
                                     rows=[
                                         dict(links_id=links_id,
                                              version=current_version,
                                              content={})
                                         for links_id in links_ids
                                     ])
        plugin = self.plugin_for_source_spec(source.spec)
        bundle_ids = plugin.list_bundles(source, prefix='42')
        bundle_ids.sort(key=attrgetter('uuid'))
        self.assertEqual(bundle_ids, [
            TDRBundleFQID(source=source, uuid='42-abc', version=current_version),
            TDRBundleFQID(source=source, uuid='42-def', version=current_version),
            TDRBundleFQID(source=source, uuid='42-ghi', version=current_version)
        ])

    def test_fetch_bundle(self):
        bundle = self._load_canned_bundle(self.bundle_fqid)
        # Test valid links
        self._test_fetch_bundle(bundle, load_tables=True)

        # FIXME: Rewrite this part to use BigQuery DML statements
        #        https://github.com/DataBiosphere/azul/issues/5046
        if False:
            self._test_invalid_links(bundle)

    def _test_invalid_links(self, bundle: TDRHCABundle):
        # Directly modify the canned tables to test invalid links not present
        # in the canned bundle.
        dataset = self.source.spec.bq_name
        links_table = self.tinyquery.tables_by_name[dataset + '.links']
        links_content_column = links_table.columns['content'].values
        links_content = json.loads(one(links_content_column))
        link = first(link
                     for link in links_content['links']
                     if link['link_type'] == 'supplementary_file_link')
        # Test invalid entity_type in supplementary_file_link
        assert link['entity']['entity_type'] == 'project'
        link['entity']['entity_type'] = 'cell_suspension'
        # Update table
        links_content_column[0] = json.dumps(links_content)
        # Invoke code under test
        with self.assertRaises(RequirementError):
            self._test_fetch_bundle(bundle,
                                    load_tables=False)  # Avoid resetting tables to canned state

        # Undo previous change
        link['entity']['entity_type'] = 'project'
        # Test invalid entity_id in supplementary_file_link
        link['entity']['entity_id'] += '_wrong'
        # Update table
        links_content_column[0] = json.dumps(links_content)
        # Invoke code under test
        with self.assertRaises(RequirementError):
            self._test_fetch_bundle(bundle, load_tables=False)

    @patch('azul.plugins.repository.tdr_hca.Plugin._find_upstream_bundles')
    def test_subgraph_stitching(self, _mock_find_upstream_bundles):
        downstream_uuid = '4426adc5-b3c5-5aab-ab86-51d8ce44dfbe'
        upstream_uuids = [
            'b0c2c714-45ee-4759-a32b-8ccbbcf911d4',
            'bd4939c1-a078-43bd-8477-99ae59ceb555',
        ]
        # FIXME: Fix the crash in bigquery-emulator and remove the mock
        #        https://github.com/DataBiosphere/azul/issues/5847
        _mock_find_upstream_bundles.side_effect = [
            {SourcedBundleFQID(source=self.source,
                               uuid=uuid,
                               version='2020-08-10T21:24:26.174274Z')}
            for uuid in upstream_uuids
        ]
        bundle = self._load_canned_bundle(SourcedBundleFQID(source=self.source,
                                                            uuid=downstream_uuid,
                                                            version='2020-08-10T21:24:26.174274Z'))
        assert any(e['is_stitched'] for e in bundle.manifest)
        self._test_fetch_bundle(bundle, load_tables=True)
        self.assertEqual(_mock_find_upstream_bundles.call_count,
                         len(upstream_uuids))

    def _test_fetch_bundle(self,
                           test_bundle: TDRHCABundle,
                           *,
                           load_tables: bool):
        if load_tables:
            self._make_mock_tdr_tables(test_bundle.fqid)
        plugin = self.plugin_for_source_spec(test_bundle.fqid.source.spec)
        emulated_bundle = plugin.fetch_bundle(test_bundle.fqid)

        self.assertEqual(test_bundle.fqid, emulated_bundle.fqid)
        assert isinstance(emulated_bundle, TDRHCABundle)
        # Manifest and metadata should both be sorted by entity UUID
        self.assertEqual(test_bundle.manifest, emulated_bundle.manifest)
        self.assertEqual(test_bundle.metadata_files, emulated_bundle.metadata_files)


class TestTDRSourceList(AzulUnitTestCase):

    def _mock_snapshots(self, access_token: str) -> JSONs:
        return [{
            'id': 'foo',
            'name': f'{access_token}_snapshot'
        }]

    def _mock_tdr_enumerate_snapshots(self,
                                      tdr_client: TDRClient
                                      ) -> Callable[..., urllib3.HTTPResponse]:
        called = False

        def _mock_urlopen(_http_client, method, url, *, headers, **_kwargs):
            nonlocal called
            self.assertEqual(method, 'GET')
            self.assertEqual(furl(url).remove(query=True),
                             tdr_client._repository_endpoint('snapshots'))
            headers = {k.capitalize(): v for k, v in headers.items()}
            token = headers['Authorization'].split('Bearer ').pop()
            body = json.dumps({
                'total': 1,
                'filteredTotal': 1,
                'items': [] if called else self._mock_snapshots(token)
            }).encode()
            response = urllib3.HTTPResponse(status=200, body=BytesIO(body))
            called = True
            return response

        return _mock_urlopen

    def _mock_google_oauth_tokeninfo(self):
        body = json.dumps({'azp': config.google_oauth2_client_id}).encode()
        response = urllib3.HTTPResponse(status=200, body=BytesIO(body))
        mock_urlopen = Mock()
        mock_urlopen.return_value = response
        return mock_urlopen

    def _patch_urlopen(self, **kwargs):
        return mock.patch.object(target=urllib3.poolmanager.PoolManager,
                                 attribute='urlopen',
                                 **kwargs)

    def _patch_client_id(self):
        return patch.object(type(config),
                            'google_oauth2_client_id',
                            '123-foobar.apps.googleusercontent.com')

    def test_auth_list_snapshots(self):
        for token in ('mock_token_1', 'mock_token_2'):
            with self._patch_client_id():
                with self._patch_urlopen(new=self._mock_google_oauth_tokeninfo()):
                    tdr_client = TDRClient.for_registered_user(OAuth2(token))
            expected_snapshots = {
                snapshot['id']: snapshot['name']
                for snapshot in self._mock_snapshots(token)
            }
            # The patching here is deliberately "deep" into the implementation
            # to ensure that the proper authorization headers are being sent
            # when nothing is mocked.
            with self._patch_urlopen(new=self._mock_tdr_enumerate_snapshots(tdr_client)):
                self.assertEqual(tdr_client.snapshot_names_by_id(), expected_snapshots)

    def test_list_snapshots_paging(self):
        for page_size in [1, 10]:
            for num_full_pages in [0, 1, 2]:
                for last_page_size in [0, 1, 2]:
                    for filter in (None, 'snapshot'):
                        with self.subTest(page_size=page_size,
                                          num_full_pages=num_full_pages,
                                          last_page_size=last_page_size,
                                          filter=filter):
                            tdr_client = TDRClient.for_anonymous_user()
                            page_size = 1000
                            snapshots = [
                                {'id': str(n), 'name': f'snapshot_{n}'}
                                for n in range(page_size * num_full_pages + last_page_size)
                            ]
                            expected = {
                                snapshot['id']: snapshot['name']
                                for snapshot in snapshots
                            }

                            def responses():
                                iterator = iter(snapshots)
                                while True:
                                    items = take(page_size, iterator)
                                    body = json.dumps({
                                        'total': len(snapshots) + (0 if filter is None else 42),
                                        'filteredTotal': len(snapshots),
                                        'items': list(items)
                                    }).encode()
                                    yield urllib3.HTTPResponse(status=200, body=BytesIO(body))
                                    if not items:
                                        break

                            with mock.patch.object(TDRClient, 'page_size', new=page_size):
                                self.assertEqual(page_size, tdr_client.page_size)
                                with mock.patch.object(TerraClient, '_request') as _request:
                                    _request.side_effect = responses()
                                    actual = tdr_client.snapshot_names_by_id(filter=filter)
                            self.assertEqual(expected, actual)
                            num_expected_calls = max(1, num_full_pages + (1 if last_page_size else 0))
                            self.assertEqual(num_expected_calls, _request.call_count)
                            for call in _request.mock_calls:
                                method, url = call.args
                                assert isinstance(url, furl)
                                if filter:
                                    self.assertEqual('snapshot', url.args['filter'])
                                else:
                                    self.assertNotIn('filter', url.args)
