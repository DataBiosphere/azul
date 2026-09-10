from abc import (
    abstractmethod,
)
from io import (
    BytesIO,
)
import json
import logging
from operator import (
    attrgetter,
)
from typing import (
    Any,
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
from google.cloud._helpers import (
    _datetime_from_microseconds,
)
from google.cloud.bigquery._helpers import (
    CellDataParser,
    _not_null,
)
from google.oauth2.service_account import (
    Credentials as ServiceAccountCredentials,
)
from more_itertools import (
    first,
    one,
    take,
)
import urllib3

from azul import (
    config,
)
from azul.auth import (
    AccessTokenAuthentication,
)
from azul.docker import (
    resolve_docker_image_for_launch,
)
from azul.lib import (
    cached_property,
)
from azul.lib.bigquery import (
    BigQueryRow,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    JSONs,
    reify,
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
    TDRSourceRef,
    log as plugin_log,
)
from azul.source import (
    Prefix,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
    TerraClient,
    TerraCredentialsProvider,
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

    def _job_has_result(self, job_info: JSON) -> bool:
        # Some queries (such as the one in Plugin._downstream_from_files()) can
        # produce job stats that are empty in the test environment.
        return True


@attr.s(frozen=True, auto_attribs=True)
class MockCredentials(ServiceAccountCredentials):
    _project_id: str


@attr.s(frozen=True, auto_attribs=True)
class MockCredentialsProvider(TerraCredentialsProvider):
    project_id: str

    def insufficient_access(self, resource: str) -> Exception:
        pass

    def scoped_credentials(self) -> ScopedCredentials:
        return MockCredentials(self.project_id)

    def oauth2_scopes(self) -> Sequence[str]:
        pass


TDR_PLUGIN = TypeVar('TDR_PLUGIN', bound=TDRPlugin)


class TDRPluginTestCase(TDRTestCase,
                        DockerContainerTestCase,
                        CannedFileTestCase,
                        Generic[TDR_PLUGIN]):

    @classmethod
    @abstractmethod
    def _plugin_cls(cls) -> Type[TDR_PLUGIN]:
        raise NotImplementedError

    @classmethod
    def _patch_tdr_client(cls):
        source = cls.source.ref.spec
        credentials_provider = MockCredentialsProvider(project_id=source.subdomain)
        tdr = MockTDRClient(credentials_provider=credentials_provider)
        assert cls.netloc is not None
        MockTDRClient.netloc = cls.netloc
        cls.addClassPatch(mock.patch.object(TDRPlugin, '_tdr', return_value=tdr))

    @cached_property
    def plugin(self) -> TDR_PLUGIN:
        plugin_cls = self._plugin_cls()
        return plugin_cls(catalog=self.catalog)

    netloc: tuple[str, int] | None = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        image = resolve_docker_image_for_launch('bigquery_emulator')
        cls.netloc = cls._create_container(image=image,
                                           container_port=9050,
                                           command=[
                                               '--log-level=debug',
                                               '--port=9050',
                                               '--project=' + cls.source.ref.spec.subdomain,
                                               '--dataset=' + cls.source.ref.spec.name
                                           ])
        cls._patch_tdr_client()
        cls._patch_timestamp_conversion()

    @classmethod
    def _patch_timestamp_conversion(cls):
        """
        BigQueryEmulator returns a TIMESTAMP column as a string with the decimal
        representation of a real number of seconds since 1970 with microsecond
        precision in the decimal part. It does so even though the BigQuery
        Python API specifies the 'formatOptions.useInt64Timestamp' option along
        most requests which should trigger these columns to be returned as a
        string with the decimal representation of a 64-bit integer number of
        microseconds since 1970. The fact that the emulator seems to ignore that
        option breaks the API when it's trying to parse the string.

        https://github.com/goccy/bigquery-emulator/issues/32#issuecomment-2661687330
        """

        def timestamp_to_py(self, value, field):
            if _not_null(value, field):
                us = cls._float_seconds_to_int_microseconds(value)
                return _datetime_from_microseconds(us)
            else:
                return None

        cls.addClassPatch(mock.patch.object(CellDataParser,
                                            'timestamp_to_py',
                                            new=timestamp_to_py))

    @classmethod
    def _float_seconds_to_int_microseconds(cls, value) -> int | Any:
        """
        >>> f = TDRPluginTestCase._float_seconds_to_int_microseconds
        >>> f('0')
        0

        >>> f('1769204298.1')
        1769204298100000

        >>> f('1769204298.999999')
        1769204298999999

        Loss of microsecond precision in year 2242 (note last digit)

        >>> f('8589934592.000001')
        8589934592000002
        """
        return int(float(value) * 10 ** 6)

    def _make_mock_tables(self, source: TDRSourceRef) -> None:
        can = self._load_canned_file_version(uuid=source.id,
                                             version=None,
                                             extension='tables.tdr')
        for name, table in can['tables'].items():
            self._make_mock_table(source.spec, name, table['rows'], table.get('schema'))

    def _make_mock_table(self,
                         source: TDRSourceSpec,
                         table_name: str,
                         rows: JSONs,
                         canned_schema: JSON | None = None
                         ) -> None:
        if canned_schema is None:
            assert len(rows) > 0, 'Empty tables require an explicit schema'
            schema = self._bq_schema_from_row(rows[0])
        else:
            schema = [
                bigquery.SchemaField(**column_schema)
                for column_schema in canned_schema['columns']
            ]

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

        plugin = self.plugin
        bq = plugin.tdr._bigquery(source.subdomain)
        table_name = plugin._full_table_name(source, table_name)
        # https://youtrack.jetbrains.com/issue/PY-50178
        # noinspection PyTypeChecker
        table = bigquery.Table(table_name, schema)
        bq.create_table(table=table)
        self.addCleanup(bq.delete_table, table)
        bq.insert_rows(table=table, selected_fields=schema, rows=map(dump_row, rows))

    def _bq_schema_from_row(self, row: BigQueryRow) -> list[bigquery.SchemaField]:
        def field_type(key: str, value: AnyJSON) -> str:
            if key == 'version':
                return 'TIMESTAMP'
            elif isinstance(value, bool):
                return 'BOOLEAN'
            elif isinstance(value, int):
                return 'INTEGER'
            else:
                return 'STRING'

        return [
            bigquery.SchemaField(name=k,
                                 field_type=field_type(k, v),
                                 mode='REPEATED' if isinstance(v, list) else 'NULLABLE')
            for k, v in row.items()
        ]


class TestTDRHCAPlugin(DCP2CannedBundleTestCase,
                       TDRPluginTestCase[tdr_hca.Plugin]):

    @classmethod
    def _plugin_cls(cls) -> Type[tdr_hca.Plugin]:
        return tdr_hca.Plugin

    def test_list_and_count_bundles(self):
        source = self.source.ref
        current_version = '2001-01-01T00:00:00.100001Z'
        links_ids = ['42-abc', '42-def', '42-ghi', '86-xyz']
        self._make_mock_table(source=source.spec,
                              table_name='links',
                              rows=[
                                  dict(links_id=links_id,
                                       version=current_version,
                                       content={})
                                  for links_id in links_ids
                              ])
        expected_bundle_ids = [
            TDRBundleFQID(source=source, uuid='42-abc', version=current_version),
            TDRBundleFQID(source=source, uuid='42-def', version=current_version),
            TDRBundleFQID(source=source, uuid='42-ghi', version=current_version)
        ]
        with self.subTest('list_bundles'):
            actual_bundle_ids = self.plugin.list_bundles(source, prefix='42')
            actual_bundle_ids.sort(key=attrgetter('uuid'))
            self.assertEqual(expected_bundle_ids, actual_bundle_ids)
        with self.subTest('count_bundles_unpartitioned'):
            unpartitioned_src = attr.evolve(source, prefix=None)
            actual_bundle_count = self.plugin.count_bundles(unpartitioned_src)
            self.assertEqual(len(links_ids), actual_bundle_count)
        with self.subTest('count_bundles_partitioned'):
            partitioned_src = attr.evolve(source, prefix=Prefix.parse('42/0'))
            actual_bundle_count = self.plugin.count_bundles(partitioned_src)
            self.assertEqual(len(expected_bundle_ids), actual_bundle_count)

    def test_list_and_count_files(self):
        source = self.source.ref
        self._make_mock_tables(source)
        tables = self._load_canned_file_version(uuid=source.id,
                                                version=None,
                                                extension='tables.tdr')['tables']
        all_files = [
            row['descriptor']
            for table_name, table_contents in tables.items()
            if table_name.endswith('_file')
            for row in table_contents['rows']
        ]
        prefix = '3'
        partition_file_uuids = [
            file['file_id']
            for file in all_files
            if file['sha256'].startswith(prefix)
        ]
        self.assertGreater(len(partition_file_uuids), 0, 'Empty prefix')
        with mock.patch.object(tdr_hca.Plugin, '_assert_partition'):
            with self.subTest('list_files'):
                actual_file_uuids = [
                    file.uuid
                    for file in self.plugin.list_files(source, prefix=prefix)
                ]
                self.assertEqual(partition_file_uuids, actual_file_uuids)
        with self.subTest('count_files_unpartitioned'):
            unpartitioned_src = attr.evolve(source, prefix=None)
            actual_file_count = self.plugin.count_files(unpartitioned_src)
            self.assertEqual(len(all_files), actual_file_count)
        with self.subTest('count_files_partitioned'):
            partitioned_src = attr.evolve(source, prefix=Prefix.parse(f'{prefix}/0'))
            actual_file_count = self.plugin.count_files(partitioned_src)
            self.assertEqual(len(partition_file_uuids), actual_file_count)

    def test_fetch_bundle(self):
        fqid = self.bundle_fqid(uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                                version='2019-09-24T09:35:06.958773Z')
        bundle = self._load_canned_bundle(fqid)
        # Test valid links
        self._test_fetch_bundle(bundle, load_tables=True)
        # Test invalid links by modifying the canned bundle
        spec = self.source.ref.spec
        plugin = self.plugin
        links_id = bundle.uuid
        links = one(plugin.tdr.run_sql(f'''
            SELECT links_id, content
            FROM {plugin._full_table_name(spec, 'links')}
            WHERE links_id = {links_id!r}
        '''))
        links_content = json.loads(links['content'])
        link = first(
            link
            for link in links_content['links']
            if link['link_type'] == 'supplementary_file_link'
        )
        linked_entity = link['entity']
        assert linked_entity['entity_type'] == 'project', linked_entity
        bad_link_fields = [
            {'entity_type': 'cell_suspension'},
            {'entity_id': linked_entity['entity_id'] + '_wrong'}
        ]
        for field in bad_link_fields:
            link['entity'] = linked_entity | field
            # Update table with invalid link
            plugin.tdr.run_sql(f'''
                UPDATE {plugin._full_table_name(spec, 'links')}
                SET content = {json.dumps(links_content)!r}
                WHERE links_id = "{links_id}"
            ''')
            # Invoke code under test
            with self.assertRaises(AssertionError):
                self._test_fetch_bundle(bundle,
                                        load_tables=False)  # Avoid resetting tables to canned state

    def test_subgraph_stitching(self):
        downstream_uuid = '4426adc5-b3c5-5aab-ab86-51d8ce44dfbe'
        upstream_uuids = [
            'b0c2c714-45ee-4759-a32b-8ccbbcf911d4',
            'bd4939c1-a078-43bd-8477-99ae59ceb555',
        ]
        fqid = self.bundle_fqid(uuid=downstream_uuid,
                                version='2020-08-10T21:24:26.174274Z')
        bundle = self._load_canned_bundle(fqid)
        assert len(bundle.stitched) > 0
        with self.assertLogs(plugin_log, level=logging.DEBUG) as cm:
            self._test_fetch_bundle(bundle, load_tables=True)
        record = one(r for r in cm.records if 'Stitched 2 bundle(s): ' in r.message)
        for upstream_uuid in upstream_uuids:
            self.assertIn("uuid='" + upstream_uuid, record.message)

    def _test_fetch_bundle(self,
                           test_bundle: TDRHCABundle,
                           *,
                           load_tables: bool):
        if load_tables:
            self._make_mock_tables(test_bundle.fqid.source)
        emulated_bundle = self.plugin.fetch_bundle(test_bundle.fqid)

        self.assertEqual(test_bundle.fqid, emulated_bundle.fqid)
        assert isinstance(emulated_bundle, TDRHCABundle)
        # Manifest and metadata should both be sorted by entity UUID
        self.assertEqual(test_bundle.manifest, emulated_bundle.manifest)
        self.assertEqual(test_bundle.metadata, emulated_bundle.metadata)
        self.assertEqual(test_bundle.links, emulated_bundle.links)


class TestTDRSourceList(AzulUnitTestCase):

    def _mock_snapshots(self, access_token: str) -> JSONs:
        return [{
            'id': 'foo',
            'name': f'{access_token}_snapshot',
            'dataProject': 'mock_project'
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
        body = json.dumps({
            'azp': config.google_oauth2_client_id,
            'aud': config.google_oauth2_client_id,
            'sub': config.google_oauth2_client_id.split('.')[0].split('-')[0],
            'scope': 'https://www.googleapis.com/auth/userinfo.email openid',
            'exp': '1689645319',
            'expires_in': '3511',
            'email': 'hannes@ucsc.edu',
            'email_verified': 'true',
            'access_type': 'online'
        }).encode()
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
        for token in ('ya29.mock_token_1', 'ya29.mock_token_2'):
            with self._patch_client_id():
                with self._patch_urlopen(new=self._mock_google_oauth_tokeninfo()):
                    tdr_client = TDRClient.for_registered_user(AccessTokenAuthentication(token))
            expected_snapshots = {
                snapshot['id']: snapshot
                for snapshot in self._mock_snapshots(token)
            }
            # The patching here is deliberately "deep" into the implementation
            # to ensure that the proper authorization headers are being sent
            # when nothing is mocked.
            with self._patch_urlopen(new=self._mock_tdr_enumerate_snapshots(tdr_client)):
                self.assertEqual(tdr_client.list_snapshots(), expected_snapshots)

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
                                {
                                    'id': str(n),
                                    'name': f'snapshot_{n}',
                                    'dataProject': 'mock-project'
                                }
                                for n in range(page_size * num_full_pages + last_page_size)
                            ]
                            expected = {snapshot['id']: snapshot for snapshot in snapshots}

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
                                    actual = tdr_client.list_snapshots(filter=filter)
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
