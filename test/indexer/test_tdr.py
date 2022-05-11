from collections.abc import (
    Iterable,
    Mapping,
)
from datetime import (
    timezone,
)
import json
from operator import (
    attrgetter,
)
from typing import (
    Callable,
)
import unittest
from unittest import (
    mock,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    first,
    one,
    take,
)
from tinyquery import (
    tinyquery,
)
from tinyquery.context import (
    Column,
)
import urllib3
from urllib3 import (
    HTTPResponse,
)

from azul import (
    RequirementError,
    cache,
    cached_property,
    config,
    lru_cache,
)
from azul.auth import (
    OAuth2,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.plugins.repository import (
    tdr,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRSourceRef,
)
from azul.terra import (
    TDRClient,
    TDRSourceSpec,
    TerraClient,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul_test_case import (
    AzulTestCase,
)
from indexer import (
    CannedBundleTestCase,
)


class TestTDRPlugin(CannedBundleTestCase):
    snapshot_id = 'cafebabe-feed-4bad-dead-beaf8badf00d'

    bundle_uuid = '1b6d8348-d6e9-406a-aa6a-7ee886e52bf9'

    mock_service_url = furl('https://azul_tdr_service_url_testing.org')
    partition_prefix_length = 2
    source = f'tdr:test_project:snapshot/snapshot:/{partition_prefix_length}'
    source = TDRSourceRef(id='test_id',
                          spec=TDRSourceSpec.parse(source))

    @cached_property
    def tinyquery(self) -> tinyquery.TinyQuery:
        return tinyquery.TinyQuery()

    @cache
    def plugin_for_source_spec(self, source_spec) -> tdr.Plugin:
        return TestPlugin(sources={source_spec}, tinyquery=self.tinyquery)

    def test_list_bundles(self):
        source = self.source
        current_version = '2001-01-01T00:00:00.000001Z'
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

    @lru_cache
    def _canned_bundle(self, source: TDRSourceRef, uuid: str) -> TDRBundle:
        canned_result = self._load_canned_file_version(uuid=uuid,
                                                       version=None,
                                                       extension='result.tdr')
        manifest, metadata = canned_result['manifest'], canned_result['metadata']
        version = one(e['version'] for e in manifest if e['name'] == 'links.json')
        fqid = TDRBundleFQID(source=source,
                             uuid=uuid,
                             version=version)
        return TDRBundle(fqid=fqid,
                         manifest=manifest,
                         metadata_files=metadata)

    def _make_mock_tdr_tables(self,
                              bundle_fqid: SourcedBundleFQID) -> None:
        tables = self._load_canned_file_version(uuid=bundle_fqid.uuid,
                                                version=None,
                                                extension='tables.tdr')['tables']
        for table_name, table_rows in tables.items():
            self._make_mock_entity_table(bundle_fqid.source.spec,
                                         table_name,
                                         table_rows['rows'])

    def test_fetch_bundle(self):
        bundle = self._canned_bundle(self.source, self.bundle_uuid)
        # Test valid links
        self._test_fetch_bundle(bundle, load_tables=True)

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

    @patch('azul.plugins.repository.tdr.Plugin._find_upstream_bundles')
    def test_subgraph_stitching(self, _mock_find_upstream_bundles):
        downstream_uuid = '4426adc5-b3c5-5aab-ab86-51d8ce44dfbe'
        upstream_uuids = [
            'b0c2c714-45ee-4759-a32b-8ccbbcf911d4',
            'bd4939c1-a078-43bd-8477-99ae59ceb555',
        ]
        # TinyQuery/legacy SQL has no support for BQ Arrays, so it's difficult
        # to test the query in this method.
        _mock_find_upstream_bundles.side_effect = [
            {SourcedBundleFQID(source=self.source,
                               uuid=uuid,
                               version='2020-08-10T21:24:26.174274Z')}
            for uuid in upstream_uuids
        ]
        bundle = self._canned_bundle(self.source, downstream_uuid)
        assert any(e['is_stitched'] for e in bundle.manifest)
        self._test_fetch_bundle(bundle, load_tables=True)
        self.assertEqual(_mock_find_upstream_bundles.call_count,
                         len(upstream_uuids))

    @patch('azul.Config.tdr_service_url',
           new=PropertyMock(return_value=mock_service_url))
    def _test_fetch_bundle(self,
                           test_bundle: TDRBundle,
                           *,
                           load_tables: bool):
        if load_tables:
            self._make_mock_tdr_tables(test_bundle.fqid)
        plugin = self.plugin_for_source_spec(test_bundle.fqid.source.spec)
        emulated_bundle = plugin.fetch_bundle(test_bundle.fqid)

        self.assertEqual(test_bundle.fqid, emulated_bundle.fqid)
        # Manifest and metadata should both be sorted by entity UUID
        self.assertEqual(test_bundle.manifest, emulated_bundle.manifest)
        self.assertEqual(test_bundle.metadata_files, emulated_bundle.metadata_files)

    def _make_mock_entity_table(self,
                                source: TDRSourceSpec,
                                table_name: str,
                                rows: JSONs) -> None:
        schema = self._bq_schema(rows[0])
        columns = {column['name'] for column in schema}

        def dump_row(row: JSON) -> str:
            row_columns = row.keys()
            # TinyQuery's errors are typically not helpful in debugging missing/
            # extra columns in the row JSON.
            assert row_columns == columns, row_columns
            row = {
                column_name: (json.dumps(column_value)
                              if isinstance(column_value, Mapping) else
                              column_value)
                for column_name, column_value in row.items()
            }
            return json.dumps(row)

        self.tinyquery.load_table_from_newline_delimited_json(
            table_name=f'{source.bq_name}.{table_name}',
            schema=json.dumps(schema),
            table_lines=map(dump_row, rows)
        )

    def _bq_schema(self, row: BigQueryRow) -> JSONs:
        return [
            dict(name=k,
                 type='TIMESTAMP' if k == 'version' else 'STRING',
                 mode='NULLABLE')
            for k, v in row.items()
        ]

    def _drs_file_id(self, file_id):
        netloc = config.tdr_service_url.netloc
        return f'drs://{netloc}/v1_{self.snapshot_id}_{file_id}'


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class TestPlugin(tdr.Plugin):
    tinyquery: tinyquery.TinyQuery

    def _run_sql(self, query: str) -> BigQueryRows:
        columns = self.tinyquery.evaluate_query(query).columns
        num_rows = one(set(map(lambda c: len(c.values), columns.values())))
        # Tinyquery returns naive datetime objects from a TIMESTAMP type column,
        # so we manually set the tzinfo back to UTC on these values.
        # https://github.com/Khan/tinyquery/blob/9382b18b/tinyquery/runtime.py#L215
        for key, column in columns.items():
            if column.type == 'TIMESTAMP':
                values = [
                    None if d is None else d.replace(tzinfo=timezone.utc)
                    for d in column.values
                ]
                columns[key] = Column(type=column.type,
                                      mode=column.mode,
                                      values=values)
        for i in range(num_rows):
            yield {k[1]: v.values[i] for k, v in columns.items()}

    def _full_table_name(self, source: TDRSourceSpec, table_name: str) -> str:
        return source.bq_name + '.' + table_name

    def _in(self,
            columns: tuple[str, ...],
            values: Iterable[tuple[str, ...]]
            ) -> str:
        """
        >>> plugin = TestPlugin(sources=set(), tinyquery=None)
        >>> plugin._in(('foo', 'bar'), [('"abc"', '123'), ('"def"', '456')])
        '(foo = "abc" AND bar = 123) OR (foo = "def" AND bar = 456)'
        """
        return ' OR '.join(
            '(' + ' AND '.join(
                f'{column} = {inner_value}'
                for column, inner_value in zip(columns, value)
            ) + ')'
            for value in values
        )


class TestTDRSourceList(AzulTestCase):

    def _mock_snapshots(self, access_token: str) -> JSONs:
        return [{
            'id': 'foo',
            'name': f'{access_token}_snapshot'
        }]

    def _mock_urlopen(self,
                      tdr_client: TDRClient
                      ) -> Callable[..., HTTPResponse]:
        called = False

        def _mock_urlopen(http_client, method, url, *, headers, **kwargs):
            nonlocal called
            self.assertEqual(method, 'GET')
            self.assertEqual(furl(url).remove(query=True),
                             tdr_client._repository_endpoint('snapshots'))
            headers = {k.capitalize(): v for k, v in headers.items()}
            token = headers['Authorization'].split('Bearer ').pop()
            response = HTTPResponse(status=200, body=json.dumps({
                'total': 1,
                'filteredTotal': 1,
                'items': [] if called else self._mock_snapshots(token)
            }))
            called = True
            return response

        return _mock_urlopen

    def test_auth_list_snapshots(self):
        for token in ('mock_token_1', 'mock_token_2'):
            tdr_client = TDRClient.for_registered_user(OAuth2(token))
            expected_snapshots = {
                snapshot['id']: snapshot['name']
                for snapshot in self._mock_snapshots(token)
            }
            # The patching here is deliberately "deep" into the implementation
            # to ensure that the proper authorization headers are being sent
            # when nothing is mocked.
            with mock.patch.object(urllib3.poolmanager.PoolManager,
                                   'urlopen',
                                   new=self._mock_urlopen(tdr_client)):
                self.assertEqual(tdr_client.snapshot_names_by_id(), expected_snapshots)

    def test_list_snapshots_paging(self):
        tdr_client = TDRClient.for_anonymous_user()
        page_size = 100
        snapshots = [
            {'id': str(n), 'name': f'{n}_snapshot'}
            for n in range(page_size * 2 + 2)
        ]
        expected = {
            snapshot['id']: snapshot['name']
            for snapshot in snapshots
        }

        def responses():
            iterator = iter(snapshots)
            while True:
                items = take(page_size, iterator)
                yield HTTPResponse(status=200, body=json.dumps({
                    'total': len(snapshots),
                    'filteredTotal': len(snapshots),
                    'items': list(items)
                }))
                if not items:
                    break

        with mock.patch.object(TerraClient, '_request', side_effect=responses()):
            actual = tdr_client.snapshot_names_by_id()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
