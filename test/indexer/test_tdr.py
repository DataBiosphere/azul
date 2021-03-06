import json
from operator import (
    attrgetter,
)
import unittest
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
)
from tinyquery import (
    tinyquery,
)

from azul import (
    RequirementError,
    cached_property,
    config,
    lru_cache,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
)
from azul.indexer import (
    BundleFQID,
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
    TDRSourceName,
)
from azul.types import (
    JSONs,
)
from indexer import (
    CannedBundleTestCase,
)


class TestTDRPlugin(CannedBundleTestCase):
    snapshot_id = 'cafebabe-feed-4bad-dead-beaf8badf00d'

    bundle_uuid = '1b6d8348-d6e9-406a-aa6a-7ee886e52bf9'

    mock_service_url = 'https://azul_tdr_service_url_testing.org'

    source = TDRSourceRef(id='test_id',
                          name=TDRSourceName(project='test_project',
                                             name='snapshot',
                                             is_snapshot=True))

    @cached_property
    def tinyquery(self) -> tinyquery.TinyQuery:
        return tinyquery.TinyQuery()

    def test_list_bundles(self):
        source = self.source
        current_version = '2001-01-01T00:00:00.000001Z'
        links_ids = ['42-abc', '42-def', '42-ghi', '86-xyz']
        self._make_mock_entity_table(source=source.name,
                                     table_name='links',
                                     rows=[
                                         dict(links_id=links_id,
                                              version=current_version,
                                              content='{}')
                                         for links_id in links_ids
                                     ])
        plugin = TestPlugin(sources={source.name}, tinyquery=self.tinyquery)
        bundle_ids = plugin.list_bundles(source, prefix='42')
        bundle_ids.sort(key=attrgetter('uuid'))
        self.assertEqual(bundle_ids, [
            TDRBundleFQID(source=source, uuid='42-abc', version=current_version),
            TDRBundleFQID(source=source, uuid='42-def', version=current_version),
            TDRBundleFQID(source=source, uuid='42-ghi', version=current_version)
        ])

    @lru_cache
    def _canned_bundle(self, source: TDRSourceRef) -> TDRBundle:
        canned_result = self._load_canned_file_version(uuid=self.bundle_uuid,
                                                       version=None,
                                                       extension='result.tdr')
        manifest, metadata = canned_result['manifest'], canned_result['metadata']
        version = one(e['version'] for e in manifest if e['name'] == 'links.json')
        fqid = TDRBundleFQID(source=source,
                             uuid=self.bundle_uuid,
                             version=version)
        return TDRBundle(fqid=fqid,
                         manifest=manifest,
                         metadata_files=metadata)

    def _make_mock_tdr_tables(self,
                              source: TDRSourceName,
                              bundle_fqid: BundleFQID) -> None:
        tables = self._load_canned_file_version(uuid=bundle_fqid.uuid,
                                                version=None,
                                                extension='tables.tdr')['tables']
        for table_name, table_rows in tables.items():
            self._make_mock_entity_table(source, table_name, table_rows['rows'])

    def test_fetch_bundle(self):
        # Test valid links
        self._test_fetch_bundle(self.source, load_tables=True)

        # Directly modify the canned tables to test invalid links not present
        # in the canned bundle.
        dataset = self.source.name.bq_name
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
            self._test_fetch_bundle(self.source,
                                    load_tables=False)  # Avoid resetting tables to canned state

        # Undo previous change
        link['entity']['entity_type'] = 'project'
        # Test invalid entity_id in supplementary_file_link
        link['entity']['entity_id'] += '_wrong'
        # Update table
        links_content_column[0] = json.dumps(links_content)
        # Invoke code under test
        with self.assertRaises(RequirementError):
            self._test_fetch_bundle(self.source, load_tables=False)

    @patch('azul.Config.tdr_service_url',
           new=PropertyMock(return_value=mock_service_url))
    def _test_fetch_bundle(self,
                           source: TDRSourceRef,
                           *,
                           load_tables: bool):
        test_bundle = self._canned_bundle(source)
        if load_tables:
            self._make_mock_tdr_tables(source.name, test_bundle.fqid)
        plugin = TestPlugin(sources={source.name}, tinyquery=self.tinyquery)
        emulated_bundle = plugin.fetch_bundle(test_bundle.fqid)

        self.assertEqual(test_bundle.fqid, emulated_bundle.fqid)
        # Manifest and metadata should both be sorted by entity UUID
        self.assertEqual(test_bundle.manifest, emulated_bundle.manifest)
        self.assertEqual(test_bundle.metadata_files, emulated_bundle.metadata_files)

    def _make_mock_entity_table(self,
                                source: TDRSourceName,
                                table_name: str,
                                rows: JSONs) -> None:
        schema = self._bq_schema(rows[0])
        columns = {column['name'] for column in schema}
        # TinyQuery's errors are typically not helpful in debugging missing/
        # extra columns in the row JSON.
        for row in rows:
            row_columns = row.keys()
            assert row_columns == columns, row_columns
        self.tinyquery.load_table_from_newline_delimited_json(
            table_name=f'{source.bq_name}.{table_name}',
            schema=json.dumps(schema),
            table_lines=map(json.dumps, rows)
        )

    def _bq_schema(self, row: BigQueryRow) -> JSONs:
        return [
            dict(name=k,
                 type='TIMESTAMP' if k == 'version' else 'STRING',
                 mode='NULLABLE')
            for k, v in row.items()
        ]

    def _drs_file_id(self, file_id):
        netloc = furl(config.tdr_service_url).netloc
        return f'drs://{netloc}/v1_{self.snapshot_id}_{file_id}'


@attr.s(kw_only=True, auto_attribs=True, frozen=True)
class TestPlugin(tdr.Plugin):
    tinyquery: tinyquery.TinyQuery

    def _run_sql(self, query: str) -> BigQueryRows:
        columns = self.tinyquery.evaluate_query(query).columns
        num_rows = one(set(map(lambda c: len(c.values), columns.values())))
        for i in range(num_rows):
            yield {k[1]: v.values[i] for k, v in columns.items()}

    def _full_table_name(self, source: TDRSourceName, table_name: str) -> str:
        return source.bq_name + '.' + table_name


if __name__ == '__main__':
    unittest.main()
