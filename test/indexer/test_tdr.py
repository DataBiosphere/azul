import json
from operator import (
    attrgetter,
)
from typing import (
    Dict,
    Set,
)
import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)
import uuid

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
    require,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
)
from azul.indexer import (
    BundleFQID,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityReference,
)
from azul.plugins.repository import (
    tdr,
)
from azul.plugins.repository.tdr import (
    Entities,
    TDRBundle,
    TDRBundleFQID,
    TDRSourceRef,
    link_sort_key,
)
from azul.terra import (
    TDRSourceName,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSONs,
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
        bundle = self._canned_bundle(self.source)
        self._test_fetch_bundle(bundle, load_tables=True)

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

    @patch('azul.Config.tdr_service_url',
           new=PropertyMock(return_value=mock_service_url))
    def _test_fetch_bundle(self,
                           bundle: TDRBundle,
                           *,
                           load_tables: bool):
        source = bundle.fqid.source
        if load_tables:
            self._make_mock_tdr_tables(source.name, bundle.fqid)
        plugin = TestPlugin(sources={source.name}, tinyquery=self.tinyquery)
        emulated_bundle = plugin.fetch_bundle(bundle.fqid)

        self.assertEqual(bundle.fqid, emulated_bundle.fqid)
        # Manifest and metadata should both be sorted by entity UUID
        self.assertEqual(bundle.manifest, emulated_bundle.manifest)
        self.assertEqual(bundle.metadata_files, emulated_bundle.metadata_files)

    def test_subgraph_stitching(self):
        # Since the canned bundle is a single self-contained bundle, it can't be
        # used to test stitching as-is. This test extracts links producing file
        # outputs from the canned bundle, inverts them so that the files are
        # instead accepted as (dangling) inputs, synthesizes upstream
        # bundles that emit the files as outputs, and stitches these back
        # together.

        def replace_links(bundle: TDRBundle, new_links: MutableJSONs):
            content = bundle.metadata_files['links.json']
            content['links'] = new_links
            one(
                e for e in bundle.manifest if e['name'] == 'links.json'
            )['size'] = len(json.dumps(content))

        def invert_link(link: JSON):
            # Switch inputs and outputs.
            return dict(link, **{
                f'{to}s': [
                    {
                        f'{to}_{k}': ref[f'{from_}_{k}']
                        for k in ('type', 'id')
                    }
                    for ref in link[f'{from_}s']
                ]
                for (to, from_) in (('input', 'output'), ('output', 'input'))
            })

        bundle = self._canned_bundle(self.source)
        self._make_mock_tdr_tables(self.source.name, bundle.fqid)
        plugin = TestPlugin(sources={self.source.name}, tinyquery=self.tinyquery)

        downstream_row = one(plugin._run_sql(f'''
            SELECT * FROM {plugin._full_table_name(self.source.name, 'links')}
        '''))
        downstream_row['version'] = plugin.format_version(downstream_row['version'])
        downstream_content = json.loads(downstream_row['content'])

        upstream_rows = []
        downstream_links = []
        upstream_links = []
        # Find links which output files; collect and invert them
        for link in downstream_content['links']:
            file_outputs = [
                output
                for output in link.get('outputs', ())
                if output['output_type'].endswith('_file')
            ]
            if file_outputs:
                # The downstream bundle cannot retain the original links or the
                # files will not be detected as dangling inputs.
                downstream_links.append(invert_link(link))
                for output in file_outputs:
                    # A single link with multiple file outputs is expanded
                    # into multiple upstream synthetic links with one output
                    # each, to ensure we cover the case of merging multiple
                    # upstream bundles.
                    upstream_link = {**link, 'outputs': [output]}
                    upstream_links.append(upstream_link)
                    upstream_rows.append({
                        **downstream_row,
                        'content': json.dumps({**downstream_content, 'links': [upstream_link]}),
                        'links_id': str(uuid.uuid4())
                    })
            else:
                downstream_links.append(link)
        require(len(upstream_rows) > 2,
                'Insufficient file outputs in canned bundle')

        # Alter downstream bundle to include the dangling inputs.
        downstream_links.sort(key=link_sort_key)
        downstream_content['links'] = downstream_links
        downstream_row['content'] = json.dumps(downstream_content)
        replace_links(bundle, downstream_links)

        # Load updated downstream bundle into TinyQuery.
        # Do not load synthetic upstream bundles yet.
        self._make_mock_entity_table(self.source.name,
                                     'links',
                                     rows=[downstream_row])

        # Should detect dangling inputs and fail when they can't be found.
        with self.assertRaises(RequirementError) as cm:
            self._test_fetch_bundle(bundle, load_tables=False)
            self.assertTrue(one(cm.exception.args).startswith('Dangling inputs not found'))

        # Load upstream bundles.
        all_rows = (downstream_row, *upstream_rows)
        self._make_mock_entity_table(self.source.name,
                                     'links',
                                     rows=all_rows)
        assert set(plugin.list_bundles(self.source, '')) == {
            SourcedBundleFQID(uuid=row['links_id'],
                              version=row['version'],
                              source=self.source)
            for row in all_rows
        }
        # Alter downstream bundle to include the new links incorporated from
        # successful subgraph stitching.
        all_links = downstream_links + upstream_links
        all_links.sort(key=link_sort_key)
        replace_links(bundle, all_links)

        stitched_file_document_ids = {
            output['output_id']
            for link in upstream_links
            for output in link.get('outputs', [])
        }
        stitched_file_names = {
            file['file_core']['file_name']
            for file in bundle.metadata_files.values()
            if (file['schema_type'] == 'file'
                and file['provenance']['document_id'] in stitched_file_document_ids)
        }
        for e in bundle.manifest:
            e['is_stitched'] = (e['uuid'] in stitched_file_document_ids
                                # While only metadata files are directly present in the links, the
                                # manifest entries for the corresponding data files also have
                                # `is_stitched` set to true.
                                or e['name'] in stitched_file_names)
        self._test_fetch_bundle(bundle, load_tables=False)

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

    def _find_upstream_bundles(self,
                               source: TDRSourceRef,
                               outputs: Entities
                               ) -> Set[SourcedBundleFQID]:
        # TinyQuery/legacy SQL has no support for BQ Arrays, so it's impossible
        # to test the query in the overridden method.
        all_links = self._run_sql(f'''
            SELECT links_id, version, content
            FROM {self._full_table_name(source.name, 'links')}
        ''')
        outputs_by_bundle: Dict[SourcedBundleFQID, Entities] = {}
        for row in all_links:
            outputs_found = outputs & {
                EntityReference(entity_id=output['output_id'],
                                entity_type=output['output_type'])
                for link in json.loads(row['content'])['links']
                if link['link_type'] == 'process_link'
                for output in link['outputs']
            }
            if outputs_found:
                outputs_by_bundle[SourcedBundleFQID(uuid=row['links_id'],
                                                    version=self.format_version(row['version']),
                                                    source=source)] = outputs_found

        missing = outputs - set.union(*outputs_by_bundle.values())
        require(not missing,
                f'Dangling inputs not found in any bundle: {missing}')
        return set(outputs_by_bundle.keys())


if __name__ == '__main__':
    unittest.main()
