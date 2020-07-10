from functools import cached_property
import json
from operator import attrgetter
from typing import (
    Dict,
    Iterable,
    Mapping,
    Union,
)
import unittest
from unittest import mock

from more_itertools import one
from tinyquery import tinyquery

from azul import config
from azul.bigquery import AbstractBigQueryAdapter
from azul.indexer import BundleFQID
from azul.tdr import (
    AzulTDRClient,
    BigQueryDataset,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul.vendored.frozendict import frozendict
from azul_test_case import AzulTestCase


class TestTDRClient(AzulTestCase):
    _gbq_adapter_mock = None

    def setUp(self) -> None:
        self.query_adapter = TinyBigQueryAdapter()
        self._gbq_adapter_mock = mock.patch.object(AzulTDRClient,
                                                   'big_query_adapter',
                                                   new=self.query_adapter)
        self._gbq_adapter_mock.start()

    def tearDown(self) -> None:
        self._gbq_adapter_mock.stop()

    def _init_client(self, dataset: BigQueryDataset) -> AzulTDRClient:
        client = AzulTDRClient(dataset)
        assert client.big_query_adapter is self.query_adapter
        return client

    def test_list_links_ids(self):
        def test(dataset: BigQueryDataset):
            old_version = '2001-01-01T00:00:00.000000Z'
            current_version = '2001-01-01T00:00:00.000001Z'
            links_ids = ['42-abc', '42-def', '42-ghi', '86-xyz']
            versions = (current_version,) if dataset.is_snapshot else (current_version, old_version)
            self._make_mock_entity_table(dataset, 'links', [
                dict(links_id=links_id, version=version, content='{}')
                for version in versions
                for links_id in links_ids
            ])
            client = self._init_client(dataset)
            bundle_ids = client.list_links_ids(prefix='42')
            bundle_ids.sort(key=attrgetter('uuid'))
            self.assertEqual(bundle_ids, [
                BundleFQID('42-abc', current_version),
                BundleFQID('42-def', current_version),
                BundleFQID('42-ghi', current_version)
            ])

        with self.subTest('snapshot'):
            test(BigQueryDataset('test-project', 'name', True))
        with self.subTest('dataset'):
            test(BigQueryDataset('test-project', 'name', False))

    def test_tdr_dataset_config(self):
        dataset = config.tdr_bigquery_dataset
        self.assertNotEqual(dataset.is_snapshot, dataset.name.startswith('datarepo_'))

    def _make_mock_entity_table(self,
                                dataset: BigQueryDataset,
                                concrete_entity_type: str,
                                rows: JSONs = (),
                                additional_columns: Dict[str, Union[str, type]] = frozendict()):
        self.query_adapter.create_table(dataset_name=dataset.name,
                                        table_name=concrete_entity_type,
                                        schema=self._bq_schema({f'{concrete_entity_type}_id': str},
                                                               version='TIMESTAMP',
                                                               content=str,
                                                               **additional_columns),
                                        rows=rows)

    _bq_types = {
        str: 'STRING',
        bytes: 'BYTES',
        int: 'INTEGER',
        float: 'FLOAT',
        bool: 'BOOL'
    }

    def _bq_schema(self, fields: Mapping[str, Union[str, type]] = frozendict(), **kwargs: Union[str, type]) -> JSONs:
        kwargs.update(fields)
        return [
            dict(name=k,
                 type=self._bq_types.get(v, v),
                 mode='NULLABLE')
            for k, v in kwargs.items()
        ]


class TinyBigQueryAdapter(AbstractBigQueryAdapter):

    @cached_property
    def client(self):
        return tinyquery.TinyQuery()

    def run_sql(self, query: str) -> Iterable[JSON]:
        columns = self.client.evaluate_query(query).columns
        num_rows = one(set(map(lambda c: len(c.values), columns.values())))
        for i in range(num_rows):
            yield {k[1]: v.values[i] for k, v in columns.items()}

    def assert_table_exists(self, dataset_name: str, table_name: str) -> None:
        self.client.get_table(dataset_name, table_name)

    def create_table(self, dataset_name: str, table_name: str, schema: JSONs, rows: JSONs) -> None:
        # TinyQuery's errors are typically not helpful in debugging missing/extra columns in the row JSON.
        columns = sorted([column['name'] for column in schema])
        for row in rows:
            row_columns = sorted(row.keys())
            assert row_columns == columns, row_columns
        self.client.load_table_from_newline_delimited_json(table_name=f'{dataset_name}.{table_name}',
                                                           schema=json.dumps(schema),
                                                           table_lines=map(json.dumps, rows))


if __name__ == '__main__':
    unittest.main()
