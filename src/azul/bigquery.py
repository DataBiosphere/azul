import abc
from abc import (
    abstractmethod,
)
from functools import cached_property
from typing import Iterable

from google.cloud import bigquery

from azul.types import (
    JSON,
)


class AbstractBigQueryAdapter(abc.ABC):

    @abstractmethod
    def run_sql(self, query: str) -> Iterable[JSON]:
        """
        Evaluate an SQL query and iterate rows.
        """
        raise NotImplementedError

    @abstractmethod
    def assert_table_exists(self, dataset_name: str, table_name: str) -> None:
        """
        Raise exception if the specified table does not exist.
        """
        raise NotImplementedError


class BigQueryAdapter(AbstractBigQueryAdapter):

    @cached_property
    def client(self):
        from azul import config
        return bigquery.Client(project=config.tdr_bigquery_dataset.project)

    def run_sql(self, query: str) -> Iterable[JSON]:
        return self.client.query(query)

    def assert_table_exists(self, dataset_name: str, table_name: str) -> None:
        self.client.get_table(f'{dataset_name}.{table_name}')
