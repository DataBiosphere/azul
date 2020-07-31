import abc
from abc import (
    abstractmethod,
)
from datetime import (
    datetime,
)
from typing import (
    Iterable,
    Mapping,
    Union,
)

from google.cloud import (
    bigquery,
)

from azul.types import (
    JSONs,
)

BigQueryValue = Union[int, float, bool, str, bytes, datetime, None]
BigQueryRow = Mapping[str, BigQueryValue]
BigQueryRows = Iterable[BigQueryRow]


class AbstractBigQueryAdapter(abc.ABC):

    @abstractmethod
    def run_sql(self, query: str) -> BigQueryRows:
        """
        Evaluate a SQL query and iterate rows.
        """
        raise NotImplementedError

    @abstractmethod
    def create_table(self, dataset_name: str, table_name: str, schema: JSONs, rows: JSONs) -> None:
        """
        Create a new table within an existing dataset. Only used for testing.

        :param dataset_name: dataset that contains the new table

        :param table_name: name of the new table

        :param schema: sequence of column schemas, each with keys 'name', 'mode', and 'type'

        :param rows: sequence of row values mapping every column defined in the schema to a value
        """
        raise NotImplementedError


class BigQueryAdapter(AbstractBigQueryAdapter):

    def __init__(self, project) -> None:
        self.client = bigquery.Client(project=project)

    def run_sql(self, query: str) -> BigQueryRows:
        return self.client.query(query)

    def create_table(self, dataset_name: str, table_name: str, schema: JSONs, rows: JSONs = ()) -> None:
        raise NotImplementedError('Only used for testing')
