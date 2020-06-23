from functools import cached_property
import itertools
from operator import (
    itemgetter,
)
from typing import (
    Iterable,
    List,
    NamedTuple,
)

from more_itertools import one

from azul.bigquery import (
    AbstractBigQueryAdapter,
    BigQueryAdapter,
)
from azul.indexer import BundleFQID
from azul.types import (
    JSON,
    JSONs,
)
from azul.uuids import validate_uuid_prefix


class BigQueryDataset(NamedTuple):
    project: str
    name: str
    is_snapshot: bool


class AzulTDRClient:
    timestamp_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    @cached_property
    def big_query_adapter(self) -> AbstractBigQueryAdapter:
        return BigQueryAdapter()

    def __init__(self, dataset: BigQueryDataset):
        self.target = dataset
        self.big_query_adapter.assert_table_exists(dataset.name, 'links')

    def list_links_ids(self, prefix: str) -> List[BundleFQID]:
        validate_uuid_prefix(prefix)
        current_bundles = self._query_latest_version(f'''
            SELECT links_id, version
            FROM {self.target.name}.links
            WHERE STARTS_WITH(links_id, "{prefix}")
        ''', group_by='links_id')
        return [BundleFQID(uuid=row['links_id'],
                           version=row['version'].strftime(self.timestamp_format))
                for row in current_bundles]

    def _query_latest_version(self, query: str, group_by: str) -> JSONs:
        iter_rows = self.big_query_adapter.run_sql(query)
        key = itemgetter(group_by)
        groups = itertools.groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(group) for _, group in groups]

    def _choose_one_version(self, versioned_items: Iterable[JSON]) -> JSON:
        if self.target.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))
