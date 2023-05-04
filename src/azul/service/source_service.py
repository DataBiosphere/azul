import json
import logging
from time import (
    time,
)
from typing import (
    Iterable,
    Optional,
)

from azul import (
    CatalogName,
    cache,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.indexer import (
    SourceRef,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.types import (
    AnyJSON,
)

log = logging.getLogger(__name__)


class CacheMiss(Exception):
    pass


class NotFound(CacheMiss):

    def __init__(self, key: str):
        super().__init__(f'Key not found: {key!r}')


class Expired(CacheMiss):

    def __init__(self, key: str):
        super().__init__(f'Entry for key {key!r} is expired')


class SourceService:

    @cache
    def _repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def list_source_ids(self,
                        catalog: CatalogName,
                        authentication: Optional[Authentication]
                        ) -> set[str]:
        plugin = self._repository_plugin(catalog)

        cache_key = (
            catalog,
            '' if authentication is None else authentication.identity()
        )
        joiner = ':'
        assert not any(joiner in c for c in cache_key), cache_key
        cache_key = joiner.join(cache_key)
        try:
            source_ids = self._get(cache_key)
        except CacheMiss:
            source_ids = [source.id for source in plugin.list_sources(authentication)]
            self._put(cache_key, source_ids)
        return set(source_ids)

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Optional[Authentication]
                     ) -> Iterable[SourceRef]:
        return self._repository_plugin(catalog).list_sources(authentication)

    table_name = config.dynamo_sources_cache_table_name

    key_attribute = 'identity'
    value_attribute = 'sources'
    ttl_attribute = 'expiration'

    # Timespan in seconds that sources persist in the cache
    expiration = 60

    @property
    def _dynamodb(self):
        return aws.dynamodb

    def _get(self, key: str) -> list[AnyJSON]:
        response = self._dynamodb.get_item(TableName=self.table_name,
                                           Key={self.key_attribute: {'S': key}},
                                           ProjectionExpression=','.join([self.value_attribute, self.ttl_attribute]))
        try:
            result = response['Item']
        except KeyError:
            raise NotFound(key)
        else:
            # Items can persist in DynamoDB after they are marked as expired
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
            if int(result[self.ttl_attribute]['N']) < self._now():
                raise Expired(key)
            else:
                return json.loads(result[self.value_attribute]['S'])

    def _put(self, key: str, sources: list[AnyJSON]) -> None:
        item = {
            self.key_attribute: {'S': key},
            self.value_attribute: {'S': json.dumps(sources)},
            self.ttl_attribute: {
                'N': str(self._now() + self.expiration)
            }
        }
        self._dynamodb.put_item(TableName=self.table_name,
                                Item=item)

    def _now(self) -> int:
        return int(time())
