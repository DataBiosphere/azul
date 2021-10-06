import json
import logging
from time import (
    time,
)
from typing import (
    Optional,
)

from azul import (
    CatalogName,
    cache,
    cached_property,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.types import (
    JSONs,
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

    @classmethod
    @cache
    def _repository_plugin(cls, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Optional[Authentication]
                     ) -> JSONs:
        plugin = self._repository_plugin(catalog)

        cache_key = (
            catalog,
            '' if authentication is None else authentication.identity()
        )
        joiner = ':'
        assert not any(joiner in c for c in cache_key), cache_key
        cache_key = joiner.join(cache_key)
        try:
            sources = self._get(cache_key)
        except CacheMiss:
            sources = plugin.list_sources(authentication)
            self._put(cache_key, [source.to_json() for source in sources])
        else:
            sources = [
                plugin.source_from_json(source)
                for source in sources
            ]
        return [
            {
                'sourceId': source.id,
                'sourceSpec': str(source.spec)
            }
            for source in sources
        ]

    table_name = config.dynamo_sources_cache_table_name

    key_attribute = 'identity'
    value_attribute = 'sources'
    ttl_attribute = 'expiration'

    # Timespan in seconds that sources persist in the cache
    # FIXME: Streamline cache expiration
    #        https://github.com/DataBiosphere/azul/issues/3094
    expiration = 60

    @cached_property
    def _dynamodb(self):
        return aws.client('dynamodb')

    def _get(self, key: str) -> JSONs:
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

    def _put(self, key: str, sources: JSONs) -> None:
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
