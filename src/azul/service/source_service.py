import json
import logging
from time import (
    time,
)
from typing import (
    AbstractSet,
    Iterable,
    TypedDict,
)

from azul import (
    CatalogName,
    NotInLambdaContextException,
    cache,
    config,
    open_resource,
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
    JSON,
    json_element_mappings,
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


class _ConfiguredSources(TypedDict):
    all: AbstractSet[SourceRef]
    public: AbstractSet[SourceRef]


class SourceService:

    @cache
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def list_accessible_source_ids(self,
                                   catalog: CatalogName,
                                   authentication: Authentication | None
                                   ) -> set[str]:
        plugin = self.repository_plugin(catalog)

        cache_key = (
            catalog,
            '' if authentication is None else authentication.identity()
        )
        joiner = ':'
        assert not any(joiner in c for c in cache_key), cache_key
        cache_key = joiner.join(cache_key)
        try:
            source_ids = set(self._get(cache_key))
        except CacheMiss:
            source_ids = plugin.list_accessible_source_ids(authentication)
            configured_source_ids = {source.id for source in self.configured_sources}
            source_ids &= configured_source_ids
            self._put(cache_key, list(source_ids))
        return source_ids

    def list_accessible_sources(self,
                                catalog: CatalogName,
                                authentication: Authentication | None
                                ) -> Iterable[SourceRef]:
        return self.repository_plugin(catalog).list_accessible_sources(authentication)

    table_name = config.dynamo_sources_cache_table_name

    key_attribute = 'identity'
    value_attribute = 'sources'
    ttl_attribute = 'expiration'

    # Timespan in seconds that sources persist in the cache
    expiration = 5 * 60

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

    @cache
    def _configured_sources(self) -> _ConfiguredSources:
        try:
            with open_resource('sources.json') as f:
                sources = json.load(f)
        except NotInLambdaContextException:
            all_sources, public_sources = set(), set()
            for catalog in config.catalogs.values():
                if not catalog.is_integration_test_catalog:
                    all_sources.update(self.repository_plugin(catalog.name).list_sources())
                    public_sources.update(self.list_accessible_sources(catalog.name,
                                                                       authentication=None))
            return {
                'all': all_sources,
                'public': public_sources,
            }
        else:
            def parse(sources: AnyJSON) -> AbstractSet[SourceRef]:
                return frozenset(
                    SourceRef.from_json(source)
                    for source in json_element_mappings(sources)
                )

            return {
                'all': parse(sources['all']),
                'public': parse(sources['public']),
            }

    @property
    def configured_sources(self) -> AbstractSet[SourceRef]:
        return self._configured_sources()['all']

    @property
    def configured_public_sources(self) -> AbstractSet[SourceRef]:
        return self._configured_sources()['public']

    @property
    def configured_sources_for_outsourcing(self) -> JSON:
        return {
            k: [source.to_json() for source in v]
            for k, v in self._configured_sources().items()
        }
