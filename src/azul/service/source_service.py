import json
import logging
from time import (
    time,
)
from typing import (
    Iterable,
    Mapping,
)

from azul import (
    CatalogName,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
    cache,
    cached_property,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    json_element_strings,
    json_item_sequences,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.resources import (
    NotInLambdaContextException,
    open_resource,
)
from azul.source import (
    Source,
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
    def repository_plugin(self, catalog: CatalogName) -> RepositoryPlugin:
        return RepositoryPlugin.load(catalog).create(catalog)

    def list_source_ids(self,
                        catalog: CatalogName,
                        authentication: Authentication | None
                        ) -> set[str]:
        """
        List the IDs of the sources in the underlying repository that are
        accessible using the provided authentication, or the public service
        account if no authentication is provided. The result may contain the IDs
        of sources that are not included in the given catalog.

        This method may require a roundtrip to the underlying repository, but
        results are cached for a certain amount of time, depending on the
        context and whether authentication is provided.

        If authentication is provided, the result is cached for a few minutes,
        and the cached result is shared between all instances of this class in a
        single deployment, in the context of a Lambda function and outside.

        If no authentication (``None``) was provided, the caching depends on the
        context: calls within a Lambda context use the result determined at
        deployment time. Outside of that context, the first call of this method
        per instance of this class incurs a round trip to the repository, and
        the result is then cached until the instance is destroyed.
        """
        if authentication is None:
            source_ids = {source.ref.id for source in self._public_sources[catalog]}
        else:
            plugin = self.repository_plugin(catalog)
            cache_key = (catalog, authentication.identity())
            joiner = ':'
            assert not any(joiner in c for c in cache_key), cache_key
            cache_key = joiner.join(cache_key)
            try:
                source_ids = set(json_element_strings(self._get(cache_key)))
            except CacheMiss:
                source_ids = plugin.list_source_ids(authentication)
                self._put(cache_key, list(source_ids))
        return source_ids

    def list_sources(self,
                     catalog: CatalogName,
                     authentication: Authentication | None
                     ) -> Iterable[Source]:
        """
        List the sources in the given catalog that are accessible using the
        provided authentication.

        If authentication is provided, this method requires a roundtrip to the
        underlying repository.

        If no authentication (``None``) was provided, the caching depends on the
        context: calls within a Lambda context use the result determined at
        deployment time. Outside of that context, the first call of this method
        per instance of this class incurs a round trip to the repository, and
        the result is then cached until the instance is destroyed.

        """
        if authentication is None:
            return self._public_sources[catalog]
        else:
            return self._list_sources(catalog, authentication)

    def _list_sources(self,
                      catalog: CatalogName,
                      authentication: Authentication | None
                      ) -> Iterable[Source]:
        plugin = self.repository_plugin(catalog)
        refs = plugin.list_sources(authentication)
        configs_by_spec = plugin.sources

        specs_by_name = {spec.name: spec for spec in configs_by_spec.keys()}
        assert len(configs_by_spec) == len(specs_by_name), R(
            'Duplicate source names in catalog configuration', configs_by_spec)

        refs_by_name = {ref.spec.name: ref for ref in refs}
        assert len(refs) == len(refs_by_name), R(
            'Duplicate source names in repository', refs)

        sources = []
        for ref in refs:
            try:
                spec = specs_by_name[ref.spec.name]
            except KeyError:
                pass
            else:
                assert spec == ref.spec, R('Misconfigured source', spec, ref)
                source = Source(ref=ref, config=configs_by_spec[spec])
                sources.append(source)

        return sources

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

    @cached_property
    def _public_sources(self) -> Mapping[CatalogName, Iterable[Source]]:
        """
        The set of all sources included in any catalog in the current
        deployment that are accessible to the public service account. When
        invoked from a Lambda function, this will never make a roundtrip to the
        underlying repository.
        """
        try:
            with open_resource('public_sources.json') as f:
                public_sources = json.load(f)
        except NotInLambdaContextException:
            return {
                catalog.name: self._list_sources(catalog.name, authentication=None)
                for catalog in config.catalogs.values()
            }
        else:
            return {
                catalog: [Source.from_json(source) for source in sources]
                for catalog, sources in json_item_sequences(public_sources)
            }

    @property
    def public_sources_for_outsourcing(self) -> JSON:
        return {
            catalog: [source.to_json() for source in sources]
            for catalog, sources in self._public_sources.items()
        }
