from collections.abc import (
    Mapping,
)
import hashlib
import logging
from time import (
    time,
)
from typing import (
    Callable,
)

import attrs
from botocore.exceptions import (
    ClientError,
)
from furl import (
    furl,
)
import msgpack
from urllib3 import (
    BaseHTTPResponse,
    HTTPResponse,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    HttpClient,
)
from azul.lib import (
    R,
)
from azul.lib.urls import (
    normalize_url,
)

log = logging.getLogger(__name__)


@attrs.frozen(kw_only=True)
class CacheService:
    """
    Cache small binary values under a string key, for a configurable amount of
    time. The cache persists across process boundaries. Cache misses trigger a
    fetch by invoking a callback and store the return value in the cache. The
    service tries to prevent more than one concurrent active fetch for a given
    key. Multiple instances of this class can coexist and cooperate, in the same
    process, or in different processes, and all instances access the same
    persistence store, using a global key space.
    """

    #: How long a cached value remains valid, in seconds. Once the value under a
    #: given key expires, the next get will be a miss, causing another fetch, to
    #: update the value. If no get is made, the persistent storage occupied by
    #: the expired value will eventually be released. The expiration of a cached
    #: value is determined at the time the value is persisted to the cache.
    #: Reading the cached value through another instance of this class with a
    #: different value for this attribute doesn't affect the expiration of that
    #: value. The instance that fetches the value again after it expires,
    #: determines the expiration of the newly fetched value.
    #:
    expiration: int

    #: A lock is used to avoid many concurrent fetches on a miss. This parameter
    #: specifies the number of seconds before that lock is automatically
    #: released to account for crashed fetchers. Should exceed the maximum
    #: expected fetch time. If a fetch takes longer, the lock expires and
    #: another caller may start a concurrent fetch. The first to finish, the
    #: winner, caches its result. The loser returns the winner's cached value,
    #: even if its own fetch yields a different one. If the winner's value has
    #: already expired, the loser retries.
    #:
    lock_expiration: int

    type Fetcher = Callable[[], bytes]

    class ConcurrentFetchError(RuntimeError):

        def __init__(self, cache_key: str):
            super().__init__(f'Cache line {cache_key!r} is locked')

    def get(self, cache_key: str, fetcher: Fetcher) -> bytes:
        """
        Return the cached value for the given key, or call the fetcher to
        produce it.

        :raise ConcurrentFetchError: Another caller is already fetching the same
                                     key. Callers should handle this exception
                                     by waiting the average expected fetch time
                                     before calling this method again.
        """
        assert len(cache_key) > 0, R('Key is empty')
        assert len(cache_key) <= self._max_key_size, R(
            'Key too large', len(cache_key), self._max_key_size)
        while True:
            value = self._get_value(cache_key)
            if value is None:
                self._acquire_lock(cache_key)
                try:
                    value = fetcher()
                    assert len(value) <= self._max_value_size, R(
                        'Value too large', len(value), self._max_value_size)
                except BaseException:
                    self._release_lock(cache_key)
                    raise
                else:
                    if self._put_value(cache_key, value):
                        return value
            else:
                return value

    _table_name = config.dynamo_object_cache_table_name

    _key_attribute = 'cache_key'
    _value_attribute = 'value'
    _ttl_attribute = 'expiration'

    # SHA-1 hex digest
    _max_key_size = 40

    # DynamoDB numbers use approximately ceil(digits / 2) + 1 bytes.
    # A Unix timestamp has at most 10 significant digits until year 2286.
    _max_ttl_size = -(-10 // 2) + 1

    _max_value_size = (400 * 1024
                       - len(_key_attribute) - _max_key_size
                       - len(_value_attribute)
                       - len(_ttl_attribute) - _max_ttl_size)

    @property
    def _dynamodb(self):
        return aws.dynamodb

    def _now(self) -> int:
        return int(time())

    def _get_value(self, cache_key: str) -> bytes | None:
        now = self._now()
        response = self._dynamodb.get_item(
            TableName=self._table_name,
            Key={self._key_attribute: {'S': cache_key}}
        )
        item = response.get('Item')
        if item is not None:
            if self._value_attribute in item and int(item[self._ttl_attribute]['N']) >= now:
                log.info('Cache hit for %r', cache_key)
                return item[self._value_attribute]['B']
        log.info('Cache miss for %r', cache_key)
        return None

    def _acquire_lock(self, cache_key: str) -> None:
        now = self._now()
        try:
            self._dynamodb.put_item(
                TableName=self._table_name,
                Item={
                    self._key_attribute: {'S': cache_key},
                    self._ttl_attribute: {'N': str(now + self.lock_expiration)},
                },
                ConditionExpression='attribute_not_exists(#key) OR #exp < :now',
                ExpressionAttributeNames={
                    '#key': self._key_attribute,
                    '#exp': self._ttl_attribute,
                },
                ExpressionAttributeValues={
                    ':now': {'N': str(now)},
                },
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise self.ConcurrentFetchError(cache_key) from e
            else:
                raise
        else:
            log.info('Lock acquired for %r, fetching', cache_key)

    def _release_lock(self, cache_key: str) -> None:
        try:
            self._dynamodb.delete_item(
                TableName=self._table_name,
                Key={self._key_attribute: {'S': cache_key}},
                ConditionExpression='attribute_not_exists(#val)',
                ExpressionAttributeNames={
                    '#val': self._value_attribute,
                },
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                log.info('Lock for %r was overtaken', cache_key)
            else:
                raise

    def _put_value(self, cache_key: str, value: bytes) -> bool:
        now = self._now()
        try:
            self._dynamodb.put_item(
                TableName=self._table_name,
                Item={
                    self._key_attribute: {'S': cache_key},
                    self._value_attribute: {'B': value},
                    self._ttl_attribute: {'N': str(now + self.expiration)},
                },
                ConditionExpression='attribute_not_exists(#val)',
                ExpressionAttributeNames={
                    '#val': self._value_attribute,
                },
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                log.info('Lock for %r was overtaken', cache_key)
                return False
            else:
                raise
        else:
            log.info('Cached value for %r', cache_key)
            return True


@attrs.frozen(kw_only=True)
class UrlCacheService(CacheService):
    """
    A cache of responses to HTTP(S) requests. The key under which responses are
    cached is the hash of the URL and the request headers. The first caller for
    a given URL and headers will cause the request to be made. Subsequent
    callers passing the same URL and headers will get the same response, without
    causing a request to be made, until the cached response expires. The
    expiration time of the response is configurable per instance, independent of
    any HTTP caching headers. Like the superclass, this class tries to avoid
    making the same request concurrently, even with concurrent callers, against
    different instances of this class, in different processes.
    """
    http_client: HttpClient

    def get_url(self, url: furl, headers: Mapping[str, str] | None = None) -> BaseHTTPResponse:
        """
        Return a cached HTTP response for the given URL and headers, fetching
        it first if necessary.

        :raise ConcurrentFetchError: if another caller is already fetching the
                                     same URL
        """
        assert not str(url.fragment), R(
            'URLs with a fragment cannot be cached', url)
        if headers is None:
            headers = {}
        cache_key = self._cache_key(url, headers)

        def fetcher() -> bytes:
            response = self.http_client.urlopen('GET', str(url), headers=headers)
            if 200 <= response.status < 400:
                return self._serialize_response(response)
            else:
                raise self._UncacheableResponse(response)

        try:
            cached = self.get(cache_key, fetcher)
        except self._UncacheableResponse as e:
            return e.args[0]
        else:
            return self._deserialize_response(cached)

    class _UncacheableResponse(BaseException):
        pass

    def _serialize_response(self, response: BaseHTTPResponse) -> bytes:
        assert 200 <= response.status < 400, R('Unexpected response status', response.status)
        return b'v1_' + msgpack.packb([
            response.status,
            dict(response.headers),
            response.data,
        ])

    def _deserialize_response(self, data: bytes) -> HTTPResponse:
        version, sep, packed = data.partition(b'_')
        assert version == b'v1' and sep, R('Unexpected version', version)
        status, headers, body = msgpack.unpackb(packed, raw=False)
        return HTTPResponse(body=body, status=status, headers=headers)

    def _cache_key(self, url: furl, headers: Mapping[str, str]) -> str:
        h = hashlib.sha1()
        h.update(str(normalize_url(url)).encode())
        for key in sorted(headers):
            h.update(key.encode())
            h.update(headers[key].encode())
        return h.hexdigest()
