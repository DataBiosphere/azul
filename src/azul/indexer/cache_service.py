from collections.abc import (
    Mapping,
)
import hashlib
from itertools import (
    count,
)
import logging
import random
from time import (
    sleep,
    time,
)
from typing import (
    Callable,
)
from uuid import (
    uuid4,
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

type CacheFetcher = Callable[[], bytes]


class ConcurrentCacheFetchError(RuntimeError):
    pass


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

    def get(self,
            cache_key: str,
            fetcher: CacheFetcher,
            *,
            group_key: str | None = None
            ) -> bytes:
        """
        Return the cached value for the given key, or call the fetcher to
        produce it.

        :param group_key: Ignored. See :meth:`RateLimitingCacheService.get`
                          for a cache service that supports this parameter.

        :raise ConcurrentFetchError: Another caller is already fetching the same
                                     key. Callers should handle this exception
                                     by waiting the average expected fetch time
                                     before calling this method again.
        """
        self._validate_key(cache_key)
        cache_key = self._key_prefix + cache_key
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
    _key_prefix = 'C'
    _value_attribute = 'value'
    _ttl_attribute = 'expiration'

    # SHA-1 hex digest
    _max_key_size = 40

    # DynamoDB numbers use approximately ceil(digits / 2) + 1 bytes.
    # A Unix timestamp has at most 10 significant digits until year 2286.
    _max_ttl_size = -(-10 // 2) + 1

    _max_value_size = (400 * 1024
                       - len(_key_attribute) - len(_key_prefix) - _max_key_size
                       - len(_value_attribute)
                       - len(_ttl_attribute) - _max_ttl_size)

    def _validate_key(self, key: str) -> None:
        assert len(key) > 0, R('Key is empty')
        assert len(key) <= self._max_key_size, R(
            'Key too large', len(key), self._max_key_size)

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
        if (
            item is not None
            and self._value_attribute in item
            and int(item[self._ttl_attribute]['N']) >= now
        ):
            log.info('Cache hit for %r', cache_key)
            return item[self._value_attribute]['B']
        else:
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
                raise ConcurrentCacheFetchError('Cache key is locked', cache_key) from e
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
class RateLimitingCacheService:
    """
    A decorator for :class:`CacheService` that limits the number of concurrent
    cache fetches ("slots") per group of related cache items. A slot can only
    be held for a limited duration (see :attr:`CacheService.lock_expiration`)
    before the holder is assumed to have crashed and the slot becomes available
    again. A group is identified by a simple string key provided by the caller
    which thereby controls the distribution of cache keys over groups.
    """

    #: The maximum number of concurrently active fetches per group
    #:
    max_slots: int

    _inner: CacheService

    _version_attribute = 'version'
    _slots_attribute = 'slots'
    _holder_attribute = 'holder'
    _key_prefix = 'G'

    def get(self,
            cache_key: str,
            fetcher: CacheFetcher,
            *,
            group_key: str | None = None
            ) -> bytes:
        """
        Return the cached value for the given key, or call the fetcher to
        produce it.

        :param group_key: If not None, the fetcher is rate-limited to
                          :attr:`max_slots` concurrent fetches per group.
                          If None, no rate-limiting is applied.

        :raise ConcurrentFetchError: Another caller is already fetching the same
                                     key or, if ``group_key`` was provided,
                                     more than :attr:`max_slots` fetchers are
                                     currently active for the given group.
                                     Callers should handle this exception by
                                     waiting the average expected fetch time
                                     before calling this method again.
        """
        if group_key is None:
            return self._inner.get(cache_key, fetcher)
        else:
            self._inner._validate_key(group_key)
            group_key = self._key_prefix + group_key

            def rate_limited_fetcher():
                holder = str(uuid4())
                self._acquire_slot(group_key, holder)
                try:
                    return fetcher()
                finally:
                    self._release_slot(group_key, holder)

            return self._inner.get(cache_key, rate_limited_fetcher)

    def _acquire_slot(self, group_key: str, holder: str) -> None:
        while True:
            sem, version = self._get_semaphore(group_key)
            if sem is None:
                sem = self._Semaphore(
                    max_slots=self.max_slots,
                    slot_expiration=self._inner.lock_expiration,
                    slots=[]
                )
            if not sem.acquire(holder):
                raise ConcurrentCacheFetchError('No free slots for group key', group_key)
            elif self._put_semaphore(group_key, sem, version):
                log.info('Semaphore slot acquired for %r (%d/%d)',
                         group_key, sem.active_slots, self.max_slots)
                return

    def _release_slot(self, group_key: str, holder: str) -> None:
        while True:
            sem, version = self._get_semaphore(group_key)
            if sem is None:
                return
            elif not sem.release(holder):
                return
            elif self._put_semaphore(group_key, sem, version):
                return

    def _get_semaphore(self, group_key: str) -> tuple[_Semaphore | None, int]:
        response = aws.dynamodb.get_item(
            TableName=self._inner._table_name,
            Key={self._inner._key_attribute: {'S': group_key}},
        )
        item = response.get('Item')
        if item is None:
            return None, 0
        else:
            sem = self._Semaphore(
                max_slots=self.max_slots,
                slot_expiration=self._inner.lock_expiration,
                slots=[
                    (m['M'][self._holder_attribute]['S'],
                     int(m['M'][self._inner._ttl_attribute]['N']))
                    for m in item[self._slots_attribute]['L']
                ],
            )
            version = int(item[self._version_attribute]['N'])
            return sem, version

    def _put_semaphore(self, group_key: str, sem: _Semaphore, version: int) -> bool:
        max_exp = sem.max_expiration
        try:
            aws.dynamodb.put_item(
                TableName=self._inner._table_name,
                Item={
                    self._inner._key_attribute: {'S': group_key},
                    self._slots_attribute: {'L': [
                        {'M': {
                            self._holder_attribute: {'S': h},
                            self._inner._ttl_attribute: {'N': str(exp)},
                        }}
                        for h, exp in sem.slots
                    ]},
                    self._version_attribute: {'N': str(version + 1)},
                    self._inner._ttl_attribute: {'N': str(max_exp)},
                },
                ConditionExpression='attribute_not_exists(#key) OR #ver = :ver',
                ExpressionAttributeNames={
                    '#key': self._inner._key_attribute,
                    '#ver': self._version_attribute,
                },
                ExpressionAttributeValues={
                    ':ver': {'N': str(version)},
                },
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            else:
                raise
        else:
            return True

    @attrs.frozen(kw_only=True)
    class _Semaphore:
        """
        A semaphore backed by a list of slots. Each slot pairs a holder ID with
        an expiration time. The expiration is used to handle crashed holders. The
        semaphore is capped at ``max_slots``. When the list of slots is at
        capacity, :meth:`acquire` evicts the oldest expired slot to make room.
        """

        #: See :attr:`RateLimitingCacheService.max_slots`
        max_slots: int

        #: See :attr:`CacheService.lock_expiration`
        slot_expiration: int

        slots: list[tuple[str, int]]
        now: int = attrs.field(init=False, default=attrs.Factory(lambda: int(time())))

        def __attrs_post_init__(self):
            assert self.max_slots > 0, R(
                'max_slots must be positive', self.max_slots)
            assert self.slot_expiration > 0, R(
                'slot_expiration must be positive', self.slot_expiration)
            assert len(self.slots) <= self.max_slots, R(
                'Too many slots', len(self.slots), self.max_slots)

        def acquire(self, holder: str) -> bool:
            """
            Try to acquire a slot. Returns True on success, False if all slots
            are held by active (non-expired) holders.
            """
            expiration = self.now + self.slot_expiration
            if len(self.slots) < self.max_slots:
                self.slots.append((holder, expiration))
                return True
            else:
                evict = None
                for i, (h, exp) in enumerate(self.slots):
                    if exp < self.now and (evict is None or exp < self.slots[evict][1]):
                        evict = i
                if evict is not None:
                    self.slots[evict] = (holder, expiration)
                    return True
                else:
                    return False

        @property
        def max_expiration(self) -> int:
            return max((exp for _, exp in self.slots), default=self.now)

        @property
        def active_slots(self) -> int:
            return sum(1 for _, exp in self.slots if exp >= self.now)

        def release(self, holder: str) -> bool:
            """
            Release the slot held by the given holder. Returns True if the
            holder's slot was found and removed, False otherwise.
            """
            index = next((i for i, (h, _) in enumerate(self.slots) if h == holder), None)
            if index is not None:
                del self.slots[index]
                return True
            else:
                return False


@attrs.frozen(kw_only=True)
class RetryingCacheService:
    """
    A decorator for :class:`CacheService` that retries a limited number of times
    on :class:`ConcurrentFetchError` instead of immediately raising it.
    """

    #: The number of times to retry after a :class:`ConcurrentFetchError`. If
    # this is 0, this class behaves exactly like CacheService.
    #:
    num_retries: int

    #: The number of seconds to wait between retries.
    #:
    retry_delay: float

    _inner: CacheService | RateLimitingCacheService

    def get(self,
            cache_key: str,
            fetcher: CacheFetcher,
            *,
            group_key: str | None = None
            ) -> bytes:
        """
        Return the cached value for the given key, or call the fetcher to
        produce it.

        :param group_key: Forwarded to the inner cache service. See
                          :meth:`RateLimitingCacheService.get` for a cache
                          service that supports this parameter.

        :raise ConcurrentFetchError: Another caller was observed to already be
                                     fetching the same key for
                                     :attr:`num_retries` + 1 *
                                     :attr:`retry_delay` seconds.
        """
        retries = count()
        while True:
            try:
                return self._inner.get(cache_key, fetcher, group_key=group_key)
            except ConcurrentCacheFetchError:
                if (i := next(retries)) < self.num_retries:
                    log.info('Cache get for %r failed (attempt %d of %d)',
                             cache_key, i + 1, self.num_retries + 1)
                    sleep(self.retry_delay * random.uniform(0.5, 1.5))
                else:
                    raise


@attrs.frozen(kw_only=True)
class UrlCacheService:
    """
    A decorator for :class:`CacheService` (or :class:`RetryingCacheService`)
    that caches responses to HTTP(S) requests. The key under which responses are
    cached is the hash of the URL and the request headers. The first caller for
    a given URL and headers will cause the request to be made. Subsequent
    callers passing the same URL and headers will get the same response, without
    causing a request to be made, until the cached response expires. The
    expiration time of the response is configurable per instance, independent of
    any HTTP caching headers.
    """

    #: The client to be used when fetching the URL on a cache miss. If the
    #: client is configured to follow redirects, the final response will be
    #: returned and cached under the given URL, otherwise the redirect
    #: response will be returned and cached.
    #:
    http_client: HttpClient

    _inner: CacheService | RetryingCacheService | RateLimitingCacheService

    def get_url(self,
                url: furl,
                headers: Mapping[str, str] | None = None,
                **kwargs
                ) -> BaseHTTPResponse:
        """
        Return a cached HTTP response for the given URL and headers, fetching it
        first if necessary. Any additional keyword arguments are forwarded to
        :meth:`HttpClient.urlopen`. If ``redirect=True`` is passed, the final
        response will be returned and cached under the given URL, otherwise
        a redirect response, if any, will be returned and cached as-is.

        :raise ConcurrentFetchError: if another caller is already fetching the
                                     same URL and the inner cache service does
                                     not retry
        """
        assert not str(url.fragment), R(
            'URLs with a fragment cannot be cached', url)
        if headers is None:
            headers = {}
        cache_key = self._cache_key(url, headers)

        def fetcher() -> bytes:
            response = self.http_client.urlopen('GET', str(url), headers=headers, **kwargs)
            if 200 <= response.status < 400:
                return self._serialize_response(response)
            else:
                raise self._UncacheableResponse(response)

        try:
            cached = self._inner.get(cache_key, fetcher, group_key=self._group_key(url))
        except self._UncacheableResponse as e:
            return e.args[0]
        else:
            return self._deserialize_response(cached)

    class _UncacheableResponse(BaseException):
        pass

    def _serialize_response(self, response: BaseHTTPResponse) -> bytes:
        assert 200 <= response.status < 400, R(
            'Unexpected response status', response.status)
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

    def _group_key(self, url: furl) -> str | None:
        host = url.host
        if host is None:
            return None
        else:
            return hashlib.sha1(host.encode()).hexdigest()
