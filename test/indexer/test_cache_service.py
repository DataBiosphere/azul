from collections.abc import (
    Mapping,
)
import random
import time
from unittest import (
    TestCase,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from moto import (
    mock_aws,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from azul.indexer.cache_service import (
    CacheService,
    ConcurrentCacheFetchError,
    RetryingCacheService,
)
from azul.lib.strings import (
    hex_digits,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_aws
class TestCacheService(DynamoDBTestCase):

    def _dynamodb_table_name(self) -> str:
        return CacheService._table_name

    def _dynamodb_attributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        return {CacheService._key_attribute: 'S'}

    def _dynamodb_hash_key(self) -> str:
        return CacheService._key_attribute

    def test_miss_then_hit(self):
        service = CacheService(expiration=60, lock_expiration=10)
        call_count = 0

        def fetcher() -> bytes:
            nonlocal call_count
            call_count += 1
            return b'hello'

        result = service.get('key1', fetcher)
        self.assertEqual(result, b'hello')
        self.assertEqual(call_count, 1)

        result = service.get('key1', fetcher)
        self.assertEqual(result, b'hello')
        self.assertEqual(call_count, 1)

    wait = 2

    def test_expiration(self):
        service = CacheService(expiration=self.wait, lock_expiration=1)
        call_count = 0

        def fetcher() -> bytes:
            nonlocal call_count
            call_count += 1
            return b'data'

        service.get('key1', fetcher)
        self.assertEqual(call_count, 1)

        time.sleep(self.wait + 1)

        service.get('key1', fetcher)
        self.assertEqual(call_count, 2)

    def test_concurrent_fetch_error(self):
        service = CacheService(expiration=60, lock_expiration=10)
        service._dynamodb.put_item(
            TableName=service._table_name,
            Item={
                service._key_attribute: {'S': 'locked_key'},
                service._ttl_attribute: {'N': str(service._now() + 60)},
            }
        )
        with self.assertRaises(ConcurrentCacheFetchError):
            service.get('locked_key', lambda: b'should not run')

    def test_expired_lock(self):
        service = CacheService(expiration=60, lock_expiration=self.wait)
        service._dynamodb.put_item(
            TableName=service._table_name,
            Item={
                service._key_attribute: {'S': 'stale_key'},
                service._ttl_attribute: {'N': str(service._now() + self.wait)},
            }
        )

        time.sleep(self.wait + 1)

        result = service.get('stale_key', lambda: b'recovered')
        self.assertEqual(result, b'recovered')

    def test_fetcher_failure_cleans_up_lock(self):
        service = CacheService(expiration=60, lock_expiration=10)

        class FetchError(Exception):
            pass

        def bad_fetcher() -> bytes:
            raise FetchError('boom')

        with self.assertRaises(FetchError):
            service.get('fail_key', bad_fetcher)

        response = service._dynamodb.get_item(
            TableName=service._table_name,
            Key={service._key_attribute: {'S': 'fail_key'}}
        )
        self.assertNotIn('Item', response)

    def test_distinct_keys(self):
        service = CacheService(expiration=60, lock_expiration=10)
        service.get('a', lambda: b'value_a')
        service.get('b', lambda: b'value_b')
        self.assertEqual(service.get('a', lambda: b'wrong'), b'value_a')
        self.assertEqual(service.get('b', lambda: b'wrong'), b'value_b')

    def test_key_or_value_too_large(self):
        service = CacheService(expiration=60, lock_expiration=10)
        max_key = ''.join(random.choices(hex_digits, k=service._max_key_size))
        cases = [
            ('Key is empty', '', random.randbytes(1)),
            ('Key too large', (max_key + '0'), random.randbytes(1)),
            ('Value too large', max_key, random.randbytes(service._max_value_size + 1)),
        ]
        for assertion, key, value in cases:
            with self.subTest(assertion):
                with self.assertRaises(AssertionError) as cm:
                    service.get(key, lambda: value)
                self.assertEqual(assertion, cm.exception.args[0].args[0])
                # Show that no cached value was left behind. DynamoDB disallows
                # empty keys for both reads and writes so if the key is empty
                # there cannot be a value.
                if key:
                    response = service._dynamodb.get_item(
                        TableName=service._table_name,
                        Key={service._key_attribute: {'S': key}}
                    )
                    self.assertNotIn('Item', response)

    def test_max_key_and_value(self):
        service = CacheService(expiration=60, lock_expiration=10)
        key = ''.join(random.choices(hex_digits, k=service._max_key_size))
        value = random.randbytes(service._max_value_size)
        # Work around https://github.com/getmoto/moto/issues/10176
        with patch.object(CacheService, '_put_value', return_value=True):
            result = service.get(key, lambda: value)
        self.assertEqual(result, value)


class TestRetryingCacheService(TestCase):

    def _make_service(self, num_retries=3, retry_delay=0.1):
        inner = MagicMock(spec=CacheService)
        service = RetryingCacheService(inner=inner,
                                       num_retries=num_retries,
                                       retry_delay=retry_delay)
        return service, inner

    @patch('azul.indexer.cache_service.sleep')
    def test_success_on_first_attempt(self, mock_sleep):
        service, inner = self._make_service()
        inner.get.return_value = b'hello'
        result = service.get('key1', lambda: b'hello')
        self.assertEqual(result, b'hello')
        inner.get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('azul.indexer.cache_service.sleep')
    def test_success_after_retries(self, mock_sleep):
        service, inner = self._make_service(num_retries=3, retry_delay=1.0)
        inner.get.side_effect = [
            ConcurrentCacheFetchError('key1'),
            ConcurrentCacheFetchError('key1'),
            b'hello',
        ]
        result = service.get('key1', lambda: b'hello')
        self.assertEqual(result, b'hello')
        self.assertEqual(inner.get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)
        for call in mock_sleep.call_args_list:
            delay = call[0][0]
            self.assertGreaterEqual(delay, 0.5)
            self.assertLessEqual(delay, 1.5)

    @patch('azul.indexer.cache_service.sleep')
    def test_raises_after_exhausted_retries(self, mock_sleep):
        service, inner = self._make_service(num_retries=2, retry_delay=1.0)
        inner.get.side_effect = ConcurrentCacheFetchError('key1')
        with self.assertRaises(ConcurrentCacheFetchError):
            service.get('key1', lambda: b'hello')
        self.assertEqual(inner.get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('azul.indexer.cache_service.sleep')
    def test_zero_retries(self, mock_sleep):
        service, inner = self._make_service(num_retries=0)
        inner.get.side_effect = ConcurrentCacheFetchError('key1')
        with self.assertRaises(ConcurrentCacheFetchError):
            service.get('key1', lambda: b'hello')
        inner.get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('azul.indexer.cache_service.sleep')
    def test_other_exceptions_not_retried(self, mock_sleep):
        service, inner = self._make_service()
        inner.get.side_effect = RuntimeError('boom')
        with self.assertRaises(RuntimeError):
            service.get('key1', lambda: b'hello')
        inner.get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('azul.indexer.cache_service.sleep')
    def test_jitter_range(self, mock_sleep):
        service, inner = self._make_service(num_retries=100, retry_delay=10.0)
        inner.get.side_effect = [ConcurrentCacheFetchError('key1')] * 100 + [b'ok']
        service.get('key1', lambda: b'ok')
        delays = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertTrue(any(d < 10.0 for d in delays))
        self.assertTrue(any(d > 10.0 for d in delays))
        for d in delays:
            self.assertGreaterEqual(d, 5.0)
            self.assertLessEqual(d, 15.0)
