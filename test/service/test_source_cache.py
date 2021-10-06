import time
from unittest import (
    mock,
)

from azul.service.source_service import (
    Expired,
    NotFound,
    SourceCacheService,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


class TestSourceCache(DynamoDBTestCase):
    ddb_table_name = SourceCacheService.table_name
    ddb_attrs = {SourceCacheService.key_attribute: 'S'}
    ddb_hash_key = SourceCacheService.key_attribute

    wait = 2

    @mock.patch.object(SourceCacheService, attribute='expiration', new=wait)
    def test_source_cache(self):
        key = 'foo'
        value = [{'bar': 'baz'}]
        service = SourceCacheService()

        with self.assertRaises(NotFound):
            service.get('')

        service.put(key, value)
        self.assertEqual(service.get(key), value)

        time.sleep(self.wait + 1)
        with self.assertRaises(Expired):
            service.get(key)
