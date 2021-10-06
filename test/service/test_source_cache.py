import time
from unittest import (
    mock,
)

from azul.service.source_service import (
    Expired,
    NotFound,
    SourceService,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


class TestSourceCache(DynamoDBTestCase):
    ddb_table_name = SourceService.table_name
    ddb_attrs = {SourceService.key_attribute: 'S'}
    ddb_hash_key = SourceService.key_attribute

    wait = 2

    @mock.patch.object(SourceService, attribute='expiration', new=wait)
    def test_source_cache(self):
        key = 'foo'
        value = [{'bar': 'baz'}]
        service = SourceService()

        with self.assertRaises(NotFound):
            service.get('')

        service.put(key, value)
        self.assertEqual(service.get(key), value)

        time.sleep(self.wait + 1)
        with self.assertRaises(Expired):
            service.get(key)
