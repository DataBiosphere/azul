import time
from unittest import (
    mock,
)

from moto import (
    mock_dynamodb,
)

from azul.service.source_service import (
    Expired,
    NotFound,
    SourceService,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


@mock_dynamodb
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
            service._get('nil')

        service._put(key, value)
        self.assertEqual(service._get(key), value)

        time.sleep(self.wait + 1)
        with self.assertRaises(Expired):
            service._get(key)
