import time
from typing import (
    Mapping,
)
from unittest import (
    mock,
)

from moto import (
    mock_dynamodb,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
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

    def _dynamodb_table_name(self) -> str:
        return SourceService.table_name

    def _dynamodb_atttributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        return {SourceService.key_attribute: 'S'}

    def _dynamodb_hash_key(self) -> str:
        return SourceService.key_attribute

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
