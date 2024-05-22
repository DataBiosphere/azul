from typing import (
    Mapping,
)

from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from azul import (
    config,
)
from azul.version_service import (
    VersionService,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)


class VersionTableTestCase(DynamoDBTestCase):

    def _dynamodb_table_name(self) -> str:
        return config.dynamo_object_version_table_name

    def _dynamodb_atttributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        return {VersionService.key_name: 'S'}

    def _dynamodb_hash_key(self) -> str:
        return VersionService.key_name
