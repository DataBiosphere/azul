from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Mapping,
)

from moto import (
    mock_dynamodb,
)
from mypy_boto3_dynamodb import (
    DynamoDBClient,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulUnitTestCase,
)


class DynamoDBTestCase(AzulUnitTestCase, metaclass=ABCMeta):
    # Moto's dynamodb backend doesn't support government regions.
    _aws_test_region = 'ap-south-1'

    @abstractmethod
    def _dynamodb_table_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _dynamodb_atttributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        raise NotImplementedError

    @abstractmethod
    def _dynamodb_hash_key(self) -> str:
        raise NotImplementedError

    @property
    def dynamodb(self) -> DynamoDBClient:
        return aws.dynamodb

    def setUp(self):
        super().setUp()
        self.addPatch(mock_dynamodb())
        self.dynamodb.create_table(TableName=self._dynamodb_table_name(),
                                   BillingMode='PAY_PER_REQUEST',
                                   AttributeDefinitions=[
                                       dict(AttributeName=attr_name, AttributeType=attr_type)
                                       for attr_name, attr_type in self._dynamodb_atttributes().items()
                                   ],
                                   KeySchema=[
                                       dict(AttributeName=self._dynamodb_hash_key(), KeyType='HASH')
                                   ])

    def tearDown(self):
        self.dynamodb.delete_table(TableName=self._dynamodb_table_name())
        super().tearDown()
