from collections.abc import (
    Mapping,
)

from moto import (
    mock_dynamodb,
)

from azul.deployment import (
    aws,
)
from azul_test_case import (
    AzulUnitTestCase,
)


@mock_dynamodb
class DynamoDBTestCase(AzulUnitTestCase):
    """
    Don't forget to decorate concrete subclasses with @mock_dynamodb as well!
    """
    # Moto's dynamodb backend doesn't support government regions.
    _aws_test_region = 'ap-south-1'

    ddb_table_name: str = NotImplemented
    ddb_attrs: Mapping[str, str] = NotImplemented
    ddb_hash_key: str = NotImplemented

    def setUp(self):
        super().setUp()
        self.ddb_client = aws.dynamodb

        self.ddb_client.create_table(TableName=self.ddb_table_name,
                                     BillingMode='PAY_PER_REQUEST',
                                     AttributeDefinitions=[
                                         dict(AttributeName=attr_name, AttributeType=attr_type)
                                         for attr_name, attr_type in self.ddb_attrs.items()
                                     ],
                                     KeySchema=[
                                         dict(AttributeName=self.ddb_hash_key, KeyType='HASH')
                                     ])

    def tearDown(self):
        self.ddb_client.delete_table(TableName=self.ddb_table_name)
        super().tearDown()
