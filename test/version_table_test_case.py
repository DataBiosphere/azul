import unittest

from moto import (
    mock_dynamodb2,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.version_service import (
    VersionService,
)
from azul_test_case import (
    AzulUnitTestCase,
)


@mock_dynamodb2
class VersionTableTestCase(AzulUnitTestCase):
    # Moto's dynamodb backend doesn't support government regions.
    _aws_test_region = 'ap-south-1'

    def setUp(self):
        super().setUp()
        self.ddb_client = aws.client('dynamodb')

        self.ddb_client.create_table(TableName=config.dynamo_object_version_table_name,
                                     AttributeDefinitions=[
                                         dict(AttributeName=VersionService.key_name, AttributeType='S'),
                                         dict(AttributeName=VersionService.value_name, AttributeType='S')
                                     ],
                                     KeySchema=[
                                         dict(AttributeName=VersionService.key_name, KeyType='HASH'),
                                     ])

    def tearDown(self):
        self.ddb_client.delete_table(TableName=config.dynamo_object_version_table_name)
        super().tearDown()


if __name__ == '__main__':
    unittest.main()
