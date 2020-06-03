import unittest

import boto3
from moto import mock_dynamodb2

from azul import config
from azul.version_service import VersionService
from azul_test_case import AzulUnitTestCase


@mock_dynamodb2
class VersionTableTestCase(AzulUnitTestCase):
    # Moto's dynamodb backend doesn't support government regions.
    _aws_test_region = 'ap-south-1'

    def setUp(self):
        self.ddb_client = boto3.client('dynamodb')

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


if __name__ == '__main__':
    unittest.main()
