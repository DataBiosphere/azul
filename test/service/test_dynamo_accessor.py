from unittest import TestCase, main

from moto import mock_dynamodb2, mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor


class TestDynamoAccessor(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dynamo_accessor = DynamoDataAccessor()

    def create_tables(self):
        self.dynamo_accessor.dynamo_client.create_table(TableName='Carts',
                                                        KeySchema=[
                                                            {
                                                                'AttributeName': 'UserId',
                                                                'KeyType': 'HASH'
                                                            },
                                                            {
                                                                'AttributeName': 'Name',
                                                                'KeyType': 'Range'
                                                            }
                                                        ],
                                                        AttributeDefinitions=[
                                                            {
                                                                'AttributeName': 'UserId',
                                                                'AttributeType': 'S'
                                                            },
                                                            {
                                                                'AttributeName': 'Name',
                                                                'AttributeType': 'S'
                                                            },
                                                            {
                                                                'AttributeName': 'EntityType',
                                                                'AttributeType': 'S'
                                                            }
                                                        ],
                                                        ProvisionedThroughput={
                                                            'ReadCapacityUnits': 1,
                                                            'WriteCapacityUnits': 1
                                                        },
                                                        GlobalSecondaryIndexes=[
                                                            {
                                                                'IndexName': 'EntityTypeIndex',
                                                                'KeySchema': [{
                                                                    'AttributeName': 'EntityType',
                                                                    'KeyType': 'HASH'
                                                                }],
                                                                'Projection': {
                                                                    'ProjectionType': 'ALL'
                                                                },
                                                                'ProvisionedThroughput': {
                                                                    'ReadCapacityUnits': 1,
                                                                    'WriteCapacityUnits': 1
                                                                }
                                                            }
                                                        ])

    @mock_dynamodb2
    @mock_sts
    def test_query_empty(self):
        self.create_tables()
        result = self.dynamo_accessor.query('Carts', key_conditions={'UserId': '123'})
        self.assertEqual(0, len(result))

    @mock_dynamodb2
    @mock_sts
    def test_insert_one_and_query(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test1', 'EntityType': 'files'})
        valid_result = self.dynamo_accessor.query('Carts',
                                                  key_conditions={'UserId': '123'})
        self.assertEqual(1, len(valid_result))
        empty_result = self.dynamo_accessor.query('Carts',
                                                  key_conditions={'UserId': '124'})
        self.assertEqual(0, len(empty_result))

    @mock_dynamodb2
    @mock_sts
    def test_insert_and_get_single_item(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '1', 'Name': 'test1'})
        valid_result = self.dynamo_accessor.get_item('Carts',
                                                     keys={'UserId': '1', 'Name': 'test1'})
        self.assertIsNotNone(valid_result)
        empty_result = self.dynamo_accessor.get_item('Carts',
                                                     keys={'UserId': '1', 'Name': 'test2'})
        self.assertIsNone(empty_result)

    @mock_dynamodb2
    @mock_sts
    def test_insert_and_delete_single(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test1'})
        single_result = self.dynamo_accessor.query('Carts',
                                                   key_conditions={'UserId': '123'})
        self.assertEqual(1, len(single_result))
        self.dynamo_accessor.delete_item('Carts',
                                         keys={'UserId': '123', 'Name': 'test1'})
        empty_result = self.dynamo_accessor.query('Carts',
                                                  key_conditions={'UserId': '123'})
        self.assertEqual(0, len(empty_result))

    @mock_dynamodb2
    @mock_sts
    def test_query_secondary_index(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test1', 'EntityType': 'files'})
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test2', 'EntityType': 'files'})
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test3', 'EntityType': 'not_files'})
        valid_result = self.dynamo_accessor.query('Carts',
                                                  key_conditions={'EntityType': 'files'},
                                                  index_name='EntityTypeIndex')
        self.assertEqual(2, len(valid_result))

    @mock_dynamodb2
    @mock_sts
    def test_update_item(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'Name': 'test1', 'Value': 'value1'})
        self.dynamo_accessor.update_item('Carts',
                                         keys={'UserId': '123', 'Name': 'test1'},
                                         update_values={'Value': 'value2'})
        updated_item = self.dynamo_accessor.get_item('Carts',
                                                     keys={'UserId': '123', 'Name': 'test1'})
        self.assertEqual('value2', updated_item['Value'])

    # TODO: Test alternate and failure paths


if __name__ == '__main__':
    main()
