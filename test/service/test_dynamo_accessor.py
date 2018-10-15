from unittest import TestCase

from moto import mock_dynamodb2, mock_sts

from azul.service.responseobjects.dynamo_data_access import DynamoDataAccessor


class TestDynamoAccessor(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dynamo_accessor = DynamoDataAccessor()

    def create_tables(self):
        self.dynamo_accessor.create_table('Carts',
                                          keys={'user_id': 'HASH', 'name': 'RANGE'},
                                          attributes={'user_id': 'S', 'name': 'S'})

    @mock_dynamodb2
    @mock_sts
    def test_query_empty(self):
        self.create_tables()
        result = self.dynamo_accessor.query('Carts', {'user_id': '123'})
        self.assertEqual(0, len(result))

    @mock_dynamodb2
    @mock_sts
    def test_insert_one_and_query(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts', {'user_id': '123', 'name': 'test1', 'entity_type': 'files'})
        valid_result = self.dynamo_accessor.query('Carts', {'user_id': '123'})
        self.assertEqual(1, len(valid_result))
        empty_result = self.dynamo_accessor.query('Carts', {'user_id': '124'})
        self.assertEqual(0, len(empty_result))

    @mock_dynamodb2
    @mock_sts
    def test_insert_and_delete_single(self):
        self.create_tables()
        self.dynamo_accessor.insert_item('Carts', {'user_id': '123', 'name': 'test1', 'entity_type': 'files'})
        single_result = self.dynamo_accessor.query('Carts', {'user_id': '123'})
        self.assertEqual(1, len(single_result))
        self.dynamo_accessor.delete_item('Carts', {'user_id': '123', 'name': 'test1'})
        empty_result = self.dynamo_accessor.query('Carts', {'user_id': '123'})
        self.assertEqual(0, len(empty_result))
