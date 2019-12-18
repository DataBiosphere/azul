from azul.logging import configure_test_logging
from azul.service.dynamo_data_access import ConditionalUpdateItemError
from dynamo_test_case import DynamoTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestDynamoAccessor(DynamoTestCase):
    table_name = 'Carts'

    def create_tables(self):
        self.dynamo_accessor.dynamo_client.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'UserId',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'CartName',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'UserId',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'CartName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'EntityType',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
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
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ])

    def setUp(self):
        super().setUp()
        self.create_tables()

    def tearDown(self):
        self.dynamo_accessor.get_table(self.table_name).delete()
        super().tearDown()

    def test_query_empty(self):
        """
        Query an empty table should return a generator yielding no results
        """
        result = list(self.dynamo_accessor.query(table_name='Carts', key_conditions={'UserId': '123'}))
        self.assertEqual(0, len(result))

    def test_insert_one_and_query(self):
        """
        Inserting an item should allow the item to show up in a query result
        """
        item = {'UserId': '123', 'CartName': 'test1', 'EntityType': 'files'}
        self.dynamo_accessor.insert_item('Carts', item=item)
        valid_result = list(self.dynamo_accessor.query(table_name='Carts', key_conditions={'UserId': '123'}))
        self.assertEqual(1, len(valid_result))
        self.assertEqual(item, valid_result[0])
        empty_result = list(self.dynamo_accessor.query(table_name='Carts', key_conditions={'UserId': '124'}))
        self.assertEqual(0, len(empty_result))

    def test_insert_and_get_single_item(self):
        """
        Inserting an item should allow the item to be retrieved with the get action
        """
        item = {'UserId': '1', 'CartName': 'test1'}
        self.dynamo_accessor.insert_item('Carts', item=item)
        valid_result = self.dynamo_accessor.get_item('Carts', keys=item)
        self.assertEqual(item, valid_result)
        empty_result = self.dynamo_accessor.get_item('Carts', keys={'UserId': '1', 'CartName': 'test2'})
        self.assertIsNone(empty_result)

    def test_insert_and_get_with_nonstring_types(self):
        """
        An inserted item's attributes should retain correct types when retrieved
        """
        self.dynamo_accessor.insert_item(
            'Carts',
            item={'UserId': '1', 'CartName': 'test1', 'Number': 10, 'SS': {'a', 'b', 'c'}, 'Bool': False,
                  'Bytes': b'abc'}
        )
        result = self.dynamo_accessor.get_item('Carts', keys={'UserId': '1', 'CartName': 'test1'})
        self.assertEqual(10, result['Number'])
        self.assertEqual({'a', 'c', 'b'}, result['SS'])
        self.assertEqual(False, result['Bool'])
        self.assertEqual(b'abc', result['Bytes'])

    def test_insert_and_delete_single(self):
        """
        Deleting an item should make the item not retrievable
        """
        self.dynamo_accessor.insert_item('Carts', item={'UserId': '123', 'CartName': 'test1'})
        single_result = list(self.dynamo_accessor.query(table_name='Carts', key_conditions={'UserId': '123'}))
        self.assertEqual(1, len(single_result))
        self.dynamo_accessor.delete_item('Carts', keys={'UserId': '123', 'CartName': 'test1'})
        empty_result = list(self.dynamo_accessor.query(table_name='Carts', key_conditions={'UserId': '123'}))
        self.assertEqual(0, len(empty_result))

    def test_query_secondary_index(self):
        """
        Passing an index name argument into the query method should allow querying of a global secondary index
        """
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'CartName': 'test1', 'EntityType': 'files'})
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'CartName': 'test2', 'EntityType': 'files'})
        self.dynamo_accessor.insert_item('Carts',
                                         item={'UserId': '123', 'CartName': 'test3', 'EntityType': 'not_files'})
        valid_result = list(self.dynamo_accessor.query(table_name='Carts',
                                                       key_conditions={'EntityType': 'files'},
                                                       index_name='EntityTypeIndex'))
        self.assertEqual(2, len(valid_result))

    def test_update_item(self):
        """
        Updating an item should change the given attribute of the item to the given value
        """
        self.dynamo_accessor.insert_item('Carts', item={'UserId': '123', 'CartName': 'test1', 'Val': 'value1'})
        self.dynamo_accessor.update_item('Carts',
                                         keys={'UserId': '123', 'CartName': 'test1'},
                                         update_values={'Val': 'value2'})
        updated_item = self.dynamo_accessor.get_item('Carts', keys={'UserId': '123', 'CartName': 'test1'})
        self.assertEqual('value2', updated_item['Val'])

    def test_update_item_conditional_pass(self):
        """
        Updating an item using a conditional test that passes should successfully update the item
        """
        self.dynamo_accessor.insert_item('Carts', item={'UserId': '123', 'CartName': 'test1', 'Val': 'value1'})
        self.dynamo_accessor.update_item('Carts',
                                         keys={'UserId': '123', 'CartName': 'test1'},
                                         update_values={'Val': 'value2'},
                                         conditions={'Val': 'value1'})
        updated_item = self.dynamo_accessor.get_item('Carts', keys={'UserId': '123', 'CartName': 'test1'})
        self.assertEqual('value2', updated_item['Val'])

    def test_update_item_conditional_fail(self):
        """
        Updating an item using a conditional test that fails should raise an exception and not update the item
        """
        self.dynamo_accessor.insert_item('Carts', item={'UserId': '123', 'CartName': 'test1', 'Val': 'value1'})
        with self.assertRaises(ConditionalUpdateItemError):
            self.dynamo_accessor.update_item('Carts',
                                             keys={'UserId': '123', 'CartName': 'test1'},
                                             update_values={'Val': 'value2'},
                                             conditions={'Val': 'value2'})
        updated_item = self.dynamo_accessor.get_item('Carts', keys={'UserId': '123', 'CartName': 'test1'})
        self.assertEqual('value1', updated_item['Val'])

    def test_batch_write_and_query_pagination(self):
        """
        Writing a batch of items should write all the items in the batch to the database
        """
        items = []
        for i in range(1000):
            items.append({
                'UserId': '123',
                'CartName': str(i)
            })
        self.dynamo_accessor.batch_write(self.table_name, items)
        count = self.dynamo_accessor.count(table_name=self.table_name,
                                           key_conditions={'UserId': '123'})
        self.assertEqual(1000, count)

        results = self.dynamo_accessor.query(table_name=self.table_name,
                                             key_conditions={'UserId': '123'},
                                             limit=100)
        for i, item in enumerate(sorted(results, key=lambda x: int(x['CartName']))):
            self.assertEqual(i, int(item['CartName']))

    def test_delete_by_key(self):
        """
        Deleting items by key should delete all the items matching the key conditions and nothing else
        """
        for i in range(200):
            item = {
                'UserId': '123',
                'CartName': f'a{i}'
            }
            self.dynamo_accessor.insert_item(self.table_name, item)
        for i in range(100):
            item = {
                'UserId': '124',
                'CartName': f'b{i}'
            }
            self.dynamo_accessor.insert_item(self.table_name, item)
        total_inserted = (self.dynamo_accessor.count(table_name=self.table_name,
                                                     key_conditions={'UserId': '123'}) +
                          self.dynamo_accessor.count(table_name=self.table_name,
                                                     key_conditions={'UserId': '124'}))
        self.assertEqual(300, total_inserted)
        self.dynamo_accessor.delete_by_key(self.table_name, key_conditions={'UserId': '123'})
        after_delete = (self.dynamo_accessor.count(table_name=self.table_name,
                                                   key_conditions={'UserId': '123'}) +
                        self.dynamo_accessor.count(table_name=self.table_name,
                                                   key_conditions={'UserId': '124'}))
        self.assertEqual(100, after_delete)
