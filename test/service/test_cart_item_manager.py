from contextlib import contextmanager
from unittest import mock
from unittest.mock import patch

import requests

from azul import config
from azul.logging import configure_test_logging
from azul.service.cart_item_manager import (
    CartItemManager,
    DuplicateItemError,
    ResourceAccessError,
)
from azul.service.elasticsearch_service import ElasticsearchService
from dynamo_test_case import DynamoTestCase
from service import WebServiceTestCase


def setUpModule():
    configure_test_logging()


class TestCartItemManager(WebServiceTestCase, DynamoTestCase):
    number_of_documents = 1500

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()
        cls._fill_index(cls.number_of_documents)
        with mock.patch('azul.deployment.aws.dynamo') as dynamo:
            dynamo.return_value = cls.dynamo_accessor.dynamo_client
            cls.cart_item_manager = CartItemManager()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.create_tables()

    def tearDown(self):
        self.dynamo_accessor.get_table(config.dynamo_cart_table_name).delete()
        self.dynamo_accessor.get_table(config.dynamo_cart_item_table_name).delete()
        self.dynamo_accessor.get_table(config.dynamo_user_table_name).delete()
        super().tearDown()

    def create_tables(self):
        # Table definitions here must match definitions in Terraform

        self.dynamo_accessor.dynamo_client.create_table(
            TableName=config.dynamo_cart_table_name,
            KeySchema=[
                {
                    'AttributeName': 'UserId',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'CartId',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'CartId',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'UserId',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'CartName',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'UserIndex',
                    'KeySchema': [{
                        'AttributeName': 'UserId',
                        'KeyType': 'HASH'
                    }],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 1,
                        'WriteCapacityUnits': 1
                    }
                },
                {
                    'IndexName': 'UserCartNameIndex',
                    'KeySchema': [
                        {
                            'AttributeName': 'UserId',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'CartName',
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ]
        )

        self.dynamo_accessor.dynamo_client.create_table(
            TableName=config.dynamo_cart_item_table_name,
            KeySchema=[
                {
                    'AttributeName': 'CartId',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'CartItemId',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'CartId',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'CartItemId',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        self.dynamo_accessor.dynamo_client.create_table(
            TableName=config.dynamo_user_table_name,
            KeySchema=[
                {
                    'AttributeName': 'UserId',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'UserId',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

    def test_cart_creation(self):
        """
        Creating a cart should add a cart with the given name and user ID to the cart table and return the cart ID
        """
        user_id = '123'
        cart_name = 'cart name'
        cart_id = self.cart_item_manager.create_cart(user_id, cart_name, False)
        cart = self.dynamo_accessor.get_item(config.dynamo_cart_table_name, {'UserId': user_id, 'CartId': cart_id})
        self.assertEqual(cart['UserId'], user_id)
        self.assertEqual(cart['CartName'], cart_name)
        self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], None)

    def test_cart_creation_default(self):
        """
        Creating a default cart should create a cart with the ID 'default' and DefaultCart flag set to True
        """
        mock_cart_id = 'test_default_cart'
        user_id = '123'
        cart_name = 'cart name'
        with patch('uuid.uuid4', side_effect=[mock_cart_id]):
            cart_id = self.cart_item_manager.create_cart(user_id, cart_name, True)
        cart = self.dynamo_accessor.get_item(config.dynamo_cart_table_name, {'UserId': user_id, 'CartId': cart_id})
        self.assertEqual(cart['UserId'], user_id)
        self.assertEqual(cart['CartName'], cart_name)
        self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], cart['CartId'])
        self.assertEqual(cart['CartId'], mock_cart_id)

    def test_cart_creation_duplicate_name(self):
        """
        Trying to create a cart with a name that matches an existing cart belonging to the user should raise an error
        """
        cart_name = 'cart name'
        self.cart_item_manager.create_cart('123', cart_name, False)
        self.cart_item_manager.create_cart('124', cart_name, False)
        with self.assertRaises(DuplicateItemError):
            self.cart_item_manager.create_cart('123', cart_name, False)

    def test_cart_creation_duplicate_default_will_not_create_new_cart(self):
        """
        Trying to create a default cart when the user already has a default cart should not create a new default cart.
        """
        test_user_id = '123'
        cart_id_1 = self.cart_item_manager.create_cart(test_user_id, 'Cart1', True)
        cart_id_2 = self.cart_item_manager.create_cart(test_user_id, 'Cart3', True)
        self.assertEqual(cart_id_1, cart_id_2)
        self.assertEqual(1, len(self.cart_item_manager.get_user_carts(test_user_id)))

    def test_get_cart(self):
        """
        Getting a cart item should return the same item as performing get item on the carts table
        """
        user_id = '123'
        cart_id = self.cart_item_manager.create_cart(user_id, 'Cart1', True)
        self.assertEqual(self.cart_item_manager.get_cart(user_id, cart_id),
                         self.dynamo_accessor.get_item(config.dynamo_cart_table_name,
                                                       keys={'UserId': user_id, 'CartId': cart_id}))

    def test_get_default_cart_with_existing_default_cart(self):
        """
        If the default cart already exists, the manager should not create a new cart.
        """
        mock_cart_id = 'test_default_cart'
        user_id = '123'
        cart_name = 'cart name'
        with patch('uuid.uuid4', side_effect=[mock_cart_id]):
            self.cart_item_manager.create_cart(user_id, cart_name, True)
        cart = self.cart_item_manager.get_default_cart(user_id)
        self.assertEqual(cart['UserId'], user_id)
        self.assertEqual(cart['CartName'], cart_name)
        self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], mock_cart_id)
        self.assertEqual(cart['CartId'], mock_cart_id)
        self.assertEqual(1, len(self.cart_item_manager.get_user_carts(user_id)))

    def test_get_default_cart_with_no_default_cart(self):
        """
        If the default cart does not exist, the manager should create a new cart and register that cart in the user
        object as the default cart.
        """
        user_id = '123'
        self.assertEqual(0, len(self.cart_item_manager.get_user_carts(user_id)))
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.get_default_cart(user_id)

    def test_get_or_create_default_cart_with_existing_default_cart(self):
        """
        If the default cart already exists, the manager should not create a new cart.
        """
        mock_cart_id = 'test_default_cart'
        user_id = '123'
        cart_name = 'cart name'
        with patch('uuid.uuid4', side_effect=[mock_cart_id]):
            self.cart_item_manager.create_cart(user_id, cart_name, True)
        cart = self.cart_item_manager.get_or_create_default_cart(user_id)
        self.assertEqual(cart['UserId'], user_id)
        self.assertEqual(cart['CartName'], cart_name)
        self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], mock_cart_id)
        self.assertEqual(cart['CartId'], mock_cart_id)
        self.assertEqual(1, len(self.cart_item_manager.get_user_carts(user_id)))

    def test_get_or_create_default_cart_with_no_default_cart(self):
        """
        If the default cart does not exist, the manager should create a new cart and register that cart in the user
        object as the default cart.
        """
        mock_cart_id = 'test_default_cart'
        user_id = '123'
        cart_name = 'Default Cart'
        self.assertEqual(0, len(self.cart_item_manager.get_user_carts(user_id)))
        with patch('uuid.uuid4', side_effect=[mock_cart_id]):
            cart1 = self.cart_item_manager.get_or_create_default_cart(user_id)
            self.assertEqual(1, len(self.cart_item_manager.get_user_carts(user_id)))
            self.assertEqual(cart1['UserId'], user_id)
            self.assertEqual(cart1['CartName'], cart_name)
            self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], mock_cart_id)
            self.assertEqual(cart1['CartId'], mock_cart_id)
            # The second call should return the same cart.
            cart2 = self.cart_item_manager.get_or_create_default_cart(user_id)
            self.assertEqual(cart1['CartId'], cart2['CartId'])
        self.assertEqual(1, len(self.cart_item_manager.get_user_carts(user_id)))

    def test_get_user_carts(self):
        """
        Getting a user's carts should return all of and only the user's carts
        """
        self.cart_item_manager.create_cart('123', 'Cart1', True)
        self.cart_item_manager.create_cart('123', 'Cart2', False)
        self.cart_item_manager.create_cart('123', 'Cart3', False)
        self.cart_item_manager.create_cart('124', 'Cart2', True)
        self.assertEqual(3, len(self.cart_item_manager.get_user_carts('123')))

    def test_delete_cart(self):
        """
        Deleting a cart should remove only that cart from the database as well as items belonging to the cart
        """
        user_id = '123'
        cart_id1 = self.cart_item_manager.create_cart(user_id, 'Cart1', True)
        cart_id2 = self.cart_item_manager.create_cart(user_id, 'Cart2', False)
        cart_id3 = self.cart_item_manager.create_cart(user_id, 'Cart3', False)
        self.cart_item_manager.add_cart_item(user_id, cart_id1, '1', 'entity_type', 'entity_version')
        self.cart_item_manager.add_cart_item(user_id, cart_id1, '2', 'entity_type', 'entity_version')
        self.cart_item_manager.add_cart_item(user_id, cart_id2, '2', 'entity_type', 'entity_version')
        # Delete the non-default cart.
        # NOTE: The default cart should be left untouched.
        self.cart_item_manager.delete_cart(user_id, cart_id3)
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.get_cart(user_id, cart_id3)
        self.assertEqual(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'], cart_id1)
        self.assertIsNotNone(self.cart_item_manager.get_cart(user_id, cart_id1))
        # Delete the default cart.
        # NOTE: At this point, the user object should have the default cart ID undefined.
        self.cart_item_manager.delete_cart(user_id, cart_id1)
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.get_cart(user_id, cart_id1)
        self.assertIsNone(self.cart_item_manager.user_service.get_or_create(user_id)['DefaultCartId'])
        self.assertIsNotNone(self.cart_item_manager.get_cart(user_id, cart_id2))

    def test_update_cart_name(self):
        """
        Updating a cart's name should change the cart's name and other attributes should be the same
        """
        user_id = '123'
        cart_id = self.cart_item_manager.create_cart(user_id, 'Cart1', False)
        self.cart_item_manager.update_cart(user_id, cart_id, {'CartName': 'Cart2'})
        self.assertEqual('Cart2', self.cart_item_manager.get_cart(user_id, cart_id)['CartName'])

    def test_update_cart_name_duplicate(self):
        """
        Updating a cart's name to one that already exists should raise an error
        """
        user_id = '123'
        cart_id = self.cart_item_manager.create_cart(user_id, 'Cart1', False)
        self.cart_item_manager.create_cart(user_id, 'Cart2', False)
        with self.assertRaises(DuplicateItemError):
            self.cart_item_manager.update_cart(user_id, cart_id, {'CartName': 'Cart2'})

    def test_update_cart_name_same_name(self):
        """
        Updating a cart's name to the same name should not raise an error
        """
        user_id = '123'
        cart_id = self.cart_item_manager.create_cart(user_id, 'Cart1', False)
        self.cart_item_manager.update_cart(user_id, cart_id, {'CartName': 'Cart1'})

    def test_update_cart_invalid_attributes(self):
        """
        Invalid attributes passed to the update cart function should be ignored
        """
        user_id = '123'
        cart_id = self.cart_item_manager.create_cart(user_id, 'Cart1', False)
        self.cart_item_manager.update_cart(user_id, cart_id, {'InvalidAttribute': 'Cart2'})
        self.assertEqual('Cart1', self.cart_item_manager.get_cart(user_id, cart_id)['CartName'])

    def test_add_cart_item(self):
        """
        Adding a cart item to a cart should put an item into cart items table and return the item ID
        """
        user_id = '111'
        cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
        item_id = self.cart_item_manager.add_cart_item(user_id, cart_id, 'entity_id', 'entity_type', 'entity_version')
        self.assertIsNotNone(self.dynamo_accessor.get_item(
            config.dynamo_cart_item_table_name,
            {'CartItemId': item_id, 'CartId': cart_id}))

    def test_add_cart_item_unauthorized_user(self):
        """
        Adding a cart item to a cart that does not belong to the user should raise an error
        """
        cart_id = self.cart_item_manager.create_cart('111', 'test cart', False)
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.add_cart_item('112', cart_id, 'entity_id', 'entity_type', 'entity_version')

    def test_add_cart_item_nonexistent_cart(self):
        """
        Adding a cart item to a cart that does not exist should raise an error
        """
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.add_cart_item('111', '123', 'entity_id', 'entity_type', 'entity_version')

    def test_get_cart_items(self):
        """
        Getting cart items should return all items belonging to the cart
        """
        user_id = '111'
        cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
        item_ids = [
            self.cart_item_manager.add_cart_item(user_id, cart_id, '1', 'entity_type', 'entity_version'),
            self.cart_item_manager.add_cart_item(user_id, cart_id, '2', 'entity_type', 'entity_version'),
            self.cart_item_manager.add_cart_item(user_id, cart_id, '3', 'entity_type', 'entity_version'),
            self.cart_item_manager.add_cart_item(user_id, cart_id, '4', 'entity_type', 'entity_version')
        ]
        retrieved_item_ids = [item['CartItemId'] for item in
                              self.cart_item_manager.get_cart_items(user_id, cart_id)]
        self.assertEqual(sorted(item_ids), sorted(retrieved_item_ids))

    def test_get_cart_items_unauthorized(self):
        """
        Getting items in a cart that does not belong to the user should raise an error
        """
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.get_cart_items('123', '1')

    def test_delete_cart_item(self):
        """
        Deleting a cart item should remove the item from the cart item table
        """
        user_id = '111'
        cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
        item_id1 = self.cart_item_manager.add_cart_item(user_id, cart_id, '1', 'entity_type', 'entity_version')
        item_id2 = self.cart_item_manager.add_cart_item(user_id, cart_id, '2', 'entity_type', 'entity_version')
        self.cart_item_manager.delete_cart_item(user_id, cart_id, item_id2)
        retrieved_item_ids = [item['CartItemId'] for item in
                              self.cart_item_manager.get_cart_items(user_id, cart_id)]
        self.assertEqual([item_id1], retrieved_item_ids)

    def test_delete_cart_item_unauthorized(self):
        """
        Deleting a cart item from a cart that does not belong to the user should raise an error
        """
        user_id = '111'
        cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
        item_id1 = self.cart_item_manager.add_cart_item(user_id, cart_id, '1', 'entity_type', 'entity_version')
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.delete_cart_item('112', cart_id, item_id1)

    def test_cart_item_batch_write_unauthorized(self):
        """
        Trying to batch write to a cart that does not belong to the user should raise an error
        """
        user_id = '111'
        cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
        with self.assertRaises(ResourceAccessError):
            self.cart_item_manager.start_batch_cart_item_write('112', cart_id, 'foo', {}, 12345, 10000)

    def test_transform_cart_item_pagination(self):
        """
        Cart item request transform should return pages of a max size of the given value and return
        a search_after string each time that will allow pagination through all documents in the index
        """
        size = 700
        service = ElasticsearchService()
        hits, search_after = service.transform_cart_item_request(entity_type='files',
                                                                 size=size)
        self.assertEqual(size, len(hits))
        hits, search_after = service.transform_cart_item_request(entity_type='files',
                                                                 size=size,
                                                                 search_after=search_after)
        self.assertEqual(size, len(hits))
        hits, search_after = service.transform_cart_item_request(entity_type='files',
                                                                 size=size,
                                                                 search_after=search_after)
        self.assertEqual(100, len(hits))

    @mock.patch('azul.deployment.aws.dynamo')
    def test_cart_item_write_batch_lambda(self, dynamo):
        """
        One call to the cart item batch write function should write one batch of the given batch size to Dynamo
        """
        dynamo.return_value = self.dynamo_accessor.dynamo_client

        cart_id = '123'
        write_params = {
            'entity_type': 'samples',
            'filters': {},
            'cart_id': cart_id,
            'batch_size': 1000
        }
        write_response = self.app_module.cart_item_write_batch(write_params, None)
        inserted_items = self.dynamo_accessor.query(table_name=config.dynamo_cart_item_table_name,
                                                    key_conditions={'CartId': cart_id})

        self.assertEqual(write_response['count'], len(list(inserted_items)))

    @contextmanager
    def _mock_auth(self, user_id):
        with mock.patch.object(self.app_module, 'Authenticator') as jwt_auth:
            jwt_auth.return_value.authenticate_bearer_token.return_value = {'sub': user_id}
            yield

    @mock.patch('azul.service.cart_item_manager.CartItemManager.step_function_helper')
    @mock.patch('azul.deployment.aws.dynamo')
    def test_add_all_results_to_cart_endpoint(self, dynamo, step_function_helper):
        """
        Write all results endpoint should start an execution of the cart item write state machine and
        return the name of the execution and the number items that will be written
        """
        dynamo.return_value = self.dynamo_accessor.dynamo_client

        execution_id = '89a68f98-48cb-43d0-88ad-5ffd8aa26b9d'
        step_function_helper.start_execution.return_value = {
            'executionArn': f'arn:aws:states:us-east-1:1234567890:execution:state_machine:{execution_id}'
        }

        json_body = {'filters': '{}', 'entityType': 'files'}

        user_id = '123'
        with self._mock_auth(user_id):
            cart_id = self.cart_item_manager.create_cart(user_id, 'test cart', False)
            response = requests.post(f"{self.base_url}/resources/carts/{cart_id}/items/batch",
                                     json=json_body,
                                     headers={'authorization': 'foo'})

        response.raise_for_status()
        response = response.json()
        self.assertEqual(response['count'], self.number_of_documents)
        token = response['statusUrl'].split('/')[-1]
        params = self.cart_item_manager.decode_token(token)
        self.assertTrue(execution_id, params['execution_id'])
        step_function_helper.start_execution.assert_called_once()
