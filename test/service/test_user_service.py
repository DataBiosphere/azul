from unittest import mock

from azul import config
from azul.logging import configure_test_logging
from azul.service.user_service import (
    UserService,
    UpdateError,
)
from dynamo_test_case import DynamoTestCase


def setUpModule():
    configure_test_logging()


class TestUserService(DynamoTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with mock.patch('azul.deployment.aws.dynamo') as dynamo:
            dynamo.return_value = cls.dynamo_accessor.dynamo_client
            cls.user_service = UserService()

    def setUp(self):
        super().setUp()
        self.create_tables()

    def tearDown(self):
        self.dynamo_accessor.get_table(config.dynamo_user_table_name).delete()
        super().tearDown()

    def create_tables(self):
        # Table definitions here must match definitions in Terraform

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

    def test_get_or_create(self):
        # This user object does not exists before. In this case, the user
        # object will be create on the demand.
        test_user_id = 'user_1'
        user = self.user_service.get_or_create(test_user_id)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertIsNone(user['DefaultCartId'])

    def test_update_with_default_cart_id_defined_ok(self):
        # This user object does not exists before. In this case, the user
        # object will be create on the demand.
        test_user_id = 'user_1'
        mock_cart_id = 'cart_1'
        user = self.user_service.get_or_create(test_user_id)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertIsNone(user['DefaultCartId'])
        # Test to set a random cart ID to the user object.
        user = self.user_service.update(test_user_id, mock_cart_id)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertEqual(user['DefaultCartId'], mock_cart_id)

    def test_update_with_default_cart_id_defined_and_removed_ok(self):
        # This user object does not exists before. In this case, the user
        # object will be create on the demand.
        test_user_id = 'user_1'
        mock_cart_id = 'cart_1'
        self.user_service.get_or_create(test_user_id)
        user = self.user_service.update(test_user_id, mock_cart_id)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertEqual(user['DefaultCartId'], mock_cart_id)
        # Test to set NO cart ID to the user object.
        user = self.user_service.update(test_user_id, None)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertIsNone(user['DefaultCartId'])

    def test_update_to_reset_default_cart_id_ok(self):
        # This user object does not exists before. In this case, the user
        # object will be create on the demand.
        test_user_id = 'user_1'
        self.user_service.get_or_create(test_user_id)
        # Test to reset the cart ID even if it is not defined.
        user = self.user_service.update(test_user_id, None)
        self.assertEqual(user['UserId'], test_user_id)
        self.assertIsNone(user['DefaultCartId'])

    def test_update_non_existing_user_raises_update_error(self):
        test_user_id = 'user_1'
        mock_cart_id = 'cart_1'
        with self.assertRaises(UpdateError):
            self.user_service.update(test_user_id, mock_cart_id)

    def test_update_default_cart_id_raises_update_error(self):
        test_user_id = 'user_1'
        mock_cart_id = 'cart_1'
        # Create a user object.
        self.user_service.get_or_create(test_user_id)
        # The first update should not raise any exception.
        self.user_service.update(test_user_id, mock_cart_id)
        # The second update will raise UpdateError.
        with self.assertRaises(UpdateError):
            self.user_service.update(test_user_id, 'asf')
