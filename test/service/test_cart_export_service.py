from unittest import TestCase
from unittest.mock import patch

import responses

from azul.service.responseobjects.cart_export_service import CartExportService, ExpiredAccessTokenError
from azul.service.responseobjects.collection_data_access import CollectionDataAccess

from retorts import ResponsesHelper


class TestCartExportService(TestCase):

    @patch('azul.deployment.aws.dynamo')
    def test_get_content_with_no_resume_token_returning_results_without_next_resume_token(self, dynamodb_client):
        mock_entity_1 = dict(EntityId='entity1', EntityType='foo', EntityVersion='bar')
        mock_entity_2 = dict(EntityId='entity2', EntityType='foo', EntityVersion='bar')
        expected_content_item_1 = dict(type='file',
                                       uuid=mock_entity_1['EntityId'],
                                       version=mock_entity_1['EntityVersion'])

        def mock_get_paginable_cart_items(**kwargs):
            self.assertIsNone(kwargs['resume_token'])
            return dict(items=[mock_entity_1, mock_entity_2],
                        last_evaluated_key=None)

        service = CartExportService()
        with patch.object(service.cart_item_manager,
                          'get_paginable_cart_items',
                          side_effect=mock_get_paginable_cart_items):
            content = service.get_content('user1', 'cart1', 'collection1', 'ver1', None)
        content_items = content['items']
        self.assertIsNone(content['resume_token'])
        self.assertEquals(2, len(content_items))
        self.assertIn(expected_content_item_1, content_items)

    @patch('azul.deployment.aws.dynamo')
    def test_get_content_with_no_resume_token_returning_no_results_without_next_resume_token(self, dynamodb_client):
        def mock_get_paginable_cart_items(**kwargs):
            self.assertIsNone(kwargs['resume_token'])
            return dict(items=[],
                        last_evaluated_key=None)

        service = CartExportService()
        with patch.object(service.cart_item_manager,
                          'get_paginable_cart_items',
                          side_effect=mock_get_paginable_cart_items):
            content = service.get_content('user1', 'cart1', 'collection1', 'ver1', None)
        self.assertIsNone(content['resume_token'])
        self.assertEquals(0, len(content['items']))

    @patch('azul.deployment.aws.dynamo')
    def test_get_content_with_resume_token_returning_results_with_next_resume_token(self, dynamodb_client):
        mock_resume_token = 'abc'
        mock_entity_1 = dict(EntityId='entity1', EntityType='foo', EntityVersion='bar')
        mock_entity_2 = dict(EntityId='entity2', EntityType='foo', EntityVersion='bar')
        expected_content_item_1 = dict(type='file',
                                       uuid=mock_entity_1['EntityId'],
                                       version=mock_entity_1['EntityVersion'])

        def mock_get_paginable_cart_items(**kwargs):
            self.assertIsNotNone(kwargs['resume_token'])
            return dict(items=[mock_entity_1, mock_entity_2],
                        last_evaluated_key={'foo': 'bar'})

        service = CartExportService()
        with patch.object(service.cart_item_manager,
                          'get_paginable_cart_items',
                          side_effect=mock_get_paginable_cart_items):
            content = service.get_content('user1', 'cart1', 'collection1', 'ver1', mock_resume_token)
        content_items = content['items']
        self.assertNotEquals(mock_resume_token, content['resume_token'])
        self.assertEquals(2, len(content_items))
        self.assertIn(expected_content_item_1, content_items)

    @responses.activate
    @patch('azul.deployment.aws.dynamo')
    def test_export_create_new_collection(self, dynamodb_client):
        expected_collection = dict(uuid='abc', version='123')
        expected_get_content_result = dict(resume_token='rt1',
                                           items=[1, 2, 3, 4])  # NOTE: This is just for the test.
        service = CartExportService()
        with patch.object(service.cart_item_manager, 'get_cart', side_effect=[dict(CartName='abc123')]):
            with patch.object(service, 'get_content', side_effect=[expected_get_content_result]):
                with ResponsesHelper() as helper:
                    helper.add(responses.Response(responses.PUT,
                                                  CollectionDataAccess.endpoint_url('collections'),
                                                  status=201,
                                                  json=expected_collection))
                    result = service.export(export_id='export1',
                                            user_id='user1',
                                            cart_id='cart1',
                                            access_token='at1',
                                            collection_uuid=expected_collection['uuid'],
                                            collection_version='ver1',
                                            resume_token=None)
        self.assertEquals(expected_collection, result['collection'])
        self.assertEquals(expected_get_content_result['resume_token'], result['resume_token'])
        self.assertEquals(len(expected_get_content_result['items']), result['exported_item_count'])

    @responses.activate
    @patch('azul.deployment.aws.dynamo')
    def test_export_append_items_to_collection_ok(self, dynamodb_client):
        expected_collection = dict(uuid='abc', version='123')
        expected_get_content_result = dict(resume_token='rt1',
                                           items=[1, 2, 3, 4])  # NOTE: This is just for the test.
        service = CartExportService()
        with patch.object(service, 'get_content', side_effect=[expected_get_content_result]):
            with ResponsesHelper() as helper:
                helper.add(responses.Response(
                    responses.PATCH,
                    CollectionDataAccess.endpoint_url('collections', expected_collection['uuid']),
                    json=expected_collection
                ))
                result = service.export(export_id='export1',
                                        user_id='user1',
                                        cart_id='cart1',
                                        access_token='at1',
                                        collection_uuid=expected_collection['uuid'],
                                        collection_version='ver1',
                                        resume_token='rt0')
        self.assertEquals(expected_collection, result['collection'])
        self.assertEquals(expected_get_content_result['resume_token'], result['resume_token'])
        self.assertEquals(len(expected_get_content_result['items']), result['exported_item_count'])

    @responses.activate
    @patch('azul.deployment.aws.dynamo')
    def test_export_append_items_to_collection_raises_expired_access_token_error(self, dynamodb_client):
        expected_collection = dict(uuid='abc', version='123')
        expected_get_content_result = dict(resume_token='rt1',
                                           items=[1, 2, 3, 4])  # NOTE: This is just for the test.
        service = CartExportService()
        with self.assertRaises(ExpiredAccessTokenError):
            with patch.object(service, 'get_content', side_effect=[expected_get_content_result]):
                with ResponsesHelper() as helper:
                    url = CollectionDataAccess.endpoint_url('collections', expected_collection['uuid'])
                    helper.add(responses.Response(responses.PATCH, url, status=401, json=dict(code='abc')))
                    service.export(export_id='export1',
                                   user_id='user1',
                                   cart_id='cart1',
                                   access_token='at1',
                                   collection_uuid=expected_collection['uuid'],
                                   collection_version='ver1',
                                   resume_token='rt0')
