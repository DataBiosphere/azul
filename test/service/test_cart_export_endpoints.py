from unittest.mock import patch

import requests
import responses

from azul.service.responseobjects.cart_export_job_manager import CartExportJobManager, InvalidExecutionTokenError
from azul.service.responseobjects.collection_data_access import CollectionDataAccess
from azul import config

from app_test_case import AuthLocalAppTestCase
from retorts import AuthResponseHelper


class CartExportEndpointTest(AuthLocalAppTestCase):
    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @responses.activate
    def test_unified_endpoint_initiate_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='RUNNING',
            user_id=mock_jwt_subject,
            final=False,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export'
        with patch.object(CartExportJobManager, 'initiate', side_effect=[expected_export_token]),\
                patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
            with AuthResponseHelper(self.base_url) as helper:
                test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                response = requests.post(export_url,
                                         headers=dict(Authorization=f'Bearer {test_jwt}'),
                                         allow_redirects=False)
        self.assertEquals(301, response.status_code)
        self.assertEquals(f'{export_url}?token={expected_export_token}',
                          response.headers['Location'])

    @responses.activate
    def test_unified_endpoint_initiate_returns_http_400_due_to_soon_to_be_expiring_token(self):
        test_jwt_ttl = 10
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export'
        with patch.object(CartExportJobManager, 'initiate', side_effect=[expected_export_token]):
            with AuthResponseHelper(self.base_url) as helper:
                test_jwt = helper.generate_test_jwt('something@foo.bar', ttl=test_jwt_ttl)
                response = requests.post(export_url,
                                         headers=dict(Authorization=f'Bearer {test_jwt}'),
                                         allow_redirects=False)
        self.assertEquals(400, response.status_code)

    @responses.activate
    def test_unified_endpoint_check_status_on_running_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='RUNNING',
            user_id=mock_jwt_subject,
            final=False,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(301, response.status_code)
        self.assertEquals(export_url, response.headers['Location'])
        self.assertEquals('10', response.headers['Retry-After'])

    @responses.activate
    def test_unified_endpoint_check_status_with_invalid_token_responed_with_http_400(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[InvalidExecutionTokenError()]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(400, response.status_code)

    @responses.activate
    def test_unified_endpoint_check_status_on_running_job_responded_with_http_404(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='RUNNING',
            user_id=mock_jwt_subject,
            final=False,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(404, response.status_code)

    @responses.activate
    def test_unified_endpoint_check_status_on_succeeded_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_collection_uuid = 'ci-123'
        mock_collection_ver = 'cv-123'
        mock_job = dict(
            status='SUCCEEDED',
            user_id=mock_jwt_subject,
            final=True,
            last_update=dict(
                state=dict(
                    collection_uuid=mock_collection_uuid,
                    collection_version=mock_collection_ver
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(200, response.status_code)
        prefix_expected_url = CollectionDataAccess.endpoint_url('collections', mock_collection_uuid)
        self.assertEquals(
            f'{prefix_expected_url}?version={mock_collection_ver}&replica=aws',
            response.json()['CollectionUrl']
        )

    @responses.activate
    def test_unified_endpoint_check_status_on_failed_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='FAILED',
            user_id=mock_jwt_subject,
            final=True,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(500, response.status_code)

    @responses.activate
    def test_unified_endpoint_check_status_on_aborted_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='ABORTED',
            user_id=mock_jwt_subject,
            final=True,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(410, response.status_code)

    @responses.activate
    def test_unified_fetch_endpoint_initiate_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='RUNNING',
            user_id=mock_jwt_subject,
            final=False,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/fetch/resources/carts/{mock_cart_uuid}/export'
        with patch.object(CartExportJobManager, 'initiate', side_effect=[expected_export_token]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.post(export_url,
                                             headers=dict(Authorization=f'Bearer {test_jwt}'),
                                             allow_redirects=False)
        self.assertEquals(200, response.status_code)
        response_body = response.json()
        self.assertEquals(301, response_body['Status'])
        self.assertEquals(f'{export_url}?token={expected_export_token}', response_body['Location'])

    @responses.activate
    def test_unified_fetch_endpoint_check_status_on_running_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_job = dict(
            status='RUNNING',
            user_id=mock_jwt_subject,
            final=False,
            last_update=dict(
                state=dict(
                    collection_uuid='ci-123',
                    collection_version='cv-123'
                )
            )
        )
        export_url = f'{self.base_url}/fetch/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(200, response.status_code)
        response_body = response.json()
        self.assertEquals(301, response_body['Status'])
        self.assertEquals(export_url, response_body['Location'])
        self.assertEquals('10', response_body['Retry-After'])

    @responses.activate
    def test_unified_fetch_endpoint_check_status_on_succeeded_job_ok(self):
        test_jwt_ttl = config.cart_export_min_access_token_ttl + 10
        mock_user_id = 'user1'
        mock_jwt_subject = 'fake|' + mock_user_id
        mock_cart_uuid = 'mock-cart-1234'
        expected_export_token = 'abc123'
        mock_collection_uuid = 'ci-123'
        mock_collection_ver = 'cv-123'
        mock_job = dict(
            status='SUCCEEDED',
            user_id=mock_jwt_subject,
            final=True,
            last_update=dict(
                state=dict(
                    collection_uuid=mock_collection_uuid,
                    collection_version=mock_collection_ver
                )
            )
        )
        export_url = f'{self.base_url}/fetch/resources/carts/{mock_cart_uuid}/export?token={expected_export_token}'
        # NOTE The empty side_effect is to ensure that "initiate" never get called.
        with patch.object(CartExportJobManager, 'initiate', side_effect=[]):
            with patch.object(CartExportJobManager, 'get', side_effect=[mock_job]):
                with AuthResponseHelper(self.base_url) as helper:
                    test_jwt = helper.generate_test_jwt('something@foo.bar', identifier=mock_user_id, ttl=test_jwt_ttl)
                    response = requests.get(export_url,
                                            headers=dict(Authorization=f'Bearer {test_jwt}'),
                                            allow_redirects=False)
        self.assertEquals(200, response.status_code)
        response_body = response.json()
        self.assertEquals(200, response_body['Status'])
        prefix_expected_url = CollectionDataAccess.endpoint_url('collections', mock_collection_uuid)
        self.assertEquals(
            f'{prefix_expected_url}?version={mock_collection_ver}&replica=aws',
            response_body['Location']
        )
