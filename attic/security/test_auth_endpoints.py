import logging

from jwt import (
    encode as jwt_encode,
)
import requests
import responses

from app_test_case import (
    AuthLocalAppTestCase,
)
from retorts import (
    AuthResponseHelper,
)

log = logging.getLogger(__name__)


class AuthEndpointTest(AuthLocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @responses.activate
    def test_authenticate_via_fusillade_ok(self):
        with AuthResponseHelper(self.base_url) as helper:
            test_jwt = helper.generate_test_jwt('something@foo.bar')
            response = requests.get(f'{self.base_url}/auth',
                                    headers=dict(Authorization=f'Bearer {test_jwt}'),
                                    allow_redirects=False)
        self.assertEqual(200, response.status_code)

    @responses.activate
    def test_authenticate_via_fusillade_redirects_to_fusillade(self):
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/auth', allow_redirects=False)
        self.assertEqual(302, response.status_code)
        self.assertRegex(response.headers['Location'], r'^https://auth(\.[a-z]+|)\.data\.humancellatlas.org/')

    @responses.activate
    def test_access_info_ok(self):
        test_email = 'testuser@hca.org'
        test_group = 'hca'
        with AuthResponseHelper(self.base_url) as helper:
            test_jwt = helper.generate_test_jwt(test_email, group=test_group)
            response = requests.get(f'{self.base_url}/me',
                                    headers=dict(Authorization=f'Bearer {test_jwt}'))
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertTrue(0 < response_data['ttl'] <= 60)
        response_claims = response_data['claims']
        self.assertEqual(test_email, response_claims['https://auth.data.humancellatlas.org/email'])
        self.assertEqual(test_group, response_claims['https://auth.data.humancellatlas.org/group'])

    @responses.activate
    def test_access_info_blocks_access_without_jwt(self):
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/me')
        self.assertEqual(401, response.status_code)

    @responses.activate
    def test_access_info_blocks_access_with_invalid_jwt(self):
        invalid_jwt = jwt_encode({'foo': 'bar'}, None, None).decode()
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/me',
                                    headers=dict(Authorization=f'Bearer {invalid_jwt}'))
        self.assertEqual(403, response.status_code)

    @responses.activate
    def test_access_info_blocks_access_due_to_server_error(self):
        claims = {
            "iss": f'http://{self.server_thread.address[0]}:12345/'  # This issuer is inaccessible.
        }
        problematic_jwt = jwt_encode(claims, None, None).decode()
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/me',
                                    headers=dict(Authorization=f'Bearer {problematic_jwt}'))
        self.assertEqual(403, response.status_code)

    @responses.activate
    def test_handle_callback_from_fusillade_ok(self):
        payload = dict(access_token='abc',
                       id_token='def',
                       expires_in=1234,
                       decoded_token='{"foo": "bar"}',
                       state='')
        expected_response = dict(access_token='abc',
                                 id_token='def',
                                 expires_in='1234',
                                 decoded_token=dict(foo='bar'),
                                 state='')
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/auth/callback', payload)
        self.assertEqual(200, response.status_code)
        self.assertEqual(expected_response, {k: v for k, v in response.json().items() if k != 'login_url'})

    @responses.activate
    def test_handle_callback_from_fusillade_without_some_payload(self):
        payload = dict(id_token='def')
        with AuthResponseHelper(self.base_url):
            response = requests.get(f'{self.base_url}/auth/callback', payload)
        self.assertEqual(400, response.status_code)
