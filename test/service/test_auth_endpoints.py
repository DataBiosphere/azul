import logging
from jwt import encode as jwt_encode
import requests
from app_test_case import LocalAppTestCase

log = logging.getLogger(__name__)


class AuthEndpointTest(LocalAppTestCase):
    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _get_test_jwt(self, email:str = None, group:str = None):
        return requests.post(
            f'{self.base_url}/test/token',
            json=dict(email=email or 'robot@hca.org',
                      group=group or 'public')
        ).json()['jwt']

    def test_authenticate_via_fusillade_ok(self):
        test_jwt = self._get_test_jwt()
        response = requests.get(f'{self.base_url}/auth',
                                headers=dict(Authorization=f'Bearer {test_jwt}'),
                                allow_redirects=False)
        self.assertEqual(200, response.status_code)

    def test_authenticate_via_fusillade_redirects_to_fusillade(self):
        response = requests.get(f'{self.base_url}/auth', allow_redirects=False)
        self.assertEqual(302, response.status_code)
        self.assertRegex(response.headers['Location'], '^https://auth(\.[a-z]+|)\.data\.humancellatlas.org/')

    def test_access_info_ok(self):
        test_email = 'testuser@hca.org'
        test_group = 'hca'
        test_jwt = self._get_test_jwt(test_email, test_group)
        response = requests.get(f'{self.base_url}/me',
                                headers=dict(Authorization=f'Bearer {test_jwt}'))
        self.assertEqual(200, response.status_code)
        response_data = response.json()
        self.assertTrue(0 < response_data['ttl'] <= 60)
        response_claims = response_data['claims']
        self.assertEqual(test_email, response_claims['https://auth.data.humancellatlas.org/email'])
        self.assertEqual(test_group, response_claims['https://auth.data.humancellatlas.org/group'])

    def test_access_info_blocks_access_without_jwt(self):
        response = requests.get(f'{self.base_url}/me')
        self.assertEqual(401, response.status_code)

    def test_access_info_blocks_access_with_invalid_jwt(self):
        invalid_jwt = jwt_encode({'foo': 'bar'}, None, None).decode()
        response = requests.get(f'{self.base_url}/me',
                                headers=dict(Authorization=f'Bearer {invalid_jwt}'))
        self.assertEqual(403, response.status_code)

    def test_access_info_blocks_access_due_to_server_error(self):
        claims = {
            "iss": f'http://{self.server_thread.address[0]}:12345/'  # This issuer is inaccessible.
        }
        problematic_jwt = jwt_encode(claims, None, None).decode()
        response = requests.get(f'{self.base_url}/me',
                                headers=dict(Authorization=f'Bearer {problematic_jwt}'))
        self.assertEqual(403, response.status_code)

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
        response = requests.get(f'{self.base_url}/auth/callback',payload)
        self.assertEqual(200, response.status_code)
        self.assertEqual(expected_response, {k:v for k, v in response.json().items() if k != 'login_url'})

    def test_handle_callback_from_fusillade_without_some_payload(self):
        payload = dict(id_token='def')
        response = requests.get(f'{self.base_url}/auth/callback',payload)
        self.assertEqual(400, response.status_code)
