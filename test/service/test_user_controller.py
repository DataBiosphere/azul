from collections.abc import (
    Mapping,
)
import json
from unittest.mock import (
    PropertyMock,
    patch,
)

import jwt
from moto import (
    mock_aws,
)
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.lib.types import (
    not_none,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.oauth2 import (
    OAuth2Client,
    TokenForCodeResponse,
)
from azul.service.user_service import (
    User,
    UserService,
)
from azul_test_case import (
    DCP2TestCase,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_aws
class TestUserController(DCP2TestCase,
                         LocalAppTestCase,
                         DynamoDBTestCase,
                         HasCachedHttpClient):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    def _dynamodb_table_name(self) -> str:
        return UserService._table_name

    def _dynamodb_attributes(self) -> Mapping[str, ScalarAttributeTypeType]:
        return {UserService.key_attribute: 'S'}

    def _dynamodb_hash_key(self) -> str:
        return UserService.key_attribute

    _mock_iss = 'https://accounts.google.com'
    _mock_sub = '105096702580025601450'
    _mock_email = 'user@example.com'
    _mock_access_token = 'ya29.mock_access_token'
    _mock_refresh_token = '1//mock_refresh_token'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addClassPatch(patch.object(UserService, '_client_id',
                                       new_callable=PropertyMock,
                                       return_value='mock_client_id'))
        cls.addClassPatch(patch.object(UserService, '_client_secret',
                                       new_callable=PropertyMock,
                                       return_value='mock_client_secret'))

    def _mock_token_response(self,
                             *,
                             access_token: str | None = None,
                             refresh_token: str | None = None,
                             refresh_token_expires_in: int | None = None
                             ) -> TokenForCodeResponse:
        id_token = {
            'iss': self._mock_iss,
            'sub': self._mock_sub,
            'email': self._mock_email,
            'email_verified': True,
        }
        id_token = jwt.encode(payload=id_token, key='a' * 32, algorithm='HS256')
        response: TokenForCodeResponse = {
            'access_token': access_token or self._mock_access_token,
            'refresh_token': refresh_token or self._mock_refresh_token,
            'expires_in': 3600,
            'scope': 'openid email',
            'token_type': 'Bearer',
            'id_token': id_token,
        }
        if refresh_token_expires_in is not None:
            response['refresh_token_expires_in'] = refresh_token_expires_in
        return response

    def _authorize(self, *, scope='openid email'):
        client = self._http_client
        url = str(self.base_url.set(path='/user/authorize'))
        body = json.dumps({
            'code': 'mock_auth_code',
            'scope': scope
        }).encode()
        return client.request('POST', url,
                              body=body,
                              headers={'Content-Type': 'application/json'})

    def _get_user(self) -> User:
        service = self._app.user_controller._service  # type: ignore[attr-defined]
        return not_none(service.get_user(self._mock_iss, self._mock_sub))

    @patch.object(OAuth2Client, 'token_for_code')
    def test_authorize(self, mock_token_for_code):
        mock_token_for_code.return_value = self._mock_token_response()
        response = self._authorize()
        self.assertEqual(200, response.status)
        mock_token_for_code.assert_called_once_with(
            authorization_code='mock_auth_code',
            client_id='mock_client_id',
            client_secret='mock_client_secret'
        )
        body = json.loads(response.data)
        self.assertEqual(self._mock_access_token, body['access_token'])
        self.assertNotIn('refresh_token', body)
        self.assertIn('id_token', body)
        user = self._get_user()
        self.assertEqual(self._mock_access_token, user['access_token'])
        self.assertEqual(self._mock_refresh_token, user['refresh_token'])
        self.assertEqual(self._mock_email, user['email'])
        self.assertTrue(user['email_verified'])

    @patch.object(OAuth2Client, 'token_for_code')
    def test_authorize_with_refresh_token_expiration(self, mock_token_for_code):
        mock_token_for_code.return_value = self._mock_token_response(
            refresh_token_expires_in=86400
        )
        response = self._authorize()
        self.assertEqual(200, response.status)
        user = self._get_user()
        now = UserService()._now()
        self.assertAlmostEqual(86400, user['expiration'] - now, delta=5)

    @patch.object(OAuth2Client, 'token_for_code')
    def test_authorize_default_expiration(self, mock_token_for_code):
        mock_token_for_code.return_value = self._mock_token_response()
        response = self._authorize()
        self.assertEqual(200, response.status)
        user = self._get_user()
        now = UserService()._now()
        self.assertAlmostEqual(UserService._default_expiration,
                               user['expiration'] - now,
                               delta=5)

    @patch.object(OAuth2Client, 'token_for_code')
    def test_authorize_updates_existing_user(self, mock_token_for_code):
        mock_token_for_code.return_value = self._mock_token_response()
        self._authorize()
        new_access_token = 'ya29.new_access_token'
        new_refresh_token = '1//new_refresh_token'
        mock_token_for_code.return_value = self._mock_token_response(
            access_token=new_access_token,
            refresh_token=new_refresh_token
        )
        response = self._authorize()
        self.assertEqual(200, response.status)
        user = self._get_user()
        self.assertEqual(new_access_token, user['access_token'])
        self.assertEqual(new_refresh_token, user['refresh_token'])

    def test_authorize_missing_required_scope(self):
        for scope in ('email', 'openid', 'profile'):
            with self.subTest(scope=scope):
                response = self._authorize(scope=scope)
                self.assertEqual(400, response.status)
