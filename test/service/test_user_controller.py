from abc import (
    ABCMeta,
)
from collections.abc import (
    Mapping,
)
from contextlib import (
    contextmanager,
)
from datetime import (
    datetime,
    timezone,
)
import json
from typing import (
    Any,
    ContextManager,
)
from unittest.mock import (
    Mock,
    PropertyMock,
    patch,
)
import urllib.parse

import jwt
from mypy_boto3_dynamodb.literals import (
    ScalarAttributeTypeType,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    Config,
    config,
)
from azul.auth import (
    AccessTokenAuthentication,
    UnverifiedAccessTokenAuthentication,
    UnverifiedPersonalAccessTokenAuthentication,
)
from azul.deployment import (
    aws,
)
from azul.http import (
    HasCachedHttpClient,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.time import (
    dcp2_datetime_pattern,
    format_dcp2_datetime,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.oauth2 import (
    InvalidAccessTokenError,
    InvalidRefreshTokenError,
    OAuth2Client,
    TokenForCodeResponse,
    TokenInfoResponse,
    TokenResponse,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.service.user_service import (
    InvalidPersonalAccessTokenError,
    RejectedAccessTokenError,
    UnknownUserException,
    User,
    UserService,
)
from azul_test_case import (
    AnvilTestCase,
    DCP2TestCase,
)
from dynamodb_test_case import (
    DynamoDBTestCase,
)
from service.contract_test_case import (
    ContractTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class UserControllerTestCase(LocalAppTestCase,
                             DynamoDBTestCase,
                             HasCachedHttpClient,
                             metaclass=ABCMeta):

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
    _mock_client_id = '123-mock_client_id.apps.googleusercontent.com'

    @property
    def _mock_identity(self) -> str:
        return self._service._encode_identity(self._mock_iss, self._mock_sub)

    _mock_email = 'user@example.com'
    _mock_access_token = 'ya29.mock_access_token'

    # An authorization code consists of '4/', a digit and a variable, but much
    # larger number of base64url characters. A mock code has to reproduce that
    # shape, or it won't be redactable.
    _mock_auth_code = '4/0mock_auth_code'.ljust(73, '0')

    @classmethod
    def _mock_refresh_token(cls, variable_part: str = 'mock_refresh_token') -> str:
        """
        A refresh token consists of '1//', two digits and a variable, but much
        larger number of base64url characters. A mock token has to reproduce
        that shape, or it won't be redactable.
        """
        return f'1//01{variable_part}'.ljust(102, '0')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.addClassPatch(patch.object(Config, 'google_oauth2_client_id',
                                       new_callable=PropertyMock,
                                       return_value=cls._mock_client_id))
        cls.addClassPatch(patch.object(UserService, '_client_secret',
                                       new_callable=PropertyMock,
                                       return_value='mock_client_secret'))

    def setUp(self):
        super().setUp()
        self.dynamodb.create_table(
            TableName=UserService._access_tokens_table_name,
            BillingMode='PAY_PER_REQUEST',
            AttributeDefinitions=[
                dict(AttributeName=UserService.access_token_key_attribute,
                     AttributeType='S')
            ],
            KeySchema=[
                dict(AttributeName=UserService.access_token_key_attribute,
                     KeyType='HASH')
            ]
        )
        key = aws.kms.create_key(KeyUsage='SIGN_VERIFY',
                                 CustomerMasterKeySpec='ECC_NIST_P256')
        aws.kms.create_alias(AliasName=config.apat_kms_key.alias,
                             TargetKeyId=key['KeyMetadata']['KeyId'])
        # Each test creates a new KMS key, but the UserService instance
        # persists across tests (as part of the class-level app fixture),
        # so we must invalidate the cached public key material.
        UserService._apat_public_key.fdel(self._service)

    def tearDown(self):
        self.dynamodb.delete_table(TableName=UserService._access_tokens_table_name)
        super().tearDown()

    @contextmanager
    def _mock_token_info(self, aud: str | None = None):
        if aud is None:
            aud = self._mock_client_id
        with patch.object(OAuth2Client, 'token_info') as mock:
            mock.return_value = TokenInfoResponse(
                azp=aud,
                aud=aud,
                sub=self._mock_sub,
                scope='https://www.googleapis.com/auth/userinfo.email openid',
                exp='9999999999',
                expires_in='3600',
                email=self._mock_email,
                email_verified='true',
                access_type='online'
            )
            yield mock

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
            'refresh_token': refresh_token or self._mock_refresh_token(),
            'expires_in': 3600,
            'scope': 'openid email',
            'token_type': 'Bearer',
            'id_token': id_token,
        }
        if refresh_token_expires_in is not None:
            response['refresh_token_expires_in'] = refresh_token_expires_in
        return response

    @contextmanager
    def _mock_token_for_code(self, **kwargs):
        with patch.object(OAuth2Client, 'token_for_code') as mock:
            mock.return_value = self._mock_token_response(**kwargs)
            yield mock

    def _authorize(self, *, scope='openid email'):
        client = self._http_client
        url = str(self.base_url.set(path='/user/authorize'))
        body = json.dumps({
            'code': self._mock_auth_code,
            'scope': scope
        }).encode()
        return client.request('POST', url,
                              body=body,
                              headers={'Content-Type': 'application/json'})

    def _authorize_form(self, **params: str):
        client = self._http_client
        url = str(self.base_url.set(path='/user/authorize'))
        body = urllib.parse.urlencode(params).encode()
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        return client.request('POST', url, body=body, headers=headers)

    def _get_token(self, access_token: str | None = None):
        client = self._http_client
        url = str(self.base_url.set(path='/user/token'))
        headers = {}
        if access_token is not None:
            headers['Authorization'] = f'Bearer {access_token}'
        return client.request('GET', url, headers=headers)

    def _revoke(self, access_token: str | None = None):
        client = self._http_client
        url = str(self.base_url.set(path='/user/revoke'))
        headers = {}
        if access_token is not None:
            headers['Authorization'] = f'Bearer {access_token}'
        return client.request('POST', url, headers=headers)

    @cached_property
    def _service(self) -> UserService:
        return self._app.user_controller._service  # type: ignore[attr-defined]

    def _load_user(self) -> User:
        return self._service._load_user(self._mock_identity)

    def _authorized_until(self) -> str:
        """
        When the stored user's authorization lapses, as it should appear in a
        response
        """
        expiration = datetime.fromtimestamp(self._load_user()['expiration'], timezone.utc)
        return format_dcp2_datetime(expiration)

    def _authorize_and_mint(self) -> UnverifiedPersonalAccessTokenAuthentication:
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
                return self._service.mint_personal_access_token(auth)

    def _get_user(self, access_token: str | None = None):
        return self._request('GET', '/user', access_token)

    def _patch_user(self, body: AnyJSON, access_token: str | None = None):
        return self._request('PATCH', '/user', access_token, body)

    def _request(self,
                 method: str,
                 path: str,
                 access_token: str | None = None,
                 body: AnyJSON | None = None):
        url = str(self.base_url.set(path=path))
        headers = {}
        if access_token is not None:
            headers['Authorization'] = f'Bearer {access_token}'
        if body is not None:
            headers['Content-Type'] = 'application/json'
        return self._http_client.request(method,
                                         url,
                                         headers=headers,
                                         body=None if body is None else json.dumps(body),
                                         redirect=False)


class TestUserController(DCP2TestCase, UserControllerTestCase):

    def test_authorize(self):
        with self._mock_token_for_code() as token_for_code:
            response = self._authorize()
            self.assertEqual(200, response.status)
            token_for_code.assert_called_once_with(
                authorization_code=self._mock_auth_code,
                client_id=self._mock_client_id,
                client_secret='mock_client_secret',
                redirect_uri=None
            )
            body = json.loads(response.data)
            self.assertEqual(self._mock_access_token, body['access_token'])
            self.assertNotIn('refresh_token', body)
            self.assertIn('id_token', body)
            user = self._load_user()
            self.assertEqual(self._mock_access_token, user['access_token'])
            self.assertEqual(self._mock_refresh_token(), user['refresh_token'])
            self.assertEqual(self._mock_email, user['email'])
            self.assertTrue(user['email_verified'])

    def test_authorize_with_refresh_token_expiration(self):
        with self._mock_token_for_code(refresh_token_expires_in=86400):
            response = self._authorize()
            self.assertEqual(200, response.status)
            user = self._load_user()
            now = self._service._now()
            self.assertAlmostEqual(86400, user['expiration'] - now, delta=5)

    def test_authorize_default_expiration(self):
        with self._mock_token_for_code():
            response = self._authorize()
            self.assertEqual(200, response.status)
            user = self._load_user()
            now = self._service._now()
            self.assertAlmostEqual(UserService._default_expiration,
                                   user['expiration'] - now,
                                   delta=5)

    def test_authorize_updates_existing_user(self):
        with self._mock_token_for_code() as token_for_code:
            self._authorize()
            new_access_token = 'ya29.new_access_token'
            new_refresh_token = self._mock_refresh_token('new_refresh_token')
            token_for_code.return_value = self._mock_token_response(
                access_token=new_access_token,
                refresh_token=new_refresh_token
            )
            response = self._authorize()
            self.assertEqual(200, response.status)
            user = self._load_user()
            self.assertEqual(new_access_token, user['access_token'])
            self.assertEqual(new_refresh_token, user['refresh_token'])

    def test_authorize_missing_required_scope(self):
        for scope in ('email', 'openid', 'profile'):
            with self.subTest(scope=scope):
                response = self._authorize(scope=scope)
                self.assertEqual(400, response.status)

    def test_authorize_form(self):
        redirect_uri = 'https://example.org/oauth2-redirect.html'
        params = dict(code=self._mock_auth_code, redirect_uri=redirect_uri)
        with self._mock_token_for_code() as token_for_code:
            response = self._authorize_form(**params)
            self.assertEqual(200, response.status)
            token_for_code.assert_called_once_with(
                authorization_code=self._mock_auth_code,
                client_id=self._mock_client_id,
                client_secret='mock_client_secret',
                redirect_uri=redirect_uri
            )
            body = json.loads(response.data)
            self.assertEqual(self._mock_access_token, body['access_token'])
            self.assertNotIn('refresh_token', body)

    def test_authorize_form_missing_parameter(self):
        params = dict(code=self._mock_auth_code,
                      redirect_uri='https://example.org/oauth2-redirect.html')
        with self._mock_token_for_code() as token_for_code:
            for name in params:
                with self.subTest(name=name):
                    other_params = {k: v for k, v in params.items() if k != name}
                    response = self._authorize_form(**other_params)
                    self.assertEqual(400, response.status)
                    message = json.loads(response.data)['Message']
                    self.assertEqual(f'Missing form parameter `{name}`', message)
            with self.subTest('unrelated parameter'):
                response = self._authorize_form(foo='bar')
                self.assertEqual(400, response.status)
            token_for_code.assert_not_called()

    def test_authorize_malformed_body(self):
        url = str(self.base_url.set(path='/user/authorize'))
        json_type, form_type = 'application/json', 'application/x-www-form-urlencoded'
        params = urllib.parse.urlencode(dict(code=self._mock_auth_code,
                                             redirect_uri='https://example.org/oauth2-redirect.html'))
        with self._mock_token_for_code() as token_for_code:
            for description, content_type, body in [
                ('JSON array', json_type, b'[]'),
                ('JSON null', json_type, b'null'),
                ('JSON string', json_type, b'"foo"'),
                ('unknown charset', form_type + '; charset=nonsense', params.encode()),
                ('invalid UTF-8', form_type, b'\xff' + params.encode())
            ]:
                with self.subTest(description):
                    headers = {'Content-Type': content_type}
                    response = self._http_client.request('POST', url, body=body, headers=headers)
                    self.assertEqual(400, response.status)
            token_for_code.assert_not_called()

    def test_mint_personal_access_token(self):
        with self._mock_token_for_code():
            with self._mock_token_info() as token_info:
                self._authorize()
                auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
                apat_auth = self._service.mint_personal_access_token(auth)
                # Authorizing cached the access token, so it was verified
                # without asking the authorization server.
                token_info.assert_not_called()
                header = jwt.get_unverified_header(apat_auth.token)
                self.assertEqual('ES256', header['alg'])
                claims = jwt.decode(apat_auth.token,
                                    options={'verify_signature': False})
                self.assertNotIn('iss', claims)
                self.assertEqual('#' + self._mock_sub, claims['sub'])
                self.assertEqual('0', claims['jti'])
                now = self._service._now()
                self.assertAlmostEqual(UserService._apat_expiration,
                                       claims['exp'] - now,
                                       delta=5)

    def test_mint_personal_access_token_unknown_user(self):
        with self._mock_token_info():
            auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
            with self.assertRaises(UnknownUserException):
                self._service.mint_personal_access_token(auth)

    def test_mint_personal_access_token_foreign_token(self):
        with self._mock_token_info() as token_info:
            token_info.return_value['aud'] = '456-other_client_id.apps.googleusercontent.com'
            auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
            with self.assertRaises(RejectedAccessTokenError):
                self._service.mint_personal_access_token(auth)

    def test_verify_access_token(self):
        client_id = self._mock_client_id
        project = 'mock-project'
        user = {
            'azp': client_id,
            'aud': client_id,
            'email': self._mock_email,
            'email_verified': 'true'
        }
        service_account_id = '123456789012345678901'
        service_account = {
            'azp': service_account_id,
            'aud': service_account_id,
            'sub': service_account_id,
            'email': f'indexer@{project}.iam.gserviceaccount.com',
            'email_verified': 'true'
        }
        # Maps each case to the fields to set, or to remove if None, and the
        # expected subject, or None if the token should be rejected
        cases: list[tuple[str, Mapping[str, str | None], str | None]] = [
            ('user', user, self._mock_sub),
            ('user without subject', {
                **user,
                'sub': None
            }, None),
            ('foreign client', {
                **user,
                'azp': '456-other.apps.googleusercontent.com'
            }, None),
            ('foreign audience', {
                **user,
                'aud': '456-other.apps.googleusercontent.com'
            }, None),
            ('service account', service_account, service_account_id),
            ('service account without subject', {
                **service_account,
                'sub': None
            }, service_account_id),
            ('service account token for another audience', {
                **service_account,
                'aud': '210987654321098765432'
            }, None),
            ('unverified service account', {
                **service_account,
                'email_verified': 'false'
            }, None),
            ('service account without verification status', {
                **service_account,
                'email_verified': None
            }, None),
            ('foreign project', {
                **service_account,
                'email': 'indexer@other-project.iam.gserviceaccount.com'
            }, None),
            ('service account without email', {
                **service_account,
                'email': None,
                'email_verified': None
            }, None),
            ('unexpected party', {
                **user,
                'azp': service_account_id
            }, None)
        ]
        # Each case uses a different token because valid tokens are cached
        for i, (case, token_info_fields, subject) in enumerate(cases):
            auth = UnverifiedAccessTokenAuthentication(f'{self._mock_access_token}_{i}')
            with self.subTest(case):
                with (
                    patch.object(config, 'google_project', return_value=project),
                    self._mock_token_info() as token_info
                ):
                    for field, value in token_info_fields.items():
                        if value is None:
                            del token_info.return_value[field]
                        else:
                            token_info.return_value[field] = value
                    if subject is None:
                        with self.assertRaises(RejectedAccessTokenError):
                            self._service.verify_access_token(auth)
                    else:
                        verified_auth = self._service.verify_access_token(auth)
                        self.assertIsInstance(verified_auth, AccessTokenAuthentication)
                        self.assertEqual(auth.token, verified_auth.token)
                        identity = self._service._encode_identity(self._mock_iss, subject)
                        self.assertEqual(identity, verified_auth.identity())
        with self.subTest('rejected by authorization server'):
            auth = UnverifiedAccessTokenAuthentication(f'{self._mock_access_token}_rejected')
            with self._mock_token_info() as token_info:
                token_info.side_effect = InvalidAccessTokenError
                with self.assertRaises(InvalidAccessTokenError):
                    self._service.verify_access_token(auth)

    def test_verify_cached_access_token(self):
        auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
        with self._mock_token_info() as token_info:
            first, second = (self._service.verify_access_token(auth) for _ in range(2))
            token_info.assert_called_once_with(auth.token)
        self.assertEqual(first, second)
        self.assertEqual(self._mock_identity, second.identity())

    def test_verify_expired_cached_access_token(self):
        auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
        now = self._service._now()
        with self._mock_token_info() as token_info:
            self._service.verify_access_token(auth)
            # The mock reports that the token expires in an hour
            with patch.object(UserService, '_now', return_value=now + 3600):
                self._service.verify_access_token(auth)
            self.assertEqual(2, token_info.call_count)

    def test_authorize_caches_access_token(self):
        with self._mock_token_for_code():
            self._authorize()
        auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
        with self._mock_token_info() as token_info:
            verified_auth = self._service.verify_access_token(auth)
            token_info.assert_not_called()
        self.assertEqual(self._mock_identity, verified_auth.identity())

    def test_exchange_token(self):
        apat = self._authorize_and_mint()
        result = self._service.exchange_token(apat)
        self.assertIsInstance(result, UnverifiedAccessTokenAuthentication)
        self.assertEqual(self._mock_access_token, result.token)

    @patch.object(OAuth2Client, 'token_for_refresh')
    def test_exchange_token_refreshes_expired(self, mock_token_for_refresh):
        apat = self._authorize_and_mint()
        refreshed_token = 'ya29.refreshed_access_token'
        mock_token_for_refresh.return_value = TokenResponse(
            access_token=refreshed_token,
            expires_in=3600,
            scope='openid email',
            token_type='Bearer'
        )
        with patch.object(UserService, '_now', return_value=2 ** 31):
            result = self._service.exchange_token(apat)
        self.assertEqual(refreshed_token, result.token)
        mock_token_for_refresh.assert_called_once_with(
            refresh_token=self._mock_refresh_token(),
            client_id=self._mock_client_id,
            client_secret='mock_client_secret'
        )
        user = self._load_user()
        self.assertEqual(refreshed_token, user['access_token'])

    def test_exchange_token_rejected_refresh_token(self):
        apat = self._authorize_and_mint()
        with (
            patch.object(OAuth2Client, 'token_for_refresh', side_effect=InvalidRefreshTokenError),
            patch.object(UserService, '_now', return_value=2 ** 31)
        ):
            with self.assertRaises(InvalidPersonalAccessTokenError):
                self._service.exchange_token(apat)

    def test_exchange_token_expired_user(self):
        apat = self._authorize_and_mint()
        key = self._mock_identity
        aws.dynamodb.delete_item(TableName=UserService._table_name,
                                 Key={UserService.key_attribute: {'S': key}})
        with self.assertRaises(InvalidPersonalAccessTokenError):
            self._service.exchange_token(apat)

    def test_token_for_refresh(self):
        for status, error, expected in [
            (400, 'invalid_grant', InvalidRefreshTokenError),
            (400, 'invalid_client', AssertionError),
            (500, 'invalid_grant', AssertionError)
        ]:
            with self.subTest(status=status, error=error):
                response = Mock(status=status, data=json.dumps({'error': error}).encode())
                http_client = Mock(request=Mock(return_value=response))
                with patch.object(OAuth2Client, '_http_client',
                                  new_callable=PropertyMock,
                                  return_value=http_client):
                    with self.assertRaises(expected):
                        OAuth2Client().token_for_refresh(refresh_token=self._mock_refresh_token(),
                                                         client_id=self._mock_client_id,
                                                         client_secret='GOCSPX-mock_client_secret')
                http_client.request.assert_called_once()

    def test_exchange_token_invalid_jwt(self):
        auth = UnverifiedPersonalAccessTokenAuthentication(token='eyJ.not.a.jwt')
        with self.assertRaises(InvalidPersonalAccessTokenError):
            self._service.exchange_token(auth)

    def test_exchange_token_forged_jwt(self):
        from cryptography.hazmat.primitives.asymmetric import (
            ec,
        )
        private_key = ec.generate_private_key(ec.SECP256R1())
        token = jwt.encode(
            {'sub': '#y'},
            key=private_key,
            algorithm='ES256'
        )
        auth = UnverifiedPersonalAccessTokenAuthentication(token=token)
        with self.assertRaises(InvalidPersonalAccessTokenError):
            self._service.exchange_token(auth)

    def test_exchange_token_survives_reauthorization(self):
        apat = self._authorize_and_mint()
        with self._mock_token_for_code(refresh_token='1//new_refresh_token'):
            self._authorize()
        result = self._service.exchange_token(apat)
        self.assertIsInstance(result, UnverifiedAccessTokenAuthentication)

    def test_exchange_token_revoked(self):
        apat = self._authorize_and_mint()
        with self._mock_token_info():
            auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
            self._service.revoke_personal_access_tokens(auth)
        with self.assertRaises(InvalidPersonalAccessTokenError):
            self._service.exchange_token(apat)

    def test_multiple_apats(self):
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
                apat1 = self._service.mint_personal_access_token(auth)
                apat2 = self._service.mint_personal_access_token(auth)
        result1 = self._service.exchange_token(apat1)
        result2 = self._service.exchange_token(apat2)
        self.assertIsInstance(result1, UnverifiedAccessTokenAuthentication)
        self.assertIsInstance(result2, UnverifiedAccessTokenAuthentication)

    def test_authorize_stores_access_token_expiration(self):
        with self._mock_token_for_code():
            self._authorize()
            user = self._load_user()
            now = self._service._now()
            self.assertAlmostEqual(3600,
                                   user['access_token_expiration'] - now,
                                   delta=5)

    def test_token(self):
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                response = self._get_token(self._mock_access_token)
                self.assertEqual(200, response.status)
                body = json.loads(response.data)
                self.assertIn('token', body)
                header = jwt.get_unverified_header(body['token'])
                self.assertEqual('ES256', header['alg'])

    def test_token_rejects_pat(self):
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                auth = UnverifiedAccessTokenAuthentication(self._mock_access_token)
                apat = self._service.mint_personal_access_token(auth)
                response = self._get_token(apat.token)
                self.assertEqual(403, response.status)

    def test_token_unauthenticated(self):
        response = self._get_token()
        self.assertEqual(401, response.status)

    def test_revoke(self):
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                response = self._revoke(self._mock_access_token)
                self.assertEqual(204, response.status)

    def test_revoke_invalidates_apat(self):
        apat = self._authorize_and_mint()
        with self._mock_token_info():
            response = self._revoke(self._mock_access_token)
            self.assertEqual(204, response.status)
        with self.assertRaises(InvalidPersonalAccessTokenError):
            self._service.exchange_token(apat)

    def test_revoke_rejects_pat(self):
        apat = self._authorize_and_mint()
        response = self._revoke(apat.token)
        self.assertEqual(403, response.status)

    def test_revoke_unauthenticated(self):
        response = self._revoke()
        self.assertEqual(401, response.status)

    def test_unusable_authorization_header(self):
        url = str(self.base_url.set(path='/user/token'))
        # A refresh token is redactable, so it passes the syntax check in the
        # app and is rejected by `BearerTokenAuthentication.for_token` instead
        refresh_token = self._mock_refresh_token()
        for description, header, message in [
            ('no scheme', self._mock_access_token, 'Malformed Authorization header'),
            ('other scheme', 'Basic dXNlcjpwYXNz', 'Unsupported authorization scheme'),
            ('unredactable token', 'Bearer not_a_token', 'Unexpected token syntax'),
            ('token of another kind', f'Bearer {refresh_token}', 'Unexpected token syntax')
        ]:
            with self.subTest(description):
                headers = {'Authorization': header}
                response = self._http_client.request('GET', url, headers=headers)
                self.assertEqual(401, response.status)
                body = response.data.decode()
                self.assertEqual(message, json.loads(body)['Message'])
                self.assertNotIn(header, body)

    def test_unexpected_token_info_response(self):
        # A response from the authorization server that we aren't prepared to
        # interpret is not necessarily the client's fault, and we don't know
        # what to make of it. The resulting 5xx trips an alarm, which is how
        # we'd learn that it needs handling.
        token = self._mock_access_token
        error = AssertionError(R('Unexpected response status', 503))
        with patch.object(OAuth2Client, 'token_info', side_effect=error):
            for response in self._token_responses(token):
                self.assertEqual(500, response.status)

    def test_unusable_access_token(self):
        token = self._mock_access_token
        # The user of a foreign token is never loaded, and the user of the
        # token in the third case was never authorized, so neither case
        # reaches the point of needing a user
        contexts: list[tuple[str, ContextManager]] = [
            (
                'rejected by the authorization server',
                patch.object(OAuth2Client, 'token_info', side_effect=InvalidAccessTokenError)
            ),
            (
                'issued to another client',
                self._mock_token_info(aud='456-other_client_id.apps.googleusercontent.com')
            ),
            (
                'issued to an unknown user',
                self._mock_token_info()
            )
        ]
        for description, context in contexts:
            with self.subTest(description):
                with context:
                    for response in self._token_responses(token):
                        self.assertEqual(401, response.status)
                        expected_body = {
                            'Code': 'UnauthorizedError',
                            'Message': 'Valid access token required'
                        }
                        self.assertEqual(expected_body, json.loads(response.data))

    def _token_responses(self, token: str):
        """
        The responses from every endpoint that requires an access token
        """
        return [
            self._get_token(token),
            self._revoke(token),
            self._get_user(token),
            self._patch_user({}, token)
        ]

    def test_get_user(self):
        with self._mock_token_for_code():
            self._authorize()
            with self._mock_token_info():
                response = self._get_user(self._mock_access_token)
                self.assertEqual(200, response.status)
                expected = {
                    'email': self._mock_email,
                    'authorized_until': self._authorized_until(),
                    'accepted_contracts': {}
                }
                self.assertEqual(expected, json.loads(response.data))

    def test_user_rejects_pat(self):
        apat = self._authorize_and_mint()
        for response in self._get_user(apat.token), self._patch_user({}, apat.token):
            self.assertEqual(403, response.status)

    def test_user_unauthenticated(self):
        for response in self._get_user(), self._patch_user({}):
            self.assertEqual(401, response.status)

    def test_unsupported_endpoint_rejects_pat(self):
        apat = self._authorize_and_mint()
        url = str(self.base_url.set(path='/index/summary'))
        headers = {'Authorization': f'Bearer {apat.token}'}
        response = self._http_client.request('GET', url, headers=headers)
        self.assertEqual(401, response.status)
        self.assertEqual({
            'Code': 'UnauthorizedError',
            'Message': 'This endpoint does not support a '
                       'personal access token (APAT)'
        }, json.loads(response.data))


class TestContracts(AnvilTestCase, ContractTestCase, UserControllerTestCase):

    def test_plugin_contract(self):
        plugin: MetadataPlugin = MetadataPlugin.load(self.catalog).create()
        self.assertEqual(self._contract, plugin.registered_access_contract)

    def _accepted_contracts(self, response) -> dict[str, Any]:
        self.assertEqual(200, response.status)
        body = json.loads(response.data)
        self.assertEqual(self._mock_email, body['email'])
        self.assertEqual(self._authorized_until(), body['authorized_until'])
        return body['accepted_contracts']

    def _accept(self, version: int | None = 1) -> dict[str, Any]:
        acceptance: JSON | None
        if version is None:
            acceptance = None
        else:
            acceptance = {'version': version, 'sha256': self._contract_sha256(version)}
        body = {'accepted_contracts': {self._contract: acceptance}}
        response = self._patch_user(body, self._mock_access_token)
        return self._accepted_contracts(response)

    def test_accept_contract(self):
        with self._mock_token_for_code():
            self._authorize()
        with self._mock_token_info():
            accepted_contracts = self._accept()
            self.assertEqual([self._contract], list(accepted_contracts))
            acceptance = accepted_contracts[self._contract]
            self.assertEqual(1, acceptance['version'])
            self.assertEqual(self._contract_sha256(1), acceptance['sha256'])
            self.assertRegex(acceptance['time'], f'^{dcp2_datetime_pattern}$')
            response = self._get_user(self._mock_access_token)
            self.assertEqual(accepted_contracts, self._accepted_contracts(response))
            # Accepting a contract again updates the time of acceptance
            accepted_contracts = self._accept()
            self.assertLess(acceptance['time'], accepted_contracts[self._contract]['time'])

    def test_retract_contract(self):
        with self._mock_token_for_code():
            self._authorize()
        with self._mock_token_info():
            # Retracting a contract that was never accepted changes nothing
            self.assertEqual({}, self._accept(None))
            self._accept()
            self.assertEqual({}, self._accept(None))
            response = self._get_user(self._mock_access_token)
            self.assertEqual({}, self._accepted_contracts(response))

    def test_retract_all_contracts(self):
        with self._mock_token_for_code():
            self._authorize()
        with self._mock_token_info():
            body = {'accepted_contracts': None}
            response = self._patch_user(body, self._mock_access_token)
            self.assertEqual({}, self._accepted_contracts(response))
            self._accept()
            response = self._patch_user(body, self._mock_access_token)
            self.assertEqual({}, self._accepted_contracts(response))

    def test_empty_update(self):
        with self._mock_token_for_code():
            self._authorize()
        with self._mock_token_info():
            accepted_contracts = self._accept()
            bodies: list[JSON] = [{}, {'accepted_contracts': {}}]
            for body in bodies:
                with self.subTest(body=body):
                    response = self._patch_user(body, self._mock_access_token)
                    self.assertEqual(accepted_contracts, self._accepted_contracts(response))

    def test_acceptance_survives_reauthorization(self):
        now = self._service._now()
        with self._mock_token_for_code():
            with patch.object(UserService, '_now', return_value=now):
                self._authorize()
            with self._mock_token_info():
                accepted_contracts = self._accept()
            expiration = self._load_user()['expiration']
            # Reauthorizing a day later extends the acceptance by a day
            with patch.object(UserService, '_now', return_value=now + 24 * 60 * 60):
                self._authorize()
            self.assertEqual(expiration + 24 * 60 * 60, self._load_user()['expiration'])
            with self._mock_token_info():
                response = self._get_user(self._mock_access_token)
                self.assertEqual(accepted_contracts, self._accepted_contracts(response))

    def test_invalid_update(self):
        with self._mock_token_for_code():
            self._authorize()
        sha256 = self._contract_sha256(1)
        acceptance: JSON = {'version': 1, 'sha256': sha256}
        time = '2026-11-04T15:55:12.000034Z'
        cases: list[tuple[str, AnyJSON]] = [
            ('immutable attribute', {'email': 'foo@example.com'}),
            ('unknown attribute', {'foo': 'bar'}),
            ('time of acceptance', {'accepted_contracts': {self._contract: {**acceptance, 'time': time}}}),
            ('missing version', {'accepted_contracts': {self._contract: {'sha256': sha256}}}),
            ('string version', {'accepted_contracts': {self._contract: {**acceptance, 'version': '1'}}}),
            ('boolean version', {'accepted_contracts': {self._contract: {**acceptance, 'version': True}}}),
            ('unknown version', {'accepted_contracts': {self._contract: {**acceptance, 'version': 2}}}),
            ('zero version', {'accepted_contracts': {self._contract: {**acceptance, 'version': 0}}}),
            ('missing SHA-256', {'accepted_contracts': {self._contract: {'version': 1}}}),
            ('null SHA-256', {'accepted_contracts': {self._contract: {**acceptance, 'sha256': None}}}),
            ('wrong SHA-256', {'accepted_contracts': {self._contract: {**acceptance, 'sha256': '0' * 64}}}),
            ('uppercase SHA-256', {'accepted_contracts': {self._contract: {**acceptance, 'sha256': sha256.upper()}}}),
            ('unknown contract', {'accepted_contracts': {'no_such_contract': acceptance}}),
            ('retracting unknown contract', {'accepted_contracts': {'no_such_contract': None}}),
            ('list of contracts', {'accepted_contracts': [self._contract]}),
            ('not an object', [])
        ]
        for description, body in cases:
            with self.subTest(description):
                with self._mock_token_info():
                    response = self._patch_user(body, self._mock_access_token)
                    self.assertEqual(400, response.status)
                    self.assertEqual({}, self._load_user()['accepted_contracts'])

    def test_update_expired_user(self):
        with self._mock_token_for_code():
            self._authorize()
        load_user = self._service._load_user

        def load_and_expire_user(identity: str) -> User:
            user = load_user(identity)
            aws.dynamodb.delete_item(TableName=UserService._table_name,
                                     Key={UserService.key_attribute: {'S': identity}})
            return user

        with (
            self._mock_token_info(),
            patch.object(self._service, '_load_user', side_effect=load_and_expire_user)
        ):
            response = self._patch_user({'accepted_contracts': None},
                                        self._mock_access_token)
            self.assertEqual(401, response.status)
        # The update must not have resurrected the item
        with self.assertRaises(UnknownUserException):
            self._load_user()

    def test_accept_contract_from_files(self):
        with self._mock_token_for_code():
            self._authorize()
        with self._mock_contract_files(self._contract_files), self._mock_token_info():
            # A version newer than the current one can be accepted
            accepted_contracts = self._accept(4)
            self.assertEqual(4, accepted_contracts[self._contract]['version'])
            sha256 = self._contract_sha256(4)
            # The SHA-256 digest of one version doesn't match another version
            for version in [2, 3, 5, 6]:
                with self.subTest(version=version):
                    acceptance: JSON = {'version': version, 'sha256': sha256}
                    body = {'accepted_contracts': {self._contract: acceptance}}
                    response = self._patch_user(body, self._mock_access_token)
                    self.assertEqual(400, response.status)
