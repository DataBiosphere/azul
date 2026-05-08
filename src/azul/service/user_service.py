import logging
from time import (
    time,
)
from typing import (
    TypedDict,
)

from jwt.api_jwt import (
    PyJWT,
)

from azul import (
    config,
)
from azul.auth import (
    AccessTokenAuthentication,
    PersonalAccessTokenAuthentication,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    not_none,
)
from azul.oauth2 import (
    Authorization,
    OAuth2Client,
    TokenForCodeResponse,
)

log = logging.getLogger(__name__)


class User(TypedDict):
    #: The OAuth 2.0 access token issued by the authorization server
    access_token: str
    #: The OAuth 2.0 refresh token used to obtain new access tokens
    refresh_token: str
    #: The user's email address from the ID token
    email: str
    #: Whether the email address has been verified by the identity provider
    email_verified: bool
    #: The Unix timestamp after which the refresh token expires
    expiration: int


class UnknownUserException(Exception):
    pass


class UserService:
    required_scopes = {'openid', 'email'}

    key_attribute = 'identity'
    ttl_attribute = 'expiration'

    _table_name = config.dynamo_users_table_name
    _key_separator = '#'
    _default_expiration = 180 * 24 * 60 * 60

    @cached_property
    def _oauth_client(self) -> OAuth2Client:
        return OAuth2Client()

    @property
    def _client_secret(self) -> str:
        path = config.oauth2_client_secret_path()
        response = aws.secretsmanager.get_secret_value(SecretId=path)
        return response['SecretString']

    @property
    def _client_id(self) -> str:
        return not_none(config.google_oauth2_client_id)

    @property
    def _dynamodb(self):
        return aws.dynamodb

    def authorize(self, authorization: Authorization) -> TokenForCodeResponse:
        scopes = set(authorization['scope'].split())
        assert self.required_scopes.issubset(scopes), R(
            'Be sure to include the required scopes when requesting the '
            'authorization code:', self.required_scopes)
        response = self._oauth_client.token_for_code(
            authorization_code=authorization['code'],
            client_id=self._client_id,
            client_secret=self._client_secret
        )
        assert 'id_token' in response, response
        self._store_tokens(response)
        return response

    def get_user(self, iss: str, sub: str) -> User | None:
        key = self._key_separator.join([iss, sub])
        response = self._dynamodb.get_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': key}}
        )
        item = response.get('Item')
        if item is None:
            return None
        else:
            return User(
                access_token=item['access_token']['S'],
                refresh_token=item['refresh_token']['S'],
                email=item['email']['S'],
                email_verified=item['email_verified']['BOOL'],
                expiration=int(item[self.ttl_attribute]['N'])
            )

    _google_issuer = 'https://accounts.google.com'

    @cached_property
    def _jwt(self) -> PyJWT:
        return PyJWT()

    def mint_personal_access_token(self,
                                   authentication: AccessTokenAuthentication
                                   ) -> PersonalAccessTokenAuthentication:
        token_info = self._oauth_client.token_info(authentication.access_token)
        assert token_info['aud'] == self._client_id, R(
            'Token was not issued for this application')
        iss, sub = self._google_issuer, token_info['sub']
        user = self.get_user(iss, sub)
        if user is None:
            raise UnknownUserException(iss, sub)
        raise NotImplementedError('APAT minting')

    def _store_tokens(self, response: TokenForCodeResponse) -> None:
        # Signature verification is unnecessary per OIDC 3.1.3.7 since the
        # token was received directly from the token endpoint over TLS.
        id_claims = self._jwt.decode(response['id_token'],
                                     options={'verify_signature': False})
        iss, sub = id_claims['iss'], id_claims['sub']
        assert self._key_separator not in iss, R(
            'Unexpected separator in issuer', iss)
        key = self._key_separator.join([iss, sub])
        expiration = response.get('refresh_token_expires_in',
                                  self._default_expiration)
        email = id_claims['email']
        email_verified = id_claims['email_verified']
        self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': key}},
            UpdateExpression=fd('''
                SET access_token = :access_token,
                    refresh_token = :refresh_token,
                    email = :email,
                    email_verified = :email_verified,
                    #expiration = :expiration
            '''),
            ExpressionAttributeNames={'#expiration': self.ttl_attribute},
            ExpressionAttributeValues={
                ':access_token': {'S': response['access_token']},
                ':refresh_token': {'S': response['refresh_token']},
                ':email': {'S': email},
                ':email_verified': {'BOOL': email_verified},
                ':expiration': {'N': str(self._now() + expiration)},
            }
        )

    def _now(self) -> int:
        return int(time())
