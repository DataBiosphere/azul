import logging
from time import (
    time,
)
from typing import (
    TypedDict,
)

import jwt

from azul import (
    config,
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
    #: The Unix timestamp after which the refresh token expires
    expiration: int


class UserService:
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
        assert 'openid' in authorization['scope'].split(), R(
            'The authorization server did not return an OpenID Connect ID '
            'token in the response. Be sure to include the "openid" scope '
            'when requesting the authorization code.')
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
                expiration=int(item[self.ttl_attribute]['N'])
            )

    def _store_tokens(self, response: TokenForCodeResponse) -> None:
        # Signature verification is unnecessary per OIDC 3.1.3.7 since the
        # token was received directly from the token endpoint over TLS.
        id_claims = jwt.decode(response['id_token'],
                               options={'verify_signature': False})
        iss, sub = id_claims['iss'], id_claims['sub']
        assert self._key_separator not in iss, R(
            'Unexpected separator in issuer', iss)
        key = self._key_separator.join([iss, sub])
        expiration = response.get('refresh_token_expires_in',
                                  self._default_expiration)
        self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': key}},
            UpdateExpression=fd('''
                SET access_token = :access_token,
                    refresh_token = :refresh_token,
                    #expiration = :expiration
            '''),
            ExpressionAttributeNames={'#expiration': self.ttl_attribute},
            ExpressionAttributeValues={
                ':access_token': {'S': response['access_token']},
                ':refresh_token': {'S': response['refresh_token']},
                ':expiration': {'N': str(self._now() + expiration)},
            }
        )

    def _now(self) -> int:
        return int(time())
