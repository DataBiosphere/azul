import logging
from time import (
    time,
)

import jwt.algorithms
from jwt.api_jwt import (
    PyJWT,
)
import jwt.exceptions
import jwt.types

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
from azul.lib.json import (
    redact_json,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSONTypedDict,
    json_untyped_dict,
    not_none,
)
from azul.oauth2 import (
    Authorization,
    OAuth2Client,
    TokenForCodeResponse,
)

log = logging.getLogger(__name__)


class User(JSONTypedDict):
    #: The OAuth 2.0 access token issued by the authorization server
    access_token: str
    #: The OAuth 2.0 refresh token used to obtain new access tokens
    refresh_token: str
    #: The user's email address from the ID token
    email: str
    #: Whether the email address has been verified by the identity provider
    email_verified: bool
    #: The Unix timestamp after which the access token expires
    access_token_expiration: int
    #: The Unix timestamp after which the refresh token expires
    expiration: int


class UnknownUserException(Exception):
    """
    The access token was issued to this application but the user hasn't invoked
    the authorization code flow.
    """


class ForeignTokenException(Exception):
    """
    The access token was not issued to this application. It could be a rogue
    token or a token belonging to one of our service accounts, during
    integration tests.
    """


class InvalidPersonalAccessTokenError(Exception):
    """
    The token is a JWT but not a valid APAT (expired, tampered with, forged, …).
    """


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

    def authorize(self,
                  authorization: Authorization,
                  *,
                  redirect_uri: str | None = None
                  ) -> TokenForCodeResponse:
        """
        Use the given authorization by a user to request an access and a refresh
        token from the authorization server. Persist information about the user,
        including both tokens, under the user's identity so that it can be
        retrieved later. Return the response from the authorization server. The
        authorization must have been requested by the client, and that request
        must have included the ``openid`` scope. The return value will contain
        all three tokens: ID, refresh and access.
        """
        log.info("Authorizing app, using code %r", redact_json(authorization['code']))
        scopes = set(authorization['scope'].split())
        assert self.required_scopes.issubset(scopes), R(
            'Be sure to include the required scopes when requesting the '
            'authorization code:', self.required_scopes)
        response = self._oauth_client.token_for_code(
            authorization_code=authorization['code'],
            client_id=self._client_id,
            client_secret=self._client_secret,
            redirect_uri=redirect_uri
        )
        assert 'id_token' in response, response
        self._store_tokens(response)
        return response

    def get_user(self, iss: str, sub: str) -> User:
        """
        Retrieve previously stored information about the user of the given
        identity. The identity of a user can be obtained from the ``iss` and
        ``sub`` claims of a valid ID token, or from the ``iss`` and ``sub``
        properties of a response from the authorization server's /tokeninfo
        endpoint.

        :param iss: The issuer of the user's identity

        :param sub: The user's identity
        """
        log.info('Retrieving user %r from %r', sub, iss)
        key = self._encode_identity(iss, sub)
        response = self._dynamodb.get_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': key}}
        )
        item = response.get('Item')
        if item is None:
            log.warning('No user %r from %r', sub, iss)
            raise UnknownUserException(iss, sub)
        else:
            try:
                access_token_expiration = int(item['access_token_expiration']['N'])
            except KeyError:
                access_token_expiration = 0
            user = User(
                access_token=item['access_token']['S'],
                refresh_token=item['refresh_token']['S'],
                email=item['email']['S'],
                email_verified=item['email_verified']['BOOL'],
                access_token_expiration=access_token_expiration,
                expiration=int(item[self.ttl_attribute]['N'])
            )
            log.info('Retrieved user %r', redact_json(json_untyped_dict(user)))
            return user

    _google_issuer = 'https://accounts.google.com'

    def _encode_identity(self, iss: str, sub: str) -> str:
        assert iss, iss
        if iss == self._google_issuer:
            iss = ''
        return self._key_separator.join([iss, sub])

    def _decode_identity(self, identity: str) -> tuple[str, str]:
        iss, sub = identity.split(self._key_separator, 1)
        if iss == '':
            iss = self._google_issuer
        return iss, sub

    _apat_algorithm = 'ES256'
    _apat_expiration = 30 * 24 * 60 * 60

    @cached_property
    def _jwt(self) -> PyJWT:
        jwt = PyJWT()
        jwt._jws.unregister_algorithm(self._apat_algorithm)
        jwt._jws.register_algorithm(self._apat_algorithm, KMSSigningAlgorithm())
        return jwt

    def mint_personal_access_token(self,
                                   at_auth: AccessTokenAuthentication
                                   ) -> PersonalAccessTokenAuthentication:
        """
        Given a valid access token for a user, issue a long-lived,
        application-specific, personal access token (APAT) for the same user.
        The token must be kept confidential, not because it contains secrets,
        but because it can be used to authenticate a request to the application,
        thereby providing access to any operation the user is permitted to
        perform. The APAT can be thought of as a proxy secret for the user's
        refresh token.
        """
        log.info('Minting APAT for %r', at_auth.redacted())
        token_info = self._oauth_client.token_info(at_auth.token)
        aud = token_info['aud']
        if aud != self._client_id:
            log.warning('Unexpected access token audience %r for token %r',
                        aud, at_auth.redacted())
            raise ForeignTokenException(aud)
        else:
            iss, sub = self._google_issuer, token_info['sub']
            self.get_user(iss, sub)
            now = self._now()
            payload = {
                'sub': self._encode_identity(iss, sub),
                'exp': now + self._apat_expiration,
                'iat': now
            }
            apat = self._jwt.encode(payload,
                                    key=config.apat_kms_key.alias,
                                    algorithm=self._apat_algorithm)
            self._jwt.decode(apat,
                             key=config.apat_kms_key.alias,
                             algorithms=[self._apat_algorithm])
            apat_auth = PersonalAccessTokenAuthentication(token=apat)
            log.info('Minted APAT %r for access token %r',
                     apat_auth.redacted(), at_auth.redacted())
            return apat_auth

    def exchange_token(self,
                       apat_auth: PersonalAccessTokenAuthentication
                       ) -> AccessTokenAuthentication:
        """
        Return a usable access token in exchange for an APAT if valid and not
        yet expired.
        """
        log.info('Getting access token for APAT %r', apat_auth.redacted())
        try:
            claims = self._jwt.decode(apat_auth.token,
                                      key=config.apat_kms_key.alias,
                                      algorithms=[self._apat_algorithm])
        except jwt.exceptions.PyJWTError as e:
            log.warning('Invalid APAT %r', apat_auth.redacted(), exc_info=e)
            raise InvalidPersonalAccessTokenError from e
        else:
            iss, sub = self._decode_identity(claims['sub'])
            user = self.get_user(iss, sub)
            access_token = user['access_token']
            if user['access_token_expiration'] < self._now() + 60:
                response = self._oauth_client.token_for_refresh(
                    refresh_token=user['refresh_token'],
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
                access_token, expires_in = response['access_token'], response['expires_in']
                self._update_access_token(iss, sub, access_token, expires_in)
            at_auth = AccessTokenAuthentication(access_token)
            log.info('Exchanged %r for APAT %r', at_auth.redacted(), apat_auth.redacted())
            return at_auth

    def _store_tokens(self, response: TokenForCodeResponse) -> None:
        log.debug('Storing tokens %r', redact_json(json_untyped_dict(response)))
        # Signature verification is unnecessary per OIDC 3.1.3.7 since the
        # token was received directly from the token endpoint over TLS.
        options = jwt.types.Options(verify_signature=False)
        id_claims = self._jwt.decode(response['id_token'], options=options)
        iss, sub = id_claims['iss'], id_claims['sub']
        assert iss == self._google_issuer, R('Unexpected issuer', iss)
        key = self._encode_identity(iss, sub)
        email, email_verified = id_claims['email'], id_claims['email_verified']
        now = self._now()
        self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': key}},
            UpdateExpression=fd('''
                SET access_token = :access_token,
                    access_token_expiration = :access_token_expiration,
                    refresh_token = :refresh_token,
                    email = :email,
                    email_verified = :email_verified,
                    #expiration = :expiration
            '''),
            ExpressionAttributeNames={'#expiration': self.ttl_attribute},
            ExpressionAttributeValues={
                ':access_token': {'S': response['access_token']},
                ':access_token_expiration': {'N': str(now + response['expires_in'])},
                ':refresh_token': {'S': response['refresh_token']},
                ':email': {'S': email},
                ':email_verified': {'BOOL': email_verified},
                ':expiration': {'N': str(now + response.get('refresh_token_expires_in',
                                                            self._default_expiration))},
            }
        )

    def _update_access_token(self,
                             iss: str,
                             sub: str,
                             access_token: str,
                             expires_in: int
                             ) -> None:
        log.debug('Updating access token of user %r at %r to %r',
                  sub, iss, redact_json(access_token))
        self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': self._encode_identity(iss, sub)}},
            UpdateExpression=fd('''
                SET access_token = :access_token,
                    access_token_expiration = :access_token_expiration
            '''),
            ExpressionAttributeValues={
                ':access_token': {'S': access_token},
                ':access_token_expiration': {'N': str(self._now() + expires_in)},
            }
        )

    def _now(self) -> int:
        return int(time())


class KMSSigningAlgorithm(jwt.algorithms.Algorithm):

    def prepare_key(self, key: str) -> str:
        return key

    @staticmethod
    def from_jwk(jwk):
        raise NotImplementedError

    @staticmethod
    def to_jwk(key, as_dict=False):
        raise NotImplementedError

    def sign(self, msg: bytes, key: str) -> bytes:
        log.debug('Signing %d bytes with key %r', len(msg), key)
        response = aws.kms.sign(KeyId=key,
                                Message=msg,
                                MessageType='RAW',
                                SigningAlgorithm='ECDSA_SHA_256')
        return response['Signature']

    def verify(self, msg: bytes, key: str, sig: bytes) -> bool:
        log.debug('Verifying %d bytes with key %r', len(msg), key)
        try:
            response = aws.kms.verify(KeyId=key,
                                      Message=msg,
                                      MessageType='RAW',
                                      Signature=sig,
                                      SigningAlgorithm='ECDSA_SHA_256')
        except aws.kms.exceptions.KMSInvalidSignatureException:
            return False
        else:
            return response['SignatureValid']
