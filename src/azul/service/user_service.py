from collections.abc import (
    Mapping,
)
from datetime import (
    datetime,
    timezone,
)
import json
import logging
from time import (
    time,
)
from typing import (
    NotRequired,
    Self,
    TYPE_CHECKING,
)

import attrs
from cryptography.exceptions import (
    InvalidSignature,
)
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDSA,
    EllipticCurvePublicKey,
    SECP256R1,
)
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)
from cryptography.hazmat.primitives.hashes import (
    SHA256,
)
from cryptography.hazmat.primitives.serialization import (
    load_der_public_key,
)
import jwt.algorithms
from jwt.algorithms import (
    ECAlgorithm,
)
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
from azul.lib.objects import (
    Sentinel,
    absent,
)
from azul.lib.strings import (
    assert_redactable,
    format_and_dedent as fd,
)
from azul.lib.time import (
    format_dcp2_datetime,
)
from azul.lib.types import (
    FlatJSON,
    JSON,
    JSONTypedDict,
    PrimitiveJSON,
    json_untyped_dict,
    json_untyped_flat_dict,
    not_none,
)
from azul.oauth2 import (
    Authorization,
    OAuth2Client,
    TokenForCodeResponse,
)
from azul.resources import (
    NotInLambdaContextException,
    open_resource,
)

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.type_defs import (
        AttributeValueTypeDef,
        UpdateItemInputTypeDef,
    )

log = logging.getLogger(__name__)


class ContractAcceptance(JSONTypedDict):
    #: The version of the contract the user accepted
    version: int
    #: The SHA-256 digest of the text of that version, in hexadecimal
    sha256: str


class AcceptedContract(ContractAcceptance):
    #: When the user accepted the contract, as formatted by
    #: :func:`format_dcp2_datetime`
    time: str


class User(JSONTypedDict):
    #: The issuer of the user's identity. Together with `sub`, it forms the key
    #: of the item representing the user, rather than an attribute of its own.
    iss: str
    #: The user's identity, unique within the issuer
    sub: str
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
    #: APATs with a jti below this value are considered revoked
    min_jti: NotRequired[int]
    #: The jti to assign to the APAT requested next
    max_jti: NotRequired[int]
    #: The contracts the user accepted, by name
    accepted_contracts: NotRequired[dict[str, AcceptedContract]]


def format_authorized_until(user: User) -> str:
    """
    When the given user's authorization lapses, as formatted by
    :func:`format_dcp2_datetime`. This is when the user's refresh token
    expires. The user's acceptance of contracts lapses along with it.
    """
    return format_dcp2_datetime(datetime.fromtimestamp(user['expiration'], timezone.utc))


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
    assert ttl_attribute in User.__annotations__

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

    @cached_property
    def _apat_public_key(self) -> EllipticCurvePublicKey:
        try:
            with open_resource('apat_public_key.json') as f:
                jwk = json.load(f)
        except NotInLambdaContextException:
            response = aws.kms.get_public_key(KeyId=config.apat_kms_key.alias)
            public_key = load_der_public_key(response['PublicKey'])
            assert isinstance(public_key, EllipticCurvePublicKey), type(public_key)
            return public_key
        else:
            public_key = ECAlgorithm.from_jwk(jwk)
            assert isinstance(public_key, EllipticCurvePublicKey), type(public_key)
            return public_key

    @property
    def apat_public_key_for_outsourcing(self) -> JSON:
        return json.loads(ECAlgorithm.to_jwk(self._apat_public_key))

    @property
    def _dynamodb(self):
        return aws.dynamodb

    def _now(self) -> int:
        return int(time())

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
            'authorization code:', sorted(self.required_scopes))
        response = self._oauth_client.token_for_code(
            authorization_code=authorization['code'],
            client_id=self._client_id,
            client_secret=self._client_secret,
            redirect_uri=redirect_uri
        )
        assert 'id_token' in response, response
        # Signature verification is unnecessary per OIDC 3.1.3.7 since the
        # token was received directly from the token endpoint over TLS.
        options = jwt.types.Options(verify_signature=False)
        id_claims = self._jwt.decode(response['id_token'], options=options)
        iss, sub = id_claims['iss'], id_claims['sub']
        assert iss == self._google_issuer, R('Unexpected issuer', iss)
        now = self._now()
        expiration = response.get('refresh_token_expires_in',
                                  self._default_expiration)
        user = User(
            iss=iss,
            sub=sub,
            access_token=response['access_token'],
            refresh_token=response['refresh_token'],
            email=id_claims['email'],
            email_verified=id_claims['email_verified'],
            access_token_expiration=now + response['expires_in'],
            expiration=now + expiration
        )
        self._store_user(user)
        return response

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

    def _identity(self, user: User) -> str:
        return self._encode_identity(user['iss'], user['sub'])

    _apat_algorithm = 'ES256'
    _apat_expiration = 30 * 24 * 60 * 60

    @cached_property
    def _jwt(self) -> PyJWT:
        jwt = PyJWT()
        jwt._jws.unregister_algorithm(self._apat_algorithm)
        jwt._jws.register_algorithm(self._apat_algorithm, KMSSigningAlgorithm())
        return jwt

    def _verify_access_token(self,
                             at_auth: AccessTokenAuthentication
                             ) -> User:
        token_info = self._oauth_client.token_info(at_auth.token)
        aud = token_info['aud']
        if aud != self._client_id:
            log.warning('Unexpected access token audience %r for token %s',
                        aud, at_auth)
            raise ForeignTokenException(aud)
        else:
            return self._load_user(self._google_issuer, token_info['sub'])

    def get_user(self, at_auth: AccessTokenAuthentication) -> User:
        """
        Return the stored information about the user identified by the given
        access token.
        """
        return self._verify_access_token(at_auth)

    def update_accepted_contracts(self,
                                  at_auth: AccessTokenAuthentication,
                                  accepted_contracts: Mapping[str, ContractAcceptance | None] | None
                                  ) -> User:
        """
        Record that the user identified by the given access token accepted, or
        retracted the acceptance of, the given contracts, and return the updated
        information about that user.

        :param at_auth: The access token of the user

        :param accepted_contracts: Maps the name of each contract to the version
                                   the user accepted and its SHA-256 digest, or
                                   to None if the user retracts the acceptance
                                   of that contract. Contracts absent from this
                                   mapping are left alone. None retracts the
                                   acceptance of every contract.
        """
        user = self._verify_access_token(at_auth)
        if accepted_contracts is None:
            user = self._update_user(user,
                                     'REMOVE accepted_contracts',
                                     names={},
                                     values={})
            log.info('User %r from %r retracted the acceptance of all contracts',
                     user['sub'], user['iss'])
        elif accepted_contracts:
            # An attribute nested in a map can't be set or removed unless the
            # map exists
            self._update_user(user,
                              'SET accepted_contracts = if_not_exists(accepted_contracts, :empty)',
                              names={},
                              values={':empty': {'M': {}}})
            now = format_dcp2_datetime(datetime.now(timezone.utc))
            names: dict[str, str] = {}
            values: dict[str, AttributeValueTypeDef] = {}
            assignments, removals = [], []
            for i, (name, acceptance) in enumerate(accepted_contracts.items()):
                names[f'#c{i}'] = name
                path = f'accepted_contracts.#c{i}'
                if acceptance is None:
                    removals.append(path)
                else:
                    assignments.append(f'{path} = :c{i}')
                    values[f':c{i}'] = {
                        'M': {
                            'version': {'N': str(acceptance['version'])},
                            'sha256': {'S': acceptance['sha256']},
                            'time': {'S': now}
                        }
                    }
            expression = ' '.join(
                f'{action} {", ".join(paths)}'
                for action, paths in [('SET', assignments), ('REMOVE', removals)]
                if paths
            )
            user = self._update_user(user, expression, names=names, values=values)
            # These messages are the audit trail of the acceptance of contracts,
            # so they are only logged once the update succeeded
            authorized_until = format_authorized_until(user)
            for name, acceptance in accepted_contracts.items():
                if acceptance is None:
                    log.info('User %r from %r retracted the acceptance of contract %r',
                             user['sub'], user['iss'], name)
                else:
                    accepted = user['accepted_contracts'][name]
                    log.info('User %r from %r accepted version %r with SHA-256 %r '
                             'of contract %r at %r, authorized until %r',
                             user['sub'], user['iss'],
                             accepted['version'], accepted['sha256'], name,
                             accepted['time'], authorized_until)
        return user

    def _update_user(self,
                     user: User,
                     expression: str,
                     *,
                     names: Mapping[str, str],
                     values: Mapping[str, AttributeValueTypeDef]
                     ) -> User:
        """
        Apply the given update expression to the stored item representing the
        given user, and return the updated user.
        """
        request: UpdateItemInputTypeDef = dict(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': self._identity(user)}},
            UpdateExpression=expression,
            # Without the condition, the update would create a partial item for
            # a user whose item expired after it was loaded
            ConditionExpression='attribute_exists(#key)',
            ExpressionAttributeNames={'#key': self.key_attribute, **names},
            ReturnValues='ALL_NEW'
        )
        # DynamoDB rejects an empty mapping of values
        if values:
            request['ExpressionAttributeValues'] = values
        try:
            response = self._dynamodb.update_item(**request)
        except self._dynamodb.exceptions.ConditionalCheckFailedException:
            raise UnknownUserException(user['iss'], user['sub'])
        else:
            return self._user_from_item(response['Attributes'])

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
        log.info('Minting APAT for %s', at_auth)
        user = self._verify_access_token(at_auth)
        jti = self._next_jti(user)
        now = self._now()
        payload = {
            'sub': self._identity(user),
            'exp': now + self._apat_expiration,
            'iat': now,
            'jti': str(jti)
        }
        apat = self._jwt.encode(payload,
                                key=config.apat_kms_key.alias,
                                algorithm=self._apat_algorithm)
        self._jwt.decode(apat,
                         key=self._apat_public_key,
                         algorithms=[self._apat_algorithm])
        assert_redactable(apat)
        apat_auth = PersonalAccessTokenAuthentication(token=apat)
        log.info('Minted APAT %s for access token %s',
                 apat_auth, at_auth)
        return apat_auth

    def exchange_token(self,
                       apat_auth: PersonalAccessTokenAuthentication
                       ) -> AccessTokenAuthentication:
        """
        Return a usable access token in exchange for an APAT if valid and not
        yet expired.
        """
        log.info('Getting access token for APAT %s', apat_auth)
        try:
            claims = self._jwt.decode(apat_auth.token,
                                      key=self._apat_public_key,
                                      algorithms=[self._apat_algorithm])
        except jwt.exceptions.PyJWTError as e:
            log.warning('Invalid APAT %s', apat_auth, exc_info=e)
            raise InvalidPersonalAccessTokenError from e
        else:
            assert_redactable(apat_auth.token)
            iss, sub = self._decode_identity(claims['sub'])
            user = self._load_user(iss, sub)
            jti = claims.get('jti')
            if jti is None or not (user['min_jti'] <= int(jti) < user['max_jti']):
                log.warning('APAT jti %r out of range [%d, %d) for %s',
                            jti, user['min_jti'], user['max_jti'], apat_auth)
                raise InvalidPersonalAccessTokenError
            access_token = user['access_token']
            if user['access_token_expiration'] < self._now() + 60:
                response = self._oauth_client.token_for_refresh(
                    refresh_token=user['refresh_token'],
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
                access_token, expires_in = response['access_token'], response['expires_in']
                self._update_access_token(user, access_token, expires_in)
            at_auth = AccessTokenAuthentication(access_token)
            log.info('Exchanged %s for APAT %s', at_auth, apat_auth)
            return at_auth

    def revoke_personal_access_tokens(self,
                                      at_auth: AccessTokenAuthentication
                                      ) -> None:
        """
        Revoke all APATs for the user identified by the given access token.
        """
        log.info('Revoking all APATs for %s', at_auth)
        user = self._verify_access_token(at_auth)
        self._revoke_jti(user)

    def _store_user(self, user: User) -> None:
        key = self._identity(user)
        user = json_untyped_flat_dict(user)
        log.debug('Storing user %r', redact_json(user))
        self._dynamodb.update_item(
            **DynamoDBItemUpdate(
                table_name=self._table_name,
                key={self.key_attribute: {'S': key}}
            )
            .set_from(user, 'access_token')
            .set_from(user, 'access_token_expiration')
            .set_from(user, 'refresh_token')
            .set_from(user, 'email')
            .set_from(user, 'email_verified')
            .set_from(user, 'expiration', alias=True)
            .set_from(user, 'min_jti', default=0)
            .set_from(user, 'max_jti', default=0)
            .to_update_item_input()
        )

    def _load_user(self, iss: str, sub: str) -> User:
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
            user = self._user_from_item(item)
            log.info('Retrieved user %r', redact_json(json_untyped_dict(user)))
            return user

    def _user_from_item(self, item: Mapping[str, AttributeValueTypeDef]) -> User:
        iss, sub = self._decode_identity(item[self.key_attribute]['S'])
        accepted_contracts = item.get('accepted_contracts', {'M': {}})['M']
        return User(
            iss=iss,
            sub=sub,
            access_token=item['access_token']['S'],
            refresh_token=item['refresh_token']['S'],
            email=item['email']['S'],
            email_verified=item['email_verified']['BOOL'],
            access_token_expiration=int(item['access_token_expiration']['N']),
            expiration=int(item[self.ttl_attribute]['N']),
            min_jti=int(item['min_jti']['N']),
            max_jti=int(item['max_jti']['N']),
            accepted_contracts={
                name: AcceptedContract(
                    version=int(contract['M']['version']['N']),
                    sha256=contract['M']['sha256']['S'],
                    time=contract['M']['time']['S']
                )
                for name, contract in accepted_contracts.items()
            }
        )

    def _next_jti(self, user: User) -> int:
        response = self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': self._identity(user)}},
            UpdateExpression=fd('''
                SET max_jti = max_jti + :one
            '''),
            ExpressionAttributeValues={
                ':one': {'N': '1'},
            },
            ReturnValues='UPDATED_OLD'
        )
        return int(response['Attributes']['max_jti']['N'])

    def _update_access_token(self,
                             user: User,
                             access_token: str,
                             expires_in: int
                             ) -> None:
        log.debug('Updating access token of user %r at %r to %r',
                  user['sub'], user['iss'], redact_json(access_token))
        self._dynamodb.update_item(
            **DynamoDBItemUpdate(
                table_name=self._table_name,
                key={self.key_attribute: {'S': self._identity(user)}}
            )
            .set('access_token', {'S': access_token})
            .set('access_token_expiration', {'N': str(self._now() + expires_in)})
            .to_update_item_input()
        )

    def _revoke_jti(self, user: User) -> None:
        response = self._dynamodb.update_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': self._identity(user)}},
            UpdateExpression=fd('''
                SET min_jti = max_jti
            '''),
            ReturnValues='ALL_NEW'
        )
        min_jti = response['Attributes']['min_jti']['N']
        log.info('Set min_jti for user %r from %r to %s',
                 user['sub'], user['iss'], min_jti)


@attrs.frozen(kw_only=True)
class DynamoDBItemUpdate:
    table_name: str
    key: dict[str, dict[str, str]]
    assignments: list[str] = attrs.field(init=False, factory=list)
    attributes: dict[str, AttributeValueTypeDef] = attrs.field(init=False, factory=dict)
    aliases: dict[str, str] = attrs.field(init=False, factory=dict)

    def set(self,
            attribute: str,
            value: AttributeValueTypeDef | tuple[str, AttributeValueTypeDef],
            *,
            alias: bool = False
            ) -> Self:
        """
        Instruct this item update to set the item's attribute of the given name
        to the given value. If the value is a tuple, set the attribute to the
        custom expression (the 1st tuple element). The custom expression, can
        reference the given value (the 2n tuple element) using the attribute
        name prefixed with a colon.

        To set attributes whose name is reserved, pass alias=True.
        """
        if alias:
            lhs = f'#{attribute}'
            self.aliases[lhs] = attribute
        else:
            lhs = attribute
        if isinstance(value, tuple):
            rhs, value = value
            self.assignments.append(f'{lhs} = {rhs}')
        else:
            self.assignments.append(f'{lhs} = :{attribute}')
        self.attributes[f':{attribute}'] = value
        return self

    def set_from(self,
                 mapping: FlatJSON,
                 attribute: str,
                 alias: bool = False,
                 default: PrimitiveJSON | Sentinel = absent
                 ) -> Self:
        """
        Instruct this item update to set the item's attribute of the given name
        to the corresponding value from the given mapping, if present, or the
        given default. Raise a KeyError if no default was given.

        Note that the default is only used if the attribute is also absent from
        the item. In other words, an existing attribute of an existing item will
        only be overwritten with a value the mapping, never the default.
        """
        if absent.is_(default):
            value = self._attribute_value(mapping[attribute])
            self.set(attribute, value, alias=alias)
        else:
            try:
                value = mapping[attribute]
            except KeyError:
                value = f'if_not_exists({attribute}, :{attribute})', self._attribute_value(default)
            else:
                value = self._attribute_value(value)
            self.set(attribute, value, alias=alias)
        return self

    @staticmethod
    def _attribute_value(value: PrimitiveJSON) -> AttributeValueTypeDef:
        if isinstance(value, str):
            return {'S': value}
        elif isinstance(value, bool):
            return {'BOOL': value}
        elif isinstance(value, (int, float)):
            return {'N': str(value)}
        elif value is None:
            return {'NULL': True}
        else:
            assert False, R('Unexpected type', type(value))

    def to_update_item_input(self) -> UpdateItemInputTypeDef:
        result: UpdateItemInputTypeDef = dict(
            TableName=self.table_name,
            Key=self.key,
            UpdateExpression='SET ' + ', '.join(self.assignments),
            ExpressionAttributeValues=self.attributes,
        )
        if self.aliases:
            result['ExpressionAttributeNames'] = self.aliases
        return result


class KMSSigningAlgorithm(jwt.algorithms.Algorithm):

    def prepare_key(self, key: str | EllipticCurvePublicKey) -> str | EllipticCurvePublicKey:
        return key

    @staticmethod
    def from_jwk(jwk):
        raise NotImplementedError

    @staticmethod
    def to_jwk(key, as_dict=False):
        raise NotImplementedError

    # ES256 uses the P-256 curve (RFC 7518 Section 3.4), a.k.a. SECP256R1
    _sig_component_size = SECP256R1.key_size // 8

    def sign(self, msg: bytes, key: str) -> bytes:
        log.debug('Signing %d bytes with key %r', len(msg), key)
        response = aws.kms.sign(KeyId=key,
                                Message=msg,
                                MessageType='RAW',
                                SigningAlgorithm='ECDSA_SHA_256')
        # KMS returns DER-encoded signatures, but JWS requires raw r||s
        return self._der_to_raw(response['Signature'])

    def verify(self, msg: bytes, key: EllipticCurvePublicKey, sig: bytes) -> bool:
        log.debug('Verifying %d bytes', len(msg))
        try:
            key.verify(self._raw_to_der(sig), msg, ECDSA(SHA256()))
        except InvalidSignature:
            return False
        else:
            return True

    def _der_to_raw(self, der_sig: bytes) -> bytes:
        r, s = decode_dss_signature(der_sig)
        return r.to_bytes(self._sig_component_size, 'big') + s.to_bytes(self._sig_component_size, 'big')

    def _raw_to_der(self, raw_sig: bytes) -> bytes:
        r = int.from_bytes(raw_sig[:self._sig_component_size], 'big')
        s = int.from_bytes(raw_sig[self._sig_component_size:], 'big')
        return encode_dss_signature(r, s)
