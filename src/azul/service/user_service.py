from collections.abc import (
    Mapping,
)
from datetime import (
    datetime,
    timezone,
)
import hashlib
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
    UnverifiedAccessTokenAuthentication,
    UnverifiedPersonalAccessTokenAuthentication,
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
)
from azul.oauth2 import (
    Authorization,
    InvalidAccessTokenError,
    InvalidRefreshTokenError,
    OAuth2Client,
    TokenForCodeResponse,
    TokenInfoResponse,
    TokenResponse,
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
    #: The Unix timestamp of the user's most recent authorization of this
    #: application
    authorization_time: int
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


class InvalidPersonalAccessTokenError(Exception):
    """
    The token is a JWT but not a usable APAT. Subclasses represent the specific
    reasons.
    """


class InauthenticPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The APAT is malformed or wasn't signed by this application.
    """


class ExpiredPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The APAT has expired.
    """


class OrphanedPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The user the APAT was minted for is unknown, typically because the user's
    stored information expired along with their refresh token.
    """


class RevokedPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The APAT's JWT ID is absent or outside the range of JWT IDs that are valid
    for its user, typically because the user revoked the APAT.
    """


class SuspendedPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The most recent authorization of this application by the user the APAT was
    minted for is too long ago. The APAT becomes usable again once the user
    authorizes this application again.
    """


class DeauthorizedPersonalAccessTokenError(InvalidPersonalAccessTokenError):
    """
    The authorization server rejected the refresh token or the stored access
    token of the user the APAT was minted for, typically because the user's
    authorization of this application was revoked or has expired.
    """


class RejectedAccessTokenError(Exception):
    """
    The authorization server accepted the access token but this application
    doesn't, because it was issued to a party this application doesn't accept
    tokens from, or because it is a user's access token that this application
    didn't obtain.
    """


class ReauthorizationLimitError(Exception):
    """
    The user's most recent authorization of this application is too long ago
    for their access token to be refreshed.
    """


class UserService:
    required_scopes = {'openid', 'email'}

    key_attribute = 'identity'
    ttl_attribute = 'expiration'
    assert ttl_attribute in User.__annotations__

    _table_name = config.dynamo_users_table_name
    _key_separator = '#'

    #: The name of the DynamoDB table serving as a registry of every access
    #: token this application obtained or accepted, along with the identity of
    #: the user or service account to which that token belongs. The registry
    #: lets the application verify the tokens without a round trip to the
    #: authorization server.
    #:
    _access_tokens_table_name = config.dynamo_access_tokens_table_name
    access_token_key_attribute = 'token_hash'

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
        return config.google_oauth2_client_id

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
        expiration = response.get('refresh_token_expires_in', self._default_expiration)
        user = User(
            iss=iss,
            sub=sub,
            access_token=response['access_token'],
            refresh_token=response['refresh_token'],
            email=id_claims['email'],
            email_verified=id_claims['email_verified'],
            access_token_expiration=now + response['expires_in'],
            expiration=now + expiration,
            authorization_time=now
        )
        self._store_user(user)
        at_auth = AccessTokenAuthentication(
            token=user['access_token'],
            identity=self._identity(user),
            expiration=user['access_token_expiration']
        )
        self._register_access_token(at_auth)
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

    def verify_access_token(self,
                            uat_auth: UnverifiedAccessTokenAuthentication
                            ) -> AccessTokenAuthentication:
        """
        Verify the given access token, unless it is already verified.

        :raise InvalidAccessTokenError: if the authorization server rejects the
                                        token

        :raise RejectedAccessTokenError: if this application rejects the token

        :raise Exception: if the validity of the token cannot be determined
        """
        if isinstance(uat_auth, AccessTokenAuthentication):
            at_auth = uat_auth
        else:
            at_auth = self._registered_access_token(uat_auth)
            if at_auth is None:
                #
                # We expect to encounter unregistered tokens in these cases:
                #
                # 1) The access token belongs to a service account. This is the
                #    only token we accept, and we register it here. In the
                #    remaining cases below, we reject the token.
                #
                # 2) The access token wasn't obtained from this application,
                #    using, say, the implicit flow with just the client ID and a
                #    redirect to http://localhost:8088. This works because the
                #    implicit flow doesn't require a client secret and that
                #    specific redirect URL is registered in the client
                #
                # 3) The access token was obtained from this application, but
                #    before it registered every user's access token
                #
                # 4) The application attempted to register the token, but
                #    failed due to a transient problem
                #
                # 5) The contents of the registry were lost, for example because
                #    its table was recreated.
                #
                try:
                    token_info = self._token_info(uat_auth.token)
                except InvalidAccessTokenError as e:
                    log.warning('Invalid access token %s', uat_auth, exc_info=e)
                    raise
                except RejectedAccessTokenError as e:
                    log.warning('Rejected access token %s', uat_auth, exc_info=e)
                    raise
                else:
                    if self._is_user_token(token_info):
                        # A user's access token, cases 2) through 5) above
                        log.warning('Rejected access token %s, a user token '
                                    'this application has not registered', uat_auth)
                        raise RejectedAccessTokenError('OAuth 2.0 access token was '
                                                       'not obtained by this application')
                    else:
                        # A service account's access token, case 1) above
                        at_auth = AccessTokenAuthentication(
                            token=uat_auth.token,
                            identity=self._token_identity(token_info),
                            expiration=self._now() + int(token_info['expires_in'])
                        )
                        self._register_access_token(at_auth)
        return at_auth

    def _is_user_token(self, token_info: TokenInfoResponse) -> bool:
        """
        True if the given information describes a user's access token, False
        if it describes a service account's.
        """
        # A user's token originates from an OAuth 2.0 client, a service
        # account's from the service account itself.
        return token_info['azp'].endswith('.apps.googleusercontent.com')

    def _token_identity(self, token_info: TokenInfoResponse) -> str:
        sub = token_info['sub' if self._is_user_token(token_info) else 'azp']
        return self._encode_identity(self._google_issuer, sub)

    def _access_token_key(self, access_token: str) -> str:
        return hashlib.sha256(access_token.encode()).hexdigest()

    #: A registered access token is verified again with the authorization
    #: server once this many seconds have passed since it was last verified
    #: there, so that a revoked token is rejected within that interval.
    #:
    reverification_interval = 5 * 60

    def _registered_access_token(self,
                                 uat_auth: UnverifiedAccessTokenAuthentication
                                 ) -> AccessTokenAuthentication | None:
        """
        Return the given access token, verified by the registry, or None if the
        token isn't registered, or has expired. A registered token that is due
        for reverification is verified with the authorization server first.

        :raise InvalidAccessTokenError: if the authorization server rejects the
                                        token

        :raise RejectedAccessTokenError: if this application rejects the token

        :raise Exception: if the validity of the token cannot be determined
        """
        response = self._dynamodb.get_item(
            TableName=self._access_tokens_table_name,
            Key={self.access_token_key_attribute: {'S': self._access_token_key(uat_auth.token)}},
            ConsistentRead=True
        )
        item = response.get('Item')
        if item is None:
            return None
        else:
            expiration = int(item[self.ttl_attribute]['N'])
            # Items can persist in DynamoDB after they are marked as expired
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
            if expiration <= self._now():
                return None
            else:
                at_auth = AccessTokenAuthentication(
                    token=uat_auth.token,
                    identity=item['identity']['S'],
                    expiration=expiration
                )
                verification_time = int(item['verification_time']['N'])
                if verification_time + self.reverification_interval <= self._now():
                    self._reverify_access_token(at_auth)
                return at_auth

    def _reverify_access_token(self, at_auth: AccessTokenAuthentication) -> None:
        try:
            token_info = self._token_info(at_auth.token)
        except InvalidAccessTokenError as e:
            log.warning('Registered access token %s is no longer valid', at_auth, exc_info=e)
            raise
        except RejectedAccessTokenError as e:
            log.warning('Rejected registered access token %s', at_auth, exc_info=e)
            raise
        else:
            if self._token_identity(token_info) == at_auth.identity():
                log.info('Reverified access token %s', at_auth)
                self._register_access_token(at_auth)
            else:
                log.warning('Rejected registered access token %s, '
                            'its identity has changed', at_auth)
                # The message intentionally lacks detail, for confidentiality
                raise RejectedAccessTokenError('OAuth 2.0 subject does not match')

    def _register_access_token(self, at_auth: AccessTokenAuthentication) -> None:
        """
        Register the given access token, recording that it was verified with
        the authorization server just now. A token this application just
        obtained from that server counts as verified.
        """
        self._dynamodb.put_item(
            TableName=self._access_tokens_table_name,
            Item={
                self.access_token_key_attribute: {'S': self._access_token_key(at_auth.token)},
                'identity': {'S': at_auth.identity()},
                self.ttl_attribute: {'N': str(at_auth.expiration)},
                'verification_time': {'N': str(self._now())}
            }
        )

    def _token_info(self, access_token: str) -> TokenInfoResponse:
        """
        Retrieve information about the given token from the authorization server
        and ensure that it is suitable for this application.

        :raise InvalidAccessTokenError: if the authorization server rejects the
                                        token

        :raise RejectedAccessTokenError: if this application rejects the token

        :raise Exception: if the validity of the token cannot be determined
        """

        def require(condition: bool, message: str) -> None:
            if not condition:
                raise RejectedAccessTokenError(message)

        token_info = self._oauth_client.token_info(access_token)
        # The error messages here intentionally lack detail, for confidentiality
        authorized_party, audience = token_info['azp'], token_info['aud']
        email = token_info.get('email')
        if self._is_user_token(token_info):
            # A user's access token originating from an OAuth 2.0 client
            require(authorized_party == self._client_id,
                    'OAuth 2.0 client does not match')
            require(audience == self._client_id,
                    'OAuth 2.0 audience does not match')
            require('sub' in token_info,
                    'OAuth 2.0 subject is missing')
        elif email is not None and email.endswith('.iam.gserviceaccount.com'):
            # A service account's bare access token
            require(token_info.get('email_verified') == 'true',
                    'Service account email is not verified')
            require(audience == authorized_party,
                    'Service account audience does not match')
            # The `sub` (subject) is present it must match `azp`
            require(token_info.get('sub', authorized_party) == authorized_party,
                    'Service account subject does not match')
            local_part, _, host = email.partition('@')
            host, _, domain = host.partition('.')
            require(host == config.google_project(),
                    'Service account project does not match')
        else:
            require(False, 'Unexpected type of authorized party')
        return token_info

    def get_user(self, uat_auth: UnverifiedAccessTokenAuthentication) -> User:
        """
        Return the stored information about the user identified by the given
        access token.
        """
        at_auth = self.verify_access_token(uat_auth)
        return self._load_user(at_auth.identity())

    def update_accepted_contracts(self,
                                  uat_auth: UnverifiedAccessTokenAuthentication,
                                  accepted_contracts: Mapping[str, ContractAcceptance | None] | None
                                  ) -> User:
        """
        Record that the user identified by the given access token accepted, or
        retracted the acceptance of, the given contracts, and return the updated
        information about that user.

        :param uat_auth: The access token of the user

        :param accepted_contracts: Maps the name of each contract to the version
                                   the user accepted and its SHA-256 digest, or
                                   to None if the user retracts the acceptance
                                   of that contract. Contracts absent from this
                                   mapping are left alone. None retracts the
                                   acceptance of every contract.
        """
        at_auth = self.verify_access_token(uat_auth)
        user = self._load_user(at_auth.identity())
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
                                   uat_auth: UnverifiedAccessTokenAuthentication
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
        log.info('Minting APAT for %s', uat_auth)
        at_auth = self.verify_access_token(uat_auth)
        user = self._load_user(at_auth.identity())
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
        uapat_auth = UnverifiedPersonalAccessTokenAuthentication(token=apat)
        apat_auth = self.verify_personal_access_token(uapat_auth)
        log.info('Minted %s for %s', apat_auth, at_auth)
        return apat_auth

    def verify_personal_access_token(self,
                                     uapat_auth: UnverifiedPersonalAccessTokenAuthentication
                                     ) -> PersonalAccessTokenAuthentication:
        """
        Verify the signature and expiration of the given APAT. Determining
        whether the APAT was revoked is not part of verification.

        :raise InvalidPersonalAccessTokenError: if the APAT is malformed, its
                                                signature is invalid or it has
                                                expired
        """
        try:
            claims = self._jwt.decode(uapat_auth.token,
                                      key=self._apat_public_key,
                                      algorithms=[self._apat_algorithm])
        except jwt.exceptions.ExpiredSignatureError as e:
            log.warning('Expired APAT %s', uapat_auth, exc_info=e)
            raise ExpiredPersonalAccessTokenError from e
        except jwt.exceptions.PyJWTError as e:
            log.warning('Invalid APAT %s', uapat_auth, exc_info=e)
            raise InauthenticPersonalAccessTokenError from e
        else:
            assert_redactable(uapat_auth.token)
            return PersonalAccessTokenAuthentication(token=uapat_auth.token,
                                                     identity=claims['sub'],
                                                     jti=claims.get('jti'))

    def exchange_token(self,
                       uapat_auth: UnverifiedPersonalAccessTokenAuthentication
                       ) -> AccessTokenAuthentication:
        """
        Return an access token in exchange for a valid APAT. For an APAT to be
        considered valid, it must be signed, it must not be expired, it must
        not have been revoked, and the user owning the APAT must have logged in
        within the last :attr:`Config.reauthorization_limit` seconds.

        :raise InvalidPersonalAccessTokenError: The APAT cannot be used to
                                                obtain an access token, either
                                                permanently or temporarily. See
                                                the specific subclasses of
                                                the exception for details.
        """
        log.info('Getting access token for APAT %s', uapat_auth)
        apat_auth = self.verify_personal_access_token(uapat_auth)
        try:
            user = self._load_user(apat_auth.identity())
        except UnknownUserException as e:
            # The user's item expires along with their refresh token
            log.warning('Unknown user of %s', apat_auth, exc_info=e)
            raise OrphanedPersonalAccessTokenError from e
        jti = apat_auth.jti
        if jti is None or not (user['min_jti'] <= int(jti) < user['max_jti']):
            log.warning('The jti of %s is out of range [%d, %d)',
                        apat_auth, user['min_jti'], user['max_jti'])
            raise RevokedPersonalAccessTokenError
        if user['authorization_time'] + config.reauthorization_limit <= self._now():
            log.warning('Unable to exchange %s for an access token. '
                        'The user needs to log in again', apat_auth)
            raise SuspendedPersonalAccessTokenError
        at_auth: AccessTokenAuthentication | None
        # The access token must remain valid for the rest of the request. Only
        # the REST API Lambda handles requests authenticated with an APAT.
        if user['access_token_expiration'] < self._now() + 2 * config.api_gateway_lambda_timeout:
            at_auth = None
        else:
            uat_auth = UnverifiedAccessTokenAuthentication(token=user['access_token'])
            try:
                at_auth = self._registered_access_token(uat_auth)
            except InvalidAccessTokenError as e:
                log.warning('Unable to exchange %s for an access token. '
                            "The user's access token is no longer valid",
                            apat_auth, exc_info=e)
                raise DeauthorizedPersonalAccessTokenError from e
            else:
                if at_auth is None:
                    # A refresh confirms that the user's authorization of this
                    # application is still in effect
                    log.warning('Stored %s is not registered, refreshing it', uat_auth)
                else:
                    assert at_auth.identity() == self._identity(user)
        if at_auth is None:
            try:
                response = self._oauth_client.token_for_refresh(
                    refresh_token=user['refresh_token'],
                    client_id=self._client_id,
                    client_secret=self._client_secret
                )
            except InvalidRefreshTokenError as e:
                log.warning('Unable to exchange %s for an access token. '
                            "The user's refresh token is invalid or has been revoked",
                            apat_auth, exc_info=e)
                raise DeauthorizedPersonalAccessTokenError from e
            else:
                at_auth = AccessTokenAuthentication(
                    token=response['access_token'],
                    identity=self._identity(user),
                    expiration=self._now() + response['expires_in']
                )
                self._update_access_token(user, at_auth.token, response['expires_in'])
                self._register_access_token(at_auth)
        log.info('Exchanged %s for %s', at_auth, apat_auth)
        return at_auth

    #: An access token that expires within this many seconds can be exchanged
    #: for a fresh one
    #:
    reauthorization_window = 15 * 60

    def reauthorize(self,
                    uat_auth: UnverifiedAccessTokenAuthentication
                    ) -> TokenResponse:
        """
        Return the given access token if it remains valid for longer than
        :attr:`reauthorization_window`. Otherwise, return the user's stored
        access token if that remains valid for longer than the window, or a
        fresh access token for the user, obtained with the user's refresh token,
        if it doesn't. The scopes of a returned access token other than a fresh
        one are unknown, so they are reported as None.

        :raise ReauthorizationLimitError: A fresh access token is needed but
                                          the user last authorized this
                                          application longer than
                                          :attr:`Config.reauthorization_limit`
                                          seconds ago

        :raise InvalidRefreshTokenError: if the authorization server rejects
                                         the user's refresh token
        """
        at_auth = self.verify_access_token(uat_auth)
        user = self._load_user(at_auth.identity())
        expires_in = at_auth.expiration - self._now()
        if expires_in > self.reauthorization_window:
            log.info('No need to reauthorize %s, it is still fresh', at_auth)
            # The registry lacks the token's scopes. Google always reports
            # them, hence the type, but here they are unknown.
            response = TokenResponse(
                access_token=at_auth.token,
                expires_in=expires_in,
                scope=None,  # type: ignore[typeddict-item]
                token_type='Bearer'
            )
        elif user['authorization_time'] + config.reauthorization_limit <= self._now():
            log.warning('Cannot reauthorize %s, the user needs to log in again', at_auth)
            raise ReauthorizationLimitError
        elif user['access_token_expiration'] - self._now() > self.reauthorization_window:
            # Another request has already obtained a fresh access token
            at_reauth = AccessTokenAuthentication(
                token=user['access_token'],
                identity=self._identity(user),
                expiration=user['access_token_expiration']
            )
            assert at_reauth.identity() == at_auth.identity()
            stored_expires_in = at_reauth.expiration - self._now()
            log.info('No need to reauthorize %s, reusing stored %s', at_auth, at_reauth)
            response = TokenResponse(
                access_token=at_reauth.token,
                expires_in=stored_expires_in,
                scope=None,  # type: ignore[typeddict-item]
                token_type='Bearer'
            )
        else:
            response = self._oauth_client.token_for_refresh(
                refresh_token=user['refresh_token'],
                client_id=self._client_id,
                client_secret=self._client_secret
            )
            at_reauth = AccessTokenAuthentication(
                token=response['access_token'],
                identity=self._identity(user),
                expiration=self._now() + response['expires_in']
            )
            assert at_reauth.identity() == at_auth.identity()
            log.info('Reauthorized %s as %s', at_auth, at_reauth)
            self._update_access_token(user, at_reauth.token, response['expires_in'])
            self._register_access_token(at_reauth)
        return response

    def revoke_personal_access_tokens(self,
                                      uat_auth: UnverifiedAccessTokenAuthentication
                                      ) -> None:
        """
        Revoke all APATs for the user identified by the given access token.
        """
        log.info('Revoking all APATs for %s', uat_auth)
        at_auth = self.verify_access_token(uat_auth)
        user = self._load_user(at_auth.identity())
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
            .set_from(user, 'authorization_time')
            .set_from(user, 'min_jti', default=0)
            .set_from(user, 'max_jti', default=0)
            .to_update_item_input()
        )

    def _load_user(self, identity: str) -> User:
        """
        Retrieve previously stored information about the user of the given
        identity, as encoded by :meth:`_encode_identity`. A verified access
        token carries the identity of its user, and so does the ``sub`` claim of
        an APAT.
        """
        iss, sub = self._decode_identity(identity)
        log.info('Retrieving user %r from %r', sub, iss)
        response = self._dynamodb.get_item(
            TableName=self._table_name,
            Key={self.key_attribute: {'S': identity}}
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
            authorization_time=int(item.get('authorization_time', {'N': '0'})['N']),
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
