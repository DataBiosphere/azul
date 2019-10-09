from base64 import urlsafe_b64decode
import logging
from re import compile
from typing import (
    Dict,
    Any,
)
from urllib.parse import (
    urlencode,
    urlparse,
)

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import jwt
from jwt.exceptions import (
    DecodeError,
    PyJWTError,
)
from requests import Session

from azul import config

logger = logging.getLogger(__name__)


class Authenticator:
    valid_redirect_uri_pattern = compile(r'^(http://(127.0.0.1|localhost):\d+|https://[^/]+\.humancellatlas.org)/')

    def __init__(self):
        self.session = Session()

    @staticmethod
    def get_fusillade_url(request_path):
        return f'{config.fusillade_endpoint}/{request_path}'

    @staticmethod
    def get_fusillade_login_url(redirect_uri: str = None) -> str:
        """
        Get the login URL.

        :param redirect_uri: The URI that Fusillade will redirect back to

        According to the documentation, client_id is the service's domain name
        (azul.config.domain_name). However, specifying client_id will cause an
        Auth0 misconfiguration error. This method intentionally excludes
        ``client_id`` from the request to Fusillade.
        """
        if redirect_uri:
            if not Authenticator.valid_redirect_uri_pattern.search(redirect_uri):
                raise InvalidRedirectUriError(redirect_uri)
        else:
            redirect_uri = config.service_endpoint() + '/auth/callback'
        query = dict(response_type="code",
                     scope="openid email",
                     redirect_uri=redirect_uri,
                     state='')
        return '?'.join([Authenticator.get_fusillade_url('authorize'),
                         urlencode(query)])

    def is_client_authenticated(self, request_headers: Dict[str, str]) -> bool:
        """
        Check if the client is authenticated.

        :param request_headers: the dictionary of request headers
        """
        try:
            self.get_access_token(request_headers)
        except AuthenticationError as e:
            logger.warning(f'{type(e).__name__}: {e} (given: {dict(request_headers)})')
            return False

        return True

    def get_access_token(self, request_headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Get the access token (JWT) from the request headers.

        :param request_headers: the dictionary of request headers
        """
        try:
            assert 'authorization' in request_headers, "missing_authorization_header"
            authorization_token = request_headers['authorization']
        except AssertionError as e:
            raise AuthenticationError(e.args[0])

        return self.authenticate_bearer_token(authorization_token)

    def authenticate_bearer_token(self, authorization_token: str) -> Dict[str, Any]:
        """
        Authenticate the bearer token
        """
        bearer_token_prefix = "Bearer "
        if not authorization_token.startswith(bearer_token_prefix):
            raise AuthenticationError("not_bearer_token")
        access_token = authorization_token[len(bearer_token_prefix):]
        if not access_token:
            raise AuthenticationError("missing_bearer_token")

        try:
            return self.verify_jwt(access_token)
        except NonDecodableTokenError:
            logger.warning('Detected a broken token')
            raise AuthenticationError('non_decodable_token')
        except InvalidTokenError:
            logger.info('Detected the use of an invalid token')
            raise AuthenticationError('invalid_token')

    def verify_jwt(self, token: str):
        """
        Verify a JWT string

        :param token: a JWT string
        :return: the decoded claims of the given JWT string if valid
        :rtype: dict
        :raises NonDecodableTokenError: if the token is not decodable at all (without verification)
        :raises InvalidTokenError: if the required data (headers or claims) is
                                   missing, the issuer is not recognized, or
                                   the token is invalid (after verification).
        """
        try:
            unverified_token = jwt.decode(token, verify=False)
        except DecodeError:
            logger.warning(f"Failed to decode JWT without verification: {token}", exc_info=True)
            raise NonDecodableTokenError(token)

        try:
            issuer = unverified_token['iss']
        except KeyError:
            raise InvalidTokenError(token)

        if not self.is_valid_issuer(issuer):
            logger.warning(f"Detected a JWT with UNKNOWN ISSUER. ({issuer})", exc_info=True)
            raise InvalidTokenError(token)

        public_keys = self.get_public_keys(issuer)
        token_header = jwt.get_unverified_header(token)

        try:
            public_key_id = token_header["kid"]
        except KeyError:
            raise InvalidTokenError(token)

        public_key = public_keys[public_key_id]
        verification_options = dict(key=public_key,
                                    issuer=issuer,
                                    audience=config.access_token_audience_list,
                                    algorithms=('RS256',))

        try:
            return jwt.decode(token, **verification_options)
        except PyJWTError:
            logger.warning(f"Detected a JWT with INVALID SIGNATURE.", exc_info=True)
            raise InvalidTokenError(token)

    @staticmethod
    def is_valid_issuer(issuer: str):
        given_issuer = urlparse(issuer)
        expected_issuer = urlparse(config.access_token_issuer)
        return given_issuer.scheme == expected_issuer.scheme and given_issuer.netloc == expected_issuer.netloc

    def get_public_keys(self, openid_provider: str):
        keys = self.session.get(self.get_jwks_uri(openid_provider)).json()["keys"]
        return {
            key["kid"]: rsa.RSAPublicNumbers(
                e=self.convert_base64_string_to_int(key["e"]),
                n=self.convert_base64_string_to_int(key["n"])
            ).public_key(backend=default_backend())
            for key in keys
        }

    def get_openid_config(self, openid_provider: str):
        base_url = openid_provider
        if not base_url.endswith('/'):
            base_url += '/'
        res = self.session.get(f"{base_url}.well-known/openid-configuration")
        res.raise_for_status()
        return res.json()

    @staticmethod
    def convert_base64_string_to_int(value: str) -> int:
        padding_length = 4 - (len(value) % 4)
        padding_characters = '=' * padding_length
        return int.from_bytes(urlsafe_b64decode(f'{value}{padding_characters}'), byteorder="big")

    def get_jwks_uri(self, openid_provider: str):
        if openid_provider.endswith("iam.gserviceaccount.com"):
            return f"https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}"
        else:
            return self.get_openid_config(openid_provider)["jwks_uri"]


class AuthenticationError(RuntimeError):
    pass


class InvalidTokenError(ValueError):
    pass


class NonDecodableTokenError(ValueError):
    pass


class InvalidRedirectUriError(AssertionError):
    pass
