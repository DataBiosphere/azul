# The code is based on dss.util.security.
from base64 import urlsafe_b64decode
import logging
from urllib.parse import urlparse

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import jwt
from jwt.exceptions import DecodeError, PyJWTError
import requests

from azul import config

logger = logging.getLogger(__name__)

allowed_algorithms = ('RS256',)
gserviceaccount_domain = "iam.gserviceaccount.com"

# Shared HTTP session
session = requests.Session()


def verify(token: str):
    try:
        unverified_token = jwt.decode(token, verify=False)
    except DecodeError:
        logger.warning(f"Failed to decode JWT without verification: {token}", exc_info=True)
        raise NonDecodableTokenError(token)

    try:
        issuer = unverified_token['iss']
    except KeyError:
        raise InvalidTokenError(token)

    if not is_valid_issuer(issuer):
        logger.warning(f"Detected a JWT with UNKNOWN ISSUER. ({issuer})", exc_info=True)
        raise InvalidTokenError(token)

    public_keys = get_public_keys(issuer)
    token_header = jwt.get_unverified_header(token)

    try:
        public_key_id = token_header["kid"]
    except KeyError:
        raise InvalidTokenError(token)

    public_key = public_keys[public_key_id]
    verification_options = dict(key=public_key,
                                issuer=issuer,
                                audience=config.access_token_audience_list,
                                algorithms=allowed_algorithms)

    try:
        return jwt.decode(token, **verification_options)
    except PyJWTError:
        logger.warning(f"Detected a JWT with INVALID SIGNATURE.", exc_info=True)
        raise InvalidTokenError(token)


def is_valid_issuer(issuer):
    if issuer.startswith('http://127.0.0.1:'):
        return True

    given_issuer = urlparse(issuer)
    expected_issuer = urlparse(config.access_token_issuer)

    logger.info('Expected ISSUER: %s', expected_issuer)
    logger.info('Given ISSUER: %s', given_issuer)

    return given_issuer.scheme == expected_issuer.scheme and given_issuer.netloc == expected_issuer.netloc


def get_jwks_uri(openid_provider):
    if openid_provider.endswith(gserviceaccount_domain):
        return f"https://www.googleapis.com/service_accounts/v1/jwk/{openid_provider}"
    else:
        return get_openid_config(openid_provider)["jwks_uri"]


def get_public_keys(openid_provider:str):
    keys = session.get(get_jwks_uri(openid_provider)).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=convert_base64_string_to_int(key["e"]),
            n=convert_base64_string_to_int(key["n"])
        ).public_key(backend=default_backend())
        for key in keys
    }


def get_openid_config(openid_provider:str):
    base_url = openid_provider
    if not base_url.endswith('/'):
        base_url += '/'
    res = session.get(f"{base_url}.well-known/openid-configuration")
    res.raise_for_status()
    return res.json()


def convert_base64_string_to_int(value:str) -> int:
    padding_length = 4 - (len(value) % 4)
    padding_characters = '=' * padding_length
    return int.from_bytes(urlsafe_b64decode(f'{value}{padding_characters}'), byteorder="big")


class InvalidTokenError(ValueError):
    pass


class NonDecodableTokenError(ValueError):
    pass
