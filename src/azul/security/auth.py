# See https://allspark.dev.data.humancellatlas.org/dcp-ops/docs/wikis/Security/Authentication%20and%20Authorization/Setting%20up%20DCP%20Auth
from logging import getLogger
from typing import Dict, Any
from urllib.parse import urlencode

from azul import config
from azul.security.verifier import (verify,
                                    InvalidTokenError,
                                    NonDecodableTokenError)

logger = getLogger(__name__)

fusillade_callback_url = f'{config.service_endpoint()}/auth/callback'


def get_fusillade_url(request_path):
    return f'https://{config.fusillade_hostname}/{request_path}'


def get_fusillade_login_url() -> str:
    # According to the documentation, client_id is the service's domain name (azul.config.domain_name). However,
    # specifying client_id will cause an Auth0 misconfiguration error. This code intentionally excludes client_id from
    # the request to Fusillade.
    query = dict(response_type="code",
                 scope="openid email",
                 redirect_uri=fusillade_callback_url,
                 state='')
    return '?'.join([get_fusillade_url('authorize'),
                     urlencode(query)])


def is_client_authenticated(request_headers: Dict[str, str]) -> bool:
    """
    Check if the client is authenticated.

    :param request_headers: the dictionary of request headers
    """
    try:
        get_access_token(request_headers)
    except AuthenticationError as e:
        logger.warning(f'{type(e).__name__}: {e} (given: {dict(request_headers)})')
        return False

    return True


def get_access_token(request_headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Get the access token (JWT) from the request headers.

    :param request_headers: the dictionary of request headers
    """
    try:
        assert 'authorization' in request_headers, "missing_authorization_header"
        authorization_token = request_headers['authorization']
    except AssertionError as e:
        raise AuthenticationError(e.args[0])

    return authenticate(authorization_token)


def authenticate(authorization_token:str) -> Dict[str, Any]:
    bearer_token_prefix = "Bearer "
    if not authorization_token.startswith(bearer_token_prefix):
        raise AuthenticationError("not_bearer_token")
    access_token = authorization_token[len(bearer_token_prefix):]
    if not access_token:
        raise AuthenticationError("missing_bearer_token")

    try:
        return verify(access_token)
    except NonDecodableTokenError:
        logger.warning('Detected a broken token')
        raise AuthenticationError('non_decodable_token')
    except InvalidTokenError:
        logger.info('Detected the use of an invalid token')
        raise AuthenticationError('invalid_token')


class AuthenticationError(RuntimeError):
    pass
