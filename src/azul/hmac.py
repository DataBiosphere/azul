import logging
from typing import (
    Optional,
)

import chalice
import requests
from requests_http_signature import (
    HTTPSignatureAuth,
)

from azul import (
    require,
)
from azul.auth import (
    HMACAuthentication,
)
from azul.deployment import (
    aws,
)

logger = logging.getLogger(__name__)


def auth_from_request(request: chalice.app.Request
                      ) -> Optional[HMACAuthentication]:
    try:
        header = request.headers['Authorization']
    except KeyError:
        return None
    else:
        prefix = 'Signature '
        if header.startswith(prefix):
            key_id = verify(request)
            return HMACAuthentication(key_id)
        else:
            raise chalice.UnauthorizedError(header)


def verify(current_request: chalice.app.Request) -> str:
    try:
        current_request.headers['authorization']
    except KeyError as e:
        logger.warning('Missing authorization header: ', exc_info=e)
        chalice.UnauthorizedError('Not Authorized')

    base_url = current_request.headers['host']
    path = current_request.context['resourcePath']
    endpoint = f'{base_url}{path}'
    method = current_request.context['httpMethod']
    headers = current_request.headers
    _key_id: Optional[str] = None

    def key_resolver(*, key_id, algorithm):
        require(algorithm == 'hmac-sha256', algorithm)
        key, _ = aws.get_hmac_key_and_id_cached(key_id)
        key = key.encode()
        # Since HTTPSignatureAuth.verify doesn't return anything we need to
        # extract the key ID in this round-about way.
        nonlocal _key_id
        _key_id = key_id
        return key

    try:
        HTTPSignatureAuth.verify(requests.Request(method, endpoint, headers),
                                 key_resolver=key_resolver)
    except BaseException as e:
        logger.warning('Exception while validating HMAC: ', exc_info=e)
        raise chalice.UnauthorizedError('Invalid authorization credentials')
    else:
        assert _key_id is not None
        return _key_id


def prepare():
    key, key_id = aws.get_hmac_key_and_id()
    return HTTPSignatureAuth(key=key.encode(), key_id=key_id)
