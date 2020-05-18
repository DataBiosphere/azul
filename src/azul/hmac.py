import logging

import chalice
import requests
from requests_http_signature import HTTPSignatureAuth

from azul import require
from azul.deployment import aws

logger = logging.getLogger(__name__)


def verify(current_request):
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

    def key_resolver(key_id, algorithm):
        require(algorithm == 'hmac-sha256', algorithm)
        key, _ = aws.get_hmac_key_and_id_cached(key_id)
        return key.encode()

    try:
        HTTPSignatureAuth.verify(requests.Request(method, endpoint, headers),
                                 key_resolver=key_resolver)
    except BaseException as e:
        logger.warning('Exception while validating HMAC: ', exc_info=e)
        raise chalice.UnauthorizedError('Invalid authorization credentials')


def prepare():
    key, key_id = aws.get_hmac_key_and_id()
    return HTTPSignatureAuth(key=key.encode(), key_id=key_id)
