import chalice
import requests
from requests_http_signature import HTTPSignatureAuth

from azul import config


def verify(current_request):
    try:
        current_request.headers['authorization']
    except KeyError:
        chalice.UnauthorizedError('Not Authorized')

    base_url = current_request.headers['host']
    path = current_request.context['resourcePath']
    endpoint = f'{base_url}{path}'
    method = current_request.context['httpMethod']
    headers = current_request.headers
    hmac_secret_key = config.hmac_key.encode()
    try:
        HTTPSignatureAuth.verify(requests.Request(method, endpoint, headers),
                                 key_resolver=lambda key_id, algorithm: hmac_secret_key)
    except BaseException:
        raise chalice.UnauthorizedError('Invalid authorization credentials')


def prepare():
    return HTTPSignatureAuth(key=config.hmac_key.encode(), key_id=config.hmac_key_id)
