import hashlib
import logging

import chalice.app
from http_message_signatures import (  # type:ignore[attr-defined]
    HTTPMessageSigner,
    HTTPMessageVerifier,
    HTTPSignatureKeyResolver,
)
from http_message_signatures.algorithms import (  # type:ignore[attr-defined]
    HMAC_SHA256,
)
import http_sfv
from more_itertools import (
    one,
)
import requests
import requests.sessions
import requests.structures

from azul.auth import (
    HMACAuthentication,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    cached_property,
)

log = logging.getLogger(__name__)


class SignatureHelper(HTTPSignatureKeyResolver):
    """
    Client-side signing of HTTP requests and server-side checking of the
    resulting signatures. On the  client, requests are represented as instances
    of requests.Request. On the server, chalice.Request is used. Internally
    though, the latter is converted back to the former.

    This class should work as both a mix-in, and stand-alone.
    """

    @cached_property
    def verifier(self):
        return HTTPMessageVerifier(signature_algorithm=HMAC_SHA256,
                                   key_resolver=self)

    @cached_property
    def signer(self):
        return HTTPMessageSigner(signature_algorithm=HMAC_SHA256,
                                 key_resolver=self)

    def auth_from_request(self,
                          request: chalice.app.Request
                          ) -> HMACAuthentication | None:
        try:
            request.headers['signature']
        except KeyError:
            return None
        else:
            key_id = self.verify(request)
            return HMACAuthentication(key_id)

    def resolve_public_key(self, key_id: str) -> bytes:
        return self.resolve_private_key(key_id)

    def resolve_private_key(self, key_id: str) -> bytes:
        key, actual_key_id = aws.get_hmac_key_and_id()
        assert actual_key_id == key_id
        return key

    def verify(self, current_request: chalice.app.Request) -> str:
        try:
            base_url = current_request.headers['host']
            path = current_request.context['path']
            endpoint = f'http://{base_url}{path}'
            method = current_request.context['httpMethod']
            headers = current_request.headers
            request = requests.models.Request(method=method,
                                              url=endpoint,
                                              headers=headers,
                                              data=current_request.raw_body)
            request = request.prepare()
            result = one(self.verifier.verify(request))
        except BaseException as e:
            log.warning('Exception while validating HMAC: ', exc_info=e)
            raise chalice.app.UnauthorizedError('Invalid authorization credentials')
        else:
            return result.parameters

    def sign_and_send(self,
                      request: requests.models.Request
                      ) -> requests.models.Response:
        request = request.prepare()
        self.sign(request)
        with requests.sessions.Session() as session:
            response = session.send(request)
        return response

    def sign(self, request: requests.models.PreparedRequest):
        body = request.body
        assert body is not None
        digest = hashlib.sha256(body).digest()
        assert isinstance(request.headers, requests.structures.CaseInsensitiveDict)
        header = http_sfv.dictionary.Dictionary({'sha-256': digest})
        request.headers['Content-Digest'] = str(header)
        key, key_id = aws.get_hmac_key_and_id()
        self.signer.sign(request,
                         key_id=key_id,
                         covered_component_ids=('@method', '@path', 'content-digest'),
                         use_existing_digest=True)
