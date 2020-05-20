from base64 import b64encode
from functools import lru_cache
import json
import os
from subprocess import call
from tempfile import gettempdir
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import jwt
from math import ceil
import responses

from azul import config


class ResponsesHelper:
    """
    Work around the lack of reentrance in the `responses` library. Both `moto`
    and `responses` suffer from an inherent design flaw: they use
    unittest.mock.patch which is global but their decorators and context
    managers pretend to be reentrant. It is essentially impossible to nest a
    RequestMock CM inside another one, or in a method decorated with
    @responses.activate or one of the moto decorators. Furthermore, one can't
    combine @responses.activate with any of the moto decorators. Use this
    method in tests as follows:

    >>> import moto
    >>> import requests
    >>> @moto.mock_sts
    ... def test_foo():
    ...     with ResponsesHelper() as helper:
    ...         helper.add(responses.Response(method=responses.GET,
    ...                                       url='http://foo.bar/blah',
    ...                                       body='Duh!'))
    ...         helper.add_passthru('http://localhost:12345/')
    ...         assert requests.get('http://foo.bar/blah').content == b'Duh!'
    >>> test_foo()

    >>> with ResponsesHelper() as helper: #doctest: +ELLIPSIS
    ...     pass
    Traceback (most recent call last):
    ...
    AssertionError: This helper only works with `responses` already active. ...

    In other words, whenever you would call the global responses.add() or
    responses.add_passthru() functions, call the helper's method of the same
    name instead. The helper will remove the mock response upon exit and
    restore the set pf pass-throughs to their original value, essentially
    undoiing all .add() and .add_passthru() invocations.

    Remember that you do not need @responses.activate if one of the moto
    decorators is present since they already activate responses.
    """

    def __init__(self, request_mock: responses.RequestsMock = None) -> None:
        super().__init__()
        self.request_mock = responses._default_mock if request_mock is None else request_mock
        self.mock_responses = None
        self.passthru_prefixes = None

    def add(self, mock_response: responses.BaseResponse):
        self.request_mock.add(mock_response)
        self.mock_responses.append(mock_response)

    def add_passthru(self, prefix):
        self.request_mock.add_passthru(prefix)

    def __enter__(self):
        patcher = getattr(self.request_mock, '_patcher', None)
        # noinspection PyUnresolvedReferences,PyProtectedMember
        assert patcher is not None and hasattr(patcher, 'is_local'), (
            'This helper only works with `responses` already active. The '
            'easiest way to achieve that is to use the `@responses.activate` '
            'decorator or one or more of the moto decorators.'
        )
        self.mock_responses = []
        self.passthru_prefixes = self.request_mock.passthru_prefixes
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for mock_response in self.mock_responses:
            self.request_mock.remove(mock_response)
        self.request_mock.passthru_prefixes = self.passthru_prefixes


class AuthResponseHelper(ResponsesHelper):

    def __init__(self, passthru_url: str = None, request_mock: responses.RequestsMock = None) -> None:
        super().__init__(request_mock)
        self.passthru_url = passthru_url

    def __enter__(self):
        context = super().__enter__()
        if self.passthru_url is not None:
            self.add_passthru(self.passthru_url)

        def encode_int(x):
            return b64encode(x.to_bytes(ceil(x.bit_length() / 8), 'big')).decode('utf-8')

        def generate_test_public_keys(request):
            public_key = TestKeyManager.get_public_key()
            public_numbers = public_key.public_numbers()
            public_exponent = public_numbers.e
            public_modulus = public_numbers.n
            response_body = {
                'kid': 'local_test',
                'e': encode_int(public_exponent),
                'n': encode_int(public_modulus)
            }
            return 200, {}, json.dumps(dict(keys=[response_body]))

        self.add(responses.Response(method=responses.GET,
                                    url=f'{config.access_token_issuer}/.well-known/openid-configuration',
                                    json={'jwks_uri': f'{config.access_token_issuer}/test/public-keys'}))

        self.add(responses.CallbackResponse(method=responses.GET,
                                            url=f'{config.access_token_issuer}/test/public-keys',
                                            callback=generate_test_public_keys,
                                            content_type='application/json'))
        return context

    @staticmethod
    def generate_test_claims(email: str,
                             identifier: str = None,
                             group: str = None,
                             ttl: int = 60,
                             issued_at: float = None):
        issued_at = issued_at or time.time()
        return {
            "aud": config.access_token_audience_list,
            "azp": "fake-authorizer",
            "exp": int(issued_at + ttl),
            "https://auth.data.humancellatlas.org/email": email,
            "https://auth.data.humancellatlas.org/group": group or 'public',
            "iat": int(issued_at),
            "iss": config.access_token_issuer,
            "scope": "openid email",
            "sub": f"fake|{identifier or email}"
        }

    @staticmethod
    def generate_test_jwt(email: str,
                          identifier: str = None,
                          group: str = None,
                          ttl: int = 60,
                          issued_at: float = None):
        return jwt.encode(AuthResponseHelper.generate_test_claims(email, identifier, group, ttl, issued_at),
                          key=TestKeyManager.get_private_key(),
                          algorithm='RS256',
                          headers={'kid': 'local_test'}).decode('utf-8')


class TestKeyManager:

    @staticmethod
    @lru_cache(1)
    def public_key_path():
        return os.path.abspath(os.path.join(gettempdir(), 'public.pem'))

    @staticmethod
    @lru_cache(1)
    def private_key_path():
        return os.path.abspath(os.path.join(gettempdir(), 'private.pem'))

    @staticmethod
    def generate_test_keys():
        if not os.path.exists(TestKeyManager.private_key_path()):
            # Generate a test private key.
            call(['openssl', 'genrsa', '-out', TestKeyManager.private_key_path(), '2048'])
            # Generate a test public key.
            call(['openssl', 'rsa', '-in', TestKeyManager.private_key_path(), '-outform', 'PEM', '-pubout', '-out',
                  TestKeyManager.public_key_path()])

    @staticmethod
    def get_public_key():
        if not os.path.exists(TestKeyManager.public_key_path()):
            TestKeyManager.generate_test_keys()
        with open(TestKeyManager.public_key_path(), "rb") as key_file:
            public_key = serialization.load_pem_public_key(
                key_file.read(),
                backend=default_backend()
            )

        return public_key

    @staticmethod
    def get_private_key():
        if not os.path.exists(TestKeyManager.private_key_path()):
            TestKeyManager.generate_test_keys()
        with open(TestKeyManager.private_key_path(), "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        return private_key

    @staticmethod
    def remove_test_keys():
        if os.path.exists(TestKeyManager.public_key_path()):
            os.unlink(TestKeyManager.public_key_path())
        if os.path.exists(TestKeyManager.private_key_path()):
            os.unlink(TestKeyManager.private_key_path())
