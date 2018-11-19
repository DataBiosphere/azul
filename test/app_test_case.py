from abc import abstractmethod, ABCMeta
from base64 import b64encode
import json
import logging
import math
import os
from subprocess import call
from threading import Thread
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from jwt import encode as jwt_encode
from chalice import Response

# noinspection PyPackageRequirements
from chalice.config import Config as ChaliceConfig
# noinspection PyPackageRequirements
from chalice.local import LocalDevServer
import requests

from azul import config
from azul.modules import load_module
from azul_test_case import AzulTestCase

log = logging.getLogger(__name__)
key_dir = os.path.dirname(__file__)
public_key_path = os.path.abspath(os.path.join(key_dir, 'public.pem'))
private_key_path = os.path.abspath(os.path.join(key_dir, 'private.pem'))


def generate_test_keys():
    if not os.path.exists(private_key_path):
        call(['openssl', 'genrsa', '-out', private_key_path, '2048'])
        call(['openssl', 'rsa', '-in', private_key_path, '-outform', 'PEM', '-pubout', '-out', public_key_path])


def get_test_keys():
    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    with open(public_key_path, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )

    return private_key, public_key


def remove_test_keys():
    os.unlink(public_key_path)
    os.unlink(private_key_path)


class ChaliceServerThread(Thread):
    def __init__(self, app, config, host, port):
        super().__init__()
        self.server_wrapper = LocalDevServer(app, config, host, port)

    def run(self):
        self.__add_test_routes()
        self.server_wrapper.serve_forever()

    def kill_thread(self):
        self.server_wrapper.server.shutdown()
        self.server_wrapper.server.server_close()
        self.__remove_test_routes()

    @property
    def address(self):
        return self.server_wrapper.server.server_address

    def __add_test_routes(self):
        app = self.server_wrapper.app_object
        encode_int = lambda x: b64encode(x.to_bytes(math.ceil(x.bit_length()/8), 'big')).decode('utf-8')
        generate_json_response = lambda body: Response(json.dumps(body, indent=4, sort_keys=True),
                                                       headers={'Content-Type': 'application/json'},
                                                       status_code=200)

        private_key, public_key = get_test_keys()

        @app.route('/.well-known/openid-configuration')
        def generate_test_openid_config():
            jwks_uri = f'http://{self.address[0]}:{self.address[1]}/test/public-keys'
            return generate_json_response({'jwks_uri': jwks_uri})

        @app.route('/test/public-keys')
        def generate_test_public_keys():
            public_numbers = public_key.public_numbers()
            public_exponent = public_numbers.e
            public_modulus = public_numbers.n
            response_body = {
                'kid': 'local_test',
                'e': encode_int(public_exponent),
                'n': encode_int(public_modulus)
            }
            return generate_json_response(dict(keys=[response_body]))

        @app.route('/test/token', methods=['POST'])
        def generate_test_token():
            data = app.current_request.json_body
            issuer = f'http://{self.address[0]}:{self.address[1]}/'
            claims = {
                "aud": config.access_token_audience_list,
                "azp": "fake-authorizer",
                "exp": int(time.time() + data.get('ttl', 60)),
                "https://auth.data.humancellatlas.org/email": data['email'],
                "https://auth.data.humancellatlas.org/group": data.get('group', 'public'),
                "iat": int(time.time()),
                "iss": issuer,
                "scope": "openid email",
                "sub": f"fake|{data.get('id') or data['email']}"
            }
            return generate_json_response({
                'jwt': jwt_encode(claims, key=private_key, algorithm='RS256', headers={'kid': 'local_test'}).decode('utf-8'),
                'claims': claims
            })

    def __remove_test_routes(self):
        app = self.server_wrapper.app_object
        del app.routes['/.well-known/openid-configuration']
        del app.routes['/test/public-keys']
        del app.routes['/test/token']


class LocalAppTestCase(AzulTestCase, metaclass=ABCMeta):
    """
    A mixin for test cases against a locally running instance of a AWS Lambda Function aka Chalice application. By
    default, the local instance will use the remote AWS Elasticsearch domain configured via AZUL_ES_DOMAIN or
    AZUL_ES_ENDPOINT. To use a locally running ES instance, combine this mixin with ElasticsearchTestCase. Be sure to
    list ElasticsearchTestCase first such that this mixin picks up the environment overrides made by
    ElasticsearchTestCase.
    """

    @classmethod
    @abstractmethod
    def lambda_name(cls) -> str:
        """
        Return the name of the AWS Lambda function aka. Chalice app to start locally. Must match the name of a
        subdirectory of ${azul_home}/lambdas. Subclasses must override this to select which Chalice app to start locally.
        """
        raise NotImplementedError()

    @property
    def base_url(self):
        """
        The HTTP endpoint of the locally running Chalice application. Subclasses should use this to derive the URLs
        for the test requests that they issue.
        """
        host, port = self.server_thread.address
        return f"http://{host}:{port}"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        generate_test_keys()
        cls._path_to_app = os.path.join(config.project_root, 'lambdas', cls.lambda_name())
        sys.path.append(cls._path_to_app)

        # Load the application module without modifying `sys.path` and without adding it to `sys.modules`. This
        # simplifies tear down and isolates the app modules from different lambdas loaded by different concrete
        # subclasses. It does, however, violate this one invariant: `sys.modules[module.__name__] == module`
        path = os.path.join(config.project_root, 'lambdas', cls.lambda_name(), 'app.py')
        cls.app_module = load_module(path, '__main__')

    @classmethod
    def tearDownClass(cls):
        cls.app_module = None
        sys.path.remove(cls._path_to_app)
        remove_test_keys()
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        log.debug("Setting up tests")
        log.debug("Created Thread")
        self.server_thread = ChaliceServerThread(self.app_module.app, self.chalice_config(), 'localhost', 0)
        log.debug("Started Thread")
        self.server_thread.start()
        deadline = time.time() + 10
        while True:
            try:
                response = self._ping()
                response.raise_for_status()
            except Exception:
                if time.time() > deadline:
                    raise
                log.debug("Unable to connect to server", exc_info=True)
                time.sleep(1)
            else:
                break

    def _ping(self):
        return requests.get(self.base_url)

    def chalice_config(self):
        return ChaliceConfig()

    def tearDown(self):
        log.debug("Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')
