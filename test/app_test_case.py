from abc import (
    ABCMeta,
    abstractmethod,
)
import logging
from threading import Thread
import time

# noinspection PyPackageRequirements
from chalice.config import Config as ChaliceConfig
# noinspection PyPackageRequirements
from chalice.local import LocalDevServer
import requests

from azul.modules import load_app_module
from azul_test_case import AzulTestCase
from retorts import TestKeyManager

log = logging.getLogger(__name__)


class ChaliceServerThread(Thread):

    def __init__(self, app, config, host, port):
        super().__init__()
        self.server_wrapper = LocalDevServer(app, config, host, port)

    def run(self):
        self.server_wrapper.serve_forever()

    def kill_thread(self):
        self.server_wrapper.server.shutdown()
        self.server_wrapper.server.server_close()

    @property
    def address(self):
        return self.server_wrapper.server.server_address


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
        subdirectory of ${azul_home}/lambdas. Subclasses must override this to select which Chalice app to start
        locally.
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
        # Load the application module without modifying `sys.path` and without
        # adding it to `sys.modules`. This simplifies tear down and isolates the
        # app modules from different lambdas loaded by different concrete
        # subclasses. It does, however, violate this one invariant:
        # `sys.modules[module.__name__] == module`
        cls.app_module = load_app_module(cls.lambda_name(), unit_test=True)

    @classmethod
    def tearDownClass(cls):
        cls.app_module = None
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        self.server_thread = ChaliceServerThread(self.app_module.app, self.chalice_config(), 'localhost', 0)
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
        return requests.get(f"{self.base_url}/health/basic")

    def chalice_config(self):
        return ChaliceConfig()

    def tearDown(self):
        log.debug("Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')


class AuthLocalAppTestCase(LocalAppTestCase, metaclass=ABCMeta):

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        TestKeyManager.remove_test_keys()
