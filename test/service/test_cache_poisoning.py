from abc import (
    ABCMeta,
)
from unittest.mock import (
    patch,
)

import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul.logging import (
    configure_test_logging,
)
from azul.terra import (
    TDRClient,
)
from azul_test_case import (
    AnvilTestCase,
    DCP1TestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class CachePoisoningTestCase(LocalAppTestCase, metaclass=ABCMeta):
    snapshot_mock = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.snapshot_mock = patch.object(TDRClient,
                                         'snapshot_names_by_id',
                                         return_value={})
        cls.snapshot_mock.start()

    @classmethod
    def tearDownClass(cls):
        cls.snapshot_mock.stop()
        cls.snapshot_mock = None
        super().tearDownClass()

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _test(self):
        url = self.base_url.set(path='/repository/sources')
        response = requests.get(str(url))
        response.raise_for_status()


# Note that the test cases are named intentionally to force the order in which
# they are run. The AnVIL test case needs to be run first.

class TestCachePoisoning1(CachePoisoningTestCase, AnvilTestCase):
    """
    This test case attempts to poison the class-level cache on
    SourceService._repository_plugin with the TDR AnVIL repository plugin.
    """

    def test(self):
        self._test()


class TestCachePoisoning2(CachePoisoningTestCase, DCP1TestCase):
    """
    This test uses the same default catalog name as the test case above but it
    should be using the DSS repository plugin, the inherited default, instead.
    The source cache is patched to produce a miss and so the DSS plugin should
    be asked to list the sources. If the class-level cache on
    SourceService._repository_plugin is still poisoned, the plugin from the test
    above will be asked instead. That plugin needs service account credentials
    and so it will attempt to load them from AWS Secrets Manager which is not
    mocked, and would hit our counter measures against AWS API requests leaking
    out of unit tests (intentionally invalid credentials).
    """

    def test(self):
        self._test()
