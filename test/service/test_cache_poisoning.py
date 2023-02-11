import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    CatalogName,
    config,
)
from azul.logging import (
    configure_test_logging,
)
from indexer.test_anvil import (
    AnvilIndexerTestCase,
)
from service import (
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class CachePoisoningTestCase(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _test(self):
        url = self.base_url.set(path='/repository/sources')
        response = requests.get(str(url))
        response.raise_for_status()


# Note that the test cases are named intentionally to force the order in which
# they are run. The AnVIL test case needs to be run first.

class TestCachePoisoning1(CachePoisoningTestCase):
    """
    This test case attempts to poison the class-level cache on
    SourceService._repository_plugin with the TDR AnVIL repository plugin.
    """

    source = AnvilIndexerTestCase.source

    @classmethod
    def catalog_config(cls) -> dict[CatalogName, config.Catalog]:
        return {
            cls.catalog: config.Catalog(name=cls.catalog,
                                        atlas='anvil',
                                        internal=False,
                                        plugins=dict(metadata=config.Catalog.Plugin(name='anvil'),
                                                     repository=config.Catalog.Plugin(name='tdr_anvil')),
                                        sources={str(cls.source.spec)})
        }

    @patch_dss_source
    @patch_source_cache([source.to_json()])
    def test(self):
        self._test()


class TestCachePoisoning2(CachePoisoningTestCase):
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

    @patch_source_cache
    @patch_dss_source
    def test(self):
        self._test()
