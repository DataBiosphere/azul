from moto import (
    mock_aws,
)

from azul.lib.types import (
    MutableJSON,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul_test_case import (
    DCP1TestCase,
)
from health_check_test_case import (
    HealthCheckTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class TestServiceHealthCheck(DCP1TestCase, HealthCheckTestCase):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    def _expected_health(self,
                         endpoints_up: bool = True,
                         opensearch_up: bool = True
                         ) -> MutableJSON:
        return {
            'up': opensearch_up and endpoints_up,
            **self._expected_opensearch(up=opensearch_up),
            **self._expected_api_endpoints(up=endpoints_up),
        }

    @mock_aws
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        with self._mock(endpoints_up=False):
            response = self._test('/health/fast')
        self.assertEqual(503, response.status)
        self.assertEqual(self._expected_health(endpoints_up=False), response.json())


del HealthCheckTestCase
