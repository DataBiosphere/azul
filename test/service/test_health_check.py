import unittest

from moto import (
    mock_sqs,
    mock_sts,
)

from azul.logging import (
    configure_test_logging,
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


class TestServiceHealthCheck(DCP1TestCase, HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _expected_health(self,
                         endpoints_up: bool = True,
                         es_up: bool = True
                         ):
        return {
            'up': False,
            **self._expected_elasticsearch(es_up),
            **self._expected_api_endpoints(endpoints_up),
        }

    @mock_sts
    @mock_sqs
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        response = self._test(endpoints_up=False)
        self.assertEqual(503, response.status_code)
        self.assertEqual(self._expected_health(endpoints_up=False), response.json())


del HealthCheckTestCase

if __name__ == '__main__':
    unittest.main()
