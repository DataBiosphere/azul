from typing import Mapping
import unittest

from moto import (
    mock_sqs,
    mock_sts,
)

from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestServiceHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _expected_health(self, endpoint_states: Mapping[str, bool], es_up: bool = True):
        return {
            'up': False,
            **self._expected_elasticsearch(es_up),
            **self._expected_api_endpoints(endpoint_states),
        }

    @mock_sts
    @mock_sqs
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        endpoint_states = self._make_endpoint_states([], down_endpoints=self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual(self._expected_health(endpoint_states), health_object)

    @mock_sts
    @mock_sqs
    def test_one_api_endpoint_down(self):
        self._create_mock_queues()
        endpoint_states = self._make_endpoint_states(self.endpoints[1:], down_endpoints=self.endpoints[:1])
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual(self._expected_health(endpoint_states), health_object)


del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
