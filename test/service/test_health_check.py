import os
import unittest
from unittest import mock

from moto import (
    mock_sts,
    mock_sqs,
)

from azul import config
from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestServiceHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @mock_sts
    @mock_sqs
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        endpoint_states = self._make_endpoint_states([], down_endpoints=self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            **self._expected_elasticsearch(True),
            **self._expected_api_endpoints(endpoint_states),
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_one_api_endpoint_down(self):
        self._create_mock_queues()
        endpoint_states = self._make_endpoint_states(self.endpoints[1:], down_endpoints=self.endpoints[:1])
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            **self._expected_elasticsearch(True),
            **self._expected_api_endpoints(endpoint_states),
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        mock_endpoint = ('nonexisting-index.com', 80)
        endpoint_states = self._make_endpoint_states(self.endpoints)
        with mock.patch.dict(os.environ, **config.es_endpoint_env(es_endpoint=mock_endpoint,
                                                                  es_instance_count=1)):
            response = self._test(endpoint_states, lambdas_up=True)
            health_object = response.json()
            self.assertEqual(503, response.status_code)
            documents_ = {
                'up': False,
                **self._expected_elasticsearch(False),
                **self._expected_api_endpoints(endpoint_states),
            }
            self.assertEqual(documents_, health_object)


del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
