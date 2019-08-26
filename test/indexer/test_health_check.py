import os
import unittest
from unittest import mock

from moto import mock_sts, mock_sqs

from azul import config
from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase


def setUpModule():
    configure_test_logging()


class TestIndexerHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'indexer'

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        mock_endpoint = ('nonexisting-index.com', 80)
        endpoint_states = self._make_endpoint_states(self.endpoints)
        with mock.patch.dict(os.environ, **config.es_endpoint_env(mock_endpoint)):
            response = self._test(endpoint_states, lambdas_up=True)
            health_object = response.json()
            self.assertEqual(503, response.status_code)
            documents_ = {
                'up': False,
                **self._expected_elasticsearch(False),
                **self._expected_queues(True),
                **self._expected_progress()
            }
            self.assertEqual(documents_, health_object)

    @mock_sts
    @mock_sqs
    def test_queues_down(self):
        endpoint_states = self._make_endpoint_states(self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            **self._expected_elasticsearch(True),
            **self._expected_queues(False),
            **self._expected_progress()
        }, health_object)


del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
