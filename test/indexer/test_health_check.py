import logging
import os
from moto import mock_sqs, mock_sts
import requests
import unittest
from unittest import mock

from health_check_test_case import HealthCheckTestCase
from retorts import ResponsesHelper


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHealthCheck(HealthCheckTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'indexer'

    @mock_sts
    @mock_sqs
    def test_progress(self):
        self._create_mock_queues()
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
                response = requests.get(self.base_url + '/progress')
                health_object = response.json()
                self.assertEqual(200, response.status_code)
                self.assertEqual({
                    'unindexed_bundles': 0,
                    'unindexed_documents': 0
                }, health_object)


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
