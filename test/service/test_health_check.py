import json
from typing import Mapping
import unittest

import boto3
from moto import (
    mock_sqs,
    mock_sts,
    mock_dynamodb2,
)
import requests

from azul import config
from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase
from health_failures_test_case import TestHealthFailures
from retorts import ResponsesHelper


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestServiceHealthCheck(HealthCheckTestCase):
    # Moto's dynamodb backend doesn't support government regions.
    _aws_test_region = 'ap-south-1'

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
        endpoint_states = self._endpoint_states(up_endpoints=(),
                                                down_endpoints=self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        self.assertEqual(503, response.status_code)
        self.assertEqual(self._expected_health(endpoint_states), response.json())

    @mock_sts
    @mock_sqs
    def test_one_api_endpoint_down(self):
        self._create_mock_queues()
        endpoint_states = self._endpoint_states(up_endpoints=self.endpoints[1:],
                                                down_endpoints=self.endpoints[:1])
        response = self._test(endpoint_states, lambdas_up=True)
        self.assertEqual(503, response.status_code)
        self.assertEqual(self._expected_health(endpoint_states), response.json())

    @mock_sts
    @mock_dynamodb2
    def test_failures_endpoint(self):
        """
        Test that the failures endpoint returns the expected response for
        messages in the DynamoDB failure message table.
        """
        dynamodb = boto3.resource('dynamodb')
        for bundle_notification_count, reindex_notification_count in ((3, 0), (0, 3), (3, 3), (0, 0)):
            table = dynamodb.create_table(**TestHealthFailures.dynamo_failures_table_settings)
            failed_bundles = [
                (f'{i}aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', f'2019-10-1{i}T113344.698028Z')
                for i in range(bundle_notification_count)
            ]
            bundle_notifications = [
                {
                    'action': 'add',
                    'notification': self._fake_notification(bundle_fqid)
                } for bundle_fqid in failed_bundles
            ]
            reindex_notifications = [
                {
                    'action': 'reindex',
                    'dss_url': config.dss_endpoint,
                    'prefix': "{:02x}".format(i),
                } for i in range(reindex_notification_count)
            ]
            expected_response = {
                'up': True,
                'failed_bundle_notifications': bundle_notifications,
                'failed_reindex_notifications': len(reindex_notifications)
            }
            test_notifications = bundle_notifications + reindex_notifications
            with table.batch_writer() as writer:
                for i, notification in enumerate(test_notifications):
                    item = {
                        'MessageType': 'bundle' if 'notification' in notification.keys() else 'reindex',
                        'SentTimeMessageId': f'{i}-{i}',
                        'Body': json.dumps(notification)
                    }
                    writer.put_item(Item=item)
            with self.subTest(bundle_notification_count=bundle_notification_count,
                              reindex_notification_count=reindex_notification_count):
                with ResponsesHelper() as helper:
                    helper.add_passthru(self.base_url)
                    response = requests.get(self.base_url + '/health/failures')
                    self.assertEqual(200, response.status_code)
                    self.assertEqual(expected_response, response.json())
            table.delete()


del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
