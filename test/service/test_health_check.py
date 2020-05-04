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

from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase
from health_failures_test_case import TestHealthFailures
from retorts import ResponsesHelper


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
        dynamodb = boto3.resource('dynamodb')
        for bundle_notification_count, doc_notification_count in ((3, 0), (0, 3), (3, 3), (0, 0)):
            table = dynamodb.create_table(**TestHealthFailures.dynamo_failures_table_settings)
            failed_bundles = [
                (f'{i}aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', f'2019-10-1{i}T113344.698028Z')
                for i in range(bundle_notification_count)
            ]
            bundle_notifications = [
                self._fake_notification(bundle_fqid) for bundle_fqid in failed_bundles
            ]
            document_notifications = [
                {
                    'entity_type': 'files',
                    'entity_id': f'{i}45b6f35-7361-4029-82be-429e12dfdb45',
                    'num_contributions': 2
                } for i in range(doc_notification_count)
            ]
            expected_response = {
                'failures': {
                    'up': True,
                    'failed_bundle_notifications': bundle_notifications,
                    'other_failed_messages': len(document_notifications)
                },
                'up': True,
            }
            test_notifications = bundle_notifications + document_notifications
            with table.batch_writer() as writer:
                for i, notification in enumerate(test_notifications):
                    item = {
                        'MessageType': 'bundle' if 'subscription_id' in notification.keys() else 'other',
                        'SentTimeMessageId': f'{i}-{i}',
                        'Body': json.dumps(notification)
                    }
                    writer.put_item(Item=item)
            with self.subTest(bundle_notification_count=bundle_notification_count,
                              document_notification_count=doc_notification_count):
                with ResponsesHelper() as helper:
                    helper.add_passthru(self.base_url)
                    response = requests.get(self.base_url + '/health/failures')
                    self.assertEqual(200, response.status_code)
                    self.assertEqual(expected_response, response.json())
            table.delete()


del HealthCheckTestCase

if __name__ == "__main__":
    unittest.main()
