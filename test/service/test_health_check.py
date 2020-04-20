import json
import os
from typing import Mapping
import unittest

import boto3
import mock
from moto import (
    mock_sqs,
    mock_sts,
    mock_dynamodb2,
)
import requests

from azul import config
from azul.logging import configure_test_logging
from health_check_test_case import HealthCheckTestCase
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
        with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
            database = boto3.resource('dynamodb')
            for bundle_notification_count, doc_notification_count in ((3, 0), (0, 3), (3, 3), (0, 0)):
                database.create_table(TableName=config.dynamo_failure_message_table_name,
                                      KeySchema=[
                                          {
                                              'AttributeName': 'MessageType',
                                              'KeyType': 'HASH'
                                          },
                                          {
                                              'AttributeName': 'SentTimeMessageId',
                                              'KeyType': 'RANGE'
                                          }
                                      ],
                                      AttributeDefinitions=[
                                          {
                                              'AttributeName': 'MessageType',
                                              'AttributeType': 'S'
                                          },
                                          {
                                              'AttributeName': 'SentTimeMessageId',
                                              'AttributeType': 'S'
                                          }
                                      ],
                                      ProvisionedThroughput={
                                          'ReadCapacityUnits': 5,
                                          'WriteCapacityUnits': 5
                                      })
                table = database.Table(config.dynamo_failure_message_table_name)
                document_notifications = [{
                    "entity_type": "files",
                    "entity_id": f"{i}45b6f35-7361-4029-82be-429e12dfdb45",
                    "num_contributions": 2
                } for i in range(doc_notification_count)]
                expected_failed_bundles = [{
                    "bundle_uuid": f"{i}aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "bundle_version": f"2019-10-1{i}T113344.698028Z"
                } for i in range(bundle_notification_count)]
                expected_response = {
                    "failed_bundle_notifications": expected_failed_bundles,
                    "other_failed_messages": len(document_notifications)
                }
                bundle_notifications = [{
                    "action": "add",
                    "notification": {
                        "query": '{}',
                        "subscription_id": bundles['bundle_uuid'],
                        "transaction_id": bundles['bundle_uuid'],
                        "match": bundles,
                    }
                } for bundles in expected_failed_bundles]
                test_notifications = bundle_notifications + document_notifications
                with table.batch_writer() as writer:
                    for i, notification in enumerate(test_notifications):
                        item = {
                            'MessageType': 'bundle_notification' if 'notification' in notification.keys() else 'other',
                            'SentTimeMessageId': f"{i}-{i}",
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
