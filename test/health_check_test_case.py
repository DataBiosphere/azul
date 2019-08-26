from abc import ABCMeta
import os
from typing import List, Mapping
from unittest import TestSuite, mock

import boto3
from moto import mock_sqs, mock_sts
import requests
import responses

from app_test_case import LocalAppTestCase
from azul import config
from azul.types import JSON
from es_test_case import ElasticsearchTestCase
from retorts import ResponsesHelper


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
# noinspection PyUnusedLocal
def load_tests(loader, tests, pattern):
    suite = TestSuite()
    return suite


class HealthCheckTestCase(LocalAppTestCase, ElasticsearchTestCase, metaclass=ABCMeta):
    endpoints = ['/repository/files?size=1',
                 '/repository/projects?size=1',
                 '/repository/samples?size=1',
                 '/repository/bundles?size=1']

    def test_basic(self):
        response = requests.get(self.base_url + '/health/basic')
        self.assertEqual(200, response.status_code)
        self.assertEqual({'up': True}, response.json())

    def test_validation(self):
        for path in ['/foo', '/elasticsearch,', '/,elasticsearch', '/,', '/1']:
            response = requests.get(self.base_url + '/health' + path)
            self.assertEqual(400, response.status_code)

    @mock_sts
    @mock_sqs
    def test_health_all_ok(self):
        self._create_mock_queues()
        endpoint_states = self._make_endpoint_states(self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True, path='/health/')
        health_object = response.json()
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            **self._expected_elasticsearch(True),
            **self._expected_queues(True),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states),
            **self._expected_progress()
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_health_endpoint_keys(self):
        endpoint_states = self._make_endpoint_states(self.endpoints)
        expected = {
            keys: {
                'up': True,
                **expected_response
            } for keys, expected_response in [
                ('elasticsearch', self._expected_elasticsearch(True)),
                ('queues', self._expected_queues(True)),
                ('other_lambdas', self._expected_other_lambdas(True)),
                ('api_endpoints', self._expected_api_endpoints(endpoint_states)),
                ('progress', self._expected_progress()),
                ('progress,queues', {**self._expected_progress(), **self._expected_queues(True)}),
            ]
        }
        self._create_mock_queues()
        for keys, expected_response in expected.items():
            with self.subTest(msg=keys):
                with ResponsesHelper() as helper:
                    helper.add_passthru(self.base_url)
                    self._mock_other_lambdas(helper, up=True)
                    self._mock_service_endpoints(helper, endpoint_states)
                    with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
                        response = requests.get(self.base_url + '/health/' + keys)
                        self.assertEqual(200, response.status_code)
                        self.assertEqual(expected_response, response.json())

    @responses.activate
    def test_laziness(self):
        # Note the absence of moto decorators on this test.
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            self._mock_other_lambdas(helper, up=True)
            # If Health werent't lazy, it would fail due the lack of mocks for SQS.
            response = requests.get(self.base_url + '/health/other_lambdas')
            # The use of subTests ensures that we see the result of both
            # assertions. In the case of the health endpoint, the body of a 503
            # may carry a body with additional information.
            with self.subTest('status_code'):
                self.assertEqual(200, response.status_code)
            with self.subTest('response'):
                expected_response = {'up': True, **self._expected_other_lambdas(up=True)}
            self.assertEqual(expected_response, response.json())

    def _expected_queues(self, up: bool) -> JSON:
        return {
            'queues': {
                'up': up,
                **({
                    queue_name: {
                        'up': True,
                        'messages': {
                            'delayed': 0, 'invisible': 0, 'queued': 0
                        }
                    } if up else {
                        'up': False, 'error': 'The specified queue does not exist for this wsdl version.'
                    }
                    for queue_name in config.all_queue_names
                })
            }
        }

    def _expected_api_endpoints(self, endpoint_states: Mapping[str, bool]) -> JSON:
        return {
            'api_endpoints': {
                'up': all(up for endpoint, up in endpoint_states.items()),
                **({
                    config.service_endpoint() + endpoint: {
                        'up': up
                    } if up else {
                        'up': up,
                        'error': (
                            "HTTPError('503 Server Error: "
                            "Service Unavailable for url: "
                            f"{config.service_endpoint() + endpoint}',)")
                    } for endpoint, up in endpoint_states.items()
                })
            }
        }

    def _other_lambda_names(self) -> List[str]:
        return [
            lambda_name
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name()
        ]

    def _expected_other_lambdas(self, up: bool) -> JSON:
        return {
            'other_lambdas': {
                'up': up,
                **({
                    lambda_name: {
                        'up': up
                    } for lambda_name in self._other_lambda_names()
                })
            }
        }

    def _expected_progress(self) -> JSON:
        return {
            'progress': {
                'up': True,
                'unindexed_bundles': 0,
                'unindexed_documents': 0
            }
        }

    def _expected_elasticsearch(self, up: bool) -> JSON:
        return {
            'elasticsearch': {
                'up': up
            }
        }

    def _test(self, endpoint_states: Mapping[str, bool], lambdas_up: bool, path: str = '/health'):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            self._mock_other_lambdas(helper, lambdas_up)
            self._mock_service_endpoints(helper, endpoint_states)
            with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
                return requests.get(self.base_url + path)

    def _mock_service_endpoints(self, helper: ResponsesHelper, endpoint_states: Mapping[str, bool]) -> None:
        for endpoint, endpoint_up in endpoint_states.items():
            helper.add(responses.Response(method='HEAD',
                                          url=config.service_endpoint() + endpoint,
                                          status=200 if endpoint_up else 503,
                                          json={}))

    def _mock_other_lambdas(self, helper: ResponsesHelper, up: bool):
        for lambda_name in self._other_lambda_names():
            helper.add(responses.Response(method='GET',
                                          url=config.lambda_endpoint(lambda_name) + '/health/basic',
                                          status=200 if up else 500,
                                          json={'up': up}))

    def _create_mock_queues(self):
        sqs = boto3.resource('sqs', region_name='us-east-1')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)

    def _make_endpoint_states(self, up_endpoints: List[str], down_endpoints: List[str] = None) -> Mapping[str, bool]:
        return {
            **({endpoint: True for endpoint in up_endpoints}),
            **({endpoint: False for endpoint in down_endpoints} if down_endpoints else {})
        }
