from abc import ABCMeta
import os
from typing import List, Dict
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
                 '/repository/bundles?size=1',
                 '/repository/summary?size=1']

    def _other_lambda_names(self) -> List[str]:
        return [
            lambda_name
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name()
        ]

    @mock_sts
    @mock_sqs
    def test_ok_response(self):
        self._create_mock_queues()
        endpoint_states = self._generate_endpoint_states(self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            'elastic_search': {
                'up': True
            },
            **self._expected_queues(True),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states)
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_other_lambda_down(self):
        self._create_mock_queues()
        endpoint_states = self._generate_endpoint_states(self.endpoints)
        response = self._test(endpoint_states, lambdas_up=False)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            **self._expected_queues(True),
            **self._expected_other_lambdas(False),
            **self._expected_api_endpoints(endpoint_states)
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_queues_down(self):
        endpoint_states = self._generate_endpoint_states(self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            **self._expected_queues(False),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states)
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        endpoint_states = self._generate_endpoint_states([], down_endpoints=self.endpoints)
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            **self._expected_queues(True),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states)
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_one_api_endpoint_down(self):
        self._create_mock_queues()
        endpoint_states = self._generate_endpoint_states(self.endpoints[1:], down_endpoints=self.endpoints[:1])
        response = self._test(endpoint_states, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            **self._expected_queues(True),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states)
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        mock_endpoint = ('nonexisting-index.com', 80)
        endpoint_states = self._generate_endpoint_states(self.endpoints)
        with mock.patch.dict(os.environ, **config.es_endpoint_env(mock_endpoint)):
            response = self._test(endpoint_states, lambdas_up=True)
            health_object = response.json()
            self.assertEqual(503, response.status_code)
            documents_ = {
                'up': False,
                'elastic_search': {
                    'up': False
                },
                **self._expected_queues(True),
                **self._expected_other_lambdas(True),
                **self._expected_api_endpoints(endpoint_states)
            }
            self.assertEqual(documents_, health_object)

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

    def _expected_api_endpoints(self, endpoint_states: Dict[str, bool]) -> JSON:
        return {
            'api_endpoints': {
                'up': all(up for endpoint, up in endpoint_states.items()),
                **({
                    endpoint: {
                        'up': up
                    } if up else {
                        'up': up,
                        'error': f'503 Server Error: Service Unavailable for url: {config.service_endpoint()}'
                                 f'{endpoint}'
                    } for endpoint, up in endpoint_states.items()
                })
            }
        }

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

    def _test(self, endpoint_states: Dict[str, bool], lambdas_up: bool):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            for lambda_name in self._other_lambda_names():
                helper.add(responses.Response(method='GET',
                                              url=config.lambda_endpoint(lambda_name) + '/health/basic',
                                              status=200 if lambdas_up else 503,
                                              json={'up': lambdas_up}))
            for endpoint, endpoint_up in endpoint_states.items():
                helper.add(responses.Response(method='HEAD',
                                              url=config.service_endpoint() + endpoint,
                                              status=200 if endpoint_up else 503,
                                              json={}))
            with mock.patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
                return requests.get(self.base_url + '/health')

    def _create_mock_queues(self):
        sqs = boto3.resource('sqs', region_name='us-east-1')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)

    def _generate_endpoint_states(self, up_endpoints: List[str], down_endpoints: List[str] = None) -> Dict[str, bool]:
        return {
            **({endpoint: True for endpoint in up_endpoints}),
            **({endpoint: False for endpoint in down_endpoints} if down_endpoints else {})
        }
