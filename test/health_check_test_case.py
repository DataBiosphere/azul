from abc import ABCMeta
import os
from typing import List, Tuple
from unittest import TestSuite, mock

import boto3
from moto import mock_sqs, mock_sts
import requests
import responses

from app_test_case import LocalAppTestCase
from azul import config
from es_test_case import ElasticsearchTestCase
from retorts import ResponsesHelper


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
# noinspection PyUnusedLocal
def load_tests(loader, tests, pattern):
    suite = TestSuite()
    return suite


class HealthCheckTestCase(LocalAppTestCase, ElasticsearchTestCase, metaclass=ABCMeta):

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
        endpoints = [('/repository/files', True),
                     ('/repository/projects', True),
                     ('/repository/samples', True),
                     ('/repository/bundles', True),
                     ('/repository/summary', True)]
        response = self._test(endpoints, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                **({
                    queue_name: {'up': True, 'messages': {'delayed': 0, 'invisible': 0, 'queued': 0}}
                    for queue_name in config.all_queue_names
                })
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._other_lambda_names()
            }),
            'api_endpoints': {
                'up': True,
                **({
                    endpoint: {'up': up} for endpoint, up in endpoints
                })
            },
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_other_lambda_down(self):
        self._create_mock_queues()
        endpoints = [('/repository/files', True),
                     ('/repository/projects', True),
                     ('/repository/samples', True),
                     ('/repository/bundles', True),
                     ('/repository/summary', True)]
        response = self._test(endpoints, lambdas_up=False)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                **({
                    queue_name: {'up': True, 'messages': {'delayed': 0, 'invisible': 0, 'queued': 0}}
                    for queue_name in config.all_queue_names
                })
            },
            **({
                lambda_name: {'up': False}
                for lambda_name in self._other_lambda_names()
            }),
            'api_endpoints': {
                'up': True,
                **({
                    endpoint: {'up': up} for endpoint, up in endpoints
                })
            }
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_queues_down(self):
        endpoints = [('/repository/files', True),
                     ('/repository/projects', True),
                     ('/repository/samples', True),
                     ('/repository/bundles', True),
                     ('/repository/summary', True)]
        response = self._test(endpoints, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': False,
                **({
                    queue_name: {'up': False, 'error': 'The specified queue does not exist for this wsdl version.'}
                    for queue_name in config.all_queue_names
                })
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._other_lambda_names()
            }),
            'api_endpoints': {
                'up': True,
                **({
                    endpoint: {'up': up} for endpoint, up in endpoints
                })
            },
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_all_api_endpoints_down(self):
        self._create_mock_queues()
        endpoints = [('/repository/files', False),
                     ('/repository/projects', False),
                     ('/repository/samples', False),
                     ('/repository/bundles', False),
                     ('/repository/summary', False)]
        response = self._test(endpoints, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                **({
                    queue_name: {'up': True, 'messages': {'delayed': 0, 'invisible': 0, 'queued': 0}}
                    for queue_name in config.all_queue_names
                })
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._other_lambda_names()
            }),
            'api_endpoints': {
                'up': False,
                **({
                    endpoint: {'up': up,
                               'error': f'503 Server Error: Service Unavailable for url: '
                                        f'{config.service_endpoint()}{endpoint}'} for endpoint, up in endpoints
                })
            }
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_one_api_endpoint_down(self):
        self._create_mock_queues()
        endpoints = [('/repository/files', True),
                     ('/repository/projects', True),
                     ('/repository/samples', False),
                     ('/repository/bundles', True),
                     ('/repository/summary', True)]
        response = self._test(endpoints, lambdas_up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                **({
                    queue_name: {'up': True, 'messages': {'delayed': 0, 'invisible': 0, 'queued': 0}}
                    for queue_name in config.all_queue_names
                })
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._other_lambda_names()
            }),
            'api_endpoints': {
                'up': False,
                **({
                    endpoint: {'up': up} if up else
                              {'up': up,
                               'error': f'503 Server Error: Service Unavailable for url: {config.service_endpoint()}'
                                        f'{endpoint}'} for endpoint, up in endpoints
                })
            }
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        endpoints = [('/repository/files', True),
                     ('/repository/projects', True),
                     ('/repository/samples', True),
                     ('/repository/bundles', True),
                     ('/repository/summary', True)]
        mock_endpoint = ('nonexisting-index.com', 80)
        with mock.patch.dict(os.environ, **config.es_endpoint_env(mock_endpoint)):
            response = self._test(endpoints, lambdas_up=True)
            health_object = response.json()
            self.assertEqual(503, response.status_code)
            documents_ = {
                'up': False,
                'elastic_search': {
                    'up': False
                },
                'queues': {
                    'up': True,
                    **({
                        queue_name: {'up': True, 'messages': {'delayed': 0, 'invisible': 0, 'queued': 0}}
                        for queue_name in config.all_queue_names
                    })
                },
                **({
                    lambda_name: {'up': True}
                    for lambda_name in self._other_lambda_names()
                }),
                'api_endpoints': {
                    'up': True,
                    **{
                        endpoint: {'up': up} for endpoint, up in endpoints
                    }
                }
            }
            self.assertEqual(documents_, health_object)

    def _test(self, endpoints: List[Tuple[str, bool]], lambdas_up: bool):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            for lambda_name in self._other_lambda_names():
                helper.add(responses.Response(method='GET',
                                              url=config.lambda_endpoint(lambda_name) + '/health/basic',
                                              status=200 if lambdas_up else 503,
                                              json={'up': lambdas_up}))
            for endpoint, endpoint_up in endpoints:
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
