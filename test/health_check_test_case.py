import os

import boto3
import logging
import requests
import responses

from abc import ABCMeta

from typing import List
from unittest import mock, TestSuite
from moto import mock_sqs, mock_sts
from chalice.config import Config as ChaliceConfig

from azul import config
from app_test_case import LocalAppTestCase
from es_test_case import ElasticsearchTestCase


log = logging.getLogger(__name__)


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
# noinspection PyUnusedLocal
def load_tests(loader, tests, pattern):
    suite = TestSuite()
    return suite


class HealthCheckTestCase(LocalAppTestCase, ElasticsearchTestCase, metaclass=ABCMeta):
    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    def _supplemental_lambda_names(self) -> List[str]:
        return [
            lambda_name
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name()
        ]

    @mock_sts
    @mock_sqs
    def test_ok_response(self):
        self._create_mock_queues()
        response = self._create_mock_response(up=True)
        health_object = response.json()
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                'azul-documents-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-documents-abraham.fifo': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-fail-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-fail-abraham.fifo': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-notify-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                }
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._supplemental_lambda_names()
            }),
            'unindexed_bundles': 0,
            'unindexed_documents': 0
        }, health_object)

    @mock_sts
    @mock_sqs
    def test_supplemental_lambda_down(self):
        self._create_mock_queues()
        response = self._create_mock_response(up=False)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            'up': False,
            'elastic_search': {
                'up': True
            },
            'queues': {
                'up': True,
                'azul-documents-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-documents-abraham.fifo': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-fail-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-fail-abraham.fifo': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                },
                'azul-notify-abraham': {
                    'up': True,
                    'messages': {
                        'delayed': 0,
                        'invisible': 0,
                        'queued': 0
                    }
                }
            },
            **({
                lambda_name: {'up': False}
                for lambda_name in self._supplemental_lambda_names()
            }),
            'unindexed_bundles': 0,
            'unindexed_documents': 0
        }, health_object)
    
    @mock_sqs
    @mock_sts
    def test_queues_down(self):
        response = self._create_mock_response(up=True)
        health_object = response.json()
        self.assertEqual(503, response.status_code)
        self.assertEqual({
            "up": False,
            "elastic_search": {
                "up": True
            },
            "queues": {
                "up": False,
                "azul-documents-abraham": {
                    "up": False,
                    "error": "The specified queue does not exist for this wsdl version."
                },
                "azul-documents-abraham.fifo": {
                    "up": False,
                    "error": "The specified queue does not exist for this wsdl version."
                },
                "azul-fail-abraham": {
                    "up": False,
                    "error": "The specified queue does not exist for this wsdl version."
                },
                "azul-fail-abraham.fifo": {
                    "up": False,
                    "error": "The specified queue does not exist for this wsdl version."
                },
                "azul-notify-abraham": {
                    "up": False,
                    "error": "The specified queue does not exist for this wsdl version."
                }
            },
            **({
                lambda_name: {'up': True}
                for lambda_name in self._supplemental_lambda_names()
            }),
            "unindexed_bundles": 0,
            "unindexed_documents": 0
        }, health_object)

    @mock_sqs
    @mock_sts
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        with mock.patch.dict(os.environ, AZUL_ES_ENDPOINT='nonexisting-index.com:80'):
            response = self._create_mock_response(up=True)
            health_object = response.json()
            self.assertEqual(503, response.status_code)
            documents_ = {
                'up': False,
                'elastic_search': {
                    'up': False
                },
                'queues': {
                    'up': True,
                    'azul-documents-abraham': {
                        'up': True,
                        'messages': {
                            'delayed': 0,
                            'invisible': 0,
                            'queued': 0
                        }
                    },
                    'azul-documents-abraham.fifo': {
                        'up': True,
                        'messages': {
                            'delayed': 0,
                            'invisible': 0,
                            'queued': 0
                        }
                    },
                    'azul-fail-abraham': {
                        'up': True,
                        'messages': {
                            'delayed': 0,
                            'invisible': 0,
                            'queued': 0
                        }
                    },
                    'azul-fail-abraham.fifo': {
                        'up': True,
                        'messages': {
                            'delayed': 0,
                            'invisible': 0,
                            'queued': 0
                        }
                    },
                    'azul-notify-abraham': {
                        'up': True,
                        'messages': {
                            'delayed': 0,
                            'invisible': 0,
                            'queued': 0
                        }
                    }
                },
                **({
                    lambda_name: {'up': True}
                    for lambda_name in self._supplemental_lambda_names()
                }),
                'unindexed_bundles': 0,
                'unindexed_documents': 0
            }
            self.assertEqual(documents_, health_object)

    def _create_mock_response(self, up: bool):
        with responses.RequestsMock() as _response:
            _response.add_passthru(self.base_url)
            for lambda_name in self._supplemental_lambda_names():
                _response.add(responses.Response(method='GET',
                                                 url=config.lambda_endpoint(lambda_name) + '/health/basic',
                                                 status=200 if up else 503,
                                                 json={'up': up}))
            response = requests.get(self.base_url + '/health')
        return response

    def _create_mock_queues(self):
        sqs = boto3.resource('sqs')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)
