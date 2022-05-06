from abc import (
    ABCMeta,
    abstractmethod,
)
import os
import time
from typing import (
    List,
    Mapping,
    Tuple,
)
from unittest import (
    TestSuite,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from moto import (
    mock_s3,
    mock_sqs,
    mock_sts,
)
import requests
import responses

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
)
from azul.types import (
    JSON,
)
from es_test_case import (
    ElasticsearchTestCase,
)
from service import (
    StorageServiceTestCase,
)


# FIXME: This is inelegant: https://github.com/DataBiosphere/azul/issues/652
# noinspection PyUnusedLocal
def load_tests(loader, tests, pattern):
    suite = TestSuite()
    return suite


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class HealthCheckTestCase(LocalAppTestCase,
                          ElasticsearchTestCase,
                          StorageServiceTestCase,
                          metaclass=ABCMeta):
    endpoints = (
        '/index/files?size=1',
        '/index/projects?size=1',
        '/index/samples?size=1',
        '/index/bundles?size=1'
    )

    def test_basic(self):
        response = requests.get(str(self.base_url.set(path='/health/basic')))
        self.assertEqual(200, response.status_code)
        self.assertEqual({'up': True}, response.json())

    def test_validation(self):
        for path in ['foo', 'elasticsearch,', ',elasticsearch', ',', '1']:
            response = requests.get(str(self.base_url.set(path=('health', path))))
            self.assertEqual(400, response.status_code)

    @mock_sts
    @mock_sqs
    def test_health_all_ok(self):
        self._create_mock_queues()
        endpoint_states = self._endpoint_states()
        response = self._test(endpoint_states, lambdas_up=True, path='/health/')
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            **self._expected_elasticsearch(True),
            **self._expected_queues(True),
            **self._expected_other_lambdas(True),
            **self._expected_api_endpoints(endpoint_states),
            **self._expected_progress()
        }, response.json())

    @mock_sts
    @mock_sqs
    def test_health_endpoint_keys(self):
        endpoint_states = self._endpoint_states()
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
            with self.subTest(keys=keys):
                with self.helper() as helper:
                    self._mock_other_lambdas(helper, up=True)
                    self._mock_service_endpoints(helper, endpoint_states)
                    response = requests.get(str(self.base_url.set(path=('health', keys))))
                    self.assertEqual(200, response.status_code)
                    self.assertEqual(expected_response, response.json())

    @mock_s3
    @mock_sts
    @mock_sqs
    def test_cached_health(self):
        self.storage_service.create_bucket()
        # No health object is available in S3 bucket, yielding an error
        with self.helper():
            response = requests.get(str(self.base_url.set(path='/health/cached')))
            self.assertEqual(500, response.status_code)
            self.assertEqual('ChaliceViewError: Cached health object does not exist', response.json()['Message'])

        # A successful response is obtained when all the systems are functional
        self._create_mock_queues()
        endpoint_states = self._endpoint_states()
        app = load_app_module(self.lambda_name(), unit_test=True)
        with self.helper() as helper:
            self._mock_service_endpoints(helper, endpoint_states)
            app.update_health_cache(MagicMock(), MagicMock())
            response = requests.get(str(self.base_url.set(path='/health/cached')))
            self.assertEqual(200, response.status_code)

        # Another failure is observed when the cache health object is older than 2 minutes
        future_time = time.time() + 3 * 60
        with self.helper():
            with patch('time.time', new=lambda: future_time):
                response = requests.get(str(self.base_url.set(path='/health/cached')))
                self.assertEqual(500, response.status_code)
                self.assertEqual('ChaliceViewError: Cached health object is stale', response.json()['Message'])

    def test_laziness(self):
        # Note the absence of moto decorators on this test.
        with self.helper() as helper:
            self._mock_other_lambdas(helper, up=True)
            # If Health weren't lazy, it would fail due the lack of mocks for SQS.
            response = requests.get(str(self.base_url.set(path='/health/other_lambdas')))
            # The use of subTests ensures that we see the result of both
            # assertions. In the case of the health endpoint, the body of a 503
            # may carry a body with additional information.
            self.assertEqual(200, response.status_code)
            expected_response = {'up': True, **self._expected_other_lambdas(up=True)}
            self.assertEqual(expected_response, response.json())

    @abstractmethod
    def _expected_health(self, endpoint_states: Mapping[str, bool], es_up: bool = True):
        raise NotImplementedError

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        mock_endpoint = ('7c9f2ddb-74ca-46a3-9438-24ce1fe7050e.com', 80)
        endpoint_states = self._endpoint_states()
        with patch.dict(os.environ, **config.es_endpoint_env(es_endpoint=mock_endpoint,
                                                             es_instance_count=1)):
            response = self._test(endpoint_states, lambdas_up=True)
            self.assertEqual(503, response.status_code)
            self.assertEqual(self._expected_health(endpoint_states, es_up=False), response.json())

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
                        'up': False,
                        'error': f'The specified queue {queue_name} does not exist for'
                                 f' this wsdl version.'
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
                            f"{config.service_endpoint() + endpoint}')")
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

    def _test(self, endpoint_states: Mapping[str, bool], lambdas_up: bool, path: str = '/health/fast'):
        with self.helper() as helper:
            self._mock_other_lambdas(helper, lambdas_up)
            self._mock_service_endpoints(helper, endpoint_states)
            return requests.get(str(self.base_url.set(path=path)))

    def helper(self):
        helper = responses.RequestsMock()
        helper.add_passthru(str(self.base_url))
        # We originally shared the Requests mock with Moto which had this set
        # to False. Because of that, and without noticing, we ended up mocking
        # more responses than necessary for some of the tests. Instead of
        # rewriting the tests to only mock what is actually used, we simply
        # disable the assertion, just like Moto did.
        helper.assert_all_requests_are_fired = False
        return helper

    def _mock_service_endpoints(self,
                                helper: responses.RequestsMock,
                                endpoint_states: Mapping[str, bool]) -> None:
        for endpoint, endpoint_up in endpoint_states.items():
            helper.add(responses.Response(method='HEAD',
                                          url=config.service_endpoint() + endpoint,
                                          status=200 if endpoint_up else 503,
                                          json={}))

    def _mock_other_lambdas(self, helper: responses.RequestsMock, up: bool):
        for lambda_name in self._other_lambda_names():
            helper.add(responses.Response(method='GET',
                                          url=config.lambda_endpoint(lambda_name) + '/health/basic',
                                          status=200 if up else 500,
                                          json={'up': up}))

    def _create_mock_queues(self):
        sqs = aws.resource('sqs')
        for queue_name in config.all_queue_names:
            sqs.create_queue(QueueName=queue_name)

    def _endpoint_states(self,
                         up_endpoints: Tuple[str, ...] = endpoints,
                         down_endpoints: Tuple[str, ...] = ()
                         ) -> Mapping[str, bool]:
        return {
            **{endpoint: True for endpoint in up_endpoints},
            **{endpoint: False for endpoint in down_endpoints}
        }
