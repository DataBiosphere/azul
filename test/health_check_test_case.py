from abc import (
    ABCMeta,
    abstractmethod,
)
import os
import random
import time
from typing import (
    ContextManager,
)
from unittest import (
    TestSuite,
)
from unittest.mock import (
    MagicMock,
    patch,
)

from furl import (
    furl,
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
from azul.health import (
    Health,
)
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
)
from azul.types import (
    MutableJSON,
)
from es_test_case import (
    ElasticsearchTestCase,
)
from service import (
    StorageServiceTestMixin,
)
from sqs_test_case import (
    SqsTestCase,
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
                          StorageServiceTestMixin,
                          SqsTestCase,
                          metaclass=ABCMeta):

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
        response = self._test(path='/health/')
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'up': True,
            **self._expected_elasticsearch(up=True),
            **self._expected_queues(up=True),
            **self._expected_other_lambdas(up=True),
            **self._expected_api_endpoints(up=True),
            **self._expected_progress()
        }, response.json())

    @mock_sts
    @mock_sqs
    def test_health_endpoint_keys(self):
        expected = {
            keys: {
                'up': True,
                **expected_response
            } for keys, expected_response in [
                ('elasticsearch', self._expected_elasticsearch(up=True)),
                ('queues', self._expected_queues(up=True)),
                ('other_lambdas', self._expected_other_lambdas(up=True)),
                ('api_endpoints', self._expected_api_endpoints(up=True)),
                ('progress', self._expected_progress()),
                ('progress,queues', self._expected_progress() | self._expected_queues(up=True)),
            ]
        }
        self._create_mock_queues()
        for keys, expected_response in expected.items():
            with self.subTest(keys=keys):
                response = self._test(path=f'health/{keys}')
                self.assertEqual(200, response.status_code)
                self.assertEqual(expected_response, response.json())

    @mock_s3
    @mock_sts
    @mock_sqs
    def test_cached_health(self):
        self.storage_service.create_bucket()
        # No health object is available in S3 bucket, yielding an error
        response = self._test(path='/health/cached')
        self.assertEqual(404, response.status_code)
        expected_response = {
            'Code': 'NotFoundError',
            'Message': 'Cached health object does not exist'
        }
        self.assertEqual(expected_response, response.json())

        # A successful response is obtained when all the systems are functional
        self._create_mock_queues()
        app = load_app_module(self.lambda_name(), unit_test=True)
        app.update_health_cache(MagicMock(), MagicMock())
        response = self._test(path='/health/cached')
        self.assertEqual(200, response.status_code)

        # Another failure is observed when the cache health object is older than
        # 2 minutes
        future_time = time.time() + 3 * 60
        with patch('time.time', new=lambda: future_time):
            response = self._test(path='/health/cached')
            self.assertEqual(500, response.status_code)
            expected_response = {
                'Code': 'ChaliceViewError',
                'Message': 'Cached health object is stale'
            }
            self.assertEqual(expected_response, response.json())

    def test_laziness(self):
        # Note the absence of moto decorators on this test.
        # If Health weren't lazy, it would fail due the lack of mocks for SQS.
        response = self._test(path='/health/other_lambdas')
        # The use of subTests ensures that we see the result of both
        # assertions. In the case of the health endpoint, the body of a 503
        # may carry a body with additional information.
        self.assertEqual(200, response.status_code)
        expected_response = {'up': True, **self._expected_other_lambdas(up=True)}
        self.assertEqual(expected_response, response.json())

    @abstractmethod
    def _expected_health(self,
                         *,
                         endpoints_up: bool = True,
                         es_up: bool = True
                         ) -> MutableJSON:
        raise NotImplementedError

    @mock_sts
    @mock_sqs
    def test_elasticsearch_down(self):
        self._create_mock_queues()
        mock_endpoint = ('7c9f2ddb-74ca-46a3-9438-24ce1fe7050e.com', 80)
        with patch.dict(os.environ, **config.es_endpoint_env(es_endpoint=mock_endpoint,
                                                             es_instance_count=1)):
            response = self._test()
            self.assertEqual(503, response.status_code)
            self.assertEqual(self._expected_health(es_up=False), response.json())

    def _expected_queues(self, *, up: bool) -> MutableJSON:
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
                        'error': 'The specified queue does not exist for'
                                 ' this wsdl version.'
                    }
                    for queue_name in config.all_queue_names
                })
            }
        }

    def _expected_api_endpoints(self, *, up: bool) -> MutableJSON:
        return {
            'api_endpoints': {
                'up': up
            } if up else {
                'up': up,
                'error': (
                    "HTTPError('503 Server Error: "
                    "Service Unavailable for url: "
                    f"{self._endpoint('/index/bundles?size=1')}')"
                )
            }
        }

    def _endpoint(self, relative_url: str) -> str:
        return str(config.service_endpoint.join(furl(relative_url)))

    def _other_lambda_names(self) -> list[str]:
        return [
            lambda_name
            for lambda_name in config.lambda_names()
            if lambda_name != self.lambda_name()
        ]

    def _expected_other_lambdas(self, *, up: bool) -> MutableJSON:
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

    def _expected_progress(self) -> MutableJSON:
        return {
            'progress': {
                'up': True,
                'unindexed_bundles': 0,
                'unindexed_documents': 0
            }
        }

    def _expected_elasticsearch(self, *, up: bool) -> MutableJSON:
        return {
            'elasticsearch': {
                'up': up
            }
        }

    def _test(self,
              *,
              path: str = '/health/fast',
              endpoints_up: bool = True,
              lambdas_up: bool = True
              ):
        with self.helper() as helper:
            self._mock_other_lambdas(helper, up=lambdas_up)
            with self._mock_service_endpoints(helper, up=endpoints_up):
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
                                *,
                                up: bool
                                ) -> ContextManager:
        helper.add(responses.Response(method='HEAD',
                                      url=self._endpoint('/index/bundles?size=1'),
                                      status=200 if up else 503,
                                      json={}))
        # Patching the Health class to use a random generator with a pinned
        # seed allows us to predict the service endpoint that will be picked
        # to check the health of the service REST API.
        return patch.object(Health, '_random', random.Random(x=42))

    def _mock_other_lambdas(self, helper: responses.RequestsMock, *, up: bool):
        for lambda_name in self._other_lambda_names():
            url = config.lambda_endpoint(lambda_name).set(path='/health/basic')
            helper.add(responses.Response(method='GET',
                                          url=str(url),
                                          status=200 if up else 500,
                                          json={'up': up}))
