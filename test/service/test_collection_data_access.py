import json
from unittest import (
    TestCase,
    skipIf,
)

import responses

from azul import (
    config,
)
from azul.service.collection_data_access import (
    ClientError,
    CollectionDataAccess,
    CreationError,
    RetrievalError,
    ServerTimeoutError,
    UnauthorizedClientAccessError,
    UpdateError,
)


@skipIf(config.dss_endpoint is None,
        'DSS endpoint is not configured')
class CollectionDataAccessTestCase(TestCase):

    def setUp(self):
        fake_access_token = 'fake_access_token'
        self.cda = CollectionDataAccess(fake_access_token)

    def test_get_ok(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        fake_collection = {'hello': 'world'}
        with responses.RequestsMock() as helper:
            helper.add(responses.Response(responses.GET,
                                          self.cda.endpoint_url('collections', test_collection_uuid),
                                          json=fake_collection))
            collection = self.cda.get(test_collection_uuid, test_collection_version)
        self.assertEqual(collection,
                         dict(uuid=test_collection_uuid,
                              version=test_collection_version,
                              collection=fake_collection))

    def test_get_raises_retrival_error(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.GET,
                                                  self.cda.endpoint_url('collections', test_collection_uuid),
                                                  callback=RequestCallback(567, '{}'),
                                                  content_type='application/json'))
            with self.assertRaises(RetrievalError):
                self.cda.get(test_collection_uuid, test_collection_version)

    def test_create_ok(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        expected_collection = dict(uuid=test_collection_uuid, version=test_collection_version)
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.PUT,
                                                  self.cda.endpoint_url('collections'),
                                                  callback=RequestCallback(201, json.dumps(expected_collection)),
                                                  content_type='application/json'))
            collection = self.cda.create(test_collection_uuid, 'foo bar', 'bar', test_collection_version, [])
        self.assertEqual(collection, expected_collection)

    def test_create_raises_creation_error(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        fake_dss_response = {"code": "unknown"}
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.PUT,
                                                  self.cda.endpoint_url('collections'),
                                                  callback=RequestCallback(500, json.dumps(fake_dss_response)),
                                                  content_type='application/json'))
            with self.assertRaises(CreationError):
                self.cda.create(test_collection_uuid, 'foo bar', 'bar', test_collection_version, [])

    def test_append_with_no_items_successful(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        expected_collection = dict(uuid=test_collection_uuid, version=test_collection_version)
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.PATCH,
                                                  self.cda.endpoint_url('collections', test_collection_uuid),
                                                  callback=RequestCallback(200, json.dumps(expected_collection)),
                                                  content_type='application/json'))
            collection = self.cda.append(test_collection_uuid, test_collection_version, [])
        self.assertEqual(collection, expected_collection)

    def test_append_with_some_items_successful(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        expected_collection = dict(uuid=test_collection_uuid, version=test_collection_version)
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.PATCH,
                                                  self.cda.endpoint_url('collections', test_collection_uuid),
                                                  callback=RequestCallback(200, json.dumps(expected_collection)),
                                                  content_type='application/json'))
            collection = self.cda.append(test_collection_uuid,
                                         test_collection_version,
                                         [dict(type='foo_1', uuid='bar_1', version='baz_1'),
                                          dict(type='foo_2', uuid='bar_2', version='baz_2'),
                                          dict(type='foo_n', uuid='bar_n', version='baz_n')])
        self.assertEqual(collection, expected_collection)

    def test_append_raises_update_error(self):
        test_collection_uuid = 'abcdef123456'
        test_collection_version = '1980-01-01'
        with responses.RequestsMock() as helper:
            helper.add(responses.CallbackResponse(responses.PATCH,
                                                  self.cda.endpoint_url('collections', test_collection_uuid),
                                                  callback=RequestCallback(405, '{}'),
                                                  content_type='application/json'))
            with self.assertRaises(UpdateError):
                self.cda.append(test_collection_uuid, test_collection_version, [])

    def test_send_request_successful_with_auto_retry_on_http_504_timeout(self):
        test_collection_uuid = 'abcdef123456'
        expected_response = {'code': 'hello_world'}
        with responses.RequestsMock() as helper:
            url = self.cda.endpoint_url(test_collection_uuid)
            helper.add(responses.CallbackResponse(responses.GET,
                                                  url,
                                                  callback=RequestCallback(200,
                                                                           json.dumps(expected_response),
                                                                           delay=True),
                                                  content_type='application/json'))
            response = self.cda.send_request(test_collection_uuid, 'get', url, {})
        self.assertEqual(response.json(), expected_response)

    def test_send_request_successful_with_auto_retry_on_http_502(self):
        test_collection_uuid = 'abcdef123456'
        expected_response = {'code': 'hello_world'}

        mock_response_sequence = [
            (502, {}, '{"code": "mock_error"}'),
            (200, {}, json.dumps(expected_response))
        ]

        def mock_request_handler(_request):
            return mock_response_sequence.pop(0)

        with responses.RequestsMock() as helper:
            url = self.cda.endpoint_url(test_collection_uuid)
            helper.add(responses.CallbackResponse(responses.GET,
                                                  url,
                                                  callback=mock_request_handler,
                                                  content_type='application/json'))
            response = self.cda.send_request(test_collection_uuid, 'get', url, {})
        self.assertEqual(response.json(), expected_response)

    def test_send_request_fails_after_too_many_retries(self):
        test_collection_uuid = 'abcdef123456'
        with self.assertRaises(ServerTimeoutError):
            self.cda.send_request(test_collection_uuid, 'get', 'fake_url', {}, delay=64)

    def test_send_request_with_unexpected_response_code_raises_client_error(self):
        test_collection_uuid = 'abcdef123456'
        expected_response = {'code': 'hello_world'}
        with responses.RequestsMock() as helper:
            url = self.cda.endpoint_url(test_collection_uuid)
            helper.add(responses.CallbackResponse(responses.GET,
                                                  url,
                                                  callback=RequestCallback(201, json.dumps(expected_response)),
                                                  content_type='application/json'))
            with self.assertRaises(ClientError):
                self.cda.send_request(test_collection_uuid, 'get', url, {}, expected_status_code=200)

    def test_send_request_with_unexpected_response_code_raises_unauthorized_client_access_error(self):
        test_collection_uuid = 'abcdef123456'
        expected_response = {'code': 'mock_error'}
        with responses.RequestsMock() as helper:
            url = self.cda.endpoint_url(test_collection_uuid)
            helper.add(responses.CallbackResponse(responses.GET,
                                                  url,
                                                  callback=RequestCallback(401, json.dumps(expected_response)),
                                                  content_type='application/json'))
            with self.assertRaises(UnauthorizedClientAccessError):
                self.cda.send_request(test_collection_uuid, 'get', url, {}, expected_status_code=200)


class RequestCallback:

    def __init__(self, code, content, delay=False):
        self.content = content
        self.code = code
        self.delay = delay

    def __call__(self, request):
        if not self.delay:
            return self.code, {}, self.content
        else:
            self.delay = False
            return 504, {}, '{"code": "timed_out"}'
