import datetime
import json
from unittest import TestCase
from unittest.mock import patch

from azul.service.cart_export_job_manager import CartExportJobManager


class TestCartExportJobManager(TestCase):

    @patch('azul.service.cart_export_job_manager.CartExportJobManager.step_function_helper')
    def test_foo(self, step_function_helper):
        expected_user_id = 'user1'
        expected_cart_id = 'cart1'
        expected_access_token = 'at1'
        expected_execution_id = '123456'
        expected_collection_uuid = '567890'

        def mock_start_execution(*_args, **kwargs):
            execution_name = kwargs['execution_name']
            execution_input = kwargs['execution_input']
            self.assertEqual(expected_execution_id, execution_name)
            self.assertIn('user_id', execution_input)
            self.assertIn('cart_id', execution_input)
            self.assertIn('collection_uuid', execution_input)
            self.assertIn('collection_version', execution_input)
            self.assertIn('resume_token', execution_input)
            self.assertIn('access_token', execution_input)
            self.assertEqual(expected_user_id, execution_input['user_id'])
            self.assertEqual(expected_cart_id, execution_input['cart_id'])
            self.assertEqual(expected_access_token, execution_input['access_token'])
            self.assertEqual(expected_collection_uuid, execution_input['collection_uuid'])
            self.assertIsNotNone(execution_input['collection_version'])
            self.assertIsNone(execution_input['resume_token'])

        step_function_helper.start_execution.side_effect = mock_start_execution
        service = CartExportJobManager()
        with patch('uuid.uuid4', side_effect=[expected_execution_id, '567890']):
            token = service.initiate(expected_user_id, expected_cart_id, expected_access_token)
        self.assertEqual(expected_execution_id, service.decode_token(token)['execution_id'])

    @patch('azul.service.cart_export_job_manager.CartExportJobManager.step_function_helper')
    def test_get_on_job_failed(self, step_function_helper):
        initial_mock_input = json.dumps({
            "execution_id": "foo",
            "user_id": "mock-auth|1234",
            "cart_id": "e8205835-2b28-4a52-89a3-10876cce5e26",
            "collection_uuid": "38daf48a-46f3-4a9a-9e64-673fe78655fb",
            "collection_version": "2019-02-19T213905.000000Z",
            "resume_token": None,
            "access_token": "Bearer mock_bearer_token"
        })
        sample_events = [
            {
                'executionFailedEventDetails': {
                    'cause': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
                    'error': 'FooException'},
                'id': 6,
                'previousEventId': 5,
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 8, 105000),
                'type': 'ExecutionFailed'},
            {
                'id': 5,
                'lambdaFunctionFailedEventDetails': {
                    'cause': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
                    'error': 'FooException'},
                'previousEventId': 4,
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 8, 105000),
                'type': 'LambdaFunctionFailed'},
            {
                'id': 4,
                'previousEventId': 3,
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 6, 494000),
                'type': 'LambdaFunctionStarted'},
            {
                'id': 3,
                'lambdaFunctionScheduledEventDetails': {
                    'input': json.dumps({
                        'execution_id': 'foo',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': '38daf48a-46f3-4a9a-9e64-673fe78655fb',
                        'collection_version': '2019-02-19T213905.000000Z',
                        'resume_token': None,
                        'access_token': 'Bearer mock_bearer_token'
                    }),
                    'resource': 'arn:aws:lambda:us-east-1:12345:function:azul-service-test-cartexportpush'},
                'previousEventId': 2,
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 6, 448000),
                'type': 'LambdaFunctionScheduled'},
            {
                'id': 2,
                'previousEventId': 0,
                'stateEnteredEventDetails': {
                    'input': '{"execution_id": "foo", "user_id": '
                             '"mock-auth|1234", "cart_id": '
                             '"e8205835-2b28-4a52-89a3-10876cce5e26", "collection_uuid": '
                             '"38daf48a-46f3-4a9a-9e64-673fe78655fb", "collection_version": '
                             '"2019-02-19T213905.000000Z", "resume_token": null, "access_token": '
                             '"Bearer '
                             'mock_bearer_token"}',
                    'name': 'SendToCollectionAPI'},
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 6, 448000),
                'type': 'TaskStateEntered'},
            {
                'executionStartedEventDetails': {
                    'input': initial_mock_input,
                    'roleArn': 'arn:aws:iam::12345:role/azul-statemachine-test'},
                'id': 1,
                'previousEventId': 0,
                'timestamp': datetime.datetime(2019, 2, 19, 16, 39, 6, 423000),
                'type': 'ExecutionStarted'}]
        step_function_helper.describe_execution.return_value = dict(
            user_id='mock-auth|1234',
            status="FAILED",
            startDate=datetime.datetime(2019, 2, 19, 16, 39, 6, 423000),
            stopDate=datetime.datetime(2019, 2, 19, 16, 39, 8, 105000),
            input=initial_mock_input
        )
        step_function_helper.get_execution_history.return_value = sample_events
        service = CartExportJobManager()
        job = service.get(service.encode_token({'execution_id': 'foo'}))
        self.assertTrue(job['final'])
        self.assertIn('code', job['last_update']['error'])
        self.assertIn('cause', job['last_update']['error'])
        self.assertIn('collection_uuid', job['last_update']['state'])
        self.assertIn('collection_version', job['last_update']['state'])
        self.assertIn('when', job['last_update'])
        self.assertIn('user_id', job)
        self.assertIn('started_at', job)
        self.assertIn('stopped_at', job)
        self.assertEqual('FAILED', job['status'])

    @patch('azul.service.cart_export_job_manager.CartExportJobManager.step_function_helper')
    def test_get_on_job_succeeded(self, step_function_helper):
        initial_mock_input = json.dumps({
            "execution_id": "foo",
            "user_id": "mock-auth|1234",
            "cart_id": "e8205835-2b28-4a52-89a3-10876cce5e26",
            "collection_uuid": "38daf48a-46f3-4a9a-9e64-673fe78655fb",
            "collection_version": "2019-02-19T213905.000000Z",
            "resume_token": None,
            "access_token": "Bearer mock_bearer_token"
        })
        sample_events = [
            {
                'executionSucceededEventDetails': {
                    'output': json.dumps({
                        'execution_id': 'foo',
                        'access_token': 'Bearer mock_bearer_token',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': 'b1342920-9dbd-42eb-a73b-dcf2875b7299',
                        'collection_version': '2019-02-19T220352.707018Z',
                        'resumable': False,
                        'resume_token': None,
                        'started_at': 1550613819.834767,
                        'last_updated_at': 1550613832.8121576,
                        'exported_item_count': 260,
                        'expected_exported_item_count': 260
                    })
                },
                'id': 25,
                'previousEventId': 24,
                'timestamp': datetime.datetime(2019, 2, 19, 17, 3, 52, 955000),
                'type': 'ExecutionSucceeded'},
            {
                'id': 24,
                'previousEventId': 23,
                'stateExitedEventDetails': {
                    'name': 'SuccessState',
                    'output': json.dumps({
                        'execution_id': 'foo',
                        'access_token': 'Bearer mock_bearer_token',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': 'b1342920-9dbd-42eb-a73b-dcf2875b7299',
                        'collection_version': '2019-02-19T220352.707018Z',
                        'resumable': False,
                        'resume_token': None,
                        'started_at': 1550613819.834767,
                        'last_updated_at': 1550613832.8121576,
                        'exported_item_count': 260,
                        'expected_exported_item_count': 260
                    })
                },
                'timestamp': datetime.datetime(2019, 2, 19, 17, 3, 52, 955000),
                'type': 'SucceedStateExited'
            },
            {
                'id': 23,
                'previousEventId': 22,
                'stateEnteredEventDetails': {
                    'input': json.dumps({
                        'execution_id': 'foo',
                        'access_token': 'Bearer mock_bearer_token',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': 'b1342920-9dbd-42eb-a73b-dcf2875b7299',
                        'collection_version': '2019-02-19T220352.707018Z',
                        'resumable': False,
                        'resume_token': None,
                        'started_at': 1550613819.834767,
                        'last_updated_at': 1550613832.8121576,
                        'exported_item_count': 260,
                        'expected_exported_item_count': 260
                    }),
                    'name': 'SuccessState'},
                'timestamp': datetime.datetime(2019, 2, 19, 17, 3, 52, 955000),
                'type': 'SucceedStateEntered'}]
        step_function_helper.describe_execution.return_value = dict(
            user_id='mock-auth|1234',
            status="SUCCEEDED",
            startDate=datetime.datetime(2019, 2, 19, 16, 39, 6, 423000),
            stopDate=datetime.datetime(2019, 2, 19, 16, 39, 8, 105000),
            input=initial_mock_input
        )
        step_function_helper.get_execution_history.return_value = sample_events
        service = CartExportJobManager()
        job = service.get(service.encode_token({'execution_id': 'foo'}))
        self.assertTrue(job['final'])
        self.assertIsNone(job['last_update']['error'])
        self.assertIn('collection_uuid', job['last_update']['state'])
        self.assertIn('collection_version', job['last_update']['state'])
        self.assertIn('when', job['last_update'])
        self.assertIn('user_id', job)
        self.assertIn('started_at', job)
        self.assertIn('stopped_at', job)
        self.assertEqual('SUCCEEDED', job['status'])
        self.assertEqual(260, job['last_update']['state']['exported_item_count'])
        self.assertEqual(260, job['last_update']['state']['expected_exported_item_count'])

    @patch('azul.service.cart_export_job_manager.CartExportJobManager.step_function_helper')
    def test_get_on_job_in_progress(self, step_function_helper):
        initial_mock_input = json.dumps({
            "execution_id": "foo",
            "user_id": "mock-auth|1234",
            "cart_id": "e8205835-2b28-4a52-89a3-10876cce5e26",
            "collection_uuid": "38daf48a-46f3-4a9a-9e64-673fe78655fb",
            "collection_version": "2019-02-19T213905.000000Z",
            "resume_token": None,
            "access_token": "Bearer mock_bearer_token"
        })
        sample_events = [
            {
                'id': 24,
                'previousEventId': 23,
                'stateExitedEventDetails': {
                    'name': 'SuccessState',
                    'output': json.dumps({
                        'execution_id': 'foo',
                        'access_token': 'Bearer mock_bearer_token',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': 'b1342920-9dbd-42eb-a73b-dcf2875b7299',
                        'collection_version': '2019-02-19T220352.707018Z',
                        'resumable': False,
                        'resume_token': None,
                        'started_at': 1550613819.834767,
                        'last_updated_at': 1550613832.8121576,
                        'exported_item_count': 260,
                        'expected_exported_item_count': 260,
                    })
                },
                'timestamp': datetime.datetime(2019, 2, 19, 17, 3, 52, 955000),
                'type': 'SucceedStateExited'},
            {
                'id': 23,
                'previousEventId': 22,
                'stateEnteredEventDetails': {
                    'input': json.dumps({
                        'execution_id': 'foo',
                        'access_token': 'Bearer mock_bearer_token',
                        'user_id': 'mock-auth|1234',
                        'cart_id': 'e8205835-2b28-4a52-89a3-10876cce5e26',
                        'collection_uuid': 'b1342920-9dbd-42eb-a73b-dcf2875b7299',
                        'collection_version': '2019-02-19T220352.707018Z',
                        'resumable': False,
                        'resume_token': None,
                        'started_at': 1550613819.834767,
                        'last_updated_at': 1550613832.8121576,
                        'exported_item_count': 260,
                        'expected_exported_item_count': 260,
                    }),
                    'name': 'SuccessState'},
                'timestamp': datetime.datetime(2019, 2, 19, 17, 3, 52, 955000),
                'type': 'SucceedStateEntered'}]
        step_function_helper.describe_execution.return_value = dict(
            user_id='mock-auth|1234',
            status="RUNNING",
            startDate=datetime.datetime(2019, 2, 19, 16, 39, 6, 423000),
            stopDate=datetime.datetime(2019, 2, 19, 16, 39, 8, 105000),
            input=initial_mock_input
        )
        step_function_helper.get_execution_history.return_value = sample_events
        service = CartExportJobManager()
        job = service.get(service.encode_token({'execution_id': 'foo'}))
        self.assertFalse(job['final'])
        self.assertIsNone(job['last_update']['error'])
        self.assertIn('collection_uuid', job['last_update']['state'])
        self.assertIn('collection_version', job['last_update']['state'])
        self.assertIn('when', job['last_update'])
        self.assertIn('user_id', job)
        self.assertIn('started_at', job)
        self.assertIn('stopped_at', job)
        self.assertEqual('RUNNING', job['status'])
        # Note that the first lambda invocation will not have the following
        # information. This test simulates the later states of the execution.
        self.assertEqual(260, job['last_update']['state']['exported_item_count'])
        self.assertEqual(260, job['last_update']['state']['expected_exported_item_count'])
