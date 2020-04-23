import datetime
import json
from typing import Optional
from unittest import mock
import unittest.result

from botocore.exceptions import ClientError
from moto import mock_sts
import requests

from app_test_case import LocalAppTestCase
from azul import config
from azul.logging import configure_test_logging
from azul.service.async_manifest_service import AsyncManifestService
from azul.service.step_function_helper import (
    StateMachineError,
    StepFunctionHelper,
)
from azul_test_case import AzulTestCase
from retorts import ResponsesHelper


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


patch_step_function_helper = mock.patch('azul.service.async_manifest_service.AsyncManifestService.step_function_helper')


class TestAsyncManifestService(AzulTestCase):

    def test_token_encoding_invertibility(self):
        """
        Parameter encoding and decoding functions should be inverse of each other
        """
        uuid = {"execution_id": "6c9dfa3f-e92e-11e8-9764-ada973595c11"}
        self.assertEqual(uuid, AsyncManifestService.decode_token(AsyncManifestService.encode_token(uuid)))

    def test_token_validation(self):
        token = {'no': 'id'}
        self.assertRaises(ValueError, AsyncManifestService.decode_token, AsyncManifestService.encode_token(token))

    # @mock_sts is required for tests calling the arn helper methods in StepFunctionHelper
    # because they require an account id

    @mock_sts
    @patch_step_function_helper
    def test_manifest_status_success(self, step_function_helper):
        """
        A successful manifest job should return a 302 status and a url to the manifest
        """
        manifest_url = 'https://url.to.manifest'
        execution_id = '5b1b4899-f48e-46db-9285-2d342f3cdaf2'
        execution_success_output = {
            'executionArn': StepFunctionHelper().execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionHelper().state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': '{"filters": {}}',
            'output': json.dumps({'Location': manifest_url})
        }
        step_function_helper.describe_execution.return_value = execution_success_output
        manifest_service = AsyncManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        format_ = 'compact'
        filters = manifest_service.parse_filters('{}')
        wait_time, location = manifest_service.start_or_inspect_manifest_generation('', format_, filters, token)
        self.assertEqual(type(wait_time), int)
        self.assertEqual(wait_time, 0)
        self.assertEqual(manifest_url, location)

    @mock_sts
    @patch_step_function_helper
    def test_manifest_status_running(self, step_function_helper):
        """
        A running manifest job should return a 301 status and a url to retry checking the job status
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        execution_running_output = {
            'executionArn': StepFunctionHelper().execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionHelper().state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {}}'
        }
        step_function_helper.describe_execution.return_value = execution_running_output
        manifest_service = AsyncManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        retry_url = config.service_endpoint() + '/manifest/files'
        format_ = 'compact'
        filters = manifest_service.parse_filters('{}')
        wait_time, location = manifest_service.start_or_inspect_manifest_generation(retry_url, format_, filters, token)
        self.assertEqual(type(wait_time), int)
        self.assertEqual(wait_time, 1)
        expected_token = manifest_service.encode_token({'execution_id': execution_id, 'request_index': 1})
        self.assertEqual(f'{retry_url}?token={expected_token}', location)

    @mock_sts
    @patch_step_function_helper
    def test_manifest_status_failed(self, step_function_helper):
        """
        A failed manifest job should raise a StateMachineError
        """
        execution_id = '068579b6-9d7b-4e19-ac4e-77626851be1c'
        execution_failed_output = {
            'executionArn': StepFunctionHelper().execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionHelper().state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"organ": {"is": ["lymph node"]}}}',
        }
        step_function_helper.describe_execution.return_value = execution_failed_output
        manifest_service = AsyncManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        format_ = 'compact'
        filters = manifest_service.parse_filters('{}')
        self.assertRaises(StateMachineError,
                          manifest_service.start_or_inspect_manifest_generation, '', format_, filters, token)


class TestAsyncManifestServiceEndpoints(LocalAppTestCase):

    def run(self, result: Optional[unittest.result.TestResult] = None) -> Optional[unittest.result.TestResult]:
        # Suppress generate manifests functionality to prevent false assertion positives
        with mock.patch('azul.service.manifest_service.ManifestService.__init__') as __init__:
            __init__.return_value = None
            with mock.patch('azul.service.manifest_service.ManifestService.get_cached_manifest') as get_cached_manifest:
                get_cached_manifest.return_value = None, None
                return super().run(result)

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    patch_current_request = mock.patch('lambdas.service.app.app.current_request')

    @mock_sts
    @patch_step_function_helper
    @mock.patch('uuid.uuid4')
    def test_manifest_endpoint_start_execution(self, mock_uuid, step_function_helper):
        """
        Calling start manifest generation without a token should start an
        execution and return a response with Retry-After and Location in the
        headers.
        """
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            for fetch in True, False:
                with self.subTest(fetch=fetch):
                    execution_name = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
                    mock_uuid.return_value = execution_name
                    step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
                    format_ = 'compact'
                    filters = {'organ': {'is': ['lymph node']}}
                    params = {'filters': json.dumps(filters), 'format': format_}
                    if fetch:
                        response = requests.get(self.base_url + '/fetch/manifest/files',
                                                params=params)
                        response.raise_for_status()
                        response = response.json()
                    else:
                        response = requests.get(self.base_url + '/manifest/files',
                                                params=params,
                                                allow_redirects=False)
                    self.assertEqual(301, response['Status'] if fetch else response.status_code)
                    self.assertIn('Retry-After', response if fetch else response.headers)
                    self.assertIn('Location', response if fetch else response.headers)
                    step_function_helper.start_execution.assert_called_once_with(config.manifest_state_machine_name,
                                                                                 execution_name,
                                                                                 execution_input=dict(format=format_,
                                                                                                      filters=filters,
                                                                                                      object_key=None))
                    step_function_helper.describe_execution.assert_called_once()
                    step_function_helper.reset_mock()

    @patch_step_function_helper
    def test_manifest_endpoint_check_status(self, step_function_helper):
        """
        Calling start manifest generation with a token should check the status
        without starting an execution.
        """
        params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        response = requests.get(self.base_url + '/fetch/manifest/files', params=params)
        response.raise_for_status()
        step_function_helper.start_execution.assert_not_called()
        step_function_helper.describe_execution.assert_called_once()

    @patch_step_function_helper
    def test_manifest_endpoint_execution_not_found(self, step_function_helper):
        """
        Manifest status check should raise a BadRequestError (400 status code)
        if execution cannot be found.
        """
        params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'ExecutionDoesNotExist'
            }
        }, '')
        response = requests.get(self.base_url + '/fetch/manifest/files', params=params)
        self.assertEqual(response.status_code, 400)

    @patch_step_function_helper
    @patch_current_request
    def test_manifest_endpoint_boto_error(self, _current_request, step_function_helper):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'OtherError'
            }
        }, '')
        response = requests.get(self.base_url + '/fetch/manifest/files', params=params)
        self.assertEqual(response.status_code, 500)

    @patch_step_function_helper
    @patch_current_request
    def test_manifest_endpoint_execution_error(self, _current_request, step_function_helper):
        """
        Manifest status check should return a generic error (500 status code)
        if the execution errored.
        """
        params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.get_manifest_status.side_effect = StateMachineError
        response = requests.get(self.base_url + '/fetch/manifest/files', params=params)
        self.assertEqual(response.status_code, 500)

    @patch_current_request
    def test_manifest_endpoint_invalid_token(self, _current_request):
        """
        Manifest endpoint should raise a BadRequestError when given a token that cannot be decoded
        """
        params = {'token': 'Invalid base64'}
        response = requests.get(self.base_url + '/fetch/manifest/files', params=params)
        self.assertEqual(response.status_code, 400)
