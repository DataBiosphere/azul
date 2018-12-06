import datetime
import json
import urllib.parse
from unittest import mock, TestCase

from moto import mock_sts

from azul import config
from azul.service.responseobjects.manifest_service import ManifestService
from azul.service.responseobjects.step_function_helper import StepFunctionHelper, StateMachineError


def extract_and_decode_token(url):
    token = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)['token'][0]
    return ManifestService().decode_params(token)


class ManifestServiceTest(TestCase):
    # @mock_sts is required for tests calling the arn helper methods in StepFunctionHelper
    # because they require an account id

    def test_param_encoding_invertibility(self):
        """
        Parameter encoding and decoding functions should be inverse of each other
        """
        uuid = {"uuid": "6c9dfa3f-e92e-11e8-9764-ada973595c11"}
        self.assertEqual(uuid, ManifestService().decode_params(ManifestService().encode_params(uuid)))

        encoding = 'IjRkMWE4MGQxLWU5MmUtMTFlOC1iYzc2LWY5NTQ3MzRjNmU5YiI='
        self.assertEqual(encoding, ManifestService().encode_params(ManifestService().decode_params(encoding)))

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
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
            'input': '{"filters": {"file": {}}}',
            'output': json.dumps({'Location': manifest_url})
        }
        step_function_helper.describe_execution.return_value = execution_success_output
        manifest_service = ManifestService()
        token = manifest_service.encode_params({'execution_id': execution_id})
        response = manifest_service.get_manifest_status(token, '', False)
        self.assertEqual(302, response.status_code)
        self.assertEqual({'Location': manifest_url}, response.headers)

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
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
            'input': '{"filters": {"file": {}}}'
        }
        step_function_helper.describe_execution.return_value = execution_running_output
        manifest_service = ManifestService()
        token = manifest_service.encode_params({'execution_id': execution_id})
        retry_url = config.service_endpoint() + '/manifest/files'
        response = manifest_service.get_manifest_status(token, retry_url, False)
        self.assertEqual(301, response.status_code)
        self.assertIn('Retry-After', response.headers)
        self.assertTrue(response.headers['Location'].startswith(f'{retry_url}?token='))
        expected_params = {'execution_id': execution_id, 'wait': 1}
        self.assertEqual(expected_params, extract_and_decode_token(response.headers['Location']))

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    def test_manifest_status_running_browser_request(self, step_function_helper):
        """
        A running manifest job should return a 200 response with a 301 status and a url with a token containing the
        browser parameter when the browser_request argument is True
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        execution_running_output = {
            'executionArn': StepFunctionHelper().execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionHelper().state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {"file": {}}}'
        }
        step_function_helper.describe_execution.return_value = execution_running_output
        manifest_service = ManifestService()
        token = manifest_service.encode_params({'execution_id': execution_id})
        retry_url = config.service_endpoint() + '/manifest/files'
        response = manifest_service.get_manifest_status(token, retry_url, True)
        self.assertEqual(200, response.status_code)
        self.assertEqual(301, response.body['Status'])
        self.assertIn('Retry-After', response.body)
        self.assertTrue(response.body['Location'].startswith(f'{retry_url}?token='))
        expected_params = {'execution_id': execution_id, 'wait': 1, 'browser': True}
        self.assertEqual(expected_params, extract_and_decode_token(response.body['Location']))

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    def test_manifest_status_running_browser_in_token(self, step_function_helper):
        """
        A running manifest job should return a 200 response with a 301 status and a url with a token containing the
        browser parameter when the browser_request argument is False and the browser parameter is encoded in the token
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        execution_running_output = {
            'executionArn': StepFunctionHelper().execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionHelper().state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {"file": {}}}'
        }
        step_function_helper.describe_execution.return_value = execution_running_output
        manifest_service = ManifestService()
        token = manifest_service.encode_params({'execution_id': execution_id, 'browser': True})
        retry_url = config.service_endpoint() + '/manifest/files'
        response = manifest_service.get_manifest_status(token, retry_url, False)
        self.assertEqual(200, response.status_code)
        self.assertEqual(301, response.body['Status'])
        self.assertIn('Retry-After', response.body)
        self.assertTrue(response.body['Location'].startswith(f'{retry_url}?token='))
        expected_params = {'execution_id': execution_id, 'wait': 1, 'browser': True}
        self.assertEqual(expected_params, extract_and_decode_token(response.body['Location']))

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
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
            'input': '{"filters": {"file": {"organ": {"is": ["lymph node"]}}}}',
        }
        step_function_helper.describe_execution.return_value = execution_failed_output
        manifest_service = ManifestService()
        token = manifest_service.encode_params({'execution_id': execution_id})
        self.assertRaises(StateMachineError, manifest_service.get_manifest_status, token, '', False)
