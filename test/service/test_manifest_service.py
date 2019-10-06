import datetime
import json
from unittest import mock

from moto import mock_sts

from azul import config
from azul.logging import configure_test_logging
from azul.service.manifest import ManifestService
from azul.service.step_function_helper import StateMachineError, StepFunctionHelper
from azul_test_case import AzulTestCase


def setUpModule():
    configure_test_logging()


class ManifestServiceTest(AzulTestCase):

    def test_token_encoding_invertibility(self):
        """
        Parameter encoding and decoding functions should be inverse of each other
        """
        uuid = {"execution_id": "6c9dfa3f-e92e-11e8-9764-ada973595c11"}
        self.assertEqual(uuid, ManifestService.decode_token(ManifestService.encode_token(uuid)))

    def test_token_validation(self):
        token = {'no': 'id'}
        self.assertRaises(ValueError, ManifestService.decode_token, ManifestService.encode_token(token))

    # @mock_sts is required for tests calling the arn helper methods in StepFunctionHelper
    # because they require an account id

    @mock_sts
    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
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
        manifest_service = ManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        wait_time, location = manifest_service.start_or_inspect_manifest_generation('', token)
        self.assertEqual(type(wait_time), int)
        self.assertEqual(wait_time, 0)
        self.assertEqual(manifest_url, location)

    @mock_sts
    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
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
        manifest_service = ManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        retry_url = config.service_endpoint() + '/manifest/files'
        wait_time, location = manifest_service.start_or_inspect_manifest_generation(retry_url, token)
        self.assertEqual(type(wait_time), int)
        self.assertEqual(wait_time, 1)
        expected_token = manifest_service.encode_token({'execution_id': execution_id, 'request_index': 1})
        self.assertEqual(f'{retry_url}?token={expected_token}', location)

    @mock_sts
    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
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
        manifest_service = ManifestService()
        token = manifest_service.encode_token({'execution_id': execution_id})
        self.assertRaises(StateMachineError, manifest_service.start_or_inspect_manifest_generation, '', token)
