import datetime
from unittest import mock

import requests
from botocore.exceptions import ClientError
from chalice import BadRequestError, ChaliceViewError
from moto import mock_s3, mock_sts

from azul import config
from azul.service.responseobjects.manifest_service import ManifestService
from azul.service.responseobjects.step_function_client import StepFunctionClient, StateMachineError
from azul.service.responseobjects.storage_service import StorageService
from lambdas.service.app import generate_manifest, start_manifest_generation
from service import WebServiceTestCase


class ManifestTest(WebServiceTestCase):

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService')
    def test_manifest_endpoint_start_execution(self, MockManifestService):
        """
        Calling start manifest generation without a token should start an execution and check the status
        """
        MockManifestService.return_value.start_manifest_generation.return_value = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
        filters = {'file': {'organ': {'is': ['lymph node']}}}
        start_manifest_generation(filters=filters,
                                  manifest_service_class=MockManifestService)
        MockManifestService().start_manifest_generation.assert_called_once_with(filters)
        MockManifestService().get_manifest_status.assert_called_once()

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService')
    def test_manifest_endpoint_check_status(self, MockManifestService):
        """
        Calling start manifest generation with a token should check the status without starting an execution
        """
        start_manifest_generation(token='eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0=',
                                  manifest_service_class=MockManifestService)
        MockManifestService().start_manifest_generation.assert_not_called()
        MockManifestService().get_manifest_status.assert_called_once()

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService')
    def test_manifest_endpoint_execution_not_found(self, MockManifestService):
        """
        Manifest status check should raise a BadRequestError (400 status code) if execution cannot be found
        """
        MockManifestService.return_value.get_manifest_status.side_effect = ClientError({
            'Error': {
                'Code': 'ExecutionDoesNotExist'
            }
        }, '')
        self.assertRaises(BadRequestError,
                          start_manifest_generation,
                          token='eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0=',
                          manifest_service_class=MockManifestService)

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService')
    def test_manifest_endpoint_boto_error(self, MockManifestService):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        MockManifestService.return_value.get_manifest_status.side_effect = ClientError({
            'Error': {
                'Code': 'OtherError'
            }
        }, '')
        self.assertRaises(ClientError,
                          start_manifest_generation,
                          token='eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0=',
                          manifest_service_class=MockManifestService)

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService')
    def test_manifest_endpoint_execution_error(self, MockManifestService):
        """
        Manifest status check should return a generic error (500 status code) if the execution errored
        """
        MockManifestService.return_value.get_manifest_status.side_effect = StateMachineError
        self.assertRaises(ChaliceViewError,
                          start_manifest_generation,
                          token='eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0=',
                          manifest_service_class=MockManifestService)

    @mock_s3
    @mock_sts
    def test_manifest_generation(self):
        """
        The lambda function should create a valid manifest and upload it to s3
        """
        storage_service = StorageService()
        storage_service.create_bucket()
        manifest_url = generate_manifest(event={}, context=None)
        manifest_response = requests.get(manifest_url)
        self.assertEqual(200, manifest_response.status_code)
        self.assertTrue(len(manifest_response.text) > 0)

    def test_param_encoding_invertibility(self):
        """
        Parameter encoding and decoding functions should be inverse of each other
        """
        uuid = '{"uuid": "6c9dfa3f-e92e-11e8-9764-ada973595c11"}'
        self.assertEqual(uuid, ManifestService.decode_params(ManifestService.encode_params(uuid)))

        encoding = 'IjRkMWE4MGQxLWU5MmUtMTFlOC1iYzc2LWY5NTQ3MzRjNmU5YiI='
        self.assertEqual(encoding, ManifestService.encode_params(ManifestService.decode_params(encoding)))

    @mock_sts
    @mock.patch('azul.service.responseobjects.step_function_client.StepFunctionClient')
    def test_start_manifest_generation(self, MockStepFunctions):
        """
        Starting manifest generation should return the name of the execution
        """
        execution_id = '9e53a9da-e8d5-4fc5-948e-6bd1e771d6a1'
        execution_start_output = {
            'executionArn': StepFunctionClient.execution_arn(config.manifest_state_machine_name, execution_id),
            'startDate': datetime.datetime(2018, 11, 16, 12, 29, 12, 474000)
        }
        MockStepFunctions.start_execution.return_value = execution_start_output
        manifest_service = ManifestService(MockStepFunctions)
        self.assertEqual('9e53a9da-e8d5-4fc5-948e-6bd1e771d6a1',
                         manifest_service.start_manifest_generation({'file': {}}))

    @mock_sts
    @mock.patch('azul.service.responseobjects.step_function_client.StepFunctionClient')
    def test_manifest_status_success(self, MockStepFunctions):
        """
        A successful manifest job shouuld return a 302 status and a url to the manifest
        """
        manifest_url = '"https://url.to.manifest"'
        execution_id = '5b1b4899-f48e-46db-9285-2d342f3cdaf2'
        execution_success_output = {
            'executionArn': StepFunctionClient.execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionClient.state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': '{"filters": {"file": {}}}',
            'output': manifest_url
        }
        MockStepFunctions.return_value.describe_execution.return_value = execution_success_output
        params = {'execution_id': execution_id}
        manifest_service = ManifestService(MockStepFunctions())
        response = manifest_service.get_manifest_status(params, 1)
        expected_output = {
            'Status': 302,
            'Location': manifest_url[1:-1]  # quotation marks should not be in the location string
        }
        self.assertEqual(expected_output, response)

    @mock_sts
    @mock.patch('azul.service.responseobjects.step_function_client.StepFunctionClient')
    def test_manifest_status_running(self, MockStepFunctions):
        """
        A running manifest job should return a 301 status and a url to retry checking the job status
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        execution_running_output = {
            'executionArn': StepFunctionClient.execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionClient.state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {"file": {}}}'
        }
        MockStepFunctions.return_value.describe_execution.return_value = execution_running_output
        params = {'execution_id': execution_id}
        manifest_service = ManifestService(MockStepFunctions())
        response = manifest_service.get_manifest_status(params, 1)
        self.assertEqual(301, response['Status'])
        self.assertTrue(response['Location'].startswith(
            f'{config.service_endpoint()}{ManifestService.manifest_endpoint}'))

    @mock_sts
    @mock.patch('azul.service.responseobjects.step_function_client.StepFunctionClient')
    def test_manifest_status_failed(self, MockStepFunctions):
        """
        A failed manifest job should raise a StateMachineError
        """
        execution_id = '068579b6-9d7b-4e19-ac4e-77626851be1c'
        execution_failed_output = {
            'executionArn': StepFunctionClient.execution_arn(config.manifest_state_machine_name, execution_id),
            'stateMachineArn': StepFunctionClient.state_machine_arn(config.manifest_state_machine_name),
            'name': execution_id,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"file": {"organ": {"is": ["lymph node"]}}}}',
        }
        MockStepFunctions.return_value.describe_execution.return_value = execution_failed_output
        params = {'execution_id': execution_id}
        manifest_service = ManifestService(MockStepFunctions())
        self.assertRaises(StateMachineError, manifest_service.get_manifest_status, params, 1)
