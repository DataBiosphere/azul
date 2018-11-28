import json
from unittest import mock

import requests
from botocore.exceptions import ClientError
from chalice import BadRequestError, ChaliceViewError
from moto import mock_s3, mock_sts

from azul import config
from azul.service.responseobjects.step_function_helper import StateMachineError
from azul.service.responseobjects.storage_service import StorageService
from lambdas.service.app import generate_manifest, start_manifest_generation
from service import WebServiceTestCase


class ManifestTest(WebServiceTestCase):

    @mock_s3
    @mock_sts
    def test_manifest_generation(self):
        """
        The lambda function should create a valid manifest and upload it to s3
        """
        storage_service = StorageService()
        storage_service.create_bucket()
        manifest_url = generate_manifest(event={}, context=None)['Location']
        manifest_response = requests.get(manifest_url)
        self.assertEqual(200, manifest_response.status_code)
        self.assertTrue(len(manifest_response.text) > 0)

    @mock_sts
    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    @mock.patch('uuid.uuid4')
    def test_manifest_endpoint_start_execution(self, mock_uuid, current_request, step_function_helper):
        """
        Calling start manifest generation without a token should start an execution and check the status
        """
        execution_name = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
        mock_uuid.return_value = execution_name
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        filters = {'file': {'organ': {'is': ['lymph node']}}}
        current_request.query_params = {'filters': json.dumps(filters)}
        execution_status = start_manifest_generation()
        self.assertEqual(301, execution_status['Status'])
        step_function_helper.start_execution.assert_called_once_with(config.manifest_state_machine_name,
                                                                     execution_name,
                                                                     execution_input={'filters': filters})
        step_function_helper.describe_execution.assert_called_once()

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_check_status(self, current_request, step_function_helper):
        """
        Calling start manifest generation with a token should check the status without starting an execution
        """
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        start_manifest_generation()
        step_function_helper.start_execution.assert_not_called()
        step_function_helper.describe_execution.assert_called_once()

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_execution_not_found(self, current_request, step_function_helper):
        """
        Manifest status check should raise a BadRequestError (400 status code) if execution cannot be found
        """
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'ExecutionDoesNotExist'
            }
        }, '')
        self.assertRaises(BadRequestError, start_manifest_generation)

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_boto_error(self, current_request, step_function_helper):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'OtherError'
            }
        }, '')
        self.assertRaises(ClientError, start_manifest_generation)

    @mock.patch('azul.service.responseobjects.manifest_service.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_execution_error(self, current_request, step_function_helper):
        """
        Manifest status check should return a generic error (500 status code) if the execution errored
        """
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.get_manifest_status.side_effect = StateMachineError
        self.assertRaises(ChaliceViewError, start_manifest_generation)

    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_invalid_token(self, current_request):
        """
        Manifest endpoint should raise a BadRequestError when given a token that cannot be decoded
        """
        current_request.query_params = {'token': 'Invalid base64'}
        self.assertRaises(BadRequestError, start_manifest_generation)
