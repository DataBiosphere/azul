import json
import logging
from unittest import mock

from botocore.exceptions import ClientError
from chalice import BadRequestError, ChaliceViewError
from moto import mock_s3, mock_sts
import requests

from azul import config
from azul.service.step_function_helper import StateMachineError
from azul.service.responseobjects.storage_service import StorageService
from service import WebServiceTestCase


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class ManifestEndpointTest(WebServiceTestCase):

    @mock_s3
    @mock_sts
    def test_manifest_generation(self):
        """
        The lambda function should create a valid manifest and upload it to s3
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import generate_manifest
        storage_service = StorageService()
        storage_service.create_bucket()
        manifest_url = generate_manifest(event={}, context=None)['Location']
        manifest_response = requests.get(manifest_url)
        self.assertEqual(200, manifest_response.status_code)
        self.assertTrue(len(manifest_response.text) > 0)

    @mock_sts
    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    @mock.patch('uuid.uuid4')
    def test_manifest_endpoint_start_execution(self, mock_uuid, current_request, step_function_helper):
        """
        Calling start manifest generation without a token should start an execution and return a response
        with Retry-After and Location in the headers
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import start_manifest_generation
        execution_name = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
        mock_uuid.return_value = execution_name
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        filters = {'file': {'organ': {'is': ['lymph node']}}}
        current_request.query_params = {'filters': json.dumps(filters), 'format': format}
        response = start_manifest_generation()
        self.assertEqual(301, response.status_code)
        self.assertIn('Retry-After', response.headers)
        self.assertIn('Location', response.headers)
        step_function_helper.start_execution.assert_called_once_with(config.manifest_state_machine_name,
                                                                     execution_name,
                                                                     execution_input={'filters': filters})
        step_function_helper.describe_execution.assert_called_once()

    @mock_sts
    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    @mock.patch('uuid.uuid4')
    def test_manifest_endpoint_start_execution_browser(self, mock_uuid, current_request, step_function_helper):
        """
        Calling start manifest generation with the browser parameter should return the status code,
        Retry-After, and Location in the body of a 200 response instead of the headers
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import start_manifest_generation_fetch
        execution_name = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
        mock_uuid.return_value = execution_name
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        filters = {'file': {'organ': {'is': ['lymph node']}}}
        current_request.query_params = {'filters': json.dumps(filters), 'format': format}
        response = start_manifest_generation_fetch()
        self.assertEqual(301, response['Status'])
        self.assertIn('Retry-After', response)
        self.assertIn('Location', response)
        step_function_helper.start_execution.assert_called_once_with(config.manifest_state_machine_name,
                                                                     execution_name,
                                                                     execution_input={'filters': filters})
        step_function_helper.describe_execution.assert_called_once()

    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_check_status(self, current_request, step_function_helper):
        """
        Calling start manifest generation with a token should check the status without starting an execution
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import handle_manifest_generation_request
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.return_value = {'status': 'RUNNING'}
        handle_manifest_generation_request()
        step_function_helper.start_execution.assert_not_called()
        step_function_helper.describe_execution.assert_called_once()

    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_execution_not_found(self, current_request, step_function_helper):
        """
        Manifest status check should raise a BadRequestError (400 status code) if execution cannot be found
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import handle_manifest_generation_request
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'ExecutionDoesNotExist'
            }
        }, '')
        self.assertRaises(BadRequestError, handle_manifest_generation_request)

    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_boto_error(self, current_request, step_function_helper):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import handle_manifest_generation_request
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'OtherError'
            }
        }, '')
        self.assertRaises(ClientError, handle_manifest_generation_request)

    @mock.patch('azul.service.manifest.ManifestService.step_function_helper')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_execution_error(self, current_request, step_function_helper):
        """
        Manifest status check should return a generic error (500 status code) if the execution errored
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import handle_manifest_generation_request
        current_request.query_params = {
            'token': 'eyJleGVjdXRpb25faWQiOiAiN2M4OGNjMjktOTFjNi00NzEyLTg4MGYtZTQ3ODNlMmE0ZDllIn0='
        }
        step_function_helper.get_manifest_status.side_effect = StateMachineError
        self.assertRaises(ChaliceViewError, handle_manifest_generation_request)

    @mock.patch('lambdas.service.app.app.current_request')
    def test_manifest_endpoint_invalid_token(self, current_request):
        """
        Manifest endpoint should raise a BadRequestError when given a token that cannot be decoded
        """
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import handle_manifest_generation_request
        current_request.query_params = {'token': 'Invalid base64'}
        self.assertRaises(BadRequestError, handle_manifest_generation_request)
