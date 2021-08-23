import datetime
import json
from typing import (
    Optional,
)
from unittest import (
    mock,
)
import unittest.result

from botocore.exceptions import (
    ClientError,
)
from furl import (
    furl,
)
from moto import (
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
from azul.logging import (
    configure_test_logging,
)
from azul.modules import (
    load_app_module,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    InvalidTokenError,
    Token,
)
from azul.service.manifest_service import (
    Manifest,
    ManifestFormat,
    ManifestPartition,
    ManifestService,
)
from azul.service.step_function_helper import (
    StateMachineError,
    StepFunctionHelper,
)
from azul_test_case import (
    AzulUnitTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


patch_step_function_helper = mock.patch('azul.service.async_manifest_service'
                                        '.AsyncManifestService.step_function_helper')
state_machine_name = 'foo'


class TestAsyncManifestService(AzulUnitTestCase):

    def test_token_encoding(self):
        token = Token(execution_id='6c9dfa3f-e92e-11e8-9764-ada973595c11',
                      request_index=42,
                      wait_time=123)
        self.assertEqual(token, Token.decode(token.encode()))

    def test_token_validation(self):
        token = Token(execution_id='6c9dfa3f-e92e-11e8-9764-ada973595c11',
                      request_index=42,
                      wait_time=123)
        self.assertRaises(InvalidTokenError, token.decode, token.encode()[-1])

    @patch_step_function_helper
    def test_status_success(self, mock_helper):
        """
        A successful manifest job should return a 302 status and a url to the manifest
        """
        execution_id = '5b1b4899-f48e-46db-9285-2d342f3cdaf2'
        helper = StepFunctionHelper()
        output = {
            'foo': 'bar'
        }
        execution_success_output = {
            'executionArn': helper.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': helper.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': '{"filters": {}}',
            'output': json.dumps(output)
        }
        mock_helper.describe_execution.return_value = execution_success_output
        manifest_service = AsyncManifestService(state_machine_name)
        token = Token(execution_id=execution_id, request_index=0, wait_time=0)
        actual_output = manifest_service.inspect_generation(token)
        self.assertEqual(output, actual_output)

    @patch_step_function_helper
    def test_status_running(self, mock_helper):
        """
        A running manifest job should return a 301 status and a url to retry checking the job status
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        helper = StepFunctionHelper()
        execution_running_output = {
            'executionArn': helper.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': helper.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {}}'
        }
        mock_helper.describe_execution.return_value = execution_running_output
        manifest_service = AsyncManifestService(state_machine_name)
        token = Token(execution_id=execution_id, request_index=0, wait_time=0)
        new_token = manifest_service.inspect_generation(token)
        expected = Token(execution_id=execution_id, request_index=1, wait_time=1)
        self.assertNotEqual(new_token, token)
        self.assertEqual(expected, new_token)

    @patch_step_function_helper
    def test_status_failed(self, mock_helper):
        """
        A failed manifest job should raise a StateMachineError
        """
        execution_id = '068579b6-9d7b-4e19-ac4e-77626851be1c'
        helper = StepFunctionHelper()
        execution_failed_output = {
            'executionArn': helper.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': helper.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"organ": {"is": ["lymph node"]}}}',
        }
        mock_helper.describe_execution.return_value = execution_failed_output
        manifest_service = AsyncManifestService(state_machine_name)
        token = Token(execution_id=execution_id, request_index=0, wait_time=0)
        self.assertRaises(StateMachineError,
                          manifest_service.inspect_generation,
                          token)


class TestManifestController(LocalAppTestCase):
    object_key = '256d82c4-685e-4326-91bf-210eece8eb6e'

    def run(self, result: Optional[unittest.result.TestResult] = None) -> Optional[unittest.result.TestResult]:
        manifest = None
        with mock.patch.object(ManifestService,
                               'get_cached_manifest',
                               return_value=(self.object_key, manifest)):
            return super().run(result)

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    @mock_sts
    @patch_step_function_helper
    @mock.patch('uuid.uuid4')
    def test(self, mock_uuid, mock_helper):
        service = load_app_module('service')
        # In a LocalAppTestCase we need the actual state machine name
        state_machine_name = config.state_machine_name(service.generate_manifest.name)
        with responses.RequestsMock() as helper:
            helper.add_passthru(str(self.base_url))
            for fetch in (True, False):
                with self.subTest(fetch=fetch):
                    execution_id = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
                    mock_uuid.return_value = execution_id
                    format_ = ManifestFormat.compact
                    filters = {'organ': {'is': ['lymph node']}}
                    params = {
                        'catalog': self.catalog,
                        'format': format_.value,
                        'filters': json.dumps(filters)
                    }
                    path = '/manifest/files'
                    object_url = 'https://url.to.manifest?foo=bar'
                    file_name = 'some_file_name'
                    object_key = f'manifests/{file_name}'
                    manifest_url = self.base_url.set(path=path,
                                                     args=dict(params, objectKey=object_key))
                    manifest = Manifest(location=object_url,
                                        was_cached=False,
                                        format_=format_,
                                        catalog=self.catalog,
                                        filters=filters,
                                        object_key=object_key,
                                        file_name=file_name)
                    url = self.base_url.set(path=path, args=params)
                    if fetch:
                        url.path.segments.insert(0, 'fetch')

                    partitions = (
                        ManifestPartition(index=0,
                                          is_last=False,
                                          file_name=None,
                                          config=None,
                                          multipart_upload_id=None,
                                          part_etags=None,
                                          page_index=None,
                                          is_last_page=None,
                                          search_after=None),
                        ManifestPartition(index=1,
                                          is_last=False,
                                          file_name=file_name,
                                          config={},
                                          multipart_upload_id='some_upload_id',
                                          part_etags=('some_etag',),
                                          page_index=512,
                                          is_last_page=False,
                                          # FIXME: Should really be a tuple
                                          #        https://github.com/databiosphere/azul/issues/3291
                                          search_after=['foo', 'doc#bar'])  # type: ignore
                    )

                    with mock.patch.object(ManifestService, 'get_manifest') as mock_get_manifest:
                        for i, expected_status in enumerate(3 * [301] + [302]):
                            response = requests.get(str(url), allow_redirects=False)
                            if fetch:
                                self.assertEqual(200, response.status_code)
                                response = response.json()
                                self.assertEqual(expected_status, response.pop('Status'))
                                headers = response
                            else:
                                self.assertEqual(expected_status, response.status_code)
                                headers = response.headers
                            if expected_status == 301:
                                self.assertGreaterEqual(int(headers['Retry-After']), 0)
                            url = furl(headers['Location'])
                            if i == 0:
                                state = dict(format_=format_.value,
                                             catalog=self.catalog,
                                             filters=filters,
                                             object_key=self.object_key,
                                             partition=partitions[0].to_json())
                                mock_helper.start_execution.assert_called_once_with(
                                    state_machine_name,
                                    execution_id,
                                    execution_input=state
                                )
                                mock_helper.describe_execution.assert_not_called()
                                mock_helper.reset_mock()
                                mock_helper.describe_execution.return_value = {'status': 'RUNNING'}
                            elif i == 1:
                                mock_get_manifest.return_value = partitions[1]
                                state = self.app_module.generate_manifest(state, None)
                                self.assertEqual(partitions[1],
                                                 ManifestPartition.from_json(state['partition']))
                                mock_get_manifest.assert_called_once_with(
                                    format_=ManifestFormat(state['format_']),
                                    catalog=state['catalog'],
                                    filters=state['filters'],
                                    partition=partitions[0],
                                    object_key=state['object_key']
                                )
                                mock_get_manifest.reset_mock()
                                mock_helper.start_execution.assert_not_called()
                                mock_helper.describe_execution.assert_called_once()
                                mock_helper.reset_mock()
                                # simulate absence of output due eventual consistency
                                mock_helper.describe_execution.return_value = {'status': 'SUCCEEDED'}
                            elif i == 2:
                                mock_get_manifest.return_value = manifest
                                mock_helper.start_execution.assert_not_called()
                                mock_helper.describe_execution.assert_called_once()
                                mock_helper.reset_mock()
                                mock_helper.describe_execution.return_value = {
                                    'status': 'SUCCEEDED',
                                    'output': json.dumps(
                                        self.app_module.generate_manifest(state, None)
                                    )
                                }
                            elif i == 3:
                                mock_get_manifest.assert_called_once_with(
                                    format_=ManifestFormat(state['format_']),
                                    catalog=state['catalog'],
                                    filters=state['filters'],
                                    partition=partitions[1],
                                    object_key=state['object_key']
                                )
                                mock_get_manifest.reset_mock()
                    mock_helper.start_execution.assert_not_called()
                    mock_helper.describe_execution.assert_called_once()
                    expected_url = str(manifest_url) if fetch else object_url
                    self.assertEqual(expected_url, str(url))
                    mock_helper.reset_mock()

            manifest_states = [manifest, None]
            with mock.patch.object(ManifestService,
                                   'get_cached_manifest_with_object_key',
                                   side_effect=manifest_states):
                for manifest in manifest_states:
                    with self.subTest(manifest=manifest):
                        expected_status = 410 if manifest is None else 302
                        self.assertEqual(object_key, manifest_url.args['objectKey'])
                        response = requests.get(manifest_url.url, allow_redirects=False)
                        self.assertEqual(expected_status, response.status_code)
                        if manifest is None:
                            msg = 'GoneError: The requested manifest has expired, please request a new one'
                            self.assertEqual(msg, response.json()['Message'])
                        else:
                            self.assertEqual(object_url, response.headers['Location'])

    params = {
        'token': Token(execution_id='7c88cc29-91c6-4712-880f-e4783e2a4d9e',
                       request_index=0,
                       wait_time=0).encode()
    }

    @patch_step_function_helper
    def test_execution_not_found(self, step_function_helper):
        """
        Manifest status check should raise a BadRequestError (400 status code)
        if execution cannot be found.
        """
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'ExecutionDoesNotExist'
            }
        }, '')
        url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
        response = requests.get(str(url))
        self.assertEqual(response.status_code, 400)

    @patch_step_function_helper
    def test_boto_error(self, step_function_helper):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        step_function_helper.describe_execution.side_effect = ClientError({
            'Error': {
                'Code': 'OtherError'
            }
        }, '')
        url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
        response = requests.get(str(url))
        self.assertEqual(response.status_code, 500)

    @patch_step_function_helper
    def test_execution_error(self, step_function_helper):
        """
        Manifest status check should return a generic error (500 status code)
        if the execution errored.
        """
        step_function_helper.get_manifest_status.side_effect = StateMachineError
        url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
        response = requests.get(str(url))
        self.assertEqual(response.status_code, 500)

    def test_invalid_token(self):
        """
        Manifest endpoint should raise a BadRequestError when given a token that cannot be decoded
        """
        params = {'token': 'Invalid base64'}
        url = self.base_url.set(path='/fetch/manifest/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(response.status_code, 400)
