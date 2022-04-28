import datetime
import json
from typing import (
    Optional,
)
from unittest import (
    mock,
)
from unittest.mock import (
    patch,
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
from azul.service import (
    Filters,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    InvalidTokenError,
    StateMachineError,
    Token,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    CachedManifestSourcesChanged,
    Manifest,
    ManifestFormat,
    ManifestPartition,
    ManifestService,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from service import (
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


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

    def test_status_success(self):
        """
        A successful manifest job should return a 302 status and a url to the manifest
        """
        execution_id = '5b1b4899-f48e-46db-9285-2d342f3cdaf2'
        output = {
            'foo': 'bar'
        }
        service = AsyncManifestService(state_machine_name)
        execution_success_output = {
            'executionArn': service.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': service.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': '{"filters": {}}',
            'output': json.dumps(output)
        }
        with patch.object(type(service), '_describe_execution') as mock:
            mock.return_value = execution_success_output
            token = Token(execution_id=execution_id, request_index=0, wait_time=0)
            actual_output = service.inspect_generation(token)
        self.assertEqual(output, actual_output)

    def test_status_running(self):
        """
        A running manifest job should return a 301 status and a url to retry checking the job status
        """
        execution_id = 'd4ee1bed-0bd7-4c11-9c86-372e07801536'
        service = AsyncManifestService(state_machine_name)
        execution_running_output = {
            'executionArn': service.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': service.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {}}'
        }
        with patch.object(type(service), '_describe_execution') as mock:
            mock.return_value = execution_running_output
            token = Token(execution_id=execution_id, request_index=0, wait_time=0)
            new_token = service.inspect_generation(token)
        expected = Token(execution_id=execution_id, request_index=1, wait_time=1)
        self.assertNotEqual(new_token, token)
        self.assertEqual(expected, new_token)

    def test_status_failed(self):
        """
        A failed manifest job should raise a StateMachineError
        """
        execution_id = '068579b6-9d7b-4e19-ac4e-77626851be1c'
        service = AsyncManifestService(state_machine_name)
        execution_failed_output = {
            'executionArn': service.execution_arn(state_machine_name, execution_id),
            'stateMachineArn': service.state_machine_arn(state_machine_name),
            'name': execution_id,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"organ": {"is": ["lymph node"]}}}',
        }
        with patch.object(type(service), '_describe_execution') as mock:
            mock.return_value = execution_failed_output
            token = Token(execution_id=execution_id, request_index=0, wait_time=0)
            self.assertRaises(StateMachineError,
                              service.inspect_generation,
                              token)


@patch_dss_source
@patch_source_cache
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
    @mock.patch('uuid.uuid4')
    @mock.patch.object(AsyncManifestService, '_start_execution')
    @mock.patch.object(AsyncManifestService, '_describe_execution')
    def test(self, mock_describe_execution, mock_start_execution, mock_uuid):
        service = load_app_module('service', unit_test=True)
        # In a LocalAppTestCase we need the actual state machine name
        state_machine_name = config.state_machine_name(service.generate_manifest.name)

        def reset():
            mock_start_execution.reset_mock()
            mock_describe_execution.reset_mock()

        with responses.RequestsMock() as helper:
            helper.add_passthru(str(self.base_url))
            for fetch in (True, False):
                with self.subTest(fetch=fetch):
                    execution_id = '6c9dfa3f-e92e-11e8-9764-ada973595c11'
                    mock_uuid.return_value = execution_id
                    format_ = ManifestFormat.compact
                    filters = Filters(explicit={'organ': {'is': ['lymph node']}},
                                      source_ids={'6aaf72a6-0a45-5886-80cf-48f8d670dc26'})
                    params = {
                        'catalog': self.catalog,
                        'format': format_.value,
                        'filters': json.dumps(filters.explicit)
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
                                          search_after=('foo', 'doc#bar'))
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
                                             filters=filters.to_json(),
                                             object_key=self.object_key,
                                             authentication=None,
                                             partition=partitions[0].to_json())
                                mock_start_execution.assert_called_once_with(
                                    state_machine_name,
                                    execution_id,
                                    execution_input=state
                                )
                                mock_describe_execution.assert_not_called()
                                reset()
                                mock_describe_execution.return_value = {'status': 'RUNNING'}
                            elif i == 1:
                                mock_get_manifest.return_value = partitions[1]
                                state = self.app_module.generate_manifest(state, None)
                                self.assertEqual(partitions[1],
                                                 ManifestPartition.from_json(state['partition']))
                                mock_get_manifest.assert_called_once_with(
                                    format_=ManifestFormat(state['format_']),
                                    catalog=state['catalog'],
                                    filters=Filters.from_json(state['filters']),
                                    partition=partitions[0],
                                    authentication=None,
                                    object_key=state['object_key']
                                )
                                mock_get_manifest.reset_mock()
                                mock_start_execution.assert_not_called()
                                mock_describe_execution.assert_called_once()
                                reset()
                                # simulate absence of output due eventual consistency
                                mock_describe_execution.return_value = {'status': 'SUCCEEDED'}
                            elif i == 2:
                                mock_get_manifest.return_value = manifest
                                mock_start_execution.assert_not_called()
                                mock_describe_execution.assert_called_once()
                                reset()
                                mock_describe_execution.return_value = {
                                    'status': 'SUCCEEDED',
                                    'output': json.dumps(
                                        self.app_module.generate_manifest(state, None)
                                    )
                                }
                            elif i == 3:
                                mock_get_manifest.assert_called_once_with(
                                    format_=ManifestFormat(state['format_']),
                                    catalog=state['catalog'],
                                    filters=Filters.from_json(state['filters']),
                                    partition=partitions[1],
                                    authentication=None,
                                    object_key=state['object_key']
                                )
                                mock_get_manifest.reset_mock()
                    mock_start_execution.assert_not_called()
                    mock_describe_execution.assert_called_once()
                    expected_url = str(manifest_url) if fetch else object_url
                    self.assertEqual(expected_url, str(url))
                    reset()

            manifest_states = [
                manifest,
                CachedManifestNotFound,
                CachedManifestSourcesChanged
            ]
            with mock.patch.object(ManifestService,
                                   'get_cached_manifest_with_object_key',
                                   side_effect=manifest_states):
                for manifest in manifest_states:
                    with self.subTest(manifest=manifest):
                        self.assertEqual(object_key, manifest_url.args['objectKey'])
                        response = requests.get(str(manifest_url), allow_redirects=False)
                        if isinstance(manifest, Manifest):
                            self.assertEqual(302, response.status_code)
                            self.assertEqual(object_url, response.headers['Location'])
                        else:
                            if manifest is CachedManifestNotFound:
                                cause = 'expired'
                            elif manifest is CachedManifestSourcesChanged:
                                cause = 'become invalid due to an authorization change'
                            else:
                                assert False
                            expected_response = {
                                'Code': 'GoneError',
                                'Message': f'The requested manifest has {cause}, please request a new one'
                            }
                            self.assertEqual(410, response.status_code)
                            self.assertEqual(expected_response, response.json())

    params = {
        'token': Token(execution_id='7c88cc29-91c6-4712-880f-e4783e2a4d9e',
                       request_index=0,
                       wait_time=0).encode()
    }

    def test_execution_not_found(self):
        """
        Manifest status check should raise a BadRequestError (400 status code)
        if execution cannot be found.
        """
        with patch.object(AsyncManifestService, '_describe_execution') as mock:
            mock.side_effect = ClientError({
                'Error': {
                    'Code': 'ExecutionDoesNotExist'
                }
            }, '')
            url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
            response = requests.get(str(url))
        self.assertEqual(400, response.status_code)

    def test_boto_error(self):
        """
        Manifest status check should reraise any ClientError that is not caused by ExecutionDoesNotExist
        """
        with patch.object(AsyncManifestService, '_describe_execution') as mock:
            mock.side_effect = ClientError({
                'Error': {
                    'Code': 'OtherError'
                }
            }, '')
            url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
            response = requests.get(str(url))
        self.assertEqual(500, response.status_code)

    def test_execution_error(self):
        """
        Manifest status check should return a generic error (500 status code)
        if the execution errored.
        """
        with patch.object(AsyncManifestService, 'inspect_generation') as mock:
            mock.side_effect = StateMachineError
            url = self.base_url.set(path='/fetch/manifest/files', args=self.params)
            response = requests.get(str(url))
        self.assertEqual(500, response.status_code)

    def test_invalid_token(self):
        """
        Manifest endpoint should raise a BadRequestError when given a token that cannot be decoded
        """
        params = {'token': 'Invalid base64'}
        url = self.base_url.set(path='/fetch/manifest/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code)
