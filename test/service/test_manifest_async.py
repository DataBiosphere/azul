from contextlib import (
    contextmanager,
)
import datetime
import json
from typing import (
    ContextManager,
)
from unittest import (
    mock,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

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
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.service import (
    Filters,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    GenerationFailed,
    InvalidTokenError,
    Token,
)
from azul.service.manifest_controller import (
    ManifestGenerationState,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    Manifest,
    ManifestKey,
    ManifestPartition,
    ManifestService,
    SignedManifestKey,
)
from azul_test_case import (
    AzulUnitTestCase,
    DCP1TestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


@patch.object(AsyncManifestService, '_sfn')
class TestAsyncManifestService(AzulUnitTestCase):
    execution_id = b'42'

    def test_token_encoding(self, _sfn):
        token = Token(execution_id=self.execution_id, request_index=42, retry_after=123)
        self.assertEqual(token, Token.decode(token.encode()))

    def test_token_validation(self, _sfn):
        token = Token(execution_id=self.execution_id, request_index=42, retry_after=123)
        self.assertRaises(InvalidTokenError, token.decode, token.encode()[:-1])

    def test_status_success(self, _sfn):
        """
        A successful manifest job should return a 302 status and a URL to the
        manifest
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.execution_id)
        output = {'foo': 'bar'}
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': '{"filters": {}}',
            'output': json.dumps(output)
        }
        token = Token(execution_id=self.execution_id, request_index=0, retry_after=0)
        actual_output = service.inspect_generation(token)
        self.assertEqual(output, actual_output)

    def test_status_running(self, _sfn):
        """
        A running manifest job should return a 301 status and a URL to retry
        checking the job status
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.execution_id)
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {}}'
        }
        token = Token(execution_id=self.execution_id, request_index=0, retry_after=0)
        token = service.inspect_generation(token)
        expected = Token(execution_id=self.execution_id, request_index=1, retry_after=1)
        self.assertEqual(expected, token)

    def test_status_failed(self, _sfn):
        """
        A failed manifest job should raise a GenerationFailed
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.execution_id)
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"organ": {"is": ["lymph node"]}}}',
        }
        token = Token(execution_id=self.execution_id, request_index=0, retry_after=0)
        with self.assertRaises(GenerationFailed):
            service.inspect_generation(token)


class TestManifestController(DCP1TestCase, LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    execution_id = b'42'

    @mock_sts
    @mock.patch.object(AsyncManifestService, '_sfn')
    @mock.patch.object(ManifestService, 'get_manifest')
    @mock.patch.object(ManifestService, 'get_cached_manifest')
    def test(self, get_cached_manifest, get_manifest, _sfn):
        with responses.RequestsMock() as helper:
            helper.add_passthru(str(self.base_url))
            for fetch in (True, False):
                with self.subTest(fetch=fetch):
                    format = ManifestFormat.compact
                    filters = Filters(explicit={'organ': {'is': ['lymph node']}},
                                      source_ids={self.source.id})
                    params = {
                        'catalog': self.catalog,
                        'format': format.value,
                        'filters': json.dumps(filters.explicit)
                    }
                    path = '/manifest/files'
                    object_url = 'https://url.to.manifest?foo=bar'
                    file_name = 'some_file_name'
                    manifest_key = ManifestKey(catalog=self.catalog,
                                               format=format,
                                               manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
                                               source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
                    get_cached_manifest.side_effect = CachedManifestNotFound(manifest_key)
                    signed_manifest_key = SignedManifestKey(value=manifest_key,
                                                            signature=b'123')

                    manifest_url = self.base_url.set(path=path)
                    manifest_url.path.segments.append(signed_manifest_key.encode())
                    manifest = Manifest(location=object_url,
                                        was_cached=False,
                                        format=format,
                                        manifest_key=manifest_key,
                                        file_name=file_name)
                    initial_url = self.base_url.set(path=path, args=params)
                    if fetch:
                        initial_url.path.segments.insert(0, 'fetch')

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
                                          config=[[['foo', 'bar'], {'baz': 'blah'}]],
                                          multipart_upload_id='some_upload_id',
                                          part_etags=('some_etag',),
                                          page_index=512,
                                          is_last_page=False,
                                          search_after=('foo', 'doc#bar'))
                    )
                    service: AsyncManifestService
                    service = self.app_module.app.manifest_controller.async_service
                    execution_id = manifest_key.hash
                    execution_name = service.execution_name(execution_id)
                    machine_arn = service.machine_arn
                    execution_arn = service.execution_arn(execution_name)
                    _sfn.start_execution.return_value = {
                        'executionArn': execution_arn,
                        'startDate': 123
                    }
                    url = initial_url
                    for i, expected_status in enumerate(3 * [301] + [302]):
                        response = requests.request(method='PUT' if i == 0 else 'GET',
                                                    url=str(url),
                                                    allow_redirects=False)
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
                            state: ManifestGenerationState = dict(filters=filters.to_json(),
                                                                  manifest_key=manifest_key.to_json(),
                                                                  partition=partitions[0].to_json())
                            _sfn.start_execution.assert_called_once_with(
                                stateMachineArn=machine_arn,
                                name=execution_name,
                                input=json.dumps(state)
                            )
                            _sfn.describe_execution.assert_not_called()
                            _sfn.reset_mock()
                            _sfn.describe_execution.return_value = {'status': 'RUNNING'}
                        elif i == 1:
                            get_manifest.return_value = partitions[1]
                            state = self.app_module.generate_manifest(state, None)
                            self.assertEqual(partitions[1],
                                             ManifestPartition.from_json(state['partition']))
                            get_manifest.assert_called_once_with(
                                format=format,
                                catalog=self.catalog,
                                filters=Filters.from_json(state['filters']),
                                partition=partitions[0],
                                manifest_key=ManifestKey.from_json(state['manifest_key'])
                            )
                            get_manifest.reset_mock()
                            _sfn.start_execution.assert_not_called()
                            _sfn.describe_execution.assert_called_once()
                            _sfn.reset_mock()
                            # simulate absence of output due eventual consistency
                            _sfn.describe_execution.return_value = {'status': 'SUCCEEDED'}
                        elif i == 2:
                            get_manifest.return_value = manifest
                            _sfn.start_execution.assert_not_called()
                            _sfn.describe_execution.assert_called_once()
                            _sfn.reset_mock()
                            _sfn.describe_execution.return_value = {
                                'status': 'SUCCEEDED',
                                'output': json.dumps(
                                    self.app_module.generate_manifest(state, None)
                                )
                            }
                        elif i == 3:
                            get_manifest.assert_called_once_with(
                                format=format,
                                catalog=self.catalog,
                                filters=Filters.from_json(state['filters']),
                                partition=partitions[1],
                                manifest_key=ManifestKey.from_json(state['manifest_key'])
                            )
                            get_manifest.reset_mock()
                    _sfn.start_execution.assert_not_called()
                    _sfn.describe_execution.assert_called_once()
                    expect_redirect = fetch and format is ManifestFormat.curl
                    expected_url = str(manifest_url) if expect_redirect else object_url
                    self.assertEqual(expected_url, str(url))
                    _sfn.reset_mock()
            mock_effects = [
                manifest,
                CachedManifestNotFound(manifest_key)
            ]
            with (
                mock.patch.object(ManifestService,
                                  'get_cached_manifest_with_key',
                                  side_effect=mock_effects),
                mock.patch.object(ManifestService,
                                  'verify_manifest_key',
                                  return_value=manifest_key)
            ):
                for mock_effect in mock_effects:
                    with self.subTest(mock_effect=mock_effect):
                        assert signed_manifest_key.encode() == manifest_url.path.segments[-1]
                        response = requests.get(str(manifest_url), allow_redirects=False)
                        if isinstance(mock_effect, Manifest):
                            self.assertEqual(302, response.status_code)
                            self.assertEqual(object_url, response.headers['Location'])
                        elif isinstance(mock_effect, CachedManifestNotFound):
                            self.assertEqual(410, response.status_code)
                            expected_response = {
                                'Code': 'GoneError',
                                'Message': 'The requested manifest has expired, please request a new one'
                            }
                            self.assertEqual(expected_response, response.json())
                        else:
                            assert False, mock_effect

    token = Token.first(execution_id).encode()

    def _test(self, *, expected_status, token=token):
        url = self.base_url.set(path=['fetch', 'manifest', 'files', token])
        response = requests.get(str(url))
        self.assertEqual(expected_status, response.status_code)

    @contextmanager
    def _mock_error(self, error_code: str) -> ContextManager:
        exception_cls = type(error_code, (ClientError,), {})
        with patch.object(AsyncManifestService, '_sfn') as _sfn:
            setattr(_sfn.exceptions, error_code, exception_cls)
            error_response = {
                'Error': {
                    'Code': error_code
                }
            }
            exception = exception_cls(operation_name='DescribeExecution',
                                      error_response=error_response)
            _sfn.describe_execution.side_effect = exception
            yield

    def test_execution_not_found(self):
        """
        Manifest status check should raise a BadRequestError (400 status code)
        if execution cannot be found.
        """
        with self._mock_error('ExecutionDoesNotExist'):
            self._test(expected_status=400)

    def test_boto_error(self):
        """
        Manifest status check should reraise any ClientError that is not caused
        by ExecutionDoesNotExist
        """
        with self._mock_error('ServiceQuotaExceededException'):
            self._test(expected_status=500)

    def test_execution_error(self):
        """
        Manifest status check should return a generic error (500 status code)
        if the execution errored.
        """
        with patch.object(AsyncManifestService,
                          'inspect_generation',
                          side_effect=GenerationFailed):
            self._test(expected_status=500)

    def test_invalid_token(self):
        """
        Manifest endpoint should raise a BadRequestError when given a token that
        cannot be decoded
        """
        self._test(token='Invalid base64', expected_status=400)
