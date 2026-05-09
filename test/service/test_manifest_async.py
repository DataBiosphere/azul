from contextlib import (
    contextmanager,
)
import datetime
from functools import (
    wraps,
)
from itertools import (
    product,
)
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
    mock_aws,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul.filters import (
    Filters,
    FiltersJSON,
)
from azul.lib.types import (
    JSON,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.service.async_manifest_service import (
    AsyncManifestService,
    GenerationFailed,
    InvalidTokenError,
    Token,
)
from azul.service.manifest_controller import (
    ManifestController,
    ManifestGenerationState,
)
from azul.service.manifest_service import (
    BareManifestKey,
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
    generation_id = UUID('1ea94a54-a64d-54f1-8b41-15455fb958db')

    def test_token_encoding(self, _sfn):
        token = Token(generation_id=self.generation_id,
                      iteration=3,
                      request_index=42,
                      retry_after=123)
        self.assertEqual(token, Token.decode(token.encode()))

    def test_token_validation(self, _sfn):
        token = Token(generation_id=self.generation_id,
                      iteration=3,
                      request_index=42,
                      retry_after=123)
        self.assertRaises(InvalidTokenError, token.decode, token.encode()[:-1])

    def test_status_success(self, _sfn):
        """
        A successful manifest job should return a 302 status and a URL to the
        manifest
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.generation_id, iteration=0)
        input, output = {'filters': {}}, {'foo': 'bar'}
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'SUCCEEDED',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'stopDate': datetime.datetime(2018, 11, 15, 18, 30, 59, 295000),
            'input': json.dumps(input),
            'output': json.dumps(output)
        }
        token = Token(generation_id=self.generation_id,
                      iteration=0,
                      request_index=0,
                      retry_after=0)
        actual_result = service.inspect_generation(token)
        self.assertEqual({'input': input, 'output': output}, actual_result)

    def test_status_running(self, _sfn):
        """
        A running manifest job should return a 301 status and a URL to retry
        checking the job status
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.generation_id, iteration=0)
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'RUNNING',
            'startDate': datetime.datetime(2018, 11, 15, 18, 30, 44, 896000),
            'input': '{"filters": {}}'
        }
        token = Token(generation_id=self.generation_id,
                      iteration=0,
                      request_index=0,
                      retry_after=0)
        token = service.inspect_generation(token)
        expected = Token(generation_id=self.generation_id,
                         iteration=0,
                         request_index=1,
                         retry_after=1)
        self.assertEqual(expected, token)

    def test_status_failed(self, _sfn):
        """
        A failed manifest job should raise a GenerationFailed
        """
        service = AsyncManifestService()
        execution_name = service.execution_name(self.generation_id, iteration=0)
        _sfn.describe_execution.return_value = {
            'executionArn': service.execution_arn(execution_name),
            'stateMachineArn': service.machine_arn,
            'name': execution_name,
            'status': 'FAILED',
            'startDate': datetime.datetime(2018, 11, 14, 16, 6, 53, 382000),
            'stopDate': datetime.datetime(2018, 11, 14, 16, 6, 55, 860000),
            'input': '{"filters": {"organ": {"is": ["lymph node"]}}}',
        }
        token = Token(generation_id=self.generation_id,
                      iteration=0,
                      request_index=0,
                      retry_after=0)
        with self.assertRaises(GenerationFailed):
            service.inspect_generation(token)


class TestManifestController(DCP1TestCase, LocalAppTestCase):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    generation_id = UUID('1ea94a54-a64d-54f1-8b41-15455fb958db')

    @mock_aws
    @mock.patch.object(AsyncManifestService, '_sfn')
    @mock.patch.object(ManifestService, 'get_manifest')
    @mock.patch.object(ManifestService, 'get_cached_manifest')
    @mock.patch.object(ManifestService, 'verify_manifest_key')
    @mock.patch.object(ManifestService, 'get_cached_manifest_with_key')
    @mock.patch.object(ManifestService, 'get_manifest_url')
    @mock.patch.object(ManifestService, 'sign_manifest_key')
    def test(self,
             sign_manifest_key,
             get_manifest_url,
             get_cached_manifest_with_key,
             verify_manifest_key,
             get_cached_manifest,
             get_manifest,
             _sfn):

        mocks = [v for v in locals().values() if isinstance(v, mock.Mock)]

        def reset(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                try:
                    return f(*args, **kwargs)
                finally:
                    for m in mocks:
                        m.reset_mock(return_value=True, side_effect=True)

            return wrapper

        for format, fetch in product([ManifestFormat.compact, ManifestFormat.curl],
                                     [True, False]):
            with self.subTest(format=format, fetch=fetch):
                filters = {'fileFormat': {'is': ['txt']}, 'organ': {'is': ['heart', 'lung']}}
                filters = Filters(explicit=filters, source_ids={self.source.ref.id})
                params = {
                    'catalog': self.catalog,
                    'format': format.value,
                    'filters': json.dumps(filters.explicit)
                }
                path = ['manifest', 'files']

                initial_url = self.base_url.set(path=path.copy(), args=params)
                if fetch:
                    initial_url.path.segments.insert(0, 'fetch')

                manifest_key = ManifestKey(catalog=self.catalog,
                                           format=format,
                                           manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
                                           source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
                signed_manifest_key = SignedManifestKey(
                    value=BareManifestKey.unpack(manifest_key.pack()),
                    signature=b'123'
                )

                object_url = furl('https://url.to.manifest?foo=bar')
                file_name = 'some_file_name'
                manifest = Manifest(object_key='key/of/manifest',
                                    was_cached=False,
                                    format=format,
                                    manifest_key=manifest_key,
                                    file_name=file_name)

                partitions = [
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
                                      config={('foo', 'bar'): {'baz': 'blah'}},
                                      multipart_upload_id='some_upload_id',
                                      part_etags=('some_etag',),
                                      page_index=512,
                                      is_last_page=False,
                                      search_after=('foo', 'doc#bar'))
                ]
                input: ManifestGenerationState
                input = dict(filters=filters.to_json(),
                             manifest_key=manifest_key.to_json(),
                             partition=partitions[0].to_json())
                controller: ManifestController = self._app.manifest_controller
                service = controller._async_service
                generation_id = manifest_key.uuid
                execution_names = [
                    service.execution_name(generation_id, iteration=i)
                    for i in range(3)
                ]
                machine_arn = service.machine_arn
                execution_arns = list(map(service.execution_arn, execution_names))

                not_found = CachedManifestNotFound(manifest_key)
                execution_exists = self._mock_sfn_exception(
                    _sfn,
                    operation_name='StartExecution',
                    error_code='ExecutionAlreadyExists'
                )

                not_found = CachedManifestNotFound(manifest_key)
                execution_exists = self._mock_sfn_exception(_sfn,
                                                            operation_name='StartExecution',
                                                            error_code='ExecutionAlreadyExists')

                def assert_get_cached_manifest(filters=filters):
                    get_cached_manifest.assert_called_once_with(
                        format=format,
                        catalog=self.catalog,
                        filters=filters
                    )

                def assert_get_manifest(partition):
                    get_manifest.assert_called_once_with(
                        format=format,
                        catalog=self.catalog,
                        filters=filters,
                        partition=partitions[partition],
                        manifest_key=manifest_key
                    )

                url: furl
                state: ManifestGenerationState
                token_url: furl
                key_url: furl
                final_url: furl
                equivalent_url: furl
                equivalent_filters: FiltersJSON

                execution_inputs: list[JSON] = []

                def mock_start_execution(iteration: int):
                    _sfn.start_execution.side_effect = [
                        {
                            'executionArn': execution_arns[iteration],
                            'startDate': 1234 + iteration
                        }
                    ]

                def mock_start_execution_exists(iteration: int):
                    _sfn.start_execution.side_effect = [
                        execution_exists
                    ]

                def mock_describe_execution(*iterations: int):
                    _sfn.describe_execution.side_effect = [
                        {
                            'status': 'SUCCEEDED',
                            'input': json.dumps(execution_inputs[i]),
                            'output': json.dumps(state)
                        }
                        for i in iterations
                    ]

                def assert_start_execution(*iterations: int):
                    expected_calls = [
                        mock.call(stateMachineArn=machine_arn,
                                  name=execution_names[i],
                                  input=json.dumps(execution_inputs[i]))
                        for i in iterations
                    ]
                    self.assertEqual(expected_calls, _sfn.start_execution.mock_calls)

                def assert_describe_execution(*iterations: int):
                    expected_calls = [
                        mock.call(executionArn=execution_arns[i])
                        for i in iterations
                    ]
                    self.assertEqual(expected_calls, _sfn.describe_execution.mock_calls)

                # Request the manifest. The cached manifest does not exist
                # so we expect a StepFunction execution to be started and a
                # 301 redirect to the manifest endpoint with a token
                # embedded in the URL.
                #
                @reset
                def put():
                    nonlocal url, state, token_url
                    get_cached_manifest.side_effect = not_found
                    execution_inputs.append(input)
                    mock_start_execution(0)
                    url = self._request('PUT', initial_url, expect=301)
                    assert_get_cached_manifest()
                    assert_start_execution(0)
                    state = input
                    token_url = url

                put()

                # Follow the redirect. We expect a call to determine the
                # status of the execution, which we mock to be still
                # running, and another 301 redirect.
                #
                @reset
                def get_token_while_running():
                    nonlocal url, state
                    _sfn.describe_execution.return_value = {'status': 'RUNNING'}
                    url = self._request('GET', url, expect=301)
                    get_manifest.return_value = partitions[1]
                    state = self._app_module.generate_manifest(state, None)
                    assert_get_manifest(partition=0)
                    self.assertEqual(partitions[1],
                                     ManifestPartition.from_json(state['partition']))
                    iteration = len(execution_inputs) - 1
                    assert_describe_execution(iteration)

                get_token_while_running()

                # Follow the redirect. The StepFunction has finished but the
                # output is not yet available due to eventual consistency. We
                # did originally observe this behavior, but not anymore. Under
                # the hood, the output is probably stored on S3 which became
                # strongly consistent a few years ago.
                #
                @reset
                def get_token_when_almost_done():
                    nonlocal url
                    _sfn.describe_execution.return_value = {'status': 'SUCCEEDED'}
                    url = self._request('GET', url, expect=301)
                    _sfn.describe_execution.assert_called_once()

                get_token_when_almost_done()

                # The StepFunction has finished and the output is available.
                # We expect a 302 redirect to either the signed URL of the
                # manifest object in S3, or, when fetching a curl manifest,
                # a 302 redirect to the non-fetch endpoint with the key of
                # the manifest in the URL.
                #
                @reset
                def get_token_when_done():
                    nonlocal url, state, key_url, final_url
                    get_manifest.return_value = manifest
                    state = self._app_module.generate_manifest(state, None)
                    assert_get_manifest(partition=1)
                    iteration = len(execution_inputs) - 1
                    mock_describe_execution(iteration)
                    if fetch and format is ManifestFormat.curl:
                        key_url = self.base_url.set(path=[*path, signed_manifest_key.encode()])
                        final_url = key_url
                        sign_manifest_key.return_value = signed_manifest_key
                    else:
                        key_url = None
                        final_url = object_url
                        get_manifest_url.return_value = str(object_url)
                    get_cached_manifest_with_key.return_value = manifest
                    url = self._request('GET', url, expect=302)
                    self.assertEqual(final_url, url)
                    assert_describe_execution(iteration)
                    get_cached_manifest_with_key.assert_called_once_with(manifest_key)

                get_token_when_done()

                # Re-request the manifest at the initial URL. The manifest
                # is cached so we expect no intermediate 301 redirects.
                #
                @reset
                def repeat_put():
                    nonlocal url
                    get_cached_manifest.return_value = manifest
                    get_manifest_url.return_value = str(object_url)
                    if fetch and format is ManifestFormat.curl:
                        sign_manifest_key.return_value = signed_manifest_key
                    url = self._request('PUT', initial_url, expect=302)
                    assert_get_cached_manifest()
                    self.assertEqual(final_url, url)

                repeat_put()

                # Re-request the manifest at a URL with an insignificant
                # change to the filters parameter. The cached manifest
                # should be reused. Note that this does not cover the
                # insensitivity of the manifest key derivation to such
                # insignificant differences because we mock the manifest
                # service method where that is done. However, this test is
                # not supposed to cover the service, only the controller.
                #
                @reset
                def modified_put():
                    nonlocal url, equivalent_url, equivalent_filters
                    get_cached_manifest.return_value = manifest
                    get_manifest_url.return_value = str(object_url)
                    if key_url is not None:
                        sign_manifest_key.return_value = signed_manifest_key
                    equivalent_url = initial_url.copy()
                    equivalent_filters = json.loads(equivalent_url.args['filters'])
                    equivalent_filters['organ']['is'].reverse()
                    equivalent_url.args['filters'] = json.dumps(equivalent_filters)
                    url = self._request('PUT', equivalent_url, expect=302)
                    self.assertEqual(final_url, url)
                    assert_get_cached_manifest(filters)

                modified_put()

                # Expire the cached manifest and repeat the initial request with
                # the insignificant difference. The repeated request should be
                # considered valid and matching the completed step function
                # execution. A token corresponding for the already completed
                # execution will be returned …
                #
                @reset
                def modified_put_after_expiration():
                    nonlocal url
                    get_cached_manifest.side_effect = not_found
                    mock_start_execution_exists(0)
                    mock_describe_execution(0)
                    url = self._request('PUT', equivalent_url, expect=301)
                    self.assertEqual(token_url, url)
                    assert_get_cached_manifest()
                    assert_start_execution(0)
                    assert_describe_execution(0)

                modified_put_after_expiration()

                # … and when following the resulting, we expect yet another
                # execution to restart the generation.
                #
                @reset
                def get_stale_token_when_done():
                    nonlocal url, state, token_url
                    get_cached_manifest_with_key.side_effect = not_found
                    execution_inputs.append(input)
                    iteration = len(execution_inputs) - 1
                    mock_start_execution(iteration)
                    mock_describe_execution(iteration - 1)
                    url = self._request('GET', url, expect=301)
                    self.assertNotEqual(token_url, url)
                    get_cached_manifest_with_key.assert_called_once_with(manifest_key)
                    assert_start_execution(iteration)
                    assert_describe_execution(iteration - 1)
                    token_url = url
                    state = input

                get_stale_token_when_done()
                get_token_while_running()
                get_token_when_almost_done()
                get_token_when_done()

                # Request the manifest by its key if a URL with that key
                # was the result of the final 302 redirect above.
                #
                @reset
                def get_key():
                    nonlocal url
                    assert signed_manifest_key.encode() == key_url.path.segments[-1]
                    verify_manifest_key.return_value = manifest_key
                    get_cached_manifest_with_key.return_value = manifest
                    if key_url is not None:
                        sign_manifest_key.return_value = signed_manifest_key
                    get_manifest_url.return_value = str(object_url)
                    url = self._request('GET', key_url, expect=302)
                    self.assertEqual(object_url, url)
                    verify_manifest_key.assert_called_once_with(signed_manifest_key)
                    get_cached_manifest_with_key.assert_called_once_with(manifest_key)

                if key_url is not None:
                    get_key()

                # Expire the manifest and request the manifest by its key if a
                # URL with that key was the result of the final 302 redirect
                # above.
                #
                @reset
                def get_key_after_expiration():
                    verify_manifest_key.return_value = manifest_key
                    get_cached_manifest_with_key.side_effect = not_found
                    response = requests.get(str(key_url), allow_redirects=False)
                    self.assertEqual(410, response.status_code)
                    expected_response = {
                        'Code': 'GoneError',
                        'Message': 'The manifest has expired, please request a new one'
                    }
                    self.assertEqual(expected_response, response.json())
                    get_cached_manifest_with_key.assert_called_once_with(manifest_key)

                if key_url is not None:
                    get_key_after_expiration()

    def _request(self, method: str, url: furl, *, expect: int) -> furl:
        response = requests.request(method=method,
                                    url=str(url),
                                    allow_redirects=False)
        if url.path.segments[0] == 'fetch':
            self.assertEqual(200, response.status_code)
            response = response.json()
            self.assertEqual(expect, response.pop('Status'))
            headers = response
        else:
            self.assertEqual(expect, response.status_code)
            headers = response.headers
        if expect == 301:
            self.assertGreaterEqual(int(headers['Retry-After']), 0)
        return furl(headers['Location'])

    token = Token.first(generation_id, iteration=0).encode()

    def _test(self, *, expected_status, token=token):
        url = self.base_url.set(path=['fetch', 'manifest', 'files', token])
        response = requests.get(str(url))
        self.assertEqual(expected_status, response.status_code)

    @contextmanager
    def _mock_error(self, error_code: str) -> ContextManager:
        with patch.object(AsyncManifestService, '_sfn') as _sfn:
            # We need to mock every exception class for which there is an
            # `except` clause in the code under test. If we don't we get
            #
            # TypeError: catching classes that do not inherit from
            #            BaseException is not allowed
            #
            self._mock_sfn_exception_cls(_sfn, error_code='ExecutionDoesNotExist')
            exception = self._mock_sfn_exception(_sfn,
                                                 operation_name='DescribeExecution',
                                                 error_code=error_code)
            _sfn.describe_execution.side_effect = exception
            yield

    def _mock_sfn_exception(self,
                            _sfn: mock.MagicMock,
                            operation_name: str,
                            error_code: str
                            ) -> Exception:
        exception_cls = self._mock_sfn_exception_cls(_sfn, error_code)
        error_response = {'Error': {'Code': error_code}}
        exception = exception_cls(operation_name=operation_name,
                                  error_response=error_response)
        return exception

    def _mock_sfn_exception_cls(self, _sfn: mock.MagicMock, error_code: str) -> type:
        exception_cls = type(error_code, (ClientError,), {})
        setattr(_sfn.exceptions, error_code, exception_cls)
        return exception_cls

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
        e = GenerationFailed(status='status', output=None)
        with patch.object(AsyncManifestService, 'inspect_generation', side_effect=e):
            self._test(expected_status=500)

    def test_invalid_token(self):
        """
        Manifest endpoint should raise a BadRequestError when given a token that
        cannot be decoded
        """
        self._test(token='Invalid base64', expected_status=400)
