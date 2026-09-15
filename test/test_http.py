from contextlib import (
    contextmanager,
    nullcontext,
)
from functools import (
    partial,
)
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)
import inspect
import logging
import re
from threading import (
    Thread,
)
import time
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    patch,
)

from urllib3 import (
    Retry,
)
from urllib3.exceptions import (
    MaxRetryError,
)

from azul import (
    config,
)
from azul.http import (
    AcceptEncodingClient,
    LimitedRetryHttpClient,
    LimitedTimeoutException,
    http_client,
)
from azul.lib.collections import (
    OrderedSet,
)
from azul.logging import (
    configure_test_logging,
)
from azul_test_case import (
    AzulUnitTestCase,
)

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class TestHttp(AzulUnitTestCase):

    @contextmanager
    def http_server(self, handler: type[BaseHTTPRequestHandler]):
        with ThreadingHTTPServer(('127.0.0.1', 0), handler) as server:
            # A shorter poll intervall causes the server thread to check the
            # exit flag more frequently, but wastes more CPU. Going from the
            # default of .5 to .05 caused an improvement of the overall test
            # duration by tens of seconds.
            thread = Thread(target=partial(server.serve_forever, poll_interval=.1))
            thread.start()
            try:
                url = f'http://localhost:{server.server_port}'
                yield url
            finally:
                server.shutdown()
                thread.join()

    sub_test_locals: OrderedSet[str]

    def subTestFromLocals(self):
        locals = inspect.currentframe().f_back.f_locals
        try:
            sub_test_locals = self.sub_test_locals
        except AttributeError:
            sub_test_locals = OrderedSet(locals)
            sub_test_locals.discard('self')
            self.sub_test_locals = sub_test_locals
        return self.subTest(**{k: locals[k] for k in sub_test_locals})

    @mock.patch.object(type(config), 'debug', new=1)
    def test(self):
        #: If True (False), use a limited retry client under restricted
        #: (unrestricted) timing. If None, use a normal client.
        restricted: bool | None
        #: The number of retries to configure the client with or None to use
        #: the default. Must be None if restricted is not.
        retries: int | None
        #: Number of seconds the server sleeps before responding to a request.
        sleep: float
        #: The type of exception expected to be raised, or None of no exception
        #: is expected.
        exception: Exception | None
        #: The expected number of calls to LoggingHttpClient.urlopen(). Explicit
        #: retries by StatusRetryHttpClient will cause more than one call.
        calls: int
        #: The expected number of requests to be made. Implicit retries done by
        #: urllib3 can cause more than one request per call.
        requests: int
        #: The expected number of responses to be received.
        responses: int
        for restricted, retries, sleep, exception, calls, requests, responses in [
            # @formatter:off
            (  None,    0,           0,                    None, 1, 1, 1 ),  # noqa
            (  None,    1,           0,                    None, 2, 2, 2 ),  # noqa
            (  None,    2,           0,                    None, 3, 3, 3 ),  # noqa
            (  None, None,           0,           MaxRetryError, 6, 6, 6 ),  # noqa
            ( False, None,           0, LimitedTimeoutException, 3, 3, 3 ),  # noqa
            ( False, None, 20 / 3 + .1, LimitedTimeoutException, 1, 3, 0 ),  # noqa
            (  True, None,           0, LimitedTimeoutException, 1, 1, 1 ),  # noqa
            (  True, None,  5 / 1 + .1, LimitedTimeoutException, 1, 1, 0 ),  # noqa
            # @formatter:on
        ]:
            with self.subTestFromLocals():

                num_actual_requests = 0

                class Handler(BaseHTTPRequestHandler):

                    # noinspection PyPep8Naming
                    def do_GET(self):
                        nonlocal num_actual_requests
                        num_actual_requests += 1
                        if sleep:
                            time.sleep(sleep)
                        self.send_response(503)
                        self.send_header('Retry-After', '1')
                        self.end_headers()

                with self.http_server(Handler) as url:
                    # The standard client is a urllib3 PoolManager, wrapped by
                    # a LoggingHttpClient and a StatusRetryHttpClient instance
                    client = http_client(log)
                    with self.mock_api_gateway() if restricted else nullcontext():
                        if restricted is not None:
                            client = LimitedRetryHttpClient(client)
                            assert restricted is client._timing_is_restricted
                        with self.assertRaises(exception) if exception else nullcontext():
                            with self.assertLogs(log) as logs:
                                headers = {'Authorization': 'secret, should not be logged'}
                                if retries is None:
                                    client.request(method='GET', url=url, headers=headers)
                                else:
                                    retry = Retry(status=retries, raise_on_status=exception is not None)
                                    client.request(method='GET', url=url, retries=retry, headers=headers)

                self.assertEqual(requests, num_actual_requests)

                prefix, url = 'INFO:test_http:', re.escape(url)
                accept_encoding = AcceptEncodingClient.accept_encoding_header()
                http_header_pattern = (
                    r"\["
                    r"\('Server', 'BaseHTTP/\d+\.\d+\s+Python/\d+\.\d+\.\d+'\), "
                    r"\('Date', '[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT'\), "
                    r"\('Retry-After', '\d+'\)"
                    r"\]"
                )

                expected_logs = []
                for i in range(calls):
                    expected_logs.extend(
                        [
                            # The urlopen() call logs the method, URL and body
                            fr"^{prefix}Making GET request to '{url}'$",
                            fr'^{prefix}… without a request body$'
                        ] + [
                            # The headers are logged for the urlopen() call and
                            # every retry so if we expect one call and three
                            # requests, the headers will be logged three times.
                            fr"^{prefix}… with request headers "
                            fr"\[\('Authorization', 'REDACTED'\), "
                            fr"\('accept-encoding', '{re.escape(accept_encoding)}'\)\]$"
                        ] * (requests // calls)
                    )
                    if responses:
                        expected_logs.extend(
                            [
                                rf'^{prefix}Got 503 response after \d.\d\d\ds from GET to {url}$',
                                rf'^{prefix}… with response headers {http_header_pattern}$',
                                f"^{prefix}… without a response body$",
                            ]
                        )
                        # StatusRetryHttpClient sleeps between calls
                        if i < calls - 1:
                            expected_logs.append(f'^{prefix}Sleeping 1s to honor Retry-After header$')
                for expected_log, actual_log in zip(expected_logs, logs.output, strict=True):
                    self.assertRegex(actual_log, expected_log)

    def mock_api_gateway(self):
        return patch.object(type(config),
                            'lambda_is_handling_api_gateway_request',
                            new_callable=PropertyMock,
                            return_value=True)

    def test_lambda_context_timeout(self):
        client = LimitedRetryHttpClient(http_client(log))
        default_margin = LimitedRetryHttpClient._default_timeout_margin
        custom_margin = 30
        for remaining_ms, api_gateway, margin, expected_timeout in [
            # @formatter:off
            (    None, False, default_margin, 20                          ),  # noqa: E201,E202
            (    None,  True, default_margin, 5                           ),  # noqa: E201,E202
            ( 300_000, False, default_margin, 300 - default_margin        ),  # noqa: E201,E202
            ( 300_000, False,  custom_margin, 300 - custom_margin         ),  # noqa: E201,E202
            ( 300_000,  True, default_margin, 5                           ),  # noqa: E201,E202
            (  15_000, False, default_margin, max(5, 15 - default_margin) ),  # noqa: E201,E202
            (   5_000, False, default_margin, max(5,  5 - default_margin) ),  # noqa: E201,E202
            # @formatter:on
        ]:
            with self.subTest(remaining_ms=remaining_ms,
                              api_gateway=api_gateway,
                              margin=margin):
                if remaining_ms is not None:
                    lambda_context = MagicMock()
                    lambda_context.get_remaining_time_in_millis.return_value = remaining_ms
                else:
                    lambda_context = None
                with (
                    patch.object(type(config),
                                 'lambda_context',
                                 new_callable=PropertyMock,
                                 return_value=lambda_context),
                    self.mock_api_gateway() if api_gateway else nullcontext(),
                ):
                    self.assertEqual(expected_timeout, client._timeout(margin))
