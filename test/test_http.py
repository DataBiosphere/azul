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
from unittest.mock import (
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
from azul.collections import (
    OrderedSet,
)
from azul.http import (
    LimitedRetryHttpClient,
    LimitedTimeoutException,
    http_client,
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

    def test(self):
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
                    client = http_client(log)
                    with self.mock_api_gateway() if restricted else nullcontext():
                        if restricted is not None:
                            client = LimitedRetryHttpClient(client)
                            assert restricted is client._timing_is_restricted
                        with self.assertRaises(exception) if exception else nullcontext():
                            with self.assertLogs(log) as logs:
                                if retries is None:
                                    client.request(method='GET', url=url)
                                else:
                                    retries = Retry(status=retries,
                                                    raise_on_status=exception is not None)
                                    client.request(method='GET', url=url, retries=retries)

                self.assertEqual(requests, num_actual_requests)

                prefix, url = 'INFO:test_http:', re.escape(url)
                expected_logs = []
                for i in range(calls):
                    expected_logs.append(f"^{prefix}Making GET request to '{url}'$")
                    if i < responses:
                        expected_logs.append(
                            rf'^{prefix}Got 503 response after \d.\d\d\ds from GET to {url}$'
                        )
                        if i < calls - 1:
                            expected_logs.append(f'{prefix}Sleeping 1 to honor Retry-After header')
                for expected_log, actual_log in zip(expected_logs, logs.output, strict=True):
                    self.assertRegex(actual_log, expected_log)

    def mock_api_gateway(self):
        return patch.object(type(config),
                            'lambda_is_handling_api_gateway_request',
                            new_callable=PropertyMock,
                            return_value=True)
