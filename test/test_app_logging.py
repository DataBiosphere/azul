import json
import os
from typing import (
    Any,
    Tuple,
    cast,
)
from unittest import (
    TestCase,
    TestResult,
    TestSuite,
    mock,
)
import warnings

from chalice.config import (
    Config as ChaliceConfig,
)
from more_itertools import (
    one,
)
import requests

from app_test_case import (
    AzulUnitTestCase,
    ChaliceServerThread,
)
import azul
from azul.chalice import (
    AzulChaliceApp,
)
from azul.logging import (
    azul_log_level,
    configure_test_logging,
)
from azul_test_case import (
    AlwaysTearDownTestCase,
)


# noinspection PyPep8Naming
def setupModule():
    configure_test_logging()


class TestAppLogging(TestCase):

    def test(self):
        magic_message = 'Now you see me'
        traceback_header = 'Traceback (most recent call last):'

        for debug in 0, 1, 2:
            with mock.patch.dict(os.environ, AZUL_DEBUG=str(debug)):
                with self.subTest(debug=debug):
                    log_level = azul_log_level()
                    app = AzulChaliceApp(__name__, unit_test=True)
                    path = '/fail/path'

                    @app.route(path)
                    def fail():
                        raise ValueError(magic_message)

                    server_thread = ChaliceServerThread(app, ChaliceConfig(), 'localhost', 0)
                    server_thread.start()
                    try:
                        host, port = server_thread.address
                        with self.assertLogs(app.log, level=log_level) as app_log:
                            with self.assertLogs(azul.log, level=log_level) as azul_log:
                                response = requests.get(f'http://{host}:{port}{path}')
                    finally:
                        server_thread.kill_thread()
                        server_thread.join(timeout=10)
                        if server_thread.is_alive():
                            self.fail('Thread is still alive after joining')

                    self.assertEqual(response.status_code, 500)

                    # The request is always logged
                    self.assertEqual(len(azul_log.output), 3)
                    headers = {
                        'host': f'{host}:{port}',
                        'user-agent': 'python-requests/2.22.0',
                        'accept-encoding': 'gzip, deflate',
                        'accept': '*/*',
                        'connection': 'keep-alive'
                    }
                    self.assertEqual(azul_log.output[0],
                                     f'INFO:azul.chalice:Received GET request for {path!r}, '
                                     f'with query null and headers {json.dumps(headers)}.')
                    self.assertEqual(azul_log.output[1],
                                     'INFO:azul.chalice:Did not authenticate request.')

                    # The exception is always logged
                    self.assertEqual(len(app_log.output), 1)
                    err_log = f'ERROR:test_app_logging:Caught exception for path {path}'
                    self.assertTrue(app_log.output[0].startswith(err_log))
                    self.assertIn(magic_message, app_log.output[0])
                    self.assertIn(traceback_header, app_log.output[0])

                    if debug:
                        # In debug mode, the response includes the traceback …
                        response = response.content.decode()
                        self.assertTrue(response.startswith(traceback_header))
                        self.assertIn(magic_message, response)
                        # … and the response is logged.
                        self.assertEqual(
                            azul_log.output[2],
                            'DEBUG:azul.chalice:Returning 500 response with headers {"Content-Type": "text/plain"}. '
                            'See next line for the first 1024 characters of the body.\n' + response)
                    else:
                        # Otherwise, a generic error response is returned …
                        self.assertEqual(response.json(), {
                            'Code': 'InternalServerError',
                            'Message': 'An internal server error occurred.'
                        })
                        # … and a generic error message is logged.
                        self.assertEqual(
                            azul_log.output[2],
                            'INFO:azul.chalice:Returning 500 response. To log headers and body, set AZUL_DEBUG to 1.'
                        )


class TestPermittedWarnings(AzulUnitTestCase):

    def test_permitted_warnings(self):
        # The following warning does not get caught by the catch_warning context
        # manager in the AzulTestCase class because the message matches an
        # ignore warning filter.
        warnings.warn("unclosed <ssl.SSLSocket fd=30, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM,"
                      "proto=0, laddr=('192.168.1.11', 63179), raddr=('172.217.5.112', 443)>",
                      category=ResourceWarning)


class TestUnexpectedWarnings(TestCase):

    def test_unexpected_warning(self):
        msg = 'Testing unexpected warnings, nothing to see here.'
        category = ResourceWarning

        for parents in (
            (AzulUnitTestCase,),
            (AzulUnitTestCase, AlwaysTearDownTestCase),
            (AlwaysTearDownTestCase, AzulUnitTestCase)
        ):
            with self.subTest(parents=parents):
                class Test(*parents):

                    def test(self):
                        warnings.warn(message=msg, category=category)

                case = Test('test')
                suite = TestSuite()
                result = TestResult()
                suite.addTest(case)
                suite.run(result)

                self.assertEqual(1, result.testsRun)
                failed_test, trace_back = cast(Tuple[Any, str], one(result.errors))
                self.assertEqual(f'tearDownClass ({__name__}.{Test.__qualname__})', str(failed_test))
                error_line = trace_back.splitlines()[-1]
                self.assertRegex(error_line, '^AssertionError')
                self.assertIn(str(category(msg)), error_line)
