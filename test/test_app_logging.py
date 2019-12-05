import os
from unittest import (
    TestCase,
    mock,
)

from chalice.config import Config as ChaliceConfig
import requests

from app_test_case import ChaliceServerThread
import azul
from azul.chalice import AzulChaliceApp
from azul.logging import (
    azul_log_level,
    configure_test_logging,
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
                    app = AzulChaliceApp(__name__)

                    @app.route('/')
                    def fail():
                        raise ValueError(magic_message)

                    server_thread = ChaliceServerThread(app, ChaliceConfig(), 'localhost', 0)
                    server_thread.start()
                    try:
                        host, port = server_thread.address
                        with self.assertLogs(app.log, level=log_level) as app_log:
                            with self.assertLogs(azul.log, level=log_level) as azul_log:
                                response = requests.get(f"http://{host}:{port}/")
                    finally:
                        server_thread.kill_thread()
                        server_thread.join(timeout=10)
                        if server_thread.is_alive():
                            self.fail('Thread is still alive after joining')

                    self.assertEqual(response.status_code, 500)

                    # The request is always logged
                    self.assertEqual(len(azul_log.output), 2)
                    request_log = "INFO:azul.chalice:Received GET request to '/' without parameters."
                    self.assertEqual(azul_log.output[0], request_log)

                    # The exception is always logged
                    self.assertEqual(len(app_log.output), 1)
                    err_log = 'ERROR:test_app_logging:Caught exception for <function TestAppLogging.test.<locals>.fail'
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
                            azul_log.output[1],
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
                            azul_log.output[1],
                            'INFO:azul.chalice:Returning 500 response. To log headers and body, set AZUL_DEBUG to 1.'
                        )
