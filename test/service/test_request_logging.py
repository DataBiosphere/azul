from logging import INFO, DEBUG

import requests

from app_test_case import LocalAppTestCase
from azul.chalice import log
from azul.logging import configure_test_logging


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestRequestLogging(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    def test_request_logs(self):
        for level, response_log in [
            (INFO, 'Returning 200 response. To log headers and body, set AZUL_DEBUG to 1.'),
            (DEBUG, 'Returning 200 response without headers. '
                    'See next line for the first 1024 characters of the body.\n'
                    '{"up": true}')
        ]:
            with self.subTest(level=level):
                with self.assertLogs(logger=log, level=level) as logs:
                    url = self.base_url + '/health/basic'
                    requests.get(url)
                logs = [(r.levelno, r.getMessage()) for r in logs.records]
                self.assertEqual(logs, [
                    (INFO, "Received GET request to '/health/basic' without parameters."),
                    (level, response_log)
                ])
