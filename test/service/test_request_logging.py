from logging import (
    DEBUG,
    INFO,
)

import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul.chalice import (
    log,
)
from azul.logging import (
    configure_test_logging,
)


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
            for auth_status, auth_stmt in [
                (False, 'unauthenticated'),
                (True, "authenticated with bearer token 'foo_token'")
            ]:
                with self.subTest(level=level, authenticated=auth_status):
                    with self.assertLogs(logger=log, level=level) as logs:
                        url = self.base_url + '/health/basic'
                        headers = {'Authorization': 'Bearer foo_token'} if auth_status else {}
                        requests.get(url, headers=headers)
                    logs = [(r.levelno, r.getMessage()) for r in logs.records]
                    self.assertEqual(logs, [
                        (INFO, f"Received GET request to '/health/basic' without parameters ({auth_stmt})."),
                        (level, response_log)
                    ])
