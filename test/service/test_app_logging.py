import json
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
from indexer import (
    DCP1CannedBundleTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestServiceAppLogging(DCP1CannedBundleTestCase, LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def test_request_logs(self):
        for level in INFO, DEBUG:
            for authenticated in False, True:
                with self.subTest(level=level, authenticated=authenticated):
                    url = self.base_url.set(path='/health/basic')
                    headers = {'authorization': 'Bearer foo_token'} if authenticated else {}
                    with self.assertLogs(logger=log, level=level) as logs:
                        requests.get(str(url), headers=headers)
                    logs = [(r.levelno, r.getMessage()) for r in logs.records]
                    request_headers = {
                        'host': url.netloc,
                        'user-agent': 'python-requests/2.32.2',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept': '*/*',
                        'connection': 'keep-alive',
                        **headers,
                    }
                    response_headers = {
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Authorization,'
                                                        'Content-Type,'
                                                        'X-Amz-Date,'
                                                        'X-Amz-Security-Token,'
                                                        'X-Api-Key',
                        'Content-Security-Policy': "default-src 'self';"
                                                   "img-src 'self' data:;"
                                                   "script-src 'self';"
                                                   "style-src 'self';"
                                                   "frame-ancestors 'none'",
                        'Strict-Transport-Security': 'max-age=31536000;'
                                                     ' includeSubDomains',
                        'X-Content-Type-Options': 'nosniff',
                        'X-Frame-Options': 'DENY',
                        'Cache-Control': 'no-store'
                    }
                    self.assertEqual(logs, [
                        (
                            INFO,
                            f"Received GET request for '/health/basic', "
                            f"with {json.dumps({'query': None, 'headers': request_headers})}."),
                        (
                            INFO,
                            "Authenticated request as OAuth2(access_token='foo_token')"
                            if authenticated else
                            'Did not authenticate request.'
                        ),
                        (
                            level,
                            'Returning 200 response. To log headers and body, set AZUL_DEBUG to 1.'
                            if level == INFO else
                            f'Returning 200 response with headers {json.dumps(response_headers)}. '
                            'See next line for the first 1024 characters of the body.\n'
                            '{"up": true}'
                        )
                    ])
