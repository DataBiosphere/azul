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
                    headers = {
                        'host': url.netloc,
                        'user-agent': 'python-requests/2.31.0',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept': '*/*',
                        'connection': 'keep-alive',
                        **headers,
                    }
                    self.assertEqual(logs, [
                        (
                            INFO,
                            f"Received GET request for '/health/basic', "
                            f"with {json.dumps({'query': None, 'headers': headers})}."),
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
                            'Returning 200 response with headers {"Access-Control-Allow-Origin": '
                            '"*", "Access-Control-Allow-Headers": '
                            '"Authorization,Content-Type,X-Amz-Date,X-Amz-Security-Token,X-Api-Key", '
                            '"Strict-Transport-Security": "max-age=31536000; includeSubDomains", '
                            '"X-Content-Type-Options": "nosniff", '
                            '"X-Frame-Options": "DENY"}. '
                            'See next line for the first 1024 characters of the body.\n'
                            '{"up": true}'
                        )
                    ])
