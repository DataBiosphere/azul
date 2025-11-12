from itertools import (
    product,
)
import json
from logging import (
    DEBUG,
    INFO,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

import requests

from azul import (
    Config,
)
from azul.chalice import (
    AzulChaliceApp,
    log,
)
from azul.logging import (
    configure_test_logging,
)
from azul.types import (
    JSON,
)
from indexer import (
    DCP1CannedBundleTestCase,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestServiceAppLogging(DCP1CannedBundleTestCase, WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    def test_request_logs(self):
        prefix_len = 1024

        def filter_body(organ: str) -> JSON:
            return {'filters': json.dumps({'organ': {'is': [organ]}})}

        for debug, authenticated, body_json in product(
            [0, 1, 2],
            [False, True],
            [None, filter_body('foo'), filter_body('foo' * int(prefix_len / 3 + 1))]
        ):
            if body_json is None:
                body = b''
            else:
                body = json.dumps(body_json).encode()

            with self.subTest(azul_debug=debug,
                              authenticated=authenticated,
                              body_len=len(body)):
                url = self.base_url.set(path='/index/projects')
                request_headers = {'authorization': 'Bearer foo_token'} if authenticated else {}
                level = [INFO, DEBUG, DEBUG][debug]
                with self.assertLogs(logger=log, level=level) as logs:
                    with patch.object(Config, 'debug', new=PropertyMock(return_value=debug)):
                        if body:
                            request_headers = {
                                'content-length': str(len(body)),
                                'content-type': 'application/json',
                                **request_headers
                            }
                        response = requests.get(str(url),
                                                headers=request_headers,
                                                json=body_json)
                logs = [(r.levelno, r.getMessage()) for r in logs.records]
                body_log_level, body_log_message = logs.pop()  # asserted separately
                request_headers = {
                    'host': url.netloc,
                    'user-agent': 'python-requests/2.32.5',
                    'accept-encoding': 'gzip, deflate',
                    'accept': '*/*',
                    'connection': 'keep-alive',
                    **request_headers,
                }
                response_headers = {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Authorization,'
                                                    'Content-Type,'
                                                    'X-Amz-Date,'
                                                    'X-Amz-Security-Token,'
                                                    'X-Api-Key',
                    **AzulChaliceApp.security_headers(),
                    'Cache-Control': 'no-store'
                }
                body_prefix = body[:prefix_len - 3] + b'...'
                self.assertEqual(
                    [
                        (
                            INFO,
                            "Received GET request for '/index/projects', "
                            f'with {json.dumps(dict(query=None, headers=request_headers))}.'
                        ),
                        (
                            INFO,
                            '… without a request body'
                        )
                        if body == b'' else
                        (
                            INFO,
                            f"… with a request body of length {len(body)} and type <class 'bytes'>"
                        )
                        if debug == 0 else
                        (
                            INFO,
                            f'… with a request body of length {len(body)} starting in {body_prefix!r}'
                            if debug == 1 and len(body) > prefix_len else
                            f"… with a request body of length {len(body)} being {body!r}"
                        ),
                        (
                            INFO,
                            "Authenticated request as OAuth2(access_token='foo_token')"
                            if authenticated else
                            'Did not authenticate request.'
                        ),
                        (
                            INFO,
                            'Returning 200 response with headers ' +
                            json.dumps(dict(headers=response_headers)) + '.'
                        )
                    ],
                    logs
                )
                body = json.dumps(response.json())
                self.assertGreater(len(body), prefix_len)
                if debug == 0:
                    expected_log = "… with a response body of type (<class 'dict'>)"
                elif debug == 1:
                    expected_log = f'… with a response body starting in {body[:prefix_len]}'
                elif debug > 1:
                    expected_log = f'… with a response body of length 9137 being {body}'
                else:
                    assert False
                self.assertEqual(expected_log, body_log_message)
                self.assertEqual(INFO, body_log_level)
                self.assertEqual(200, response.status_code)
