import json
from logging import (
    DEBUG,
    INFO,
)

import requests

from azul.chalice import (
    log,
)
from azul.logging import (
    configure_test_logging,
)
from azul.strings import (
    single_quote as sq,
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
    def lambda_name(cls) -> str:
        return 'service'

    def test_request_logs(self):
        for level in INFO, DEBUG:
            for authenticated in False, True:
                with self.subTest(level=level, authenticated=authenticated):
                    url = self.base_url.set(path='/index/projects')
                    headers = {'authorization': 'Bearer foo_token'} if authenticated else {}
                    with self.assertLogs(logger=log, level=level) as logs:
                        requests.get(str(url), headers=headers)
                    logs = [(r.levelno, r.getMessage()) for r in logs.records]
                    headers = {
                        'host': url.netloc,
                        'user-agent': 'python-requests/2.32.2',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept': '*/*',
                        'connection': 'keep-alive',
                        **headers,
                    }
                    self.assertEqual(logs, [
                        (
                            INFO,
                            f"Received GET request for '/index/projects', "
                            f"with {json.dumps({'query': None, 'headers': headers})}."
                        ),
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
                            f'"Content-Security-Policy": "default-src {sq("self")}", '
                            '"Referrer-Policy": "strict-origin-when-cross-origin", '
                            '"Strict-Transport-Security": "max-age=63072000; includeSubDomains; preload", '
                            '"X-Content-Type-Options": "nosniff", '
                            '"X-Frame-Options": "DENY", '
                            '"X-XSS-Protection": "1; mode=block", '
                            '"Cache-Control": "no-store"}. '
                            'See next line for the first 1024 characters of the body.\n'
                            '{"pagination": {"count": 1, "total": 1, "size": 10, "next": null, "previous":'
                            ' null, "pages": 1, "sort": "projectTitle", "order": "asc"}, "termFacets": '
                            '{"organ": {"terms": [{"term": "pancreas", "count": 1}], "total": 1, "type": '
                            '"terms"}, "sampleEntityType": {"terms": [{"term": "specimens", "count": 1}], '
                            '"total": 1, "type": "terms"}, "dataUseRestriction": {"terms": [{"term": null, '
                            '"count": 1}], "total": 1, "type": "terms"}, "project": {"terms": [{"term": '
                            '"Single of human pancreas", "count": 1, "projectId": '
                            '["e8642221-4c2c-4fd7-b926-a68bce363c88"]}], "total": 1, "type": "terms"}, '
                            '"sampleDisease": {"terms": [{"term": "normal", "count": 1}], "total": 1, "type": '
                            '"terms"}, "nucleicAcidSource": {"terms": [{"term": "single cell", "count": 1}], '
                            '"total": 1, "type": "terms"}, "assayType": {"terms": [{"term": null, "count": 1}], '
                            '"total": 0, "type": "terms"}, "instrumentManufacturerModel": {"terms": [{"term": '
                            '"Illumina NextSeq 500", "count": 1}], "total": 1, "type": "terms"}, "institution": '
                            '{"terms": [{"term": "Farmers Tru',
                        )
                    ])
