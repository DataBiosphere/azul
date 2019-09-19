import json
import re
import requests

from service import WebServiceTestCase

from azul.chalice import log
from azul.logging import configure_test_logging


def setUpModule():
    configure_test_logging()


class TestRequestResponse(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def _get_request_logs(self, level, endpoint, params={}):
        with self.assertLogs(logger=log, level=level) as logs:
            url = self.base_url + endpoint
            requests.get(url, params=params)
            return logs.output

    def test_request_logs_debug_off(self):
        logs = self._get_request_logs(level='INFO', endpoint='/health/basic')
        self.assertEqual(1, sum(1 for msg in logs if re.search(r"^INFO.*Received GET request to '/health/basic'", msg)))
        self.assertEqual(1, sum(1 for msg in logs if re.search(r"^INFO.*Returning 200 response.*set AZUL_DEBUG", msg)))

    def test_request_logs_debug_on(self):
        logs = self._get_request_logs(level='DEBUG', endpoint='/health/basic')
        log_started_count = 0
        log_ended_count = 0
        for msg in logs:
            if re.search(r"^INFO.*Received GET request to '/health/basic'", msg):
                log_started_count += 1
            if re.search(r"^DEBUG.*Returning 200 response.*See next line", msg):
                log_ended_count += 1
                match = re.search(r'See next line.*\n({.*})$', msg)
                self.assertGreater(len(match.group(1)), 2)
                response = json.loads(match.group(1))
                self.assertIsInstance(response, dict)
                self.assertIn('up', response)
        self.assertEqual(log_started_count, 1)
        self.assertEqual(log_ended_count, 1)

