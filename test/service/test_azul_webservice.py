import os
import sys
import logging
import requests
from threading import Thread
from unittest import TestCase
from urllib.parse import urljoin
from chalice.local import LocalDevServer
from chalice.config import Config

sys.path.append(os.path.join(os.environ['AZUL_HOME'], 'lambdas', 'service'))

from app import app as service_app


log = logging.Logger('azul-indexer-tests')
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)
log.setLevel(logging.DEBUG)

TEST_PROTOCOL = "http"
TEST_DOMAIN = "localhost"
TEST_PORT = 54632
TEST_HOST = f"{TEST_PROTOCOL}://{TEST_DOMAIN}:{TEST_PORT}"

URL_REQUEST_TIMEOUT = 4


class ChaliceServerThread(Thread):
    def __init__(self, app, config, host, port):
        super().__init__()
        self.server_wrapper = LocalDevServer(app, config, host, port)

    def run(self):
        self.server_wrapper.serve_forever()

    def kill_thread(self):
        self.server_wrapper.server.shutdown()
        self.server_wrapper.server.server_close()


BAD_FACET_HTTP_ERROR_CODE = 400

FILTER_FACET_MESSAGE = {"Code": "BadRequestError",
                        "Message": "BadRequestError: Unable to filter by undefined facet bad-facet."}
SORT_FACET_MESSAGE = {"Code": "BadRequestError",
                      "Message": "BadRequestError: Unable to sort by undefined facet bad-facet."}


class ChaliceRequestTestCase(TestCase):
    def setUp(self):
        log.debug(f"(Test {__name__}): Setting up tests")
        log.debug(f"(Test {__name__}): Created Thread")
        self.server_thread = ChaliceServerThread(service_app, Config(), TEST_DOMAIN, TEST_PORT)
        log.debug(f"(Test {__name__}): Started Thread")
        self.server_thread.start()
        log.debug(f"(Test {__name__}): Created HTML Pool")

    def test_hello_world(self):
        url = urljoin(TEST_HOST, '/')
        response = requests.get(url)
        self.assertEqual(response.status_code, 200, response.json())
        self.assertEqual({"Hello": "World!"}, response.json())

    def test_bad_single_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())

    def test_bad_multiple_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet2%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_mixed_multiple_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22organPart%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_sort_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?size=15"
                                 "&filters=%7B%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(SORT_FACET_MESSAGE, response.json())

    def test_bad_sort_facet_and_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?size=15"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertTrue(response.json() in [SORT_FACET_MESSAGE, FILTER_FACET_MESSAGE])

    def test_valid_sort_facet_but_bad_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?size=15"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D"
                                 "&sort=organPart"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_specimen(self):
        url = urljoin(TEST_HOST, "repository/specimens?size=15"
                                 "&filters=%7B%22file%22:%7B%22"
                                 "organPart%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(SORT_FACET_MESSAGE, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet2%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?from=1"
                                 "&size=1"
                                 "&filters=%7B%22file%22:%7B%22organPart%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_sort_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?size=15"
                                 "&filters=%7B%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(SORT_FACET_MESSAGE, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?size=15"
                                 "&filters=%7B%22file%22:%7B%22"
                                 "bad-facet%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertTrue(response.json() in [SORT_FACET_MESSAGE, FILTER_FACET_MESSAGE])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?size=15"
                                 "&filters=%7B%22file%22:%7B%22"
                                 "organPart%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D"
                                 "&sort=bad-facet"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(SORT_FACET_MESSAGE, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):
        url = urljoin(TEST_HOST, "repository/files?size=15"
                                 "&filters=%7B%22file%22:%7B%22"
                                 "bad-facet%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D"
                                 "&sort=organPart"
                                 "&order=asc")
        response = requests.get(url)
        self.assertEqual(BAD_FACET_HTTP_ERROR_CODE, response.status_code, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_filter_facet_of_piechart(self):
        url = urljoin(TEST_HOST, "repository/files/piecharts"
                                 "?filters=%7B%22file%22:%7B%22"
                                 "bad-facet%22:%7B%22is%22:%5B%22fake-val2%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(response.status_code, BAD_FACET_HTTP_ERROR_CODE, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_bad_multiple_filter_facet_of_piechart(self):
        url = urljoin(TEST_HOST, "repository/files/piecharts"
                                 "?filters=%7B%22file%22:%7B%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet2%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(response.status_code, BAD_FACET_HTTP_ERROR_CODE, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def test_mixed_multiple_filter_facet_of_piechart(self):
        url = urljoin(TEST_HOST, "repository/files/piecharts"
                                 "?filters=%7B%22file%22:%7B%22organPart%22:%7B%22is%22:%5B%22fake-val%22%5D%7D,"
                                 "%22bad-facet%22:%7B%22is%22:%5B%22fake-val%22%5D%7D%7D%7D")
        response = requests.get(url)
        self.assertEqual(response.status_code, BAD_FACET_HTTP_ERROR_CODE, response.json())
        self.assertEqual(FILTER_FACET_MESSAGE, response.json())

    def tearDown(self):
        log.debug(f"(Test {__name__}): Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')
