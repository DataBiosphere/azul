import os
import sys
import logging
import time
from unittest import mock

import requests
from threading import Thread
from chalice.local import LocalDevServer
from chalice.config import Config as ChaliceConfig
from azul import Config as AzulConfig
from service import WebServiceTestCase

log = logging.getLogger(__name__)


def setUpModule():
    log.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stream_handler)


class ChaliceServerThread(Thread):
    def __init__(self, app, config, host, port):
        super().__init__()
        self.server_wrapper = LocalDevServer(app, config, host, port)

    def run(self):
        self.server_wrapper.serve_forever()

    def kill_thread(self):
        self.server_wrapper.server.shutdown()
        self.server_wrapper.server.server_close()

    def address(self):
        return self.server_wrapper.server.server_address


class FacetNameValidationTest(WebServiceTestCase):
    filter_facet_message = {"Code": "BadRequestError",
                            "Message": "BadRequestError: Unable to filter by undefined facet bad-facet."}
    sort_facet_message = {"Code": "BadRequestError",
                          "Message": "BadRequestError: Unable to sort by undefined facet bad-facet."}

    @property
    def base_url(self):
        address = self.server_thread.address()
        return f"http://{address[0]}:{address[1]}/"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.path_to_app = os.path.join(AzulConfig().project_root, 'lambdas', 'service')
        sys.path.append(cls.path_to_app)
        from app import app
        cls.app = app

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        sys.path.remove(cls.path_to_app)

    def setUp(self):
        super().setUp()
        log.debug("Setting up tests")
        log.debug("Created Thread")
        self.server_thread = ChaliceServerThread(self.app, ChaliceConfig(), 'localhost', 0)
        log.debug("Started Thread")
        self.server_thread.start()
        deadline = time.time() + 10
        while True:
            url = self.base_url
            try:
                response = requests.get(url)
                response.raise_for_status()
            except Exception:
                if time.time() > deadline:
                    raise
                log.debug("Unable to connect to server", exc_info=True)
                time.sleep(1)
            else:
                break

    def tearDown(self):
        log.debug("Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')

    def test_hello_world(self):
        url = self.base_url
        response = requests.get(url)
        self.assertEqual(response.status_code, 200, response.json())
        self.assertEqual({"Hello": "World!"}, response.json())

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        dirty = True
        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
            url = self.base_url + "version"
            response = requests.get(url)
            response.raise_for_status()
            expected_json = {
                'git': {
                    'commit': commit,
                    'dirty': dirty
                }
            }
            self.assertEqual(response.json(), expected_json)

    def test_bad_single_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?from=1&size=1&filters={'file':{'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?from=1&size=1" \
                               "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?size=15&filters={}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=bad-facet&order=asc"

        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_valid_sort_facet_but_bad_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=organPart&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_specimen(self):
        url = self.base_url + "repository/specimens?size=15" \
                               "&filters={'file':{'organPart':{'is':['fake-val2']}}}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = self.base_url + "repository/files?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = self.base_url + "repository/files?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = self.base_url + "repository/files?from=1&size=1" \
                               "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_file(self):
        url = self.base_url + "repository/files?size=15&sort=bad-facet&order=asc" \
                               "&filters={}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = self.base_url + "repository/files?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = self.base_url + "repository/files?size=15&sort=bad-facet&order=asc" \
                               "&filters={'file':{'organ':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):
        url = self.base_url + "repository/files?size=15&sort=organPart&order=asc" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_filter_facet_of_piechart(self):
        url = self.base_url + "repository/files/piecharts?filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_piechart(self):
        url = self.base_url + "repository/files/piecharts" \
                               "?filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_piechart(self):
        url = self.base_url + "repository/files/piecharts" \
                               "?filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_summary_endpoint(self):
        url = self.base_url + "repository/files/summary"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertGreater(summary_object['fileCount'], 0)
        self.assertGreater(summary_object['organCount'], 0)
        self.assertIsNotNone(summary_object['organSummaries'])
