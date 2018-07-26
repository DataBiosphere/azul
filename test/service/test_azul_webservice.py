import os
import sys
import logging
import requests
from threading import Thread
from unittest import TestCase
from chalice.local import LocalDevServer
from chalice.config import Config as ChaliceConfig
from azul import Config as AzulConfig

sys.path.append(os.path.join(AzulConfig().home_directory, 'lambdas', 'service'))

from app import app as service_app

log = logging.getLogger(__name__)
stream_handler = logging.StreamHandler(sys.stdout)
log.addHandler(stream_handler)
log.setLevel(logging.DEBUG)

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


class ChaliceRequestTestCase(TestCase):
    filter_facet_message = {"Code": "BadRequestError",
                            "Message": "BadRequestError: Unable to filter by undefined facet bad-facet."}
    sort_facet_message = {"Code": "BadRequestError",
                          "Message": "BadRequestError: Unable to sort by undefined facet bad-facet."}

    test_host = "http://localhost:54632/"

    def setUp(self):
        log.debug("Setting up tests")
        log.debug("Created Thread")
        self.server_thread = ChaliceServerThread(service_app, ChaliceConfig(), "localhost", 54632)
        log.debug("Started Thread")
        self.server_thread.start()
        log.debug("Created HTML Pool")

    def test_hello_world(self):
        url = self.test_host
        response = requests.get(url)
        self.assertEqual(response.status_code, 200, response.json())
        self.assertEqual({"Hello": "World!"}, response.json())

    def test_bad_single_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?from=1&size=1&filters={'file':{'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?from=1&size=1" \
                               "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?size=15&filters={}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=bad-facet&order=asc"

        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_valid_sort_facet_but_bad_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=organPart&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_specimen(self):
        url = self.test_host + "repository/specimens?size=15" \
                               "&filters={'file':{'organPart':{'is':['fake-val2']}}}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = self.test_host + "repository/files?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = self.test_host + "repository/files?from=1&size=1" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = self.test_host + "repository/files?from=1&size=1" \
                               "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_file(self):
        url = self.test_host + "repository/files?size=15&sort=bad-facet&order=asc" \
                               "&filters={}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = self.test_host + "repository/files?size=15" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = self.test_host + "repository/files?size=15&sort=bad-facet&order=asc" \
                               "&filters={'file':{'organ':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):
        url = self.test_host + "repository/files?size=15&sort=organPart&order=asc" \
                               "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_filter_facet_of_piechart(self):
        url = self.test_host + "repository/files/piecharts?filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_piechart(self):
        url = self.test_host + "repository/files/piecharts" \
                               "?filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_piechart(self):
        url = self.test_host + "repository/files/piecharts" \
                               "?filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def tearDown(self):
        log.debug("Tearing Down Data")
        self.server_thread.kill_thread()
        self.server_thread.join(timeout=10)
        if self.server_thread.is_alive():
            self.fail('Thread is still alive after joining')
