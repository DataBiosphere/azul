import csv
import json
import logging
import os
import requests
import responses
import sys
from unittest import mock

from moto import mock_s3, mock_sts

from azul import config
from azul.service import service_config
from azul.service.responseobjects.storage_service import StorageService
from service import WebServiceTestCase

log = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    log.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log.addHandler(stream_handler)


class FacetNameValidationTest(WebServiceTestCase):
    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    filter_facet_message = {"Code": "BadRequestError",
                            "Message": "BadRequestError: Unable to filter by undefined facet bad-facet."}
    sort_facet_message = {"Code": "BadRequestError",
                          "Message": "BadRequestError: Unable to sort by undefined facet bad-facet."}
    service_config_dir = os.path.dirname(service_config.__file__)

    def test_health(self):
        url = self.base_url + "health"
        response = requests.get(url)
        response.raise_for_status()
        expected_json = {
            'status': 'UP',
            'elasticsearch': {
                'domain': config.es_domain,
                'status': 'UP'
            }
        }
        self.assertEqual(response.json(), expected_json)

    def test_health_es_unreachable(self):
        with mock.patch.dict(os.environ, AZUL_ES_ENDPOINT='nonexisting-index.com:80'):
            url = self.base_url + "health"
            response = requests.get(url)
            response.raise_for_status()
            expected_json = {
                'status': 'UP',
                'elasticsearch': {
                    'domain': config.es_domain,
                    'message': 'Unable to reach the host',
                    'status': 'DOWN'
                }
            }
            self.assertEqual(response.json(), expected_json)

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        for dirty in True, False:
            with self.subTest(is_repo_dirty=dirty):
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

    def test_file_order(self):
        url = self.base_url + 'repository/files/order'
        response = requests.get(url)
        self.assertEqual(200, response.status_code, response.json())

        order_config_filepath = '{}/order_config'.format(self.service_config_dir)
        with open(order_config_filepath, 'r') as order_settings_file:
            actual_field_order = [entity_field for entity_field in response.json()['order']]
            expected_field_order = [entity_field.strip() for entity_field in order_settings_file.readlines()]
            self.assertEqual(expected_field_order, actual_field_order, "Field order is not configured correctly")

    @mock_s3
    @mock_sts
    def test_manifest(self):
        logging.getLogger('test_request_validation').warning('test_manifest is invoked')
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        responses.add_passthru('http://')
        storage_service = StorageService()
        storage_service.create_bucket()

        url = self.base_url + 'repository/files/export?filters={"file":{}}'
        response = requests.get(url)
        self.assertEqual(200, response.status_code, 'Unable to download manifest')
        tsv_file = csv.DictReader(response.iter_lines(decode_unicode=True), delimiter='\t')
        self.assertEqual(len(list(tsv_file)), 1000, 'Wrong number of files were found.')
        manifest_config = json.load(open('{}/request_config.json'.format(self.service_config_dir), 'r'))['manifest']
        expected_fieldnames = list(manifest_config['bundles'].keys()) + list(manifest_config['contents.files'].keys())
        self.assertEqual(expected_fieldnames, tsv_file.fieldnames, 'Manifest headers are not configured correctly')
