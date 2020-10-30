import json
import os
import sys
from tempfile import (
    TemporaryDirectory,
)
from unittest import (
    mock,
)

from furl import (
    furl,
)
import requests

import azul.changelog
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    MetadataPlugin,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class RequestParameterValidationTest(WebServiceTestCase):
    facet_message = {'Code': 'BadRequestError',
                     'Message': 'BadRequestError: Unknown facet `bad-facet`'}

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                for dirty in True, False:
                    with self.subTest(is_repo_dirty=dirty):
                        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
                            url = self.base_url + "/version"
                            response = requests.get(url)
                            response.raise_for_status()
                            expected_json = {
                                'commit': commit,
                                'dirty': dirty
                            }
                            self.assertEqual(response.json()['git'], expected_json)

    def test_bad_single_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_multiple_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}, 'bad-facet2': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'size': 1,
            'filters': json.dumps({}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
            'sort': 'organPart',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_sample(self):
        url = self.base_url + '/index/samples'
        params = {
            'size': 15,
            'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}, 'bad-facet2': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'size': 15,
            'sort': 'bad-facet',
            'order': 'asc',
            'filters': json.dumps({}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.facet_message, self.facet_message])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = self.base_url + '/index/files'
        params = {
            'size': 15,
            'sort': 'bad-facet',
            'order': 'asc',
            'filters': json.dumps({'organ': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):

        url = self.base_url + '/index/files'
        params = {
            'catalog': self.catalog,
            'size': 15,
            'sort': 'organPart',
            'order': 'asc',
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_single_entity_error_responses(self):
        entity_types = ['files', 'projects']
        for uuid, expected_error_code in [('2b7959bb-acd1-4aa3-9557-345f9b3c6327', 404),
                                          ('-0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb-', 400),
                                          ('FOO', 400)]:
            for entity_type in entity_types:
                with self.subTest(entity_name=entity_type, error_code=expected_error_code, uuid=uuid):
                    url = self.base_url + f'/index/{entity_type}/{uuid}'
                    response = requests.get(url)
                    self.assertEqual(expected_error_code, response.status_code)

    def test_file_order(self):
        url = self.base_url + '/index/files/order'
        response = requests.get(url)
        self.assertEqual(200, response.status_code, response.json())
        actual_field_order = response.json()['order']
        plugin = MetadataPlugin.load(self.catalog).create()
        expected_field_order = plugin.service_config().order_config
        self.assertEqual(expected_field_order, actual_field_order)

    def test_bad_query_params(self):

        def test(url, message, params):
            response = requests.get(url, params=params)
            self.assertEqual(400, response.status_code, response.content)
            response = response.json()
            code = 'BadRequestError'
            self.assertEqual(code, response['Code'])
            self.assertEqual(code + ': ' + message, response['Message'])

        for entity_type in ('files', 'bundles', 'samples'):
            url = self.base_url + f'/index/{entity_type}'
            with self.subTest(entity_type=entity_type):
                with self.subTest(test='extra parameter'):
                    test(url,
                         params=dict(catalog=self.catalog,
                                     some_nonexistent_filter=1),
                         message='Unknown query parameter `some_nonexistent_filter`')
                with self.subTest(test='malformed parameter'):
                    test(url,
                         params=dict(catalog=self.catalog,
                                     size='foo'),
                         message='Invalid value for parameter `size`')
                with self.subTest(test='malformed filter parameter'):
                    test(url,
                         params=dict(catalog=self.catalog,
                                     filters='{"}'),
                         message='The `filters` parameter is not valid JSON')
        url = self.base_url + '/integrations'
        with self.subTest(test='missing required parameter'):
            test(url,
                 params={},
                 message='Missing required query parameters `entity_type`, `integration_type`')

    def test_bad_catalog_param(self):
        url = furl(url=self.base_url, path='/index/files').url
        for catalog, error in [
            ('foo', "Catalog name 'foo' is invalid."),
            ('foo bar', "Catalog name 'foo bar' contains invalid characters.")
        ]:
            response = requests.get(url=url, params={'catalog': catalog})
            self.assertEqual(400, response.status_code, response.json())
            self.assertIn(error, response.json()['Message'])
