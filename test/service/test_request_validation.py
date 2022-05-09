import json
import os
import sys
from tempfile import (
    TemporaryDirectory,
)
from unittest import (
    mock,
)

import requests

import azul.changelog
from azul.logging import (
    configure_test_logging,
)
from service import (
    WebServiceTestCase,
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


@patch_dss_source
class RequestParameterValidationTest(WebServiceTestCase):
    expected_response = {
        'Code': 'BadRequestError',
        'Message': 'Unknown field `bad-field`'
    }

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                for dirty in True, False:
                    with self.subTest(is_repo_dirty=dirty):
                        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
                            response = requests.get(str(self.base_url.set(path='/version')))
                            response.raise_for_status()
                            expected_json = {
                                'commit': commit,
                                'dirty': dirty
                            }
                            self.assertEqual(response.json()['git'], expected_json)

    def test_bad_single_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_mixed_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_sort_field_of_sample(self):
        params = {
            'size': 1,
            'filters': json.dumps({}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_sort_field_and_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_valid_sort_field_but_bad_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'organPart',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_sort_field_but_valid_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_single_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_mixed_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_sort_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_bad_sort_field_and_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.expected_response, self.expected_response])

    def test_bad_sort_field_but_valid_filter_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({'organ': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    def test_valid_sort_field_but_bad_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'sort': 'organPart',
            'order': 'asc',
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.expected_response, response.json())

    @patch_dss_source
    @patch_source_cache
    def test_single_entity_error_responses(self):
        entity_types = ['files', 'projects']
        for uuid, expected_error_code in [('2b7959bb-acd1-4aa3-9557-345f9b3c6327', 404),
                                          ('-0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb-', 400),
                                          ('FOO', 400)]:
            for entity_type in entity_types:
                with self.subTest(entity_name=entity_type, error_code=expected_error_code, uuid=uuid):
                    url = self.base_url.set(path=('index', entity_type, uuid))
                    response = requests.get(str(url))
                    self.assertEqual(expected_error_code, response.status_code)

    def test_bad_query_params(self):

        def test(url, message, params):
            response = requests.get(url, params=params)
            self.assertEqual(400, response.status_code, response.content)
            response = response.json()
            expected_response = {
                'Code': 'BadRequestError',
                'Message': message
            }
            self.assertEqual(expected_response, response)

        for entity_type in ('files', 'bundles', 'samples'):
            url = self.base_url.set(path=('index', entity_type))
            with self.subTest(entity_type=entity_type):
                with self.subTest(test='extra parameter'):
                    test(str(url),
                         params=dict(catalog=self.catalog,
                                     some_nonexistent_filter=1),
                         message='Unknown query parameter `some_nonexistent_filter`')
                with self.subTest(test='malformed parameter'):
                    test(str(url),
                         params=dict(catalog=self.catalog,
                                     size='foo'),
                         message='Invalid value for parameter `size`')
                with self.subTest(test='malformed filter parameter'):
                    test(str(url),
                         params=dict(catalog=self.catalog,
                                     filters='{"}'),
                         message='The `filters` parameter is not valid JSON')
        with self.subTest(test='missing required parameter'):
            test(str(self.base_url.set(path='/integrations')),
                 params={},
                 message='Missing required query parameters `entity_type`, `integration_type`')

    def test_bad_catalog_param(self):
        for catalog, error in [
            ('foo', "Catalog name 'foo' is invalid."),
            ('foo ', "('Catalog name is invalid', 'foo ')")
        ]:
            url = self.base_url.set(path='/index/files', args=dict(catalog=catalog))
            response = requests.get(str(url))
            self.assertEqual(400, response.status_code, response.json())
            self.assertIn(error, response.json()['Message'])
