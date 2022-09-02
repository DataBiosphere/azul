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
from requests import (
    Response,
)

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


class RequestParameterValidationTest(WebServiceTestCase):

    def assertResponseStatus(self, url: furl, status: int) -> Response:
        response = requests.get(str(url))
        self.assertEqual(status, response.status_code, response.content)
        return response

    def assertErrorMessage(self, url: furl, status: int, code: str, message: str):
        response = self.assertResponseStatus(url, status)
        expected_response = {
            'Code': code,
            'Message': message
        }
        self.assertEqual(expected_response, response.json())

    def assertBadRequest(self, url: furl, message: str):
        self.assertErrorMessage(url, 400, 'BadRequestError', message)

    def assertNotFound(self, url: furl, message: str):
        self.assertErrorMessage(url, 404, 'NotFoundError', message)

    def assertBadField(self, url: furl):
        self.assertBadRequest(url, 'Unknown field `bad-field`')

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                for dirty in True, False:
                    with self.subTest(is_repo_dirty=dirty):
                        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
                            url = self.base_url.set(path='/version')
                            response = self.assertResponseStatus(url, 200)
                            expected_json = {
                                'commit': commit,
                                'dirty': dirty
                            }
                            self.assertEqual(expected_json, response.json()['git'])

    def test_bad_single_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_mixed_multiple_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_of_sample(self):
        params = {
            'size': 1,
            'filters': json.dumps({}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_and_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_valid_sort_field_but_bad_filter_field_of_sample(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}}),
            'sort': 'organPart',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_but_valid_filter_field_of_sample(self):
        params = {
            'size': 15,
            'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
            'sort': 'bad-field',
            'order': 'asc',
        }
        url = self.base_url.set(path='/index/samples', args=params)
        self.assertBadField(url)

    def test_bad_single_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_bad_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'bad-field': {'is': ['fake-val']}, 'bad-field2': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_mixed_multiple_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-field': {'is': ['fake-val']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_and_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_bad_sort_field_but_valid_filter_field_of_file(self):
        params = {
            'size': 15,
            'sort': 'bad-field',
            'order': 'asc',
            'filters': json.dumps({'organ': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

    def test_valid_sort_field_but_bad_filter_field_of_file(self):
        params = {
            'catalog': self.catalog,
            'size': 15,
            'sort': 'organPart',
            'order': 'asc',
            'filters': json.dumps({'bad-field': {'is': ['fake-val2']}}),
        }
        url = self.base_url.set(path='/index/files', args=params)
        self.assertBadField(url)

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
                    self.assertResponseStatus(url, expected_error_code)

    def test_bad_query_params(self):

        for entity_type in ('files', 'bundles', 'samples'):
            url = self.base_url.set(path=('index', entity_type))
            with self.subTest(entity_type=entity_type):
                with self.subTest(test='extra parameter'):
                    url.args = dict(catalog=self.catalog,
                                    some_nonexistent_filter=1)
                    self.assertBadRequest(url, 'Unknown query parameter `some_nonexistent_filter`')
                with self.subTest(test='malformed parameter'):
                    url.args = dict(catalog=self.catalog,
                                    size='foo')
                    self.assertBadRequest(url, 'Invalid value for parameter `size`')
                with self.subTest(test='malformed filter parameter'):
                    url.args = dict(catalog=self.catalog,
                                    filters='{"}')
                    self.assertBadRequest(url, 'The `filters` parameter is not valid JSON')
        with self.subTest(test='missing required parameter'):
            url = self.base_url.set(path='/integrations')
            self.assertBadRequest(url, 'Missing required query parameters `entity_type`, `integration_type`')

    def test_bad_catalog_param(self):
        for path in (*('/index/' + e for e in ('summary', 'files')),
                     '/manifest/files',
                     '/repository/files/74897eb7-0701-4e4f-9e6b-8b9521b2816b'):
            for catalog, test, message in [
                ('foo', self.assertNotFound, "Catalog name 'foo' does not exist. Must be one of %r." % {self.catalog}),
                ('foo ', self.assertBadRequest, "('Catalog name is invalid', 'foo ')")
            ]:
                with self.subTest(path=path, catalog=catalog):
                    url = self.base_url.set(path=path, args=dict(catalog=catalog))
                    test(url, message)

    def test_bad_entity_type(self):
        bad_entity_type = 'spiders'
        good_entity_types = set(self.app_module.app.metadata_plugin.exposed_indices)
        assert bad_entity_type not in good_entity_types
        url = self.base_url.set(path='/index/' + bad_entity_type)
        expected = (f'Entity type {bad_entity_type!r} is invalid for catalog '
                    f'{self.catalog!r}. Must be one of {good_entity_types}.')
        self.assertBadRequest(url, expected)

    def test_bad_manifest_format(self):
        bad_format = 'fluffy'
        good_formats = {f.value for f in self.app_module.app.metadata_plugin.manifest_formats}
        assert bad_format not in good_formats
        url = self.base_url.set(path='/manifest/files',
                                query_params={'format': bad_format})
        expected = (f'Unknown manifest format `{bad_format}`. '
                    f'Must be one of {good_formats}')
        self.assertBadRequest(url, expected)
