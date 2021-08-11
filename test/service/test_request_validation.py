import json
import operator
import os
from random import (
    shuffle,
)
import sys
from tempfile import (
    TemporaryDirectory,
)
import unittest
from unittest import (
    mock,
)

from more_itertools import (
    one,
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
    patch_dss_endpoint,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class RequestParameterValidationTest(WebServiceTestCase):

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

    def expected_invalid_response(self,
                                  path,
                                  invalid_params=None,
                                  missing_params=None,
                                  extra_params=None,
                                  http_method: str = 'get'):
        parameters_in_path = {
            parameter['name']: parameter
            for parameter in
            [
                *self.app_module.app._specs['paths'][path].get('parameters', ()),
                *self.app_module.app._specs['paths'][path][http_method].get('parameters', ())
            ]
        }

        def invalid_object(parameter, missing=False):
            parameter_spec = parameters_in_path.get(parameter['name'], {})
            parameter_details = {
                **parameter,
                **({'schema': parameter_spec['schema']}
                   if 'schema' in parameter_spec else {}),
                **({'content': parameter_spec['content']}
                   if 'content' in parameter_spec else {}),
            }
            if missing:
                parameter_details['required'] = True
            return parameter_details

        return {
            'title': 'validation error',
            **({'invalid_parameters': [invalid_object(param) for param in invalid_params]}
               if invalid_params else {}),
            **({'missing_parameters': [
                invalid_object(param, missing=True)
                for param in missing_params
            ]} if missing_params else {}),
            **({'extra_parameters': [param['name'] for param in extra_params]}
               if extra_params else {})
        }

    paths = ('/index/samples', '/index/files', '/index/projects', '/index/bundles')

    def test_bad_single_filter_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({'bad-facet': {'is': ['fake-val']}}),
                        'message': f'Unknown facet {"bad-facet"!r}'
                    }
                ]
                params = {
                    'catalog': self.catalog,
                    'size': 1,
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_multiple_filter_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({
                            'bad-facet': {'is': ['fake-val']},
                            'bad-facet2': {'is': ['fake-val2']}
                        }),
                        'message': f'Unknown facet {"bad-facet"!r}'
                    }
                ]
                params = {
                    'catalog': self.catalog,
                    'size': 1,
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_mixed_multiple_filter_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({
                            'organPart': {'is': ['fake-val']},
                            'bad-facet': {'is': ['fake-val']}
                        }),
                        'message': f'Unknown facet {"bad-facet"!r}'
                    }
                ]
                params = {
                    'catalog': self.catalog,
                    'size': 1,
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_sort_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'sort',
                        'value': 'bad-facet',
                        'message': 'Invalid parameter'
                    }
                ]
                params = {
                    'size': 1,
                    'filters': json.dumps({}),
                    'order': 'asc',
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_sort_facet_and_filter_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({'bad-facet': {'is': ['fake-val']}}),
                        'message': f'Unknown facet {"bad-facet"!r}'
                    },
                    {
                        'name': 'sort',
                        'value': 'bad-facet',
                        'message': 'Invalid parameter'
                    }
                ]
                params = {
                    'size': 15,
                    'order': 'asc',
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_valid_sort_facet_but_bad_filter_facet(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({'bad-facet': {'is': ['fake-val']}}),
                        'message': f'Unknown facet {"bad-facet"!r}'
                    }
                ]
                params = {
                    'catalog': self.catalog,
                    'size': 15,
                    'sort': 'organPart',
                    'order': 'asc',
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_sample(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'sort',
                        'value': 'bad-facet',
                        'message': 'Invalid parameter'
                    }
                ]
                params = {
                    'size': 15,
                    'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
                    'order': 'asc',
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    @patch_dss_endpoint
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

    def test_file_order(self):
        url = self.base_url.set(path='/index/files/order')
        response = requests.get(str(url))
        self.assertEqual(200, response.status_code, response.json())
        actual_field_order = response.json()['order']
        plugin = MetadataPlugin.load(self.catalog).create()
        expected_field_order = plugin.service_config().order_config
        self.assertEqual(expected_field_order, actual_field_order)

    def test_bad_filter_relation(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({'organPart': {'bad': ['fake-val2']}}),
                        'message': "Unknown relation in the 'filters' parameter entry for 'organPart'"
                    }
                ]
                params = {
                    'size': 15,
                    'sort': 'organPart',
                    'order': 'asc',
                    **({param['name']: param['value'] for param in invalid_params})

                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_single_facet_multiple_relations(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({
                            'organPart': {
                                'bad': ['fake-val2'], 'foo': ['bar']
                            }
                        }),
                        'message': "The 'filters' parameter entry for 'organPart' may only specify"
                                   " a single relation"
                    }
                ]
                params = {
                    **({param['name']: param['value'] for param in invalid_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_multiple_facets_multiple_relations(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({
                            'entryId': {
                                'is': ['bar'],
                                'within': ['baz']
                            },
                            'organPart': {
                                'is': ['foo'],
                            }
                        }),
                        'message': "The 'filters' parameter entry for 'entryId' may"
                                   " only specify a single relation"
                    }
                ]
                url = self.base_url.set(path=path)
                response = requests.get(str(url),
                                        params={**({param['name']: param['value'] for param in invalid_params})})
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_relation_type(self):
        for path in self.paths:
            with self.subTest(path=path):
                invalid_params = [
                    {
                        'name': 'filters',
                        'value': json.dumps({'organPart': "'is': ['foo']'"}),
                        'message': "The 'filters' parameter value for 'organPart'"
                                   " must be a JSON object"
                    }
                ]
                url = self.base_url.set(path=path)
                response = requests.get(str(url),
                                        params={**({param['name']: param['value'] for param in invalid_params})})
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_bad_nested_relation_value(self):
        for path in self.paths:
            facet = 'organismAgeRange'
            for relation in ['contains', 'within', 'intersects']:
                invalid_filter_item_type = {facet: {relation: [[23, 33], 'bar']}}
                invalid_filter_item_count = {facet: {relation: [[23, 33, 70]]}}
                for invalid_filter in (invalid_filter_item_type, invalid_filter_item_count):
                    with self.subTest(relation=relation, invalid_filter=invalid_filter, path=path):
                        invalid_params = [
                            {
                                'name': 'filters',
                                'value': json.dumps(invalid_filter),
                                'message': f"The value of the {relation!r} relation in the 'filters' parameter"
                                           f" entry for {facet!r} is invalid"
                            }
                        ]
                        url = self.base_url.set(path=path)
                        response = requests.get(url=url.url,
                                                params={
                                                    **({param['name']: param['value'] for param in invalid_params})})
                        self.assertEqual(400, response.status_code, response.json())
                        expected_response = self.expected_invalid_response(path=path,
                                                                           invalid_params=invalid_params)
                        self.assertEqual(expected_response, response.json())

    def test_extra_parameters(self):
        for path in self.paths:
            with self.subTest(path=path):
                extra_params = [
                    {
                        'name': 'some_nonexistent_filter',
                        'value': 1,
                        'message': 'Invalid parameter'
                    }
                ]
                params = {
                    'catalog': self.catalog,
                    **({param['name']: param['value'] for param in extra_params})
                }
                url = self.base_url.set(path=path)
                response = requests.get(str(url), params=params)
                self.assertEqual(400, response.status_code, response.content)
                expected_response = self.expected_invalid_response(path=path,
                                                                   extra_params=extra_params)
                self.assertEqual(expected_response, response.json())

    def test_malformed_filter(self):
        for path in self.paths:
            invalid_params = [
                {
                    'name': 'filters',
                    'value': '{"}',
                    'message': 'Invalid JSON'
                }
            ]
            params = {
                'catalog': self.catalog,
                **({param['name']: param['value'] for param in invalid_params})
            }
            url = self.base_url.set(path=path)
            response = requests.get(str(url), params=params)
            self.assertEqual(400, response.status_code, response.content)
            expected_response = self.expected_invalid_response(path=path,
                                                               invalid_params=invalid_params)
            self.assertEqual(expected_response, response.json())

    # FIXME: place this subtest back when the `/integrations` endpoint is spec'd out
    #        https://github.com/DataBiosphere/azul/issues/1984
    @unittest.skip(reason='No spec defined for /integration endpoint')
    def test_missing_parameters(self):
        path = '/integrations'
        params = {}
        url = self.base_url.set(path=path)
        response = requests.get(str(url), params=params)
        self.assertEqual(400, response.status_code, response.content)
        expected_response = self.expected_invalid_response(path=path,
                                                           missing_params=[])
        self.assertEqual(expected_response, response.json())

    def test_invalid_uuid(self):
        for invalid_uuid in {'foo', '53F12DA3-8585-4017-9e01-1473BDCD7BC5'}:
            with self.subTest(uuid=invalid_uuid):
                url = self.base_url.set(path=f'/repository/files/{invalid_uuid}')
                response = requests.get(str(url), params={'replica': 'aws'})
                self.assertEqual(400, response.status_code, response.json())
                self.assertIn("Invalid characters within parameter",
                              one(response.json()['invalid_parameters'])['message'])

    def test_invalid_enum_and_pattern(self):
        for path in self.paths:
            for catalog, error_message in [
                ('foo', 'Invalid parameter'),
                ('foo bar', 'Invalid characters within parameter')
            ]:
                invalid_params = [
                    {
                        'name': 'catalog',
                        'value': catalog,
                        'message': error_message
                    }
                ]
                url = self.base_url.set(path=path)
                response = requests.get(str(url),
                                        params={**({param['name']: param['value'] for param in invalid_params})})
                self.assertEqual(400, response.status_code, response.json())
                expected_response = self.expected_invalid_response(path=path,
                                                                   invalid_params=invalid_params)
                self.assertEqual(expected_response, response.json())

    def test_deterministic_response(self):
        for path in self.paths:
            shuffle_parameters = [
                ('filters', '{"}', 'Invalid JSON'),
                ('catalog', 'foo', 'Invalid parameter'),
                ('sort', 'bar', 'Invalid parameter'),
                ('order', 'asc', '')]
            shuffle(shuffle_parameters)
            params = {name: value for name, value, _ in shuffle_parameters}
            url = self.base_url.set(path=path)
            response = requests.get(str(url), params=params)
            self.assertEqual(400, response.status_code, response.json())
            print(response.json())
            invalid_params = sorted([
                {
                    'name': name,
                    'value': value,
                    'message': message
                } for name, value, message in shuffle_parameters if message
            ], key=operator.itemgetter('name'))
            expected_response = self.expected_invalid_response(path=path,
                                                               invalid_params=invalid_params)
            self.assertEqual(expected_response, response.json())

    @unittest.skip('https://github.com/DataBiosphere/azul/issues/2465')
    def test_missing_uuid(self):
        url = self.base_url.set(path='/fetch/repository/files/')
        response = requests.get(url, params={'replica': 'aws'})
        self.assertEqual(400, response.status_code)

    def test_default_for_missing_params(self):
        path = '/test/mock/endpoints'
        method_spec = {
            'parameters': [
                {
                    'in': 'query',
                    'schema': {
                        'type': 'string',
                        'pattern': '^([a-z0-9]{1,64})$',
                        'enum': [self.catalog],
                        **({} if default is None else {'default': default})
                    },
                    'required': required,
                    'name': f'required-{required}-default-{default}'
                } for required in (True, False) for default in (self.catalog, None)
            ],
            'responses': {
                '200': {
                    'description': 'OK'
                }
            }
        }

        @self.app_module.app.route(path,
                                   validate=True,
                                   path_spec=None,
                                   method_spec=method_spec,
                                   methods=['GET'])
        def test_method():
            return dict(self.app_module.app.current_request.query_params)

        url = self.base_url.set(path=path)
        with self.subTest('Test missing required parameter'):
            response = requests.get(str(url))
            self.assertEqual(400, response.status_code, response.json())
            missing_params = [
                {
                    'name': 'required-True-default-None',
                    'in': 'query'
                }
            ]
            expected_response = self.expected_invalid_response(path=path,
                                                               missing_params=missing_params)
            self.assertEqual(expected_response, response.json())
        with self.subTest('Test default value to view function'):
            response = requests.get(url=url,
                                    params={'required-True-default-None': self.catalog})
            self.assertEqual(200, response.status_code)
            self.assertEqual({
                'required-True-default-None': 'test',
                'required-True-default-test': 'test',
                'required-False-default-test': 'test'
            }, response.json())
