from unittest.mock import (
    MagicMock,
    patch,
)

from furl import (
    furl,
)
from jsonschema import (
    Draft4Validator,
)
from openapi_spec_validator import (
    validate_spec,
)
import requests

from azul.chalice import (
    AzulChaliceApp,
)
from azul.openapi import (
    params,
    schema,
)
from azul.openapi.validation import (
    ValidationError,
)
from azul_test_case import (
    AzulUnitTestCase,
)
from service import (
    WebServiceTestCase,
)


@patch('azul.chalice.AzulChaliceApp.self_url',
       MagicMock(return_value='https://fake.url'))
class TestAppSpecs(AzulUnitTestCase):

    def test_top_level_spec(self):
        spec = {'foo': 'bar'}
        app = AzulChaliceApp('testing', spec=spec)
        self.assertEqual(app._specs, {'foo': 'bar', 'paths': {}}, "Confirm 'paths' is added")
        spec['new key'] = 'new value'
        self.assertNotIn('new key', app.spec(), 'Changing input object should not affect specs')

    def test_already_annotated_top_level_spec(self):
        with self.assertRaises(AssertionError):
            AzulChaliceApp('testing', spec={'paths': {'/': {'already': 'annotated'}}})

    def test_unannotated(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'])
        def route():
            pass  # no coverage

        expected = {
            'foo': 'bar',
            'paths': {},
            'tags': [],
            'servers': [{'url': 'https://fake.url'}]
        }
        self.assertEqual(app.spec(), expected)

    def test_just_method_spec(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'], method_spec={'a': 'b'})
        def route():
            pass  # no coverage

        expected_spec = {
            'foo': 'bar',
            'paths': {
                '/foo': {
                    'get': {'a': 'b'},
                    'put': {'a': 'b'}
                }
            },
            'tags': [],
            'servers': [{'url': 'https://fake.url'}]
        }
        self.assertEqual(app.spec(), expected_spec)

    def test_just_path_spec(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'], path_spec={'a': 'b'})
        def route():
            pass  # no coverage

        expected_spec = {
            'foo': 'bar',
            'paths': {
                '/foo': {'a': 'b'}
            },
            'tags': [],
            'servers': [{'url': 'https://fake.url'}]
        }
        self.assertEqual(app.spec(), expected_spec)

    def test_fully_annotated_override(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})
        path_spec = {
            'a': 'b',
            'get': {'c': 'd'}
        }

        with self.assertRaises(AssertionError) as cm:
            @app.route('/foo', methods=['GET'], path_spec=path_spec, method_spec={'e': 'f'})
            def route():
                pass  # no coverage
        self.assertEqual(str(cm.exception), 'Only specify method_spec once per route path and method')

    def test_multiple_routes(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
        @app.route('/foo/too', methods=['GET'], path_spec={'e': 'f'}, method_spec={'g': 'h'})
        def route():
            pass  # no coverage

        expected_specs = {
            'foo': 'bar',
            'paths': {
                '/foo': {
                    'a': 'b',
                    'get': {'c': 'd'},
                    'put': {'c': 'd'}
                },
                '/foo/too': {
                    'e': 'f',
                    'get': {'g': 'h'}
                }
            },
            'tags': [],
            'servers': [{'url': 'https://fake.url'}]
        }
        self.assertEqual(app.spec(), expected_specs)

    def test_duplicate_method_specs(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        with self.assertRaises(AssertionError) as cm:
            @app.route('/foo', methods=['GET'], method_spec={'a': 'b'})
            @app.route('/foo', methods=['GET'], method_spec={'a': 'XXX'})
            def route():
                pass
        self.assertEqual(str(cm.exception), 'Only specify method_spec once per route path and method')

    def test_duplicate_path_specs(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['PUT'], path_spec={'a': 'XXX'})
        def route1():
            pass

        with self.assertRaises(AssertionError) as cm:
            @app.route('/foo', methods=['GET'], path_spec={'a': 'b'})
            def route2():
                pass
        self.assertEqual(str(cm.exception), 'Only specify path_spec once per route path')

    def test_shared_path_spec(self):
        """
        Assert that, when sharing the path_spec, routes don't overwrite each
        other's properties.
        """
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})
        shared_path_spec = {
            'parameters': [
                params.query('foo', schema.optional({'type': 'string'})),
            ]
        }
        for i in range(2):
            @app.route(f'/swagger-test-{i}',
                       methods=['GET'],
                       cors=True,
                       path_spec=shared_path_spec,
                       method_spec={'summary': f'Swagger test {i}'})
            def swagger_test():
                pass

        method_specs = app.spec()['paths'].values()
        self.assertNotEqual(*method_specs)

    def test_unused_tags(self):
        app = AzulChaliceApp('testing', spec={
            'tags': [{'name': name} for name in ('foo', 'bar', 'baz', 'qux')]
        })

        @app.route('/foo', methods=['PUT'], method_spec={'tags': ['foo', 'qux']})
        def route1():
            pass

        self.assertEqual(app.spec()['tags'], [{'name': 'foo'}, {'name': 'qux'}])


class TestSchemaHelpers(AzulUnitTestCase):

    def test_complex_object(self):
        self.assertEqual(
            schema.object(
                git=schema.object(
                    commit=str,
                    dirty=bool
                ),
                changes=schema.array(
                    schema.object(
                        title='string',
                        issues=schema.array(str),
                        upgrade=schema.array(str),
                        notes=schema.optional(str)
                    )
                )
            ),
            {
                'type': 'object',
                'properties': {
                    'git': {
                        'type': 'object',
                        'properties': {
                            'commit': {
                                'type': 'string'
                            },
                            'dirty': {
                                'type': 'boolean'
                            }
                        },
                        'required': ['commit', 'dirty'],
                        'additionalProperties': False
                    },
                    'changes': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'title': {
                                    'type': 'string'
                                },
                                'issues': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'string'
                                    }
                                },
                                'upgrade': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'string'
                                    }
                                },
                                'notes': {
                                    'type': 'string'
                                }
                            },
                            'required': [
                                'title',
                                'issues',
                                'upgrade'
                            ],
                            'additionalProperties': False
                        }
                    }
                },
                'required': ['git', 'changes'],
                'additionalProperties': False
            }
        )

    def test_misuse(self):
        # Only `object_with` handles required fields via the optional()
        # wrapper.
        try:
            # noinspection PyTypeChecker
            schema.make_type(schema.optional(str))
        except AssertionError as e:
            self.assertIn(schema.optional, e.args)
        else:
            self.fail()


class TestServiceSpecValidation(WebServiceTestCase):

    def test_validate_spec(self):
        response = requests.get(url=furl(url=self.base_url, path='openapi').url)
        response.raise_for_status()
        spec = response.json()
        validate_spec(spec)
        Draft4Validator.check_schema(spec)

    def test_default_spec_params(self):
        for path, methods in self.app_module.app.spec()['paths'].items():
            for method, spec in methods.items():
                if isinstance(spec, dict):
                    validators = self.app_module.app.get_spec_validators(path, method)
                    for validator in validators.values():
                        if validator.default:
                            with self.subTest(path=path, method=method, validator=validator.name):
                                try:
                                    validator.validate(validator.default)
                                except ValidationError:
                                    self.fail('Validation errors should not be raised for default values')
