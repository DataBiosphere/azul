from azul.chalice import AzulChaliceApp
from azul.openapi import (
    schema,
)
from azul_test_case import AzulTestCase


class TestAppSpecs(AzulTestCase):

    def test_top_level_spec(self):
        spec = {'foo': 'bar'}
        app = AzulChaliceApp('testing', spec=spec)
        self.assertEqual(app.specs, {'foo': 'bar', 'paths': {}}, "Confirm 'paths' is added")
        spec['new key'] = 'new value'
        self.assertNotIn('new key', app.specs, 'Changing input object should not affect specs')

    def test_already_annotated_top_level_spec(self):
        with self.assertRaises(AssertionError):
            AzulChaliceApp('testing', spec={'paths': {'/': {'already': 'annotated'}}})

    def test_unannotated(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'])
        def route():
            pass  # no coverage

        self.assertEqual(app.specs, {'foo': 'bar', 'paths': {}})

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
            }
        }
        self.assertEqual(app.specs, expected_spec)

    def test_just_path_spec(self):
        app = AzulChaliceApp('testing', spec={'foo': 'bar'})

        @app.route('/foo', methods=['GET', 'PUT'], path_spec={'a': 'b'})
        def route():
            pass  # no coverage

        expected_spec = {
            'foo': 'bar',
            'paths': {
                '/foo': {'a': 'b'}
            }
        }
        self.assertEqual(app.specs, expected_spec)

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
            }
        }
        self.assertEqual(app.specs, expected_specs)

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

        @app.route('/foo', ['PUT'], path_spec={'a': 'XXX'})
        def route1():
            pass

        with self.assertRaises(AssertionError) as cm:
            @app.route('/foo', ['GET'], path_spec={'a': 'b'})
            def route2():
                pass
        self.assertEqual(str(cm.exception), 'Only specify path_spec once per route path')


class TestSchemaHelpers(AzulTestCase):

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
