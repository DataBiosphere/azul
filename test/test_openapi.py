from collections import defaultdict
from unittest import mock

from azul.chalice import AzulChaliceApp
from azul.openapi import (
    get_app_specs,
    schema,
)
from azul_test_case import AzulTestCase


class TestGetAppSpecs(AzulTestCase):

    def test_unannotated(self):
        # TODO: Once all endpoints are documented, this test should assert failure
        app = self._mock_app_object()
        self._mock_app_route(app, '/foo')
        get_app_specs(app)

    def test_just_method_spec(self):
        app = self._mock_app_object()
        self._mock_app_route(app, '/foo', methods=['GET'], method_spec={'a': 'b'})
        specs = get_app_specs(app)
        self.assertEqual(specs, ({}, {('/foo', 'get'): {'a': 'b'}}))

    def test_just_path_spec(self):
        app = self._mock_app_object()
        self._mock_app_route(app, '/foo', methods=['GET'], path_spec={'a': 'b'})
        specs = get_app_specs(app)
        self.assertEqual(specs, ({'/foo': {'a': 'b'}}, {}))

    def test_fully_annotated(self):
        app = self._mock_app_object()
        self._mock_app_route(app, '/foo', methods=['GET'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
        specs = get_app_specs(app)
        self.assertEqual(specs, ({'/foo': {'a': 'b'}}, {('/foo', 'get'): {'c': 'd'}}))

    @mock.patch('chalice.Chalice.route')
    def test_multiple_routes(self, mock_route):
        app = self._mock_app_object()

        @app.route('/foo', methods=['GET', 'PUT'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
        @app.route('/foo/too', methods=['GET'], path_spec={'e': 'f'}, method_spec={'g': 'h'})
        def route():
            pass

        self.assertEqual(mock_route.call_count, 2)

        specs = get_app_specs(app)
        self.assertEqual(specs,
                         ({'/foo': {'a': 'b'}, '/foo/too': {'e': 'f'}},
                          {('/foo', 'get'): {'c': 'd'}, ('/foo', 'put'): {'c': 'd'}, ('/foo/too', 'get'): {'g': 'h'}}))

    @mock.patch('chalice.Chalice.route')
    def test_duplicate_method_specs(self, mock_route):
        app = self._mock_app_object()

        with self.assertRaises(AssertionError):
            @app.route('/foo', methods=['GET'], method_spec={'a': 'b'})
            @app.route('/foo', methods=['GET'], method_spec={'a': 'XXX'})
            def route():
                pass

        self.assertEqual(mock_route.call_count, 2)
        # Decorators are applied from the bottom up
        specs = get_app_specs(app)
        self.assertEqual(specs, ({}, {('/foo', 'get'): {'a': 'XXX'}}))

    @mock.patch('chalice.Chalice.route')
    def test_duplicate_path_specs(self, mock_route):
        app = self._mock_app_object()

        @app.route('/foo', ['PUT'], path_spec={'a': 'XXX'})
        def route1():
            pass

        with self.assertRaises(AssertionError):
            @app.route('/foo', ['GET'], path_spec={'a': 'b'})
            def route2():
                pass

        self.assertEqual(mock_route.call_count, 2)
        specs = get_app_specs(app)
        self.assertEqual(specs, ({'/foo': {'a': 'XXX'}}, {}))

    @mock.patch('chalice.Chalice.route')
    def _mock_app_route(self, app, path, mock_route, methods=None, path_spec=None, method_spec=None):
        @app.route(path, methods=methods, path_spec=path_spec, method_spec=method_spec)
        def route_func():
            pass

        mock_route.assert_called_once()
        return route_func

    @mock.patch('chalice.Chalice.__init__')
    def _mock_app_object(self, mock_init):
        """
        Takes a list of functions and mocks the structure of an Chalice app
        object that contains them
        """
        app = AzulChaliceApp('testing')
        mock_init.assert_called_once()

        # Provide attribute that's missing due to mocked superclass init
        app.routes = defaultdict(dict)
        return app


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
