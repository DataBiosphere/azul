from unittest.mock import MagicMock

from azul.openapi import (
    get_app_specs,
    openapi_spec,
)
from azul_test_case import AzulTestCase


class TestGetAppSpecs(AzulTestCase):

    def test_unannotated(self):
        # TODO: Once all endpoints are documented, this test should assert failure
        app = self._mock_app_object([lambda x: x])
        get_app_specs(app)

    def test_just_method_spec(self):
        route = self._annotate_func('/foo', ['GET'], method_spec={'a': 'b'})
        app = self._mock_app_object([route])
        specs = get_app_specs(app)
        self.assertEqual(specs, ({}, {('/foo', 'get'): {'a': 'b'}}))

    def test_just_path_spec(self):
        route = self._annotate_func('/foo', ['GET'], path_spec={'a': 'b'})
        app = self._mock_app_object([route])
        specs = get_app_specs(app)
        self.assertEqual(specs, ({'/foo': {'a': 'b'}}, {}))

    def test_fully_annotated(self):
        route = self._annotate_func('/foo', ['GET'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
        app = self._mock_app_object([route])
        specs = get_app_specs(app)
        self.assertEqual(specs, ({'/foo': {'a': 'b'}}, {('/foo', 'get'): {'c': 'd'}}))

    def test_duplicate_method_specs(self):
        """
        This doesn't fail because we expect Chalice to disallow duplicate specs
        like this inside of app.py
        """

        @openapi_spec('/foo', ['GET'], method_spec={'a': 'XXX'})
        @openapi_spec('/foo', ['GET'], method_spec={'a': 'b'})
        def route():
            pass

        app = self._mock_app_object([route])
        specs = get_app_specs(app)
        self.assertEqual(specs, ({}, {('/foo', 'get'): {'a': 'XXX'}}))

    def test_duplicate_path_specs(self):
        """
        We have to catch this manually in get_app_specs
        """

        @openapi_spec('/foo', ['PUT'], path_spec={'a': 'XXX'})
        def route1():
            pass

        @openapi_spec('/foo', ['GET'], path_spec={'a': 'b'})
        def route2():
            pass

        app = self._mock_app_object([route1, route2])
        with self.assertRaises(AssertionError):
            get_app_specs(app)

    def _annotate_func(self, route, methods, path_spec=None, method_spec=None):
        @openapi_spec(route, methods, path_spec=path_spec, method_spec=method_spec)
        def route():
            pass

        return route

    def _mock_app_object(self, funcs):
        """
        Takes a list of functions and mocks the structure of an Chalice app
        object that contains them
        """
        app = MagicMock()
        mock_routes = []
        for f in funcs:
            route = MagicMock()
            route.view_function = f
            route_wrapper = MagicMock()
            route_wrapper.values.return_value = [route]
            mock_routes.append(route_wrapper)
        app.routes.values.return_value = mock_routes
        return app
