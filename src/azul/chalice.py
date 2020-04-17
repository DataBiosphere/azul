import json
import logging
from typing import (
    Dict,
    Tuple,
    Optional,
    Iterable,
)

from chalice import Chalice
from chalice.app import Request

from azul import (
    config,
)
from azul.json import json_head
from azul.openapi import (
    clean_specs,
    merge_dicts,
    join_specs,
)
from azul.types import (
    LambdaContext,
    JSON,
)

log = logging.getLogger(__name__)


class AzulChaliceApp(Chalice):

    def __init__(self, app_name, unit_test=False):
        self.unit_test = unit_test
        self.path_specs: Dict[str, JSON] = {}
        self.method_specs: Dict[Tuple[str, str], JSON] = {}
        super().__init__(app_name, debug=config.debug > 0, configure_logs=False)

    def route(self,
              path: str,
              enabled: bool = True,
              path_spec: Optional[JSON] = None,
              method_spec: Optional[JSON] = None,
              **kwargs):
        """
        Decorates a view handler function in a Chalice application.

        See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route.

        :param path: See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route

        :param path_spec: Corresponds to an OpenAPI Paths Object. See
                          https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.3.md#pathsObject
                          If multiple `@app.route` invocations refer to the same
                          path (but with different HTTP methods), only specify
                          this argument for one of them, otherwise an
                          AssertionError will be raised.

        :param method_spec: Corresponds to an OpenAPI Operation Object. See
                            https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.3.md#operationObject
                            This should be specified for every `@app.route`
                            invocation.


        :param enabled: If False, do not route any requests to the decorated
                        view function. The application will behave as if the
                        view function wasn't decorated.
        """
        if enabled:
            methods = kwargs.get('methods', ())
            chalice_decorator = super().route(path, **kwargs)

            def decorator(view_func):
                self._register_spec(path, path_spec, method_spec, methods)
                # Stash the URL path a view function is bound to as an attribute of
                # the function itself.
                view_func.path = path
                return chalice_decorator(view_func)

            return decorator
        else:
            return lambda view_func: view_func

    def test_route(self, *args, **kwargs):
        """
        A route that's only enabled during unit tests.
        """
        return self.route(*args, enabled=self.unit_test, **kwargs)

    def annotated_specs(self, raw_specs, toplevel_spec) -> JSON:
        """
        Finds all routes in app that are decorated with @AzulChaliceApp.route and adds this
        information into the api spec downloaded from API Gateway.

        :param raw_specs: Spec from API Gateway corresponding to the Chalice app
        :param toplevel_spec: Top level OpenAPI info, definitions, etc.
        :return: The annotated specifications
        """
        clean_specs(raw_specs)
        specs = merge_dicts(toplevel_spec, raw_specs, override=True)
        return join_specs(specs, self.path_specs, self.method_specs)

    def _register_spec(self,
                       path: str,
                       path_spec: Optional[JSON],
                       method_spec: Optional[JSON],
                       methods: Iterable[str]):

        if path_spec is not None:
            assert path not in self.path_specs, 'Only specify path_spec once per route path'
            self.path_specs[path] = path_spec

        if method_spec is not None:
            new_method_specs = {
                # Method names in OpenAPI routes must be lower case
                (path, method.lower()): method_spec
                for method in methods
            }
            no_duplicates = set(new_method_specs.keys()).isdisjoint(set(self.method_specs.keys()))
            assert no_duplicates, 'Only specify method_spec once per route path and method'
            self.method_specs.update(new_method_specs)

    def _get_view_function_response(self, view_function, function_args):
        self._log_request()
        response = super()._get_view_function_response(view_function, function_args)
        self._log_response(response)
        return response

    def _log_request(self):
        if log.isEnabledFor(logging.INFO):
            context = self.current_request.context
            query = self.current_request.query_params
            if query is not None:
                # Convert MultiDict to a plain dict that can be converted to
                # JSON. Also flatten the singleton values.
                query = {k: v[0] if len(v) == 1 else v for k, v in ((k, query.getlist(k)) for k in query.keys())}
            log.info(f"Received {context['httpMethod']} request "
                     f"to '{context['path']}' "
                     f"with{' parameters ' + json.dumps(query) if query else 'out parameters'}.")

    def _log_response(self, response):
        if log.isEnabledFor(logging.DEBUG):
            n = 1024
            log.debug(f"Returning {response.status_code} response "
                      f"with{' headers ' + json.dumps(response.headers) if response.headers else 'out headers'}. "
                      f"See next line for the first {n} characters of the body.\n"
                      + (response.body[:n] if isinstance(response.body, str) else json_head(n, response.body)))
        else:
            log.info('Returning %i response. To log headers and body, set AZUL_DEBUG to 1.', response.status_code)

    # Some type annotations to help with auto-complete
    lambda_context: LambdaContext
    current_request: Request
