import json
import logging

from chalice import Chalice
from chalice.app import Request

from azul import config
from azul.json import json_head
from azul.openapi import openapi_spec
from azul.types import LambdaContext

log = logging.getLogger(__name__)


class AzulChaliceApp(Chalice):

    def __init__(self, app_name, unit_test=False):
        self.unit_test = unit_test
        super().__init__(app_name, debug=config.debug > 0, configure_logs=False)

    def route(self, path, enabled=True, path_spec=None, method_spec=None, **kwargs):
        """
        Decorates a view handler function in a Chalice application.

        See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route.

        :param path: See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route

        :param method_spec: FIXME: https://github.com/DataBiosphere/azul/issues/1613

        :param path_spec: FIXME: https://github.com/DataBiosphere/azul/issues/1613

        :param enabled: If False, do not route any requests to the decorated
                        view function. The application will behave as if the
                        view function wasn't decorated.
        """
        if enabled:
            methods = kwargs.get('methods')
            decorator = super().route(path, **kwargs)

            def _decorator(view_func):
                view_func = openapi_spec(path, methods, path_spec=path_spec, method_spec=method_spec)(view_func)
                # Stash the URL path a view function is bound to as an attribute of
                # the function itself.
                view_func.path = path
                return decorator(view_func)

            return _decorator
        else:
            return lambda view_func: view_func

    def test_route(self, *args, **kwargs):
        """
        A route that's only enabled during unit tests.
        """
        return self.route(*args, enabled=self.unit_test, **kwargs)

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
