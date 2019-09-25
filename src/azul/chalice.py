import json
import logging

from chalice import Chalice
from chalice.app import Request

from azul.json import json_head
from azul.openapi import openapi_spec
from azul.types import LambdaContext


class AzulChaliceApp(Chalice):

    def __init__(self, app_name, debug=False, env=None):
        super().__init__(app_name, debug=debug, configure_logs=False, env=env)

    def route(self, path, **kwargs):
        """
        Same as method in supper class but stashes URL path a view function is bound to as an attribute of the
        function itself.
        """
        spec = kwargs.pop('spec', None)
        decorator = super().route(path, **kwargs)

        def _decorator(view_func):
            if spec is not None:
                view_func = openapi_spec(spec)(view_func)
            view_func.path = path
            return decorator(view_func)

        return _decorator

    def _get_view_function_response(self, view_function, function_args):
        self._log_request()
        response = super()._get_view_function_response(view_function, function_args)
        self._log_response(response)
        return response

    def _log_request(self):
        if self.log.isEnabledFor(logging.INFO):
            context = self.current_request.context
            query = self.current_request.query_params
            self.log.info(f"Received {context['httpMethod']} request "
                          f"to '{context['path']}' "
                          f"with{' parameters ' + json.dumps(query) if query else 'out parameters'}.")

    def _log_response(self, response):
        if self.log.isEnabledFor(logging.DEBUG):
            n = 1024
            self.log.debug(f"Returning {response.status_code} response "
                           f"with{' headers ' + json.dumps(response.headers) if response.headers else 'out headers'}. "
                           f"See next line for the first {n} characters of the body.\n"
                           + (response.body[:n] if isinstance(response.body, str) else json_head(n, response.body)))
        else:
            self.log.info('Returning %i response. To log headers and body, set AZUL_DEBUG to 1.', response.status_code)

    # Some type annotations to help with auto-complete
    lambda_context: LambdaContext
    current_request: Request
