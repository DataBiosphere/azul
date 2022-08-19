from collections.abc import (
    Iterable,
)
import json
from json import (
    JSONEncoder,
)
import logging
import os
from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
)

import attr
from chalice import (
    Chalice,
    ChaliceViewError,
)
from chalice.app import (
    CaseInsensitiveMapping,
    MultiDict,
    Request,
    Response,
)
from furl import (
    furl,
)

from azul import (
    config,
    mutable_furl,
    open_resource,
)
from azul.auth import (
    Authentication,
)
from azul.json import (
    copy_json,
    json_head,
)
from azul.types import (
    JSON,
    LambdaContext,
    MutableJSON,
)

log = logging.getLogger(__name__)


class AzulRequest(Request):
    """
    Use only for type hints. The actual requests will be instances of the super
    class but they will have the attributes defined here.
    """
    authentication: Optional[Authentication]


# For some reason Chalice does not define an exception for the 410 status code
class GoneError(ChaliceViewError):
    STATUS_CODE = 410


# Chalice does not define any exceptions for 5xx status codes besides 500
class ServiceUnavailableError(ChaliceViewError):
    STATUS_CODE = 503


C = TypeVar('C', bound='AppController')


class AzulChaliceApp(Chalice):

    def __init__(self,
                 app_name: str,
                 app_module_path: str,
                 unit_test: bool = False,
                 spec: Optional[JSON] = None):
        assert app_module_path.endswith('/app.py'), app_module_path
        self.app_module_path = app_module_path
        self.unit_test = unit_test
        self.non_interactive_routes: set[tuple[str, str]] = set()
        if spec is not None:
            assert 'paths' not in spec, 'The top-level spec must not define paths'
            self._specs: Optional[MutableJSON] = copy_json(spec)
            self._specs['paths'] = {}
        else:
            self._specs: Optional[MutableJSON] = None
        super().__init__(app_name, debug=config.debug > 0, configure_logs=False)
        # Middleware is invoked in order of registration
        self.register_middleware(self._logging_middleware, 'http')
        self.register_middleware(self._lambda_context_middleware, 'all')
        self.register_middleware(self._authentication_middleware, 'http')

    def _logging_middleware(self, event, get_response):
        self._log_request()
        response = get_response(event)
        self._log_response(response)
        return response

    def _authentication_middleware(self, event, get_response):
        try:
            self.__authenticate()
        except ChaliceViewError as e:
            response = Response(body={'Code': type(e).__name__, 'Message': str(e)},
                                status_code=e.STATUS_CODE)
        else:
            response = get_response(event)
        return response

    def _lambda_context_middleware(self, event, get_response):
        config.lambda_context = self.lambda_context
        try:
            return get_response(event)
        finally:
            config.lambda_context = None

    def route(self,
              path: str,
              enabled: bool = True,
              interactive: bool = True,
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

        :param interactive: If False, do not show the "Try it out" button in the
                            Swagger UI.
        """
        if enabled:
            if not interactive:
                methods = kwargs['methods']
                self.non_interactive_routes.update((path, method) for method in methods)
            methods = kwargs.get('methods', ())
            chalice_decorator = super().route(path, **kwargs)

            def decorator(view_func):
                self._register_spec(path, path_spec, method_spec, methods)
                return chalice_decorator(view_func)

            return decorator
        else:
            return lambda view_func: view_func

    def test_route(self, *args, **kwargs):
        """
        A route that's only enabled during unit tests.
        """
        return self.route(*args, enabled=self.unit_test, **kwargs)

    def spec(self) -> JSON:
        """
        Return the final OpenAPI spec, stripping out unused tags.

        Only call this method after all routes are registered.
        """
        used_tags = set(
            tag
            for path in self._specs['paths'].values()
            for method in path.values() if isinstance(method, dict)
            for tag in method.get('tags', [])
        )
        assert 'servers' not in self._specs
        return {
            **self._specs,
            'tags': [
                tag for tag in self._specs.get('tags', [])
                if tag['name'] in used_tags
            ],
            'servers': [{'url': str(self.base_url.add(path='/'))}]
        }

    @property
    def self_url(self) -> mutable_furl:
        """
        The URL of the current request, including the path, but without query
        arguments. Callers can safely modify the returned `furl` instance.
        """
        path = self.current_request.context['path']
        return self.base_url.add(path=path)

    @property
    def base_url(self) -> mutable_furl:
        """
        Returns the base URL of this application. Callers can safely modify the
        returned `furl` instance. The base URL may or may not have a path and
        callers should always append to it.
        """
        if self.current_request is None:
            # Invocation via AWS StepFunctions
            self_url = config.service_endpoint
        elif isinstance(self.current_request, Request):
            try:
                scheme = self.current_request.headers['x-forwarded-proto']
            except KeyError:
                # Invocation via `chalice local` or tests
                from chalice.constants import (
                    DEFAULT_HANDLER_NAME,
                )
                assert self.lambda_context.function_name == DEFAULT_HANDLER_NAME
                scheme = 'http'
            else:
                # Invocation via API Gateway
                pass
            self_url = furl(scheme=scheme, netloc=self.current_request.headers['host'])
        else:
            assert False, self.current_request
        return self_url

    def _register_spec(self,
                       path: str,
                       path_spec: Optional[JSON],
                       method_spec: Optional[JSON],
                       methods: Iterable[str]):
        """
        Add a route's specifications to the specification object.
        """
        if path_spec is not None:
            assert path not in self._specs['paths'], 'Only specify path_spec once per route path'
            self._specs['paths'][path] = copy_json(path_spec)

        if method_spec is not None:
            for method in methods:
                # OpenAPI requires HTTP method names be lower case
                method = method.lower()
                # This may override duplicate specs from path_specs
                if path not in self._specs['paths']:
                    self._specs['paths'][path] = {}
                assert method not in self._specs['paths'][path], \
                    'Only specify method_spec once per route path and method'
                self._specs['paths'][path][method] = copy_json(method_spec)

    class _LogJSONEncoder(JSONEncoder):

        def default(self, o: Any) -> Any:
            if isinstance(o, MultiDict):
                # Convert to dict and flatten the singleton values.
                return {
                    k: v[0] if len(v) == 1 else v
                    for k, v in ((k, o.getlist(k)) for k in o.keys())
                }
            elif isinstance(o, CaseInsensitiveMapping):
                return dict(o)
            else:
                return super().default(o)

    def _authenticate(self) -> Optional[Authentication]:
        """
        Authenticate the current request, return None if it is unauthenticated,
        or raise a ChaliceViewError if it carries invalid authentication.
        """
        return None

    def __authenticate(self):
        auth = self._authenticate()
        attribute_name = 'authentication'
        assert attribute_name in AzulRequest.__annotations__
        setattr(self.current_request, attribute_name, auth)
        if auth is None:
            log.info('Did not authenticate request.')
        else:
            log.info('Authenticated request as %r', auth)

    def _log_request(self):
        if log.isEnabledFor(logging.INFO):
            context = self.current_request.context
            query = self.current_request.query_params
            headers = self.current_request.headers
            log.info('Received %s request for %r, with query %s and headers %s.',
                     context['httpMethod'],
                     context['path'],
                     json.dumps(query, cls=self._LogJSONEncoder),
                     json.dumps(headers, cls=self._LogJSONEncoder))

    def _log_response(self, response):
        if log.isEnabledFor(logging.DEBUG):
            n = 1024
            if isinstance(response.body, str):
                body = response.body[:n]
            else:
                body = json_head(n, response.body)
            log.debug('Returning %i response with headers %s. '
                      'See next line for the first %i characters of the body.\n%s',
                      response.status_code,
                      json.dumps(response.headers, cls=self._LogJSONEncoder),
                      n, body)
        else:
            log.info('Returning %i response. '
                     'To log headers and body, set AZUL_DEBUG to 1.',
                     response.status_code)

    absent = object()

    def _register_handler(self,
                          handler_type,
                          name,
                          user_handler,
                          wrapped_handler,
                          kwargs,
                          options=None):
        super()._register_handler(handler_type, name, user_handler,
                                  wrapped_handler, kwargs, options)
        # Our handlers reference the name of the corresponding Lambda function
        # which allows the handler to be the single source of truth when
        # configuring Terraform, etc. We store other parameters used to
        # configure the handler for the same reason.
        for attribute, new_value, is_additive in [
            ('name', name, False),
            ('queue', kwargs.get('queue', self.absent), False),
            ('path', kwargs.get('path', self.absent), True)
        ]:
            if new_value is not self.absent:
                try:
                    old_value = getattr(wrapped_handler, attribute)
                except AttributeError:
                    if is_additive:
                        new_value = [new_value]
                    setattr(wrapped_handler, attribute, new_value)
                else:
                    if is_additive:
                        old_value.append(new_value)
                    else:
                        assert old_value == new_value

    def load_static_resource(self, *path: str) -> str:
        return self.load_resource('static', *path)

    def load_resource(self, *path: str) -> str:
        package_root = os.path.dirname(self.app_module_path)
        with open_resource(*path, package_root=package_root) as f:
            return f.read()

    # Some type annotations to help with auto-complete
    lambda_context: LambdaContext
    current_request: AzulRequest

    def _controller(self, controller_cls: Type[C], **kwargs) -> C:
        return controller_cls(app=self, **kwargs)


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class AppController:
    app: AzulChaliceApp

    @property
    def lambda_context(self) -> LambdaContext:
        return self.app.lambda_context

    @property
    def current_request(self) -> AzulRequest:
        return self.app.current_request


def private_api_stage_config():
    """
    Returns the stage-specific fragment of Chalice configuration JSON that
    configures the Lambda function to be invoked by a private API Gateway, if
    enabled.
    """
    return {
        'api_gateway_endpoint_type': 'PRIVATE',
        'api_gateway_endpoint_vpce': [
            '${var.%s}' % config.var_vpc_endpoint_id
        ]
    } if config.private_api else {
    }


def private_api_lambda_config():
    """
    Returns the Lambda-specific fragment of Chalice configuration JSON that
    configures the Lambda function to be invoked by a private API Gateway, if
    enabled.
    """
    return {
        'subnet_ids': '${var.%s}' % config.var_vpc_subnet_ids,
        'security_group_ids': [
            '${var.%s}' % config.var_vpc_security_group_id
        ],
    } if config.private_api else {
    }


def private_api_policy(for_tf: bool = False):
    """
    Returns the fragment of IAM policy JSON needed for placing a Lambda function
    into a VPC so that it can be invoked by a private API Gateway, if enabled.
    """
    actions = [
        'ec2:CreateNetworkInterface',
        'ec2:DescribeNetworkInterfaces',
        'ec2:DeleteNetworkInterface',
    ]
    return [
        {
            'actions': actions,
            'resources': ['*'],
        } if for_tf else {
            'Effect': 'Allow',
            'Action': actions,
            'Resource': ['*']
        }
    ]
