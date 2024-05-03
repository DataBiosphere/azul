from collections.abc import (
    Iterable,
)
from enum import (
    Enum,
)
import json
from json import (
    JSONEncoder,
)
import logging
import mimetypes
import os
import pathlib
from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
)
from urllib.parse import (
    unquote,
)

import attr
import chalice
from chalice import (
    Chalice,
    ChaliceViewError,
)
from chalice.app import (
    BadRequestError,
    CaseInsensitiveMapping,
    MultiDict,
    NotFoundError,
    Request,
    Response,
)
import chevron
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
from azul.enums import (
    auto,
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
    Use only for type hints. The actual requests will be instances of the parent
    class, but they will have the attributes defined here.
    """
    authentication: Optional[Authentication]


# For some reason Chalice does not define an exception for the 410 status code
class GoneError(ChaliceViewError):
    STATUS_CODE = 410


# Chalice does not define any exceptions for 5xx status codes besides 500
class BadGatewayError(ChaliceViewError):
    STATUS_CODE = 502


class ServiceUnavailableError(ChaliceViewError):
    STATUS_CODE = 503


class LambdaMetric(Enum):
    """
    For the full list of supported metrics in the `AWS/Lambda` namespace, see:
    https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics.html
    """
    errors = auto()
    throttles = auto()

    @property
    def aws_name(self) -> str:
        return self.name.capitalize()


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class MetricThreshold:
    lambda_name: str
    handler_name: Optional[str] = attr.ib(default=None)
    metric: LambdaMetric
    value: int


C = TypeVar('C', bound='AppController')


class AzulChaliceApp(Chalice):
    # FIXME: Remove these two class attributes once upstream issue is fixed
    #        https://github.com/DataBiosphere/azul/issues/4558
    lambda_context = None
    current_request = None

    def __init__(self,
                 app_name: str,
                 app_module_path: str,
                 unit_test: bool = False,
                 spec: Optional[JSON] = None):
        self._patch_event_source_handler()
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
        self.register_middleware(self._security_headers_middleware, 'http')
        self.register_middleware(self._api_gateway_context_middleware, 'http')
        self.register_middleware(self._authentication_middleware, 'http')

    def __call__(self, event: dict, context: LambdaContext) -> dict[str, Any]:
        # Chalice does not URL-decode path parameters
        # (https://github.com/aws/chalice/issues/511)
        # This appears to actually be a bug in API Gateway, as the parameters
        # are already parsed when the event is passed to Chalice
        # (https://docs.aws.amazon.com/lambda/latest/dg/services-apigateway.html#apigateway-example-event)
        path_params = event['pathParameters']
        if path_params is not None:
            for key, value in path_params.items():
                path_params[key] = unquote(value)
        return super().__call__(event, context)

    def _patch_event_source_handler(self):
        """
        Work around https://github.com/aws/chalice/issues/856. That issue has
        been fixed for a while now but in a way that doesn't help us: it makes
        the context available in each event object whereas we need the context
        in the application object.
        """
        import chalice.app

        def patched_event_source_handler(self_, event, context):
            self.lambda_context = context
            return old_handler(self_, event, context)

        old_handler = chalice.app.EventSourceHandler.__call__
        if old_handler.__code__ != patched_event_source_handler.__code__:
            chalice.app.EventSourceHandler.__call__ = patched_event_source_handler

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

    def _api_gateway_context_middleware(self, event, get_response):
        config.lambda_is_handling_api_gateway_request = True
        try:
            return get_response(event)
        finally:
            config.lambda_is_handling_api_gateway_request = False

    def _security_headers_middleware(self, event, get_response):
        """
        Add headers to the response
        """
        response = get_response(event)
        seconds = 60 * 60 * 24 * 365
        response.headers['Strict-Transport-Security'] = f'max-age={seconds}; includeSubDomains'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        view_function = self.routes[event.path][event.method].view_function
        cache_control = getattr(view_function, 'cache_control')
        response.headers['Cache-Control'] = cache_control
        return response

    def route(self,
              path: str,
              enabled: bool = True,
              interactive: bool = True,
              cache_control: str = 'no-store',
              path_spec: Optional[JSON] = None,
              method_spec: Optional[JSON] = None,
              **kwargs):
        """
        Decorates a view handler function in a Chalice application.

        See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route.

        :param path: See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route

        :param enabled: If False, do not route any requests to the decorated
                        view function. The application will behave as if the
                        view function wasn't decorated.

        :param interactive: If False, do not show the "Try it out" button in the
                            Swagger UI.

        :param cache_control: The value to set in the 'Cache-Control' response
                              header.

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
        """
        if enabled:
            if not interactive:
                methods = kwargs['methods']
                self.non_interactive_routes.update((path, method) for method in methods)
            methods = kwargs.get('methods', ())
            chalice_decorator = super().route(path, **kwargs)

            def decorator(view_func):
                view_func.cache_control = cache_control
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
            request_info = {
                'query': self.current_request.query_params,
                'headers': self.current_request.headers
            }
            log.info('Received %s request for %r, with %s.',
                     context['httpMethod'],
                     context['path'],
                     json.dumps(request_info, cls=self._LogJSONEncoder))

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

    @property
    def catalog(self) -> str:
        request = self.current_request
        # A request is only present when this Lambda function is invoked by API
        # Gateway (or a simulation like `make local`). Prominent examples of
        # when the request is absent are `chalice package` or when the Lambda
        # function is invoked via an event schedule.
        if request is not None:
            params = request.query_params
            if params is not None:
                try:
                    return params['catalog']
                except KeyError:
                    pass
        return config.default_catalog

    def _controller(self, controller_cls: Type[C], **kwargs) -> C:
        return controller_cls(app=self, **kwargs)

    def swagger_ui(self) -> Response:
        swagger_ui_template = self.load_static_resource('swagger', 'swagger-ui.html.template.mustache')
        base_url = self.base_url
        redirect_url = furl(base_url).add(path='oauth2_redirect')
        deployment_url = furl(base_url).add(path='openapi')
        swagger_ui_html = chevron.render(swagger_ui_template, {
            'DEPLOYMENT_PATH': json.dumps(str(deployment_url.path)),
            'OAUTH2_CLIENT_ID': json.dumps(config.google_oauth2_client_id),
            'OAUTH2_REDIRECT_URL': json.dumps(str(redirect_url)),
            'NON_INTERACTIVE_METHODS': json.dumps([
                f'{path}/{method.lower()}'
                for path, method in self.non_interactive_routes
            ])
        })
        return Response(status_code=200,
                        headers={'Content-Type': 'text/html'},
                        body=swagger_ui_html)

    def swagger_resource(self, file) -> Response:
        if os.sep in file:
            raise BadRequestError(file)
        else:
            try:
                body = self.load_static_resource('swagger', file)
            except FileNotFoundError:
                raise NotFoundError(file)
            else:
                path = pathlib.Path(file)
                content_type = mimetypes.types_map[path.suffix]
                return Response(status_code=200,
                                headers={'Content-Type': content_type},
                                body=body)

    def threshold(self, *, errors: int, throttles: int):
        def wrapper(f):
            assert isinstance(f, chalice.app.EventSourceHandler), f
            f.errors_threshold = errors
            f.throttles_threshold = throttles
            return f

        return wrapper

    @property
    def metric_thresholds(self) -> list[MetricThreshold]:
        default_threshold = 0
        thresholds = []
        lambda_name, _ = config.unqualified_resource_name(self.app_name)
        for metric in LambdaMetric:
            # The api_handler lambda functions (indexer & service) aren't included
            # in the app_module's handler_map, so we account for those first.
            thresholds.append(MetricThreshold(lambda_name=lambda_name,
                                              metric=metric,
                                              value=default_threshold))
            for handler_name, handler in self.handler_map.items():
                if isinstance(handler, chalice.app.EventSourceHandler):
                    threshold = getattr(handler, f'{metric.name}_threshold', default_threshold)
                    thresholds.append(MetricThreshold(lambda_name=lambda_name,
                                                      handler_name=handler_name,
                                                      metric=metric,
                                                      value=threshold))
        return thresholds


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class AppController:
    app: AzulChaliceApp

    @property
    def lambda_context(self) -> LambdaContext:
        return self.app.lambda_context

    @property
    def current_request(self) -> AzulRequest:
        return self.app.current_request
