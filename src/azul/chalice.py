from abc import (
    ABCMeta,
)
from collections.abc import (
    Iterable,
)
from enum import (
    Enum,
)
import json
import logging
import mimetypes
import os
import pathlib
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    Self,
    Sequence,
    TypeVar,
)
from urllib.parse import (
    unquote,
)

import attrs
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
    reject,
    require,
)
from azul.auth import (
    Authentication,
)
from azul.collections import (
    deep_dict_merge,
)
from azul.csp import (
    CSP,
)
from azul.enums import (
    auto,
)
from azul.json import (
    copy_json,
    json_head,
)
from azul.openapi import (
    format_description,
    params,
    responses,
    schema,
)
from azul.strings import (
    join_words as jw,
)
from azul.types import (
    JSON,
    LambdaContext,
    MutableJSON,
    json_dict,
    json_list,
    json_str,
)

log = logging.getLogger(__name__)


class AzulRequest(Request):
    """
    Use only for type hints. The actual requests will be instances of the parent
    class, but they will have the attributes defined here.
    """
    authentication: Authentication | None


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


C = TypeVar('C', bound='AppController')


class AzulChaliceApp(Chalice):
    lambda_context: LambdaContext | None
    current_request: AzulRequest | None

    def __init__(self,
                 app_name: str,
                 app_module_path: str,
                 *,
                 unit_test: bool = False,
                 spec: JSON):
        self._patch_event_source_handler()
        require(app_module_path.endswith('/app.py'), app_module_path)
        self.app_module_path = app_module_path
        self.unit_test = unit_test
        self.non_interactive_routes: set[tuple[str, str]] = set()
        reject('paths' in spec, 'The top-level spec must not define paths')
        self._specs = self._add_contact_to_spec(spec)
        self._specs['paths'] = {}
        # The `debug` arg controls whether tracebacks appear in error responses
        super().__init__(app_name, debug=config.debug > 1, configure_logs=False)
        # Middleware is invoked in order of registration
        self.register_middleware(self._logging_middleware, 'http')
        self.register_middleware(self._security_headers_middleware, 'http')
        self.register_middleware(self._api_gateway_context_middleware, 'http')
        self.register_middleware(self._authentication_middleware, 'http')

    def _add_contact_to_spec(self, spec: JSON) -> MutableJSON:
        spec = copy_json(spec)
        info = json_dict(spec.setdefault('info', {}))
        info['description'] = json_str(info.get('description', '')) + config.contact_us
        return spec

    @property
    def unqualified_app_name(self):
        result, _ = config.unqualified_resource_name(self.app_name)
        return result

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

    @classmethod
    def security_headers(cls) -> dict[str, str]:
        """
        Default values for headers added to every response from the app, as well
        as canned 4XX and 5XX responses from API Gateway. Use of these headers
        addresses known security vulnerabilities.
        """
        hsts_max_age = 60 * 60 * 24 * 365 * 2
        csp = CSP.for_azul()
        return {
            'Content-Security-Policy': str(csp),
            'Referrer-Policy': 'strict-origin-when-cross-origin',
            'Strict-Transport-Security': jw(f'max-age={hsts_max_age};',
                                            'includeSubDomains;',
                                            'preload'),
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block'
        }

    def _security_headers_middleware(self, event, get_response):
        """
        Add headers to the response
        """
        response = get_response(event)
        # Add security headers to the response without overwriting any headers
        # that might have been added already (e.g. Content-Security-Policy)
        for k, v in self.security_headers().items():
            response.headers.setdefault(k, v)
        view_function = self.routes[event.path][event.method].view_function
        cache_control = getattr(view_function, 'cache_control')
        # Caching defeats the automatic reloading of application source code by
        # `chalice local`, which is useful, so we disable caching in that case.
        cache_control = 'no-store' if self.is_running_locally else cache_control
        response.headers['Cache-Control'] = cache_control
        return response

    def _http_cache_for(self, seconds: int):
        """
        The HTTP Cache-Control response header value that will cause the
        response to the current request to be cached for the given amount of
        time.
        """
        return f'public, max-age={seconds}, must-revalidate'

    HttpMethod = Literal['GET', 'POST', 'PUT', 'PATCH', 'HEAD', 'OPTIONS', 'DELETE']

    def route[C: Callable](self,
                           path: str,
                           *,
                           methods: Sequence[HttpMethod] = ('GET',),
                           enabled: bool = True,
                           interactive: bool = True,
                           cache_control: str = 'no-store',
                           path_spec: JSON | None = None,
                           spec: JSON | None = None,
                           **kwargs
                           ) -> Callable[[C], C]:
        """
        Decorates a view handler function in a Chalice application.

        See https://chalice.readthedocs.io/en/latest/api.html#Chalice.route.

        :param path: See https://aws.github.io/chalice/api#Chalice.route

        :param methods: See https://aws.github.io/chalice/api#Chalice.route

        :param enabled: If False, do not route any requests to the decorated
                        view function. The application will behave as if the
                        view function wasn't decorated.

        :param interactive: If False, do not show the "Try it out" button in the
                            Swagger UI.

        :param cache_control: The value to set in the 'Cache-Control' response
                              header.

        :param path_spec: Corresponds to an OpenAPI Paths Object. See

                          https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#paths-object

                          If multiple `@app.route` invocations refer to the same
                          path (but with different HTTP methods), only specify
                          this argument for one of them, otherwise an
                          AssertionError will be raised.

        :param spec: Corresponds to an OpenAPI Operation Object. See

                     https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#operation-object

                     Even though this keyword argument has a default value, it
                     must be specified for every `@app.route` invocation. The
                     reason for the default is so that the signature of the
                     override is compatible with that of the overridden method,
                     a mypy requirement.
        """
        require(spec is not None, "Argument 'spec' is required")
        assert spec is not None
        if enabled:
            if not interactive:
                require(bool(methods), 'Must list methods with interactive=False')
                self.non_interactive_routes.update((path, method) for method in methods)
            spec = deep_dict_merge(spec, self.default_specs())
            chalice_decorator = super().route(path, methods=methods, **kwargs)

            def decorator(view_func):
                view_func.cache_control = cache_control
                self._register_spec(path, methods, path_spec, spec)
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
            json_str(tag)
            for path in json_dict(self._specs['paths']).values()
            for method in json_dict(path).values() if isinstance(method, dict)
            for tag in json_list(method.get('tags', []))
        )
        reject('servers' in self._specs, "The 'servers' entry is computed")
        return {
            **self._specs,
            'tags': [
                tag for tag in json_list(self._specs.get('tags', []))
                if json_dict(tag)['name'] in used_tags
            ],
            'servers': [{'url': str(self.base_url.add(path='/'))}]
        }

    @property
    def self_url(self) -> mutable_furl:
        """
        The URL of the current request, including the path, but without query
        arguments. Callers can safely modify the returned `furl` instance.
        """
        request = self.current_request
        assert request is not None
        path = request.context['path']
        return self.base_url.add(path=path)

    @property
    def base_url(self) -> mutable_furl:
        """
        Returns the base URL of this application. Callers can safely modify the
        returned `furl` instance. The base URL may or may not have a path and
        callers should always append to it.
        """
        if self.current_request is None:
            # Invocation from outside the context of handling of a request, for
            # example, when `chalice local` loads the app module or during an
            # invocation via AWS StepFunctions
            self_url = config.service_endpoint
        elif isinstance(self.current_request, Request):
            try:
                scheme = self.current_request.headers['x-forwarded-proto']
            except KeyError:
                # Invocation via `chalice local` or tests
                from chalice.constants import (
                    DEFAULT_HANDLER_NAME,
                )
                lambda_context = self.lambda_context
                assert lambda_context is not None
                assert lambda_context.function_name == DEFAULT_HANDLER_NAME
                scheme = 'http'
            else:
                # Invocation via API Gateway
                pass
            self_url = furl(scheme=scheme, netloc=self.current_request.headers['host'])
        else:
            assert False, self.current_request
        return self_url

    @property
    def is_running_locally(self) -> bool:
        host = self.base_url.netloc.partition(':')[0]
        return host in ('localhost', '127.0.0.1')

    def _register_spec(self,
                       path: str,
                       methods: Iterable[str],
                       path_spec: JSON | None,
                       spec: JSON):
        """
        Add a route's specifications to the specification object.
        """
        paths = json_dict(self._specs['paths'])
        if path_spec is not None:
            reject(path in paths,
                   'Only specify path_spec once per route path')
            paths[path] = copy_json(path_spec)

        for method in methods:
            # OpenAPI requires HTTP method names be lower case
            method = method.lower()
            # This may override duplicate specs from path_specs
            path_methods = json_dict(paths.setdefault(path, {}))
            reject(method in path_methods,
                   "Only specify 'spec' once per route path and method")
            path_methods[method] = copy_json(spec)

    class _LogJSONEncoder(json.JSONEncoder):

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

    def _authenticate(self) -> Authentication | None:
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

    def _controller(self, controller_cls: type[C], **kwargs) -> C:
        return controller_cls(app=self, **kwargs)

    def swagger_resource(self, file_name: str) -> Response:
        if os.sep in file_name:
            raise BadRequestError(file_name)
        else:
            try:
                body = self.load_static_resource('swagger', file_name)
            except FileNotFoundError:
                raise NotFoundError(file_name)
            else:
                path = pathlib.Path(file_name)
                content_type = mimetypes.types_map[path.suffix]
                return Response(status_code=200,
                                headers={'Content-Type': content_type},
                                body=body)

    @attrs.frozen(kw_only=True)
    class HandlerDecorator(metaclass=ABCMeta):
        """
        A base class for decorators of handler functions.
        """

        #: The unqualified name of the app the handler is part of or None for an
        #: unbound decorator.
        app_name: str | None = attrs.field(default=None)

        #: The name of the handler, or None for the main handler, or for an
        #: unbound decorator.
        handler_name: str | None = attrs.field(default=None)

        def bind(self, app: Chalice, handler_name: str | None = None) -> Self:
            app_name, _ = config.unqualified_resource_name(app.app_name)
            return attrs.evolve(self, app_name=app_name, handler_name=handler_name)

        @property
        def tf_function_resource_name(self) -> str:
            assert self.app_name is not None, 'Unbound decorator'
            if self.handler_name is None:
                return self.app_name
            else:
                assert self.handler_name != ''
                return f'{self.app_name}_{self.handler_name}'

    # noinspection PyPep8Naming
    @attrs.frozen(kw_only=True)
    class metric_alarm(HandlerDecorator):
        """
        Use this decorator on a Chalice handler function to configure a metric
        alarm for the corresponding Lambda function. This decorator cannot be
        used to decorate view functions, i.e. functions also decorated with
        ``@app.route``.
        """
        #: The CloudWatch metric to configure the alarm for
        metric: LambdaMetric

        #: The number of failed or throttled lambda invocations that, when
        #: exceeded, will trigger the alarm.
        threshold: int

        #: The interval (in seconds) at which the alarm threshold is evaluated,
        #: ranging from 1 minute to 1 day. The default is 5 minutes.
        period: int

        def __call__(self, f):
            assert isinstance(f, chalice.app.EventSourceHandler), f
            try:
                metric_alarms = getattr(f, 'metric_alarms')
            except AttributeError:
                metric_alarms = f.metric_alarms = []
            metric_alarms.append(self)
            return f

        @property
        def tf_resource_name(self) -> str:
            return f'{self.tf_function_resource_name}_{self.metric.name}'

    @property
    def metric_alarms(self) -> Iterator[metric_alarm]:
        for metric in LambdaMetric:
            # The api_handler lambda functions (indexer & service) aren't
            # included in the app_module's handler_map, so we account for those
            # first.
            for_errors = metric is LambdaMetric.errors
            alarm = self.metric_alarm(metric=metric,
                                      threshold=1 if for_errors else 0,
                                      period=24 * 60 * 60 if for_errors else 5 * 60)
            yield alarm.bind(self)
        for handler_name, handler in self.handler_map.items():
            if isinstance(handler, chalice.app.EventSourceHandler):
                try:
                    metric_alarms = getattr(handler, 'metric_alarms')
                except AttributeError:
                    metric_alarms = (
                        self.metric_alarm(metric=metric,
                                          threshold=0,
                                          period=5 * 60)
                        for metric in LambdaMetric
                    )
                for metric_alarm in metric_alarms:
                    yield metric_alarm.bind(self, handler_name)

    # noinspection PyPep8Naming
    @attrs.frozen
    class retry(HandlerDecorator):
        """
        Use this decorator to specify the number of times a Lambda invocation of
        the decorated event handler function should be retried. This decorator
        cannot be used to decorate view functions, i.e. functions also decorated
        with ``@app.route``.

        https://docs.aws.amazon.com/lambda/latest/dg/invocation-retries.html
        """
        num_retries: int

        def __call__(self, f):
            assert isinstance(f, chalice.app.EventSourceHandler), f
            f.retry = self
            return f

    @property
    def retries(self) -> Iterator[retry]:
        for handler_name, handler in self.handler_map.items():
            if isinstance(handler, chalice.app.EventSourceHandler):
                try:
                    retry = getattr(handler, 'retry')
                except AttributeError:
                    pass
                else:
                    yield retry.bind(self, handler_name)

    def default_routes(self):

        @self.route(
            '/',
            interactive=False,
            spec={
                'summary': 'Redirect to the Swagger UI for interactive use of this REST API',
                'tags': ['Auxiliary'],
                'responses': {
                    '301': {
                        'description': 'A redirect to the Swagger UI'
                    }
                }
            }
        )
        def swagger_redirect():
            headers = {'Location': str(self.base_url.set(path='swagger/index.html'))}
            return Response(status_code=301, body='', headers=headers)

        @self.route(
            '/swagger/index.html',
            interactive=False,
            cache_control=self._http_cache_for(24 * 60 * 60),
            cors=False,
            spec={
                'summary': 'The Swagger UI for interactive use of this REST API',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'The response body is an HTML page containing the Swagger UI'
                    }
                }
            }
        )
        def swagger_ui():
            return self.swagger_resource('index.html')

        @self.route(
            '/swagger/swagger-initializer.js',
            interactive=False,
            cache_control=self._http_cache_for(60),
            cors=True,
            spec={
                'summary': 'Used internally by the Swagger UI',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'The response body is JavaScript used internally by the Swagger UI'
                    }
                }
            }
        )
        def swagger_initializer():
            file_name = 'swagger-initializer.js.template.mustache'
            template = self.load_static_resource('swagger', file_name)
            base_url = self.base_url
            redirect_url = furl(base_url).add(path='oauth2_redirect')
            openapi_spec = furl(base_url).add(path='openapi.json')
            body = chevron.render(template, {
                'OPENAPI_SPEC': json.dumps(str(openapi_spec.path)),
                'OAUTH2_CLIENT_ID': json.dumps(config.google_oauth2_client_id),
                'OAUTH2_REDIRECT_URL': json.dumps(str(redirect_url)),
                'NON_INTERACTIVE_METHODS': json.dumps([
                    f'{path}/{method.lower()}'
                    for path, method in self.non_interactive_routes
                ])
            })
            headers = {'Content-Type': 'application/javascript'}
            return Response(status_code=200, body=body, headers=headers)

        @self.route(
            '/swagger/{file}',
            interactive=False,
            cache_control=self._http_cache_for(24 * 60 * 60),
            cors=True,
            spec={
                'summary': 'Static files needed for the Swagger UI',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'The response body is the contents of the requested file'
                    },
                    '404': {
                        'description': 'The requested file does not exist'
                    }
                }
            },
            path_spec={
                'parameters': [
                    params.path('file', str, description='The name of a static file to be returned')
                ]
            }
        )
        def swagger_resource(file):
            return self.swagger_resource(file)

        @self.route(
            '/openapi.json',
            methods=['GET'],
            cache_control=self._http_cache_for(60),
            cors=True,
            spec={
                'summary': 'Return OpenAPI specifications for this REST API',
                'description': format_description('''
                                This endpoint returns the [OpenAPI specifications]'
                                (https://github.com/OAI/OpenAPI-Specification) for this REST
                                API. These are the specifications used to generate the page
                                you are visiting now.
                            '''),
                'responses': {
                    '200': {
                        'description': '200 response',
                        **responses.json_content(
                            schema.object(
                                openapi=str,
                                **{
                                    k: schema.object()
                                    for k in ('info', 'tags', 'servers', 'paths', 'components')
                                }
                            )
                        )
                    }
                },
                'tags': ['Auxiliary']
            }
        )
        def openapi():
            return Response(status_code=200,
                            headers={'content-type': 'application/json'},
                            body=self.spec())

        @self.route(
            '/version',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Describe current version of this REST API',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'Version endpoint is reachable.',
                        **responses.json_content(
                            schema.object(
                                git=schema.object(
                                    commit=str,
                                    dirty=bool
                                )
                            )
                        )
                    }
                }
            }
        )
        def version():
            return {
                'git': config.lambda_git_status
            }

        @self.route(
            '/robots.txt',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Robots Exclusion Protocol',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': format_description('''
                            The robots.txt resource according to
                            [RFC9309](https://datatracker.ietf.org/doc/html/rfc9309)
                        '''),
                    }
                }
            }
        )
        def robots_txt():
            body = '\n'.join(f'{k}: {v}' for k, v in [
                ('User-agent', '*'),
                ('Disallow', '/'),
                # Keep consistent with regex in scope-down statement for the
                # bot control rule set in api_gateway.tf.json.template.py
                ('Allow', '/$'),
                ('Allow', '/swagger/')
            ])
            headers = {'Content-Type': 'text/plain'}
            return Response(status_code=200, headers=headers, body=body)

        return locals()

    def default_specs(self):
        return {
            'responses': {
                '504': {
                    'description': format_description('''
                        Request timed out. When handling this response, clients
                        should wait the number of seconds specified in the
                        `Retry-After` header and then retry the request.
                    ''')
                }
            }
        }


@attrs.frozen(kw_only=True)
class AppController:
    app: AzulChaliceApp

    @property
    def lambda_context(self) -> LambdaContext:
        assert self.app.lambda_context is not None
        return self.app.lambda_context

    @property
    def current_request(self) -> AzulRequest:
        assert self.app.current_request is not None
        return self.app.current_request
