import functools
import json
from json import (
    JSONEncoder,
)
import logging
import operator
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
)

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
    RequirementError,
    config,
)
from azul.auth import (
    Authentication,
)
from azul.json import (
    copy_json,
    json_head,
)
from azul.openapi.validation import (
    ContentSpecValidator,
    SchemaSpecValidator,
    SpecValidator,
    ValidationError,
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


class AzulChaliceApp(Chalice):

    def __init__(self, app_name, unit_test=False, spec=None):
        self.unit_test = unit_test
        if spec is not None:
            assert 'paths' not in spec, 'The top-level spec must not define paths'
            self._specs: Optional[MutableJSON] = copy_json(spec)
            self._specs['paths'] = {}
        else:
            self._specs: Optional[MutableJSON] = None
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
            'servers': [{'url': self.self_url('/')}]
        }

    def self_url(self, endpoint_path=None) -> str:
        if self.current_request is None:
            # Invocation via AWS StepFunctions
            self_url = furl(config.service_endpoint())
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
        if endpoint_path is None:
            endpoint_path = self.current_request.context['path']
        return self_url.set(path=endpoint_path).url

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

    def _get_view_function_response(self, view_function, function_args):
        self._log_request()
        response = super()._get_view_function_response(self.__authenticate, {})
        if response.status_code == 200:
            assert response.body is None
            response = super()._get_view_function_response(view_function, function_args)
        self._log_response(response)
        return response

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

    # Some type annotations to help with auto-complete
    lambda_context: LambdaContext
    current_request: AzulRequest


class ValidatingAzulChaliceApp(AzulChaliceApp):

    def __init__(self, app_name, unit_test=False, spec=None):
        super().__init__(app_name=app_name, unit_test=unit_test, spec=spec)
        self.route_specs: MutableMapping[str, RouteSpec] = {}

    def route(self, *args, validate: bool = True, **kwargs):
        """
        Decorates a view handler function in a Chalice application. Refer to
        docstring from super class for additional information.

        :param validate: If False, do not validate requests submitted to the endpoint.
        """

        azul_chalice_decorator = super().route(*args, **kwargs)

        def decorator(view_func):
            # noinspection PyShadowingNames
            @functools.wraps(view_func)
            def f(*args, **kwargs):
                if validate:
                    status = self._validate()
                    if isinstance(status, Response):
                        return status
                return view_func(*args, **kwargs)

            return azul_chalice_decorator(f)

        return decorator

    def _validate(self):
        path = self.current_request.context['resourcePath']
        return self.route_specs[path].validate_params(self.current_request)

    def _register_spec(self,
                       path: str,
                       path_spec: Optional[JSON],
                       method_spec: Optional[JSON],
                       methods: Iterable[str]):
        """
        Add a route's specifications to the specification object.
        """
        if path not in self.route_specs:
            assert path not in self.specs['paths'], 'Only specify path_spec once per route path'
            route_spec = RouteSpec(path, path_spec)
            self.route_specs[path] = route_spec
        else:
            route_spec = self.route_specs[path]
        if method_spec is not None:
            for method in methods:
                # OpenAPI requires HTTP method names be lower case
                method = method.lower()
                route_spec.set_route_spec(method, method_spec)
        if len(route_spec.methods) > 0:
            self.specs['paths'][path] = route_spec.to_dict()


class RouteSpec:
    """
    Assists with managing details about API routes, provides access to request
    validation.
    """

    def __init__(self, path, path_spec=None):
        self.path: str = path
        self.specs: MutableMapping[str, Dict[Any, Any]] = {}
        self.validators: MutableMapping[str, Mapping[str, SpecValidator]] = {}

        if path_spec:
            self.path_spec = copy_json(path_spec)
        else:
            self.path_spec = {}

    @property
    def methods(self):
        return self.specs.keys()

    def get_method_spec(self, http_method) -> JSON:
        """
        Returns the method spec for a given API route
        """
        http_method = http_method.lower()
        assert http_method in self.methods
        return self.specs[http_method]

    def get_spec_validators(self, http_method) -> Mapping[str, SpecValidator]:
        """
        Returns a mapping of parameter names to their applicable SpecValidator.
        """
        http_method = http_method.lower()
        assert http_method in self.methods
        return self.validators[http_method]

    def set_route_spec(self, http_method, method_spec):
        """
        Sets spec for a give route, creates request parameter SpecValidators
        """
        assert http_method.lower() not in self.methods, 'Only specify method_spec once per route path and method'
        self.specs[http_method] = copy_json(method_spec)
        self.validators[http_method] = self._create_spec_validators(http_method)

    def extract_parameters(self, http_method: str) -> List[JSON]:
        """
        Returns a list of OpenAPI `parameters` that are applicable to a given API route
        """
        return [
            *self.path_spec.get('parameters', []),
            *self.get_method_spec(http_method).get('parameters', [])
        ]

    def _create_spec_validators(self, http_method: str) -> Mapping[str, SpecValidator]:
        spec_validators: Dict[str, SpecValidator] = dict()
        for parameter in self.extract_parameters(http_method):
            if 'schema' in parameter:
                spec_validator = SchemaSpecValidator(**parameter)
            elif 'content' in parameter:
                spec_validator = ContentSpecValidator(**parameter)
            else:
                raise RequirementError(f'Parameter {parameter} does not have a defined schema')
            spec_validators[spec_validator.name] = spec_validator
        return spec_validators

    def validate_params(self, current_request: Request) -> Optional[Response]:
        """
        Validates request parameters against the routes specifications.
        Adds in missing request parameters if a default value is specified.
        :return: Returns a 400 Response if validation against any
                 parameter fails
        """
        invalid_body = {}
        # OpenAPI requires HTTP method names be lower case
        http_method = current_request.method.lower()
        spec_parameters = self.get_spec_validators(http_method)
        mandatory_params = {name for name, spec in spec_parameters.items() if spec.required and spec.default is None}

        current_request.query_params = current_request.query_params or {}
        current_request.uri_params = current_request.uri_params or {}
        all_provided_params = {**current_request.query_params, **current_request.uri_params}

        invalid_params = []
        for param_name in all_provided_params.keys():
            if param_name in spec_parameters.keys():
                param_value = all_provided_params[param_name]
                try:
                    spec_parameters[param_name].validate(param_value)
                except ValidationError as e:
                    invalid_params.append(e.__dict__)
        if invalid_params:
            invalid_body['invalid_parameters'] = sorted(invalid_params,
                                                        key=operator.itemgetter('name'))

        extra_params = all_provided_params.keys() - spec_parameters.keys()
        if extra_params:
            invalid_body['extra_parameters'] = sorted(extra_params)

        missing_params = [{
            'name': param,
            'schema': spec_parameters[param].schema or spec_parameters[param].content,
            'in': spec_parameters[param].in_,
            'required': spec_parameters[param].required
        } for param in (mandatory_params - all_provided_params.keys())]
        if missing_params:
            invalid_body['missing_parameters'] = sorted(missing_params,
                                                        key=operator.itemgetter('name'))

        if invalid_body:
            invalid_body['title'] = 'validation error'
            return Response(body=json.dumps(invalid_body),
                            status_code=400,
                            headers={'Content-Type': 'application/problem+json'})

        for params, kind in ((current_request.query_params, 'query'),
                             (current_request.uri_params, 'path')):
            params.update({
                name: spec_parameters[name].default for name, spec in spec_parameters.items()
                if (name not in all_provided_params
                    and spec.in_ == kind
                    and spec.default is not None)
            })

    def to_dict(self):
        return dict({spec: self.get_method_spec(spec) for spec in self.specs},
                    **self.path_spec)
