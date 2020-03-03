import copy
from typing import (
    List,
    cast,
    Optional,
    MutableMapping,
    Any,
)

from azul.deployment import aws
from azul.strings import unwrap
from azul.types import (
    JSON,
    MutableJSON,
)


def openapi_spec(path: str, methods: List[str], path_spec: Optional[JSON] = None, method_spec: Optional[JSON] = None):
    """
    Add description to decorated function at specified route.

    >>> @openapi_spec('/foo', ['GET'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
    ... def foo():
    ...     pass

    >>> foo.path_specs
    {'/foo': {'a': 'b'}}

    >>> foo.method_specs
    {('/foo', 'get'): {'c': 'd'}}

    One of path_spec or method_spec maybe unspecified

    >>> @openapi_spec('/foo', ['GET'], method_spec={'c': 'd'})
    ... def foo():
    ...     pass

    >>> @openapi_spec('/foo', ['GET'], path_spec={'a': 'b'})
    ... def foo():
    ...     pass

    Although a valid, documented endpoint should not allow both to be none, we
    have to allow this behavior to prevent breakages from all currently
    undocumented endpoints.

    >>> @openapi_spec('/foo', ['GET'])
    ... def foo():
    ...     pass

    Multiple routes can be made for the same function

    >>> @openapi_spec('/foo', ['GET', 'PUT'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
    ... @openapi_spec('/foo/too', ['GET'], method_spec={'e': 'f'})
    ... def foo():
    ...     pass

    >>> foo.path_specs
    {'/foo': {'a': 'b'}}

    >>> foo.method_specs
    {('/foo/too', 'get'): {'e': 'f'}, ('/foo', 'get'): {'c': 'd'}, ('/foo', 'put'): {'c': 'd'}}

    Only one route should define the top level spec

    >>> @openapi_spec('/bar', ['PUT'], path_spec={'bad, duplicate': 'path spec'}, method_spec={'e': 'f'})
    ... @openapi_spec('/bar', ['GET'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
    ... def bar():
    ...     pass
    Traceback (most recent call last):
    ...
    AssertionError: Only specify path_spec once per route path

    At this point we can only validate duplicate specs if they occur on the same
    method. Unfortunately, this succeeds:

    >>> @openapi_spec('/bar', ['PUT'], path_spec={'a': 'b'}, method_spec={'c': 'd'})
    ... def foo():
    ...     pass
    >>> @openapi_spec('/bar', ['GET'], path_spec={'e', 'f'}, method_spec={'g': 'h'})
    ... def bar():
    ...     pass
    """

    def spec_adder(func):
        try:
            func.path_specs
        except AttributeError:
            func.path_specs = {}
        try:
            func.method_specs
        except AttributeError:
            func.method_specs = {}

        assert path not in func.path_specs, 'Only specify path_spec once per route path'
        if path_spec:
            func.path_specs[path] = path_spec

        if method_spec:
            for method in methods:
                # OpenAPI routes must be lower case
                method = method.lower()
                # No need to worry about duplicate method_specs since Chalice
                # will complain in that case
                func.method_specs[path, method] = method_spec
        return func

    return spec_adder


def annotated_specs(gateway_id, app, toplevel_spec) -> JSON:
    """
    Finds all routes in app that are decorated with @openapi_spec and adds this
    information into the api spec downloaded from API Gateway.

    :param gateway_id: API Gateway ID corresponding to the Chalice app
    :param app: App with annotated routes
    :param toplevel_spec: Top level OpenAPI info, definitions, etc.
    :return: The annotated specifications
    """
    specs = aws.api_gateway_export(gateway_id)
    clean_specs(specs)
    specs = merge_dicts(toplevel_spec, specs, override=True)
    path_specs, method_specs = get_app_specs(app)
    return join_specs(specs, path_specs, method_specs)


def clean_specs(specs):
    """
    Adjust specs from API Gateway so they pass linting.

    >>> spec = {
    ...     'paths': {
    ...         'get': {},
    ...         'options': {}
    ...     },
    ...     'servers': ['a', 'b']
    ... }

    >>> clean_specs(spec)

    >>> spec
    {'paths': {'get': {}, 'options': {}}}
    """
    # Filter out 'options' since it causes linting errors
    for path in specs['paths'].values():
        path.pop('options', None)
    # Remove listed servers since API Gateway give false results
    specs.pop('servers')


def get_app_specs(app):
    """
    Extract OpenAPI specs from a Chalice app object.
    """
    path_specs = {}
    method_specs = {}
    for entries in app.routes.values():
        func = next(iter(entries.values())).view_function
        func_path_specs = getattr(func, 'path_specs', None)
        if func_path_specs:
            for path, spec in func_path_specs.items():
                if path in path_specs:
                    assert spec == path_specs[path], f'Path spec for {spec} already exists'
            path_specs.update(func.path_specs)
        else:
            # TODO (jesse): raise if route is undocumented
            pass
        func_method_specs = getattr(func, 'method_specs', None)
        if func_method_specs:
            method_specs.update(func_method_specs)

    return path_specs, method_specs


def join_specs(toplevel_spec: JSON,
               path_specs: JSON,
               method_specs: JSON) -> MutableJSON:
    """
    Join the specifications, potentially overwriting with specs from a lower
    level.

    Arguments are given in descending order of level.

    >>> toplevel_spec = {'paths': {}}
    >>> path_specs = {'/foo': {'get': {'a': 'b'}}}
    >>> method_specs = {}

    paths_specs are inserted into toplevel_spec ...

    >>> join_specs(toplevel_spec, path_specs, method_specs)
    {'paths': {'/foo': {'get': {'a': 'b'}}}}

    ... but may overwrite preexisting spec ...

    >>> toplevel_spec['paths']['/foo'] = {'get': 'XXX'}
    >>> join_specs(toplevel_spec, path_specs, method_specs)
    {'paths': {'/foo': {'get': {'a': 'b'}}}}

    ... which may be further overwritten by method_spec.

    >>> method_specs = {('/foo', 'get'): {'a': 'XXX'}}
    >>> join_specs(toplevel_spec, path_specs, method_specs)
    {'paths': {'/foo': {'get': {'a': 'XXX'}}}}

    Method specs for the same path won't conflict

    >>> method_specs[('/foo', 'put')] = {'c': 'd'}
    >>> join_specs(toplevel_spec, path_specs, method_specs)
    {'paths': {'/foo': {'get': {'a': 'XXX'}, 'put': {'c': 'd'}}}}

    nor will path specs for different paths

    >>> path_specs['/bar'] = {'summary': {'Drink': 'up'}}
    >>> join_specs(toplevel_spec, path_specs, method_specs)
    {'paths': {'/foo': {'get': {'a': 'XXX'}, 'put': {'c': 'd'}}, '/bar': {'summary': {'Drink': 'up'}}}}
    """
    specs = cast(MutableJSON, copy.deepcopy(toplevel_spec))

    for path, path_spec in path_specs.items():
        # This may override entries from API Gateway with the same path
        specs['paths'][path] = path_spec

    for (path, method), method_spec in method_specs.items():
        # This may override duplicate specs from path_specs or API Gateway
        specs['paths'][path][method] = method_spec

    return specs


def merge_dicts(d1: JSON, d2: JSON, override: bool = False) -> MutableJSON:
    """
    Merge two dictionaries deeply such that:

    - collisions raise an error,
    - lists are appended, and
    - child dictionaries are recursively merged.

    Know that the returned dictionary will share references in d1 and d2.

    :param d1: Dictionary into which d2 is merged
    :param d2: Dictionary merged into d1
    :param override: Override collisions with values from d2
    :return: A reference to d1 which has been modified by the merge

    >>> merge_dicts({'a': 'b'}, {'z': 'b'})
    {'a': 'b', 'z': 'b'}

    >>> merge_dicts({'a': 1}, {'a': 2})
    Traceback (most recent call last):
    ...
    ValueError: Cannot merge path 'a' with values: 1, 2

    >>> merge_dicts({'a': 1}, {'a': 2}, override=True)
    {'a': 2}

    >>> merge_dicts({'a': [1, 2]}, {'a': [2, 3]})
    {'a': [1, 2, 2, 3]}

    >>> merge_dicts({'a': {'b': 'z', 'd': [1]}}, {'a': {'c': 'y', 'd': [2]}})
    {'a': {'b': 'z', 'd': [1, 2], 'c': 'y'}}

    >>> merge_dicts({'a': {'b': 'z'}}, {'a': {'b': 'y'}})
    Traceback (most recent call last):
    ...
    ValueError: Cannot merge path 'a.b' with values: z, y

    >>> merge_dicts({'a': {'b': 'z'}}, {'a': {'b': 'y'}}, override=True)
    {'a': {'b': 'y'}}
    """
    return _recursive_merge_dicts(d1, d2, [], override)


def _recursive_merge_dicts(d1: JSON, d2: JSON, path: List[str], override: bool) -> MutableJSON:
    merged = dict(copy.copy(d1))
    for k, v in d2.items():
        sub_path = path + [k]
        if k not in merged:
            merged[k] = v
        else:
            if type(merged[k]) == type(v) == list:
                merged[k].extend(v)
            elif type(merged[k]) == type(v) == dict:
                merged[k] = _recursive_merge_dicts(merged[k], v, sub_path, override)
            else:
                if override:
                    merged[k] = v
                else:
                    path_str = '.'.join(map(str, sub_path))
                    raise ValueError(f"Cannot merge path '{path_str}' with values: {merged[k]}, {v}")
    return merged


def unwrap_description(kwargs: MutableMapping[str, Any]) -> None:
    """
    >>> from azul.doctests import assert_json
    >>> kwargs = {"foo": "bar", "description": '''
    ...                                        Multi-lined,
    ...                                        indented,
    ...                                        triple-quoted string
    ...                                        '''}
    >>> unwrap_description(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar",
        "description": "Multi-lined, indented, triple-quoted string"
    }

    >>> kwargs = {"foo": "bar"}
    >>> unwrap_description(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar"
    }
    """
    try:
        unwrapped = unwrap(kwargs['description'])
    except KeyError:
        pass
    else:
        kwargs['description'] = unwrapped
