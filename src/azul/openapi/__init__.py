import copy
from textwrap import dedent
from typing import (
    Any,
    List,
    cast,
    MutableMapping,
)

from azul.types import (
    JSON,
    MutableJSON,
)


def annotated_specs(raw_specs, app, toplevel_spec) -> JSON:
    """
    Finds all routes in app that are decorated with @openapi_spec and adds this
    information into the api spec downloaded from API Gateway.

    :param raw_specs: Spec from API Gateway corresponding to the Chalice app
    :param app: App with annotated routes
    :param toplevel_spec: Top level OpenAPI info, definitions, etc.
    :return: The annotated specifications
    """
    clean_specs(raw_specs)
    specs = merge_dicts(toplevel_spec, raw_specs, override=True)
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
    undocumented = app.routes.keys() - app.path_specs.keys()
    if undocumented:
        # TODO (jesse): raise if route is undocumented (https://github.com/DataBiosphere/azul/issues/1450)
        pass

    return app.path_specs, app.method_specs


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


def format_description(string: str) -> str:
    """
    Remove common leading whitespace from every line in text.
    Useful for processing triple-quote strings.

    :param string: The string to unwrap

    >>> format_description(" c'est \\n une chaine \\n de plusieurs lignes. ")
    "c'est \\nune chaine \\nde plusieurs lignes. "

    >>> format_description('''
    ...     Multi-lined,
    ...     indented,
    ...     triple-quoted string.
    ... ''')
    '\\nMulti-lined,\\nindented,\\ntriple-quoted string.\\n'
    """
    return dedent(string)


def format_description_key(kwargs: MutableMapping[str, Any]) -> None:
    """
    Clean up the `description` key's value in `kwargs` (if it exists)

    >>> from azul.doctests import assert_json
    >>> kwargs = {"foo": "bar", "description": '''
    ...                                        Multi-lined,
    ...                                        indented,
    ...                                        triple-quoted string
    ...                                        '''}
    >>> format_description_key(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar",
        "description": "\\nMulti-lined,\\nindented,\\ntriple-quoted string\\n"
    }

    >>> kwargs = {"foo": "bar"}
    >>> format_description_key(kwargs)
    >>> assert_json(kwargs)
    {
        "foo": "bar"
    }
    """
    try:
        unwrapped = format_description(kwargs['description'])
    except KeyError:
        pass
    else:
        kwargs['description'] = unwrapped
