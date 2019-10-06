import copy
from typing import List

from azul.deployment import aws
from azul.types import JSON


def openapi_spec(description: JSON):
    """
    Add description to decorated function

    >>> @openapi_spec({'a': 'b'})
    ... def foo():
    ...     pass
    ...
    >>> foo.api_spec == {'a': 'b'}
    True
    """

    def spec_adder(func):
        func.api_spec = description
        return func

    return spec_adder


def annotated_specs(gateway_id, app, toplevel_spec) -> JSON:
    """
    Finds all routes in app that are decorated with @openapi_spec and adds this information
    into the api spec downloaded from API Gateway.

    :param gateway_id: API Gateway ID corresponding to the Chalice app
    :param app: App with annotated routes
    :param toplevel_spec: Top level OpenAPI info, definitions, etc.
    :return: The annotated specifications
    """
    specs = aws.api_gateway_export(gateway_id)
    clean_specs(specs)
    specs = merge_dicts(specs, toplevel_spec, override=True)
    docs = get_doc_specs(app)
    for path, doc_spec in docs.items():
        for verb, description in specs['paths'][path].items():
            description = merge_dicts(description, doc_spec)
            specs['paths'][path][verb] = description
    return specs


def clean_specs(specs):
    """
    Adjust specs from API Gateway so they pass linting
    """
    # Filter out 'options' since it causes linting errors
    for path in specs['paths']:
        specs['paths'][path].pop('options', None)


def get_doc_specs(app):
    docs = {}
    for route, entries in app.routes.items():
        func = next(iter(entries.values())).view_function
        if hasattr(func, 'api_spec'):
            docs[route] = func.api_spec
    return docs


def merge_dicts(d1: JSON, d2: JSON, override: bool = False) -> JSON:
    """
    Merge two dictionaries deeply such that:

    - collisions raise an error,
    - lists are appended, and
    - child dictionaries are recursively merged

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


def _recursive_merge_dicts(d1: JSON, d2: JSON, path: List[str], override: bool) -> JSON:
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
