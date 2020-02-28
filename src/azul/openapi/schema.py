from typing import (
    Mapping,
    NamedTuple,
    Optional,
    Type,
    Union,
)

from azul.types import JSON

"""
A bunch of factories for creating JSON schemas. Mainly for use in OpenAPI specs.
The two main work horses are object() and array(). The former eliminates the
need of manually maintaining the `required` schema property.
"""

TYPE = Union[None, Type, str, JSON]


# noinspection PyPep8Naming
class optional(NamedTuple):
    """
    Use in conjunction with `object` to mark certain properties as optional.
    """
    property: TYPE


# We're consciously shadowing the `object` builtin here. Two factors mitigate
# the negative effects of this decision: 1) this module is short so the shadowed
# builtin is unlikely to be used by it. 2) this module is meant to be imported
# wholesale and its members referenced by fully qualifying their name so the
# `object` builtin is not shadowed in the importing module.q


# noinspection PyShadowingBuiltins
def object(additional_properties=False, **props: Union[TYPE, optional]):
    """
    >>> from azul.doctests import assert_json
    >>> assert_json(object(x=int, y=int, relative=optional(bool)))
    {
        "type": "object",
        "properties": {
            "x": {
                "type": "integer",
                "format": "int64"
            },
            "y": {
                "type": "integer",
                "format": "int64"
            },
            "relative": {
                "type": "boolean"
            }
        },
        "required": [
            "x",
            "y"
        ],
        "additionalProperties": false
    }
    >>> assert_json(object())
    {
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }
    """
    new_props = {}
    required = []
    for name, prop in props.items():
        if isinstance(prop, optional):
            prop = prop.property
        else:
            required.append(name)
        new_props[name] = prop
    if required:
        return object_type(properties(**new_props),
                           required=required,
                           additionalProperties=additional_properties)
    else:
        return object_type(properties(**new_props),
                           additionalProperties=additional_properties)


def properties(**props: TYPE):
    """
    Returns a JSON schema `properties` attribute value.

    >>> from azul.doctests import assert_json
    >>> assert_json(properties(x=make_type(int), y=make_type(bool)))
    {
        "x": {
            "type": "integer",
            "format": "int64"
        },
        "y": {
            "type": "boolean"
        }
    }
    """
    return {name: make_type(prop) for name, prop in props.items()}


def array(item: TYPE, *items: TYPE, **kwargs):
    """
    Returns the schema for an array of items of a given type, or a sequence of
    types.

    Same as `array_type` but calls `property_type` for each positional argument,
    allowing for a more concise syntax.

    >>> from azul.doctests import assert_json
    >>> assert_json(array(str, bool, additionalItems=True))
    {
        "type": "array",
        "items": [
            {
                "type": "string"
            },
            {
                "type": "boolean"
            }
        ],
        "additionalItems": true
    }
    """
    return array_type(make_type(item), *map(make_type, items), **kwargs)


_primitive_types: Mapping[Optional[type], JSON] = {
    str: {'type': 'string'},
    bool: {'type': 'boolean'},
    int: {'type': 'integer', 'format': 'int64'},
    float: {'type': 'number', 'format': 'double'},
    None: {'type': 'null'}
}


def object_type(properties: JSON, **kwargs) -> JSON:
    """
    Returns the schema for a JSON object with the given properties.

    >>> from azul.doctests import assert_json
    >>> assert_json(object_type({'x': {'type': 'string'}}, required=['x']))
    {
        "type": "object",
        "properties": {
            "x": {
                "type": "string"
            }
        },
        "required": [
            "x"
        ]
    }
    """
    return {
        'type': 'object',
        'properties': properties,
        **kwargs
    }


def array_type(item: JSON, *items: JSON, **kwargs) -> JSON:
    """
    Returns the schema for a JSON array of items of a given type or types.

    Not very useful by itself. You will likely want to use `array` instead.

    >>> from azul.doctests import assert_json
    >>> assert_json(array_type({'type': 'string'}, {'type': 'boolean'}, additionalItems=True))
    {
        "type": "array",
        "items": [
            {
                "type": "string"
            },
            {
                "type": "boolean"
            }
        ],
        "additionalItems": true
    }
    """
    return {
        'type': 'array',
        'items': [item, *items] if items else item,
        **kwargs
    }


def make_type(t: TYPE) -> JSON:
    """
    Returns the schema for a Python primitive type such as `int` or a JSON
    schema type name such as `"boolean"`.

    For primitive JSON types, the corresponding Python types can be used:

    >>> make_type(int)
    {'type': 'integer', 'format': 'int64'}

    This is the most concise way of specifying a string schema:

    >>> make_type(str)
    {'type': 'string'}

    A JSON schema type name may be used instead:

    >>> make_type('string')
    {'type': 'string'}

    When a dictionary is passed, it is returned verbatim. This is useful in
    conjunction with the `properties` helper:

    >>> make_type({'type': 'string'})
    {'type': 'string'}

    For the JSON null schema, pass None:

    >>> make_type(None)
    {'type': 'null'}
    """
    if t is None or isinstance(t, type):
        return _primitive_types[t]
    # We can't use `JSON` directly because it is generic and parameterized
    # but __origin__ yields the unparameterized generic type.
    elif isinstance(t, str):
        return {'type': t}
    elif isinstance(t, JSON.__origin__):
        return t
    else:
        assert False, type(t)
