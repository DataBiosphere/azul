from collections.abc import (
    Mapping,
)
import re
from typing import (
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
)

from more_itertools import (
    one,
)

from azul import (
    require,
)
from azul.types import (
    JSON,
    PrimitiveJSON,
)

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
    type_: TYPE


# We're consciously shadowing the `object` builtin here. Two factors mitigate
# the negative effects of this decision: 1) this module is short so the shadowed
# builtin is unlikely to be used by it. 2) this module is meant to be imported
# wholesale and its members referenced by fully qualifying their name so the
# `object` builtin is not shadowed in the importing module.


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
        if name.endswith('_'):
            name = name[:-1]
        if isinstance(prop, optional):
            prop = prop.type_
        else:
            required.append(name)
        new_props[name] = prop
    return object_type(properties(**new_props),
                       **(dict(required=required) if required else {}),
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


def enum(*items: PrimitiveJSON, type_: TYPE = None) -> JSON:
    """
    Returns an `enum` schema for the given items. By default, the schema type of
    the items is inferred, but a type may be passed explicitly to override that.
    However, the current implementation cannot detect some cases in which the
    types of the enum values contradict the explicit type.

    >>> from azul.doctests import assert_json
    >>> assert_json(enum('foo', 'bar', type_=str))
    {
        "type": "string",
        "enum": [
            "foo",
            "bar"
        ]
    }

    >>> assert_json(enum(2, 5, 7))
    {
        "type": "integer",
        "format": "int64",
        "enum": [
            2,
            5,
            7
        ]
    }

    >>> assert_json(enum('x', type_={'type': 'string'}))
    {
        "type": "string",
        "enum": [
            "x"
        ]
    }

    >>> enum('foo', 1.0)
    Traceback (most recent call last):
    ...
    ValueError: too many items in iterable (expected 1)

    >>> enum('foo', 'bar', type_=int)
    Traceback (most recent call last):
    ...
    AssertionError

    >>> assert_json(enum('foo', 'bar', type_="integer"))
    {
        "type": "integer",
        "enum": [
            "foo",
            "bar"
        ]
    }
    """

    if isinstance(type_, type):
        assert all(isinstance(item, type_) for item in items)
    else:
        inferred_type = one(set(map(type, items)))
        if type_ is None:
            type_ = inferred_type
        else:
            # Can't easily verify type when passed as string or mapping
            pass

    return {
        **make_type(type_),
        'enum': items
    }


def pattern(regex: Union[str, re.Pattern], _type: TYPE = str):
    """
    Returns schema for a JSON string matching the given pattern.

    :param regex: An `re.Pattern` instance or a string containing the regular
                  expression that documents need to match in order to be valid.
                  If an `re.Pattern` instance is passed it should not use any
                  Python-specific regex features.

    :param _type: An optional schema to override the default of `string`. Note
                  that as of version 7.0 of JSON Schema, the `pattern` property
                  can only be used in conjunction with the `string` type.

    >>> from azul.doctests import assert_json

    >>> assert_json(pattern(r'[a-z]+'))
    {
        "type": "string",
        "pattern": "[a-z]+"
    }

    >>> assert_json(pattern(re.compile(r'[a-z]+'), _type={'type': 'string', 'length': 3}))
    {
        "type": "string",
        "length": 3,
        "pattern": "[a-z]+"
    }
    """
    if isinstance(regex, re.Pattern):
        regex = regex.pattern
    assert isinstance(regex, str)
    return {
        **make_type(_type),
        'pattern': regex
    }


def with_default(default: PrimitiveJSON, /, type_: Optional[TYPE] = None) -> JSON:
    """
    Add a documented default value to the type schema.

    >>> from azul.doctests import assert_json
    >>> assert_json(with_default('foo'))
    {
        "type": "string",
        "default": "foo"
    }

    >>> assert_json(with_default(0, type_=float))
    {
        "type": "number",
        "format": "double",
        "default": 0
    }
    """
    return {
        **make_type(type(default) if type_ is None else type_),
        'default': default
    }


N = TypeVar('N', bound=Union[int, float])


def in_range(minimum: Optional[N], maximum: Optional[N], type_: Optional[TYPE] = None) -> JSON:
    """
    >>> from azul.doctests import assert_json

    >>> assert_json(in_range(1, 2))
    {
        "type": "integer",
        "format": "int64",
        "minimum": 1,
        "maximum": 2
    }

    >>> assert_json(in_range(.5, None))
    {
        "type": "number",
        "format": "double",
        "minimum": 0.5
    }

    >>> assert_json(in_range(None, 2.0))
    {
        "type": "number",
        "format": "double",
        "maximum": 2.0
    }

    >>> assert_json(in_range(minimum=.5, maximum=2))
    Traceback (most recent call last):
    ...
    azul.RequirementError: ('Mismatched argument types', <class 'float'>, <class 'int'>)

    >>> assert_json(in_range())
    Traceback (most recent call last):
    ...
    TypeError: in_range() missing 2 required positional arguments: 'minimum' and 'maximum'

    >>> assert_json(in_range(None, None))
    Traceback (most recent call last):
    ...
    azul.RequirementError: Must pass at least one bound
    """
    if type_ is None:
        types = (type(minimum), type(maximum))
        set_of_types = set(types)
        set_of_types.discard(type(None))
        require(bool(set_of_types), 'Must pass at least one bound')
        require(len(set_of_types) == 1, 'Mismatched argument types', *types)
        type_ = one(set_of_types)
    return {
        **make_type(type_),
        **({} if minimum is None else {'minimum': minimum}),
        **({} if maximum is None else {'maximum': maximum})
    }


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
