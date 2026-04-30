from collections.abc import (
    Collection,
)
import re
from typing import (
    Mapping,
    NamedTuple,
    TypeAliasType,
    overload,
)

from more_itertools import (
    one,
)

from azul.lib import (
    R,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    PrimitiveJSON,
    reify,
)

"""
A bunch of factories for creating JSON schemas. Mainly for use in OpenAPI specs.
The two main work horses are object() and array(). The former eliminates the
need of manually maintaining the `required` schema property.
"""

Form = None | type | str | JSON | TypeAliasType


# noinspection PyPep8Naming
class optional(NamedTuple):
    """
    Use in conjunction with `object` to mark certain properties as optional.
    """
    form: Form


# We're consciously shadowing the `object` builtin here. Two factors mitigate
# the negative effects of this decision: 1) this module is short so the shadowed
# builtin is unlikely to be used by it. 2) this module is meant to be imported
# wholesale and its members referenced by fully qualifying their name so the
# `object` builtin is not shadowed in the importing module.

@overload
def object(**properties: Form | optional) -> JSON: ...


@overload
def object(*,
           additionalProperties: JSON | bool,  # noqa: N803
           **properties: Form | optional
           ) -> JSON: ...


@overload
def object(*, properties: JSON, **kwargs: AnyJSON) -> JSON: ...


def object(*,
           properties=None,
           additionalProperties=None,  # noqa: N803
           **kwargs):
    """
    >>> from azul.lib.doctests import assert_json
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

    >>> object(x=int, y=int, relative=optional(bool)) == object(
    ...     properties=dict(x=dict(type="integer", format="int64"),
    ...                     y=dict(type="integer", format="int64"),
    ...                     relative=dict(type="boolean")),
    ...     additionalProperties=False,
    ...     required=['x','y']
    ... )
    True
    """

    if properties is None:
        properties, required = {}, []
        for name, value in kwargs.items():
            if name.endswith('_'):
                name = name[:-1]
            if isinstance(value, optional):
                value = value.form
            else:
                required.append(name)
            properties[name] = schema(value)
        kwargs = {'required': required} if required else {}
        if additionalProperties is None:
            additionalProperties = False
        kwargs['additionalProperties'] = additionalProperties
    else:
        if additionalProperties is not None:
            kwargs['additionalProperties'] = additionalProperties
    return {
        'type': 'object',
        'properties': properties,
        **kwargs,
    }


def array(item: Form, *items: Form, **kwargs) -> JSON:
    """
    Returns the schema for an array of items of a given type, or a sequence of
    types.

    Same as `array_type` but calls `property_type` for each positional argument,
    allowing for a more concise syntax.

    >>> from azul.lib.doctests import assert_json
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
    return array_type(schema(item), *map(schema, items), **kwargs)


def enum(*items: PrimitiveJSON, form: Form = None) -> JSON:
    """
    Returns an `enum` schema for the given items. By default, the schema type of
    the items is inferred, but a type may be passed explicitly to override that.
    However, the current implementation cannot detect some cases in which the
    types of the enum values contradict the explicit type.

    >>> from azul.lib.doctests import assert_json
    >>> assert_json(enum('foo', 'bar', form=str))
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

    >>> assert_json(enum('x', form={'type': 'string'}))
    {
        "type": "string",
        "enum": [
            "x"
        ]
    }

    >>> enum('foo', 1.0)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
    ...
    ValueError: Expected exactly one item in iterable, but got <class '...'>, <class '...'>, and perhaps more.

    >>> enum('foo', 'bar', form=int)
    Traceback (most recent call last):
    ...
    AssertionError

    >>> assert_json(enum('foo', 'bar', form="integer"))
    {
        "type": "integer",
        "enum": [
            "foo",
            "bar"
        ]
    }
    """

    if isinstance(form, type):
        assert all(isinstance(item, form) for item in items)
    else:
        inferred_type = one(set(map(type, items)))
        if form is None:
            form = inferred_type
        else:
            # Can't easily verify type when passed as string or mapping
            pass

    return {
        **schema(form),
        'enum': items
    }


def pattern(regex: str | re.Pattern, _type: Form = str) -> JSON:
    """
    Returns schema for a JSON string matching the given pattern.

    :param regex: An `re.Pattern` instance or a string containing the regular
                  expression that documents need to match in order to be valid.
                  If an `re.Pattern` instance is passed it should not use any
                  Python-specific regex features.

    :param _type: An optional schema to override the default of `string`. Note
                  that as of version 7.0 of JSON Schema, the `pattern` property
                  can only be used in conjunction with the `string` type.

    >>> from azul.lib.doctests import assert_json

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
        **schema(_type),
        'pattern': regex
    }


def format(name: str, **kwargs) -> JSON:
    return {
        **schema(str),
        'format': name,
        **kwargs
    }


def default(default: PrimitiveJSON, /, form: Form = None) -> JSON:
    """
    Add a documented default value to the type schema.

    >>> from azul.lib.doctests import assert_json

    >>> assert_json(default('foo'))
    {
        "type": "string",
        "default": "foo"
    }

    >>> assert_json(default(0, form=float))
    {
        "type": "number",
        "format": "double",
        "default": 0
    }
    """
    return {
        **schema(type(default) if form is None else form),
        'default': default
    }


def range[N: int | float](minimum: N | None,
                          maximum: N | None,
                          form: Form = None
                          ) -> JSON:
    """
    >>> from azul.lib.doctests import assert_json

    >>> assert_json(range(1, 2))
    {
        "type": "integer",
        "format": "int64",
        "minimum": 1,
        "maximum": 2
    }

    >>> assert_json(range(.5, None))
    {
        "type": "number",
        "format": "double",
        "minimum": 0.5
    }

    >>> assert_json(range(None, 2.0))
    {
        "type": "number",
        "format": "double",
        "maximum": 2.0
    }

    >>> assert_json(range(minimum=.5, maximum=2))
    Traceback (most recent call last):
    ...
    AssertionError: R('Mismatched argument types', <class 'float'>, <class 'int'>)

    >>> assert_json(range())
    Traceback (most recent call last):
    ...
    TypeError: range() missing 2 required positional arguments: 'minimum' and 'maximum'

    >>> assert_json(range(None, None))
    Traceback (most recent call last):
    ...
    AssertionError: R('Must pass at least one bound')
    """
    if form is None:
        types = (type(minimum), type(maximum))
        set_of_types = set(types)
        set_of_types.discard(type(None))
        assert bool(set_of_types), R('Must pass at least one bound')
        assert len(set_of_types) == 1, R('Mismatched argument types', *types)
        form = one(set_of_types)
    return {
        **schema(form),
        **({} if minimum is None else {'minimum': minimum}),
        **({} if maximum is None else {'maximum': maximum})
    }


_primitive_types: Mapping[type | None, JSON] = {
    str: {'type': 'string'},
    bool: {'type': 'boolean'},
    # Note that `format` on numeric types is an OpenAPI extension to JSONSchema
    # that "serves as a hint for the tools to use a specific numeric type"
    # https://swagger.io/docs/specification/data-models/data-types/#numbers
    # I take this to mean that `1.0` is a valid `integer` in OpenAPI, just like
    # it is a valid `integer` in JSONSchema. When we deserialize a JSON value
    # that the schema declares to be `integer`, we need to be prepared to get an
    # instance of `float`. Similarly, and less surprisingly, `1` is a valid
    # `number`, so when we deserialize a JSON value that the schema declares to
    # be `number`, we need to be prepared to get an instance of `int`.
    int: {'type': 'integer', 'format': 'int64'},
    float: {'type': 'number', 'format': 'double'},
    type(None): {'type': 'null'},
    None: {'type': 'null'}
}


def array_type(item: JSON, *items: JSON, **kwargs) -> JSON:
    """
    Returns the schema for a JSON array of items of a given type or types.

    Not very useful by itself. You will likely want to use `array` instead.

    >>> from azul.lib.doctests import assert_json
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


def schema(form: Form) -> JSON:
    """
    Returns the schema for a Python primitive type such as `int` or a JSON
    schema type name such as `"boolean"`.

    For primitive JSON types, the corresponding Python types can be used:

    >>> schema(int)
    {'type': 'integer', 'format': 'int64'}

    This is the most concise way of specifying a string schema:

    >>> schema(str)
    {'type': 'string'}

    >>> schema(JSON)
    {'type': 'object'}

    A JSON schema type name may be used instead:

    >>> schema('string')
    {'type': 'string'}

    When a dictionary is passed, it is returned verbatim. This is useful in
    conjunction with the `properties` helper:

    >>> schema({'type': 'string'})
    {'type': 'string'}

    For the JSON null schema, pass `type(None)` …

    >>> schema(type(None))
    {'type': 'null'}

    … or just `None`.

    >>> schema(None)
    {'type': 'null'}
    """
    if form == JSON or form == one(reify(JSON)):
        return {'type': 'object'}
    elif form is None:
        return _primitive_types[type(form)]
    elif isinstance(form, type):
        return _primitive_types[form]
    elif isinstance(form, str):
        return {'type': form}
    elif isinstance(form, reify(JSON)):
        return form
    else:
        assert False, type(form)


make = schema


def union(*ts: Form, for_openapi: bool = True) -> JSON:
    """
    The union of one or more types.

    :param for_openapi: True to emit OpenAPI 3.0 flavor of JSONSchema, False
                        for vanilla JSONSchema

    >>> union(str, int)
    {'anyOf': [{'type': 'string'}, {'type': 'integer', 'format': 'int64'}]}

    >>> union(str, bool)
    {'anyOf': [{'type': 'string'}, {'type': 'boolean'}]}

    For vanilla JSONSchema a shorthand syntax is supported …

    >>> union(str, bool, for_openapi=False)
    {'type': ['string', 'boolean']}

    … but only if all alternatives are simple types.

    >>> union(str, int, for_openapi=False)
    {'anyOf': [{'type': 'string'}, {'type': 'integer', 'format': 'int64'}]}
    """
    ts = list(map(schema, ts))
    # There are two ways to represent a union of types in JSONSchema, …
    if not for_openapi and all(len(t) == 1 and isinstance(t.get('type'), str) for t in ts):
        # … a shortcut for simple types …
        return {'type': [t['type'] for t in ts]}
    else:
        # … and the general form. OpenAPI 3.0 only supports the latter.
        return {'anyOf': ts}


def coalesce(ts: Collection[Form]) -> JSON:
    if len(ts) == 1:
        return schema(one(ts))
    else:
        return union(*ts)


def nullable(t: Form, for_openapi: bool = True) -> JSON:
    """
    Given a schema, return a schema that additionally permits the `null` value.

    This is similar to `Optional` from Python's `typing` module but different
    to `optional` from this module, which is used to indicate that a property
    may be absent from an object.

    :param t: The schema or equivalent Python type to make nullable

    :param for_openapi: True to emit OpenAPI 3.0 flavor of JSONSchema, False
                        for vanilla JSONSchema

    >>> nullable(int)
    {'type': 'integer', 'format': 'int64', 'nullable': True}

    >>> nullable(str)
    {'type': 'string', 'nullable': True}

    OpenAPI does not support `null` but uses the `nullable` attribute.
    https://swagger.io/docs/specification/data-models/data-types/#null

    Vanilla JSONSchema would use a union …

    >>> nullable(int, for_openapi=False)
    {'anyOf': [{'type': 'null'}, {'type': 'integer', 'format': 'int64'}]}

    … or the shorthand for the union, if possible.

    >>> nullable(str, for_openapi=False)
    {'type': ['null', 'string']}
    """
    assert t is not None and t is not type(None), R('Invalid type', t)
    if for_openapi:
        return {**schema(t), 'nullable': True}
    else:
        return union(None, t, for_openapi=False)


def describe(t: Form, description: str) -> JSON:
    return {**schema(t), 'description': description}
