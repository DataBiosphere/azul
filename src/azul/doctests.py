import json

from azul.types import (
    AnyJSON,
)


def assert_json(j: AnyJSON):
    """
    Makes it easier to assert JSON in doctests. The argument is checked for
    validity and pretty-printed with and indent of four spaces.

    >>> assert_json(dict(foo=[None], bar=42))
    {
        "foo": [
            null
        ],
        "bar": 42
    }

    Compare this with a traditional doctest assertion which has to fit on a
    single line:

    >>> dict(foo=[None], bar=42)
    {'foo': [None], 'bar': 42}
    """
    print(json.dumps(j, indent=4))
