import copy
from io import StringIO
import json
from typing import cast

from azul.types import (
    AnyJSON,
    JSON,
    MutableJSON,
)


def copy_json(o: JSON) -> MutableJSON:
    """
    Make a new, mutable copy of a JSON object.

    >>> a = {'a': [1, 2]}
    >>> b = copy_json(a)
    >>> b['a'].append(3)
    >>> b
    {'a': [1, 2, 3]}
    >>> a
    {'a': [1, 2]}
    """
    return cast(MutableJSON, copy.deepcopy(o))


def json_head(n: int, o: AnyJSON) -> str:
    """
    Return the first n characters of a serialized JSON structure.

    >>> json_head(0, {})
    ''
    >>> json_head(1, {})
    '{'
    >>> json_head(2, {})
    '{}'
    >>> json_head(3, {})
    '{}'
    >>> json_head(0, "x")
    ''
    >>> json_head(1, "x")
    '"'
    >>> json_head(2, "x")
    '"x'
    >>> json_head(3, "x")
    '"x"'
    >>> json_head(4, "x")
    '"x"'
    """
    buf = StringIO()
    for chunk in json.JSONEncoder().iterencode(o):
        buf.write(chunk)
        if buf.tell() > n:
            break
    return buf.getvalue()[:n]
