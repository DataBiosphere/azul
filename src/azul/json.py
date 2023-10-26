from copy import (
    copy,
    deepcopy,
)
import hashlib
from io import (
    StringIO,
)
import json
from typing import (
    Union,
    cast,
)

from azul.types import (
    AnyJSON,
    CompositeJSON,
    JSON,
    JSONs,
    MutableCompositeJSON,
    MutableJSON,
    MutableJSONs,
)


def copy_json(o: JSON, *path: Union[str, int]) -> MutableJSON:
    """
    Make a new, mutable copy of a JSON object.

    This is a convenience wrapper of :func:`copy_composite_json` that expresses
    the covariance between argument and return types.

    >>> a = {'a': [1, 2]}
    >>> b = copy_json(a)
    >>> b['a'].append(3)
    >>> b
    {'a': [1, 2, 3]}
    >>> a
    {'a': [1, 2]}
    """
    return copy_composite_json(o, *path)


def copy_jsons(o: JSONs, *path: Union[str, int]) -> MutableJSONs:
    """
    Make a new, mutable copy of a JSON array.

    This is a convenience wrapper of :func:`copy_composite_json` that expresses
    the covariance between argument and return types.

    >>> a = [{'a': [1, 2]}, {'b': 3}]
    >>> b = copy_jsons(a)
    >>> b[0]['a'].append(3)
    >>> b
    [{'a': [1, 2, 3]}, {'b': 3}]
    >>> a
    [{'a': [1, 2]}, {'b': 3}]
    """
    return cast(MutableJSONs, copy_composite_json(o, *path))


def copy_composite_json(tree: CompositeJSON,
                        *path: Union[str, int]
                        ) -> MutableCompositeJSON:
    """
    Make a mutable, deep copy of the given JSON structure, or some part of it.

    If no path is given, any part of the return value can be modified without
    affecting the argument value. If a path is given, only the JSON node at the
    path into the return value can be modified safely. Modifying any other
    part of the return value may inadvertently affect the argument value.

    :param tree: The JSON structure to copy.

    :param path: An optional path, restricting the scope of the copying being
                 done. The first element of the path is an index or key into the
                 first argument, depending on whether the argument is a list or
                 a dictionary. The value at that index or key must, again, be
                 either a dictionary or a list. If a second path element was
                 passed, a shallow copy of the dictionary will be made and the
                 second path element is used as a key or index into that copy.
                 This process repeats until the end of the path is reached at
                 which time a deep copy of the resulting list will be made.

    Create a JSON tree with two branches, ``l`` and ``r``:

    >>> o = {'l': {'ll': [1, 2]}, 'r': {'rr': {'rrr': [3, 4]}}}

    Copy only the ``r`` branch:

    >>> c = copy_json(o, 'r')

    The ``r`` branch in the return value is now a copy:

    >>> c['r'] is o['r']
    False
    >>> c['r'] == o['r']
    True

    It could be modified without affecting the original tree ``o``.

    However, the ``l`` branch is an alias and should not be modified:

    >>> c['l'] is o['l']
    True

    The same but with a path of two nodes: Note that all nodes along the path
    are shallow copies, the leaf node is a deep copy. To make a copy of the
    leaf, the parent node's reference to must be updated, and to update the
    parent it must be copied.

    >>> c = copy_json(o, 'r', 'rr')
    >>> c['r'] is o['r']
    False
    >>> c['r'] == o['r']
    True
    >>> c['r']['rr'] is o['r']['rr']
    False
    >>> c['r']['rr'] == o['r']['rr']
    True
    >>> c['l'] is o['l']
    True

    The path can be used to traverse any lists in the structure.

    >>> o = {'a': [{'b': {'c': 1}}]}
    >>> c = copy_json(o, 'a', 0, 'b')
    >>> c['a'][0]['b'] is o['a'][0]['b']
    False
    >>> c['a'][0]['b'] == o['a'][0]['b']
    True

    However, the types of the path elements types must align with the structure:

    >>> c = copy_json(o, 'a', '0', 'b')
    Traceback (most recent call last):
    ...
    TypeError: Path element '0' cannot be used to traverse a value of <class 'list'>

    >>> c = copy_json(o, 'a', 0, 0)
    Traceback (most recent call last):
    ...
    TypeError: Path element 0 cannot be used to traverse a value of <class 'dict'>
    """
    if path:
        *path, last = path
        tree = node = copy(tree)
        for element in path:
            _check_node(node, element)
            assert isinstance(node, (dict, list))
            node[element] = copy(node[element])
            node = node[element]
        _check_node(node, last)
        assert isinstance(node, (dict, list))
        node[last] = deepcopy(node[last])
    else:
        tree = deepcopy(tree)
    return cast(MutableCompositeJSON, tree)


def _check_node(node, path_element):
    if not (
        isinstance(node, dict) and isinstance(path_element, str)
        or isinstance(node, list) and isinstance(path_element, int)
    ):
        raise TypeError(f'Path element {path_element!r} cannot be used '
                        f'to traverse a value of {type(node)}')


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


def json_hash(o: AnyJSON, hash=None):
    """
    Efficiently compute a hash of a JSON object.

    >>> o = {'foo': 1, 'bar': 2.0, 'baz': 'baz'}
    >>> json_hash(o).hexdigest()
    '08335acd02f77fdd32775f51a1766796e91bc0e1'

    >>> json_hash(o, hashlib.sha1()).hexdigest()
    '08335acd02f77fdd32775f51a1766796e91bc0e1'

    >>> json_hash(o, hashlib.md5()).hexdigest()
    'd28a433c1e34de7c7da3ea59fd9e48f9'

    >>> json_hash(o).digest() == json_hash(dict(reversed(o.items()))).digest()
    True
    """
    if hash is None:
        hash = hashlib.sha1()
    encoder = json.JSONEncoder(sort_keys=True, separators=(',', ':'))
    for chunk in encoder.iterencode(o):
        hash.update(chunk.encode())
    return hash
