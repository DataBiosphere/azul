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
    Self,
    overload,
)

from more_itertools.more import (
    mark_ends,
)

from azul import (
    R,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    CompositeJSON,
    JSON,
    JSONArray,
    MutableCompositeJSON,
    MutableJSON,
    MutableJSONArray,
    json_str,
)


def copy_any_json(v: AnyJSON) -> AnyMutableJSON:
    """
    Same as :func:`copy_json` but additionally allows passing primitive values
    for which it simply returns the argument.
    """
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    else:
        return copy_json(v)


@overload
def copy_json(tree: JSON, *path: str | int) -> MutableJSON: ...


@overload
def copy_json(tree: JSONArray, *path: str | int) -> MutableJSONArray: ...


def copy_json(tree: CompositeJSON, *path: str | int) -> MutableCompositeJSON:
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

    >>> o = {'a': [1, 2]}
    >>> c = copy_json(o)
    >>> c['a'].append(3)
    >>> c
    {'a': [1, 2, 3]}
    >>> o
    {'a': [1, 2]}

    >>> o = [{'a': [1, 2]}, {'b': 3}]
    >>> c = copy_json(o)
    >>> c[0]['a'].append(3)
    >>> c
    [{'a': [1, 2, 3]}, {'b': 3}]
    >>> o
    [{'a': [1, 2]}, {'b': 3}]

    Only composite JSON can be copied, primitives cannot.

    >>> copy_json(1)
    Traceback (most recent call last):
    ...
    AssertionError: R('First argument must be dict or list', <class 'int'>)

    Despite the argument being declared as immutable JSON, we don't actually
    support immutable sequences or mappings. The immutable types from
    ``azul.typing`` are only meant to prevent mutations statically, not enforced
    at runtime. That's why a tuple, an immutable sequence, is rejected:

    >>> copy_json(())
    Traceback (most recent call last):
    ...
    AssertionError: R('First argument must be dict or list', <class 'tuple'>)

    For a more complicated example, we create a JSON tree with two branches,
    ``l`` and ``r``:

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
    assert isinstance(tree, (dict, list)), R(
        'First argument must be dict or list', type(tree)
    )
    if path:
        node = tree = copy(tree)
        for is_first, is_last, element in mark_ends(path):
            f = deepcopy if is_last else copy
            if isinstance(node, dict) and isinstance(element, str):
                node[element] = f(node[element])
                node = node[element]
            elif isinstance(node, list) and isinstance(element, int):
                node[element] = f(node[element])
                node = node[element]
            else:
                raise TypeError(f'Path element {element!r} cannot be used '
                                f'to traverse a value of {type(node)}')
    else:
        tree = deepcopy(tree)
    return tree


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
    Quickly compute a hash of a JSON object.

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
    # We intentionally do not directly use .iterencode() here. It's still being
    # used internally by .encode() but it turns out that passing each chunk
    # individually to the hash via .update() is much slower than first joining
    # all chunks and then hashing the result in one call, which is what this
    # implementation does. The reason is that the chunks tend to be short:
    # delimiters and scalars are all individual chunks. Calling .update() that
    # frequently is slow, as was confirmed by profiling the unit tests. This
    # implementation comes at the expense of a potentially large amount of
    # memory being used, albeit briefly. We currently use this method for
    # relatively small JSON structures, under 1MiB, so that caveat is
    # acceptable. A further improved hybrid approach would cap the amount of
    # memory being used by batching chunks. An interesting tidbit: .encode()
    # does not pass the iterable from .iterencode() directly to ''.join() but
    # instead renders a list first, claiming it produces better diagnostic
    # output, with little impact on performance.
    hash.update(encoder.encode(o).encode())
    return hash


class Serializable:
    """
    A class whose instances can be transformed to and from JSON
    """

    # This is more akin to an interface (like those in Java) as opposed to an
    # abstract base class. We're intentionally refraining from using ABCMeta as
    # a metaclass here so as to allow for implementations to be instances of a
    # different metaclass.

    @classmethod
    def from_json(cls, json: AnyJSON) -> Self:
        """
        Deserialize an instance of this class from the given JSON value
        """
        raise NotImplementedError

    def to_json(self) -> AnyJSON:
        """
        Serialize this instance to JSON in a form suitable for :meth:`from_json`
        """
        raise NotImplementedError


class PolymorphicSerializable(Serializable):
    """
    A class whose subclasses' instances can be transformed to and from JSON
    while retaining the concrete type of said instances.
    """

    @classmethod
    def cls_to_json(cls) -> AnyJSON:
        """
        Serialize the given type to JSON.
        """
        raise NotImplementedError

    @classmethod
    def cls_from_json(cls, json: AnyJSON) -> type[Self]:
        """
        Deserialize a subtype of the given type from the given JSON.
        """
        raise NotImplementedError


class RegisteredPolymorphicSerializable(PolymorphicSerializable):
    """
    A polymorphically serializable class that tracks its subclasses in a
    registry and uses their name to discriminate serialized instances. It
    requires every subclass to be registered before instances of that subclass
    can be (de)serialized. It also requires the name of each subclass to be
    unique, regardless of the module the subclass is defined in.
    """

    _registry: dict[str, type[Self]] = {}

    @classmethod
    def cls_to_json(cls) -> AnyJSON:
        assert cls._registry[cls.__name__] == cls
        return cls.__name__

    @classmethod
    def cls_from_json(cls, json: AnyJSON) -> type[Self]:
        return cls._registry[json_str(json)]

    def __init_subclass__(cls):
        super().__init_subclass__()
        try:
            other_cls = cls._registry[cls.__name__]
        except KeyError:
            pass
        else:
            # For attrs classes, this hook is invoked twice: once for the
            # original class and once for the attrs-generated replacement. These
            # are two different objects, so they are neither the same nor equal
            # so it is difficult to tell wether we're dealing with the attrs
            # replacement or a genuine collision. Both original and replacement
            # reference the same containing module, so we assume that two
            # classes of the same name from the same module indicate that attrs
            # is involved and does not constitue a collision.
            assert other_cls.__module__ == cls.__module__, R(
                'Class name collision', cls, other_cls)
        cls._registry[cls.__name__] = cls


class Parseable(Serializable):
    """
    A class whose instances have a string representation that can be used in
    JSON documents.
    """

    @classmethod
    def from_json(cls, json: AnyJSON) -> Self:
        return cls.parse(json_str(json))

    def to_json(self) -> AnyJSON:
        return str(self)

    def __str__(self) -> str:
        raise NotImplementedError

    @classmethod
    def parse(cls, value: str) -> Self:
        raise NotImplementedError
