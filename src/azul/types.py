from collections.abc import (
    Mapping,
    Sequence,
)
from typing import (
    Any,
    ForwardRef,
    Generic,
    Optional,
    Protocol,
    TYPE_CHECKING,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

from more_itertools import (
    one,
)

from azul.collections import (
    OrderedSet,
)

PrimitiveJSON = Union[str, int, float, bool, None]

# Not every instance of Mapping or Sequence can be fed to json.dump() but those
# two generic types are the most specific *immutable* super-types of `list`,
# `tuple` and `dict`:

AnyJSON4 = Union[Sequence[Any], Mapping[str, Any], PrimitiveJSON]
AnyJSON3 = Union[Sequence[AnyJSON4], Mapping[str, AnyJSON4], PrimitiveJSON]
AnyJSON2 = Union[Sequence[AnyJSON3], Mapping[str, AnyJSON3], PrimitiveJSON]
AnyJSON1 = Union[Sequence[AnyJSON2], Mapping[str, AnyJSON2], PrimitiveJSON]
AnyJSON = Union[Sequence[AnyJSON1], Mapping[str, AnyJSON1], PrimitiveJSON]
JSON = Mapping[str, AnyJSON]
JSONs = Sequence[JSON]
CompositeJSON = Union[JSON, Sequence[AnyJSON]]

# For mutable JSON we can be more specific and use dict and list:

AnyMutableJSON4 = Union[list[Any], dict[str, Any], PrimitiveJSON]
AnyMutableJSON3 = Union[list[AnyMutableJSON4], dict[str, AnyMutableJSON4], PrimitiveJSON]
AnyMutableJSON2 = Union[list[AnyMutableJSON3], dict[str, AnyMutableJSON3], PrimitiveJSON]
AnyMutableJSON1 = Union[list[AnyMutableJSON2], dict[str, AnyMutableJSON2], PrimitiveJSON]
AnyMutableJSON = Union[list[AnyMutableJSON1], dict[str, AnyMutableJSON1], PrimitiveJSON]
MutableJSON = dict[str, AnyMutableJSON]
MutableJSONs = list[MutableJSON]
MutableCompositeJSON = Union[MutableJSON, list[AnyJSON]]


class LambdaContext(object):
    """
    A stub for the AWS Lambda context
    """

    @property
    def aws_request_id(self) -> str:
        raise NotImplementedError

    @property
    def log_group_name(self) -> str:
        raise NotImplementedError

    @property
    def log_stream_name(self) -> str:
        raise NotImplementedError

    @property
    def function_name(self) -> str:
        raise NotImplementedError

    @property
    def memory_limit_in_mb(self) -> str:
        raise NotImplementedError

    @property
    def function_version(self) -> str:
        raise NotImplementedError

    @property
    def invoked_function_arn(self) -> str:
        raise NotImplementedError

    def get_remaining_time_in_millis(self) -> int:
        raise NotImplementedError

    def log(self, msg: str) -> None:
        raise NotImplementedError


def is_optional(t) -> bool:
    """
    :param t: A type or type annotation.

    :return: True if theargument is equivalent to typing.Optional

    https://stackoverflow.com/a/62641842/4171119

    >>> is_optional(str)
    False
    >>> is_optional(Optional[str])
    True
    >>> is_optional(Union[str, None])
    True
    >>> is_optional(Union[None, str])
    True
    >>> is_optional(Union[str, None, int])
    True
    >>> is_optional(Union[str, int])
    False
    """
    return t == Optional[t]


def reify(t):
    """
    Given a parameterized ``Union`` or ``Optional`` construct, return a tuple of
    subclasses of ``type`` representing all possible alternatives that can pass
    for that construct at runtime. The return value is meant to be used as the
    second argument to the ``isinstance`` or ``issubclass`` built-ins.

    >>> isinstance({}, reify(AnyJSON))
    True

    >>> from collections import Counter
    >>> issubclass(Counter, reify(AnyJSON))
    True

    >>> isinstance([], reify(AnyJSON))
    True

    >>> isinstance((), reify(AnyJSON))
    True

    >>> isinstance(42, reify(AnyJSON))
    True

    >>> isinstance(set(), reify(AnyJSON))
    False

    >>> set(reify(Optional[int])) == {type(None), int}
    True

    >>> from typing import TypeVar
    >>> reify(TypeVar)
    Traceback (most recent call last):
        ...
    ValueError: ('Not a reifiable generic type', <class 'typing.TypeVar'>)

    >>> reify(Union)
    Traceback (most recent call last):
        ...
    ValueError: ('Not a reifiable generic type', typing.Union)

    >>> reify(int)
    <class 'int'>
    """
    if get_origin(t) == Union:
        def f(t):
            for a in get_args(t):
                if get_origin(a) == Union:
                    # handle Union of Union
                    yield from f(a)
                else:
                    o = get_origin(a)
                    yield a if o is None else o

        return tuple(OrderedSet(f(t)))
    elif t.__module__ != 'typing':
        return t
    else:
        raise ValueError('Not a reifiable generic type', t)


def get_generic_type_params(cls: type[Generic],
                            *required_types: type
                            ) -> Sequence[Union[type, TypeVar, ForwardRef]]:
    """
    Inspect and validate the type parameters of a subclass of `typing.Generic`.

    The type of each returned parameter may be a type, a `typing.TypeVar`, or a
    `typing.ForwardRef`, depending on how the parameter is written in the
    inspected class's definition. `*required_types` can be used to assert the
    superclasses of parameters that are types.

    >>> from typing import Generic, TypeVar
    >>> T = TypeVar(name='T')
    >>> class A(Generic[T]):
    ...     pass
    >>> class B(A[int]):
    ...     pass
    >>> class C(A['foo']):
    ...     pass

    >>> get_generic_type_params(A)
    (~T,)

    >>> get_generic_type_params(A, str)
    (~T,)

    >>> get_generic_type_params(B)
    (<class 'int'>,)

    >>> get_generic_type_params(B, str)
    Traceback (most recent call last):
    ...
    AssertionError: (<class 'int'>, <class 'str'>)

    >>> get_generic_type_params(B, int, int)
    Traceback (most recent call last):
    ...
    AssertionError: 1

    >>> get_generic_type_params(C)
    (ForwardRef('foo'),)
    """
    base_cls = one(getattr(cls, '__orig_bases__'))
    types = get_args(base_cls)
    if required_types:
        assert len(required_types) == len(types), len(types)
        for required_type, type_ in zip(required_types, types):
            if isinstance(type_, type):
                assert issubclass(type_, required_type), (type_, required_type)
            else:
                assert isinstance(type_, (TypeVar, ForwardRef)), type_
    return types


# FIXME: Remove hacky import of SupportsLessThan
#        https://github.com/DataBiosphere/azul/issues/2783
if TYPE_CHECKING:
    pass
else:
    class SupportsLessThan(Protocol):

        def __lt__(self, __other: Any) -> bool: ...
