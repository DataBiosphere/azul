from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
    get_args,
    get_origin,
)

PrimitiveJSON = Union[str, int, float, bool, None]

# Not every instance of Mapping or Sequence can be fed to json.dump() but those
# two generic types are the most specific *immutable* super-types of `list`,
# `tuple` and `dict`:

AnyJSON4 = Union[Mapping[str, Any], Sequence[Any], PrimitiveJSON]
AnyJSON3 = Union[Mapping[str, AnyJSON4], Sequence[AnyJSON4], PrimitiveJSON]
AnyJSON2 = Union[Mapping[str, AnyJSON3], Sequence[AnyJSON3], PrimitiveJSON]
AnyJSON1 = Union[Mapping[str, AnyJSON2], Sequence[AnyJSON2], PrimitiveJSON]
AnyJSON = Union[Mapping[str, AnyJSON1], Sequence[AnyJSON1], PrimitiveJSON]
JSON = Mapping[str, AnyJSON]
JSONs = Sequence[JSON]
CompositeJSON = Union[JSON, Sequence[AnyJSON]]

# For mutable JSON we can be more specific and use Dict and List:

AnyMutableJSON4 = Union[Dict[str, Any], List[Any], PrimitiveJSON]
AnyMutableJSON3 = Union[Dict[str, AnyMutableJSON4], List[AnyMutableJSON4], PrimitiveJSON]
AnyMutableJSON2 = Union[Dict[str, AnyMutableJSON3], List[AnyMutableJSON3], PrimitiveJSON]
AnyMutableJSON1 = Union[Dict[str, AnyMutableJSON2], List[AnyMutableJSON2], PrimitiveJSON]
AnyMutableJSON = Union[Dict[str, AnyMutableJSON1], List[AnyMutableJSON1], PrimitiveJSON]
MutableJSON = Dict[str, AnyMutableJSON]
MutableJSONs = List[MutableJSON]
MutableCompositeJSON = Union[MutableJSON, List[AnyJSON]]


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
    """
    if get_origin(t) != Union:
        raise ValueError('Not a reifiable generic type', t)

    def f(t):
        for a in get_args(t):
            if get_origin(a) == Union:
                # handle Union of Union
                yield from f(a)
            else:
                o = get_origin(a)
                yield a if o is None else o

    return tuple(set(f(t)))
