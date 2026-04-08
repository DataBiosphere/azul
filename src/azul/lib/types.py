from collections.abc import (
    Iterable,
    Mapping,
    Sequence,
)
from types import (
    GenericAlias,
    UnionType,
    get_original_bases,
)
import typing
from typing import (
    Any,
    Callable,
    ForwardRef,
    NotRequired,
    Optional,
    Protocol,
    ReadOnly,
    Required,
    TypeAliasType,
    TypeGuard,
    TypeIs,
    TypeVar,
    TypedDict,
    Union,
    cast,
    get_args,
    get_origin,
)

from more_itertools import (
    one,
)

from azul.lib.collections import (
    OrderedSet,
    none_safe_key,
)


def not_none[T](v: T | None) -> T:
    assert v is not None
    return v


PrimitiveJSON = str | int | float | bool | None

type AnyJSON = JSON | JSONArray | PrimitiveJSON
type JSON = Mapping[str, AnyJSON]
type JSONArray = Sequence[AnyJSON]
type JSONs = Sequence[JSON]
type CompositeJSON = JSON | JSONArray
type FlatJSON = Mapping[str, PrimitiveJSON]

# For mutable JSON we can be more specific and use dict and list:

type AnyMutableJSON = MutableJSON | MutableJSONArray | PrimitiveJSON
type MutableJSON = dict[str, AnyMutableJSON]
type MutableJSONArray = list[AnyMutableJSON]
type MutableJSONs = list[MutableJSON]
type MutableCompositeJSON = MutableJSON | MutableJSONArray
type MutableFlatJSON = dict[str, PrimitiveJSON]


def optional[A, R](f: Callable[[A], R], v: A) -> R | None:
    return v if v is None else f(v)


def json_mapping(v: AnyJSON) -> JSON:
    assert isinstance(v, Mapping), type(v)
    return v


def json_sequence(v: AnyJSON) -> JSONArray:
    assert isinstance(v, Sequence) and not isinstance(v, str), type(v)
    return v


def json_composite(v: AnyJSON) -> CompositeJSON:
    assert isinstance(v, (dict, list)), type(v)
    return v


def json_item_mappings(vs: AnyJSON) -> Iterable[tuple[str, JSON]]:
    for k, v in json_mapping(vs).items():
        yield k, json_mapping(v)


def json_item_sequences(vs: AnyJSON) -> Iterable[tuple[str, JSONArray]]:
    for k, v in json_mapping(vs).items():
        yield k, json_sequence(v)


def json_element_mappings(vs: AnyJSON) -> Iterable[JSON]:
    return map(json_mapping, json_sequence(vs))


def json_element_strings(vs: AnyJSON) -> Iterable[str]:
    return map(json_str, json_sequence(vs))


def json_sequence_of_mappings(vs: AnyJSON) -> JSONs:
    vs = json_sequence(vs)
    assert json_elements_are_mappings(vs)
    return vs


def json_elements_are_mappings(vs: JSONArray) -> TypeGuard[JSONs]:
    for v in vs:
        json_mapping(v)
    return True


def json_sequence_of_optional_strings(vs: AnyJSON) -> Sequence[str | None]:
    vs = json_sequence(vs)
    assert json_elements_are_optional_strings(vs)
    return vs


def json_elements_are_optional_strings(vs: JSONArray
                                       ) -> TypeGuard[Sequence[str | None]]:
    for v in vs:
        optional(json_str, v)
    return True


def json_items_are_sequences_of_mappings(vs: AnyJSON) -> TypeGuard[Mapping[str, JSONs]]:
    vs = json_mapping(vs)
    for v in vs.values():
        assert json_elements_are_mappings(json_sequence(v))
    return True


def json_primitive(v: AnyJSON) -> PrimitiveJSON:
    assert v is None or isinstance(v, (str, int, float, bool)), type(v)
    return v


def json_dict(v: AnyMutableJSON) -> MutableJSON:
    assert isinstance(v, dict), type(v)
    return v


def json_list(v: AnyMutableJSON) -> MutableJSONArray:
    assert isinstance(v, list), type(v)
    return v


def json_item_dicts(vs: AnyMutableJSON) -> Iterable[tuple[str, MutableJSON]]:
    for k, v in json_dict(vs).items():
        yield k, json_dict(v)


def json_item_lists(vs: AnyMutableJSON) -> Iterable[tuple[str, MutableJSONArray]]:
    for k, v in json_dict(vs).items():
        yield k, json_list(v)


def json_element_dicts(vs: AnyMutableJSON) -> Iterable[MutableJSON]:
    return map(json_dict, json_list(vs))


def json_list_of_dicts(vs: AnyMutableJSON) -> MutableJSONs:
    vs = json_list(vs)
    assert json_elements_are_dicts(vs)
    return vs


def json_elements_are_dicts(vs: MutableJSONArray) -> TypeGuard[MutableJSONs]:
    for v in vs:
        json_dict(v)
    return True


def json_dict_of_dicts(vs: MutableJSON) -> dict[str, MutableJSON]:
    assert json_items_are_dicts(vs)
    return vs


def json_items_are_dicts(vs: MutableJSON) -> TypeGuard[dict[str, MutableJSON]]:
    for v in vs.values():
        json_dict(v)
    return True


def json_dict_of_lists(vs: MutableJSON) -> dict[str, MutableJSONArray]:
    assert json_items_are_lists(vs)
    return vs


def json_items_are_lists(vs: MutableJSON) -> TypeGuard[dict[str, MutableJSONArray]]:
    for v in vs.values():
        json_list(v)
    return True


def json_sorted(vs: Iterable[PrimitiveJSON]) -> MutableJSONArray:
    return sorted(vs, key=none_safe_key(none_last=True))


def json_str(v: AnyMutableJSON | AnyJSON) -> str:
    return any_str(v)


def any_str(v: Any) -> str:
    assert isinstance(v, str), type(v)
    return v


def json_int(v: AnyMutableJSON | AnyJSON) -> int:
    return any_int(v)


def any_int(v: Any) -> int:
    assert isinstance(v, int), type(v)
    return v


def json_float(v: AnyMutableJSON | AnyJSON) -> float:
    return any_float(v)


def any_float(v: Any) -> float:
    assert isinstance(v, float), type(v)
    return v


def json_bool(v: AnyMutableJSON | AnyJSON) -> bool:
    return any_bool(v)


def any_bool(v: Any) -> bool:
    assert isinstance(v, bool), type(v)
    return v


def json_none(v: AnyMutableJSON | AnyJSON) -> None:
    return any_none(v)


def any_none(v: Any) -> None:
    assert v is None, type(v)
    return v


class JSONTypedDict(TypedDict):
    """
    Use this as a base class for TypedDict's that are also JSON.
    """
    pass


def json_untyped_dict(v: JSONTypedDict) -> MutableJSON:
    # FIXME: json_untyped_dict is unsafe
    #        https://github.com/DataBiosphere/azul/issues/7381
    return cast(MutableJSON, v)


class LambdaContext:
    """
    A stub for the AWS Lambda context
    """

    aws_request_id: str

    log_group_name: str

    log_stream_name: str

    function_name: str

    memory_limit_in_mb: str

    function_version: str

    invoked_function_arn: str

    def get_remaining_time_in_millis(self) -> int: ...  # type: ignore[empty-body]

    def log(self, msg: str) -> None: ...


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

    >>> is_optional(str | None)
    True
    >>> is_optional(None | str)
    True
    >>> is_optional(str | None | int)
    True
    >>> is_optional(str | int)
    False
    """
    return t == Optional[t]


def reify(t):
    """
    Given a parameterized type construct, return a tuple of subclasses of
    ``type`` representing all possible alternatives that can pass for that
    construct at runtime. The return value is meant to be used as the second
    argument to the ``isinstance`` or ``issubclass`` built-ins.

    >>> reify(int)
    (<class 'int'>,)

    >>> reify(Union[int])
    (<class 'int'>,)

    >>> reify(str | int)
    (<class 'str'>, <class 'int'>)

    >>> reify(Union[str, int])
    (<class 'str'>, <class 'int'>)

    >>> reify(str | Union[int, set])
    (<class 'str'>, <class 'int'>, <class 'set'>)

    >>> reify(Union[str | int, set])
    (<class 'str'>, <class 'int'>, <class 'set'>)

    >>> isinstance({}, reify(AnyJSON))
    True

    >>> isinstance({}, reify(JSON))
    True

    >>> isinstance([], reify(JSON))
    False

    >>> isinstance([], reify(JSONs))
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

    >>> reify(TypeVar)
    Traceback (most recent call last):
        ...
    ValueError: ('Not a reifiable generic type', <class 'typing.TypeVar'>)

    >>> reify(Union)
    Traceback (most recent call last):
        ...
    ValueError: ('Not a reifiable generic type', <class 'typing.Union'>)
    """

    def reify(t):
        while isinstance(t, TypeAliasType):
            t = t.__value__
        o = get_origin(t)
        # While `int | str` constructs a `UnionType` instance, `Union[str, int]`
        # constructs an instance of `Union`, so we need to handle both.
        if o in (UnionType, Union):
            for a in get_args(t):
                yield from reify(a)
        elif o is not None:
            yield o
        elif t.__module__ == 'typing':
            raise ValueError('Not a reifiable generic type', t)
        else:
            yield t

    return tuple(OrderedSet(reify(t)))


type TypeParams = dict[TypeVar, type | TypeVar | ForwardRef]


def derived_type_params(cls: type, /, *, root: Any = None) -> TypeParams:
    """
    Inspect the type parameterization of a generic class, or a class derived
    from a generic class.

    Each of the returned values is either an instance of ``type``, of
    ``typing.TypeVar``, or of ``typing.ForwardRef``, depending on how the
    parameter is written in the definition of the root class.

    The most common use case is to let a method in a potentially abstract base
    class determine the value of a type parameter that is bound by deriving
    from the class:

    >>> class Base[T]:
    ...     def make(self, s):
    ...         cls = type(self)
    ...         params = derived_type_params(cls, root=Base)
    ...         t = params[T]
    ...         assert isinstance(t, type)
    ...         return t(s)

    >>> class IntBase(Base[int]): ...
    >>> IntBase().make('1')
    1

    >>> class FloatBase(Base[float]): ...
    >>> FloatBase().make('1')
    1.0

    Caveat: This function was only tested with classes that use the syntax from
    PEP-695 to define type parameters. Every class in the ancestry of the given
    class must use the new syntax. Note that PEP-695 introduced semantic changes
    as well, mostly with respect to scoping and variance.

    Caveat: This function was not tested with multiple inheritance. It should
    generally, work but diamond-shaped ancestry may be problematic.

    :param cls: The class to be inspected

    :param root: The upper bound up to which the ancestry of ``cls`` is
                 inspected. If this argument is ``None``, only the first parent
                 of ``cls`` is inspected. Otherwise, the ancestry of ``cls`` is
                 searched for ``root``. The search is "height-first", as in
                 depth-first but going upwards. If the root is found in the
                 ancestry, the parameterization of every ancestor on the lineage
                 from ``cls`` to ``root`` is then inspected.

    :return: A dictionary mapping a type parameter (an instance of
             ``typing.TypeVar``) to its value. The value could be a reified
             type (an instance of ``type``), another type variable
             (``typing.TypeVar``) or a forward reference. For details on the
             latter two scenarios, refer to the examples below.

    A generic class:

    >>> class A[T1, T2]:
    ...     @classmethod
    ...     def t1(cls):
    ...         return derived_type_params(cls, root=A)[T1]

    Its parent (``object``) doesn't have any type parameters to be bound.

    >>> derived_type_params(A)
    {}

    A non-generic subclass:

    >>> class B(A[int, float]): ...

    >>> derived_type_params(B)
    {T1: <class 'int'>, T2: <class 'float'>}

    A non-generic subclass, using a forward reference. The reference is
    returned:

    >>> class C(A['foo', int]): ...

    >>> derived_type_params(C)
    {T1: ForwardRef('foo'), T2: <class 'int'>}

    A generic class that binds the first of the parent's parameters, but leaves
    the second one open:

    >>> class D[T](A[int, T]): ...

    >>> derived_type_params(D)
    {T1: <class 'int'>, T2: T}

    A non-generic subclass that binds the remaining parameter as well:

    >>> class E(D[float]): ...

    The value that E bind's D's parameter to:

    >>> derived_type_params(E)
    {T: <class 'float'>}

    The equivalent invocation explicitly specifying the first parent class:

    >>> derived_type_params(E, root=D)
    {T: <class 'float'>}

    E does not inherit B, so an exception is raised:

    >>> derived_type_params(E, root=B)
    Traceback (most recent call last):
    ...
    TypeError: ('Root is not an ancestor', <class 'azul.lib.types.B'>, <class 'azul.lib.types.E'>)

    Last but not least, the most useful invocation: specifying the oldest
    generic ancestor as the root. This invocation returns the parameterization
    of E's grandparent.

    >>> derived_type_params(E, root=A)
    {T1: <class 'int'>, T2: <class 'float'>}

    Same as above but through a parent that binds the second of the
    grandparent's parameters:

    >>> class F[T3](A[T3, int]): ...
    >>> class G(F[float]): ...
    >>> derived_type_params(G, root=A)
    {T1: <class 'float'>, T2: <class 'int'>}

    A parent swapping the grandparent's type parameters:

    >>> class H[T1, T2](A[T2, T1]): ...
    >>> class J(H[int, float]): ...
    >>> (params := derived_type_params(J, root=A))
    {T1: <class 'float'>, T2: <class 'int'>}

    The first argument must be a type.

    >>> from more_itertools import first
    >>> derived_type_params(first(params.keys()))
    Traceback (most recent call last):
    ...
    AssertionError: R('Not a type', T1, <class 'typing.TypeVar'>)

    Ancestry that includes a non-generic base cass:

    >>> class K(E): ...
    >>> derived_type_params(K)
    {}
    >>> derived_type_params(K, root=E)
    {}
    >>> derived_type_params(K, root=D)
    {T: <class 'float'>}
    >>> derived_type_params(K, root=A)
    {T1: <class 'int'>, T2: <class 'float'>}

    Ancestry with two strains of type variables:

    >>> class L[T3](E): ...
    >>> class M(L[str]): ...
    >>> derived_type_params(M)
    {T3: <class 'str'>}
    >>> derived_type_params(M, root=L)
    {T3: <class 'str'>}
    >>> derived_type_params(M, root=A)
    {T1: <class 'int'>, T2: <class 'float'>}
    """
    from azul.lib import (
        R,
    )
    assert isinstance(cls, type), R('Not a type', cls, type(cls))

    def ancestors(cls) -> tuple[Any, ...]:
        for base in get_original_bases(cls):
            origin = get_origin(base)
            if origin is None:
                origin = base
            if origin is root:
                return (base,)
            else:
                lineage = ancestors(origin)
                if lineage:
                    return base, *lineage
        return ()

    if root is None:
        root = get_original_bases(cls)[0]
        bases = (root,)
    else:
        bases = ancestors(cls)
        if not bases:
            raise TypeError('Root is not an ancestor', root, cls)

    mapping: TypeParams = {}
    for base in bases:
        values = get_args(base)
        values = tuple(mapping.get(value, value) for value in values)
        assert all(isinstance(value, (type, ForwardRef, TypeVar)) for value in values)
        origin = get_origin(base)
        if origin is None:
            mapping = {}
        else:
            params = origin.__type_params__
            assert all(isinstance(param, TypeVar) for param in params)
            mapping = dict(zip(params, values))
    assert base is root or origin is root
    return mapping


class SupportsLessAndGreaterThan(Protocol):

    def __lt__(self, __other: Any) -> bool: ...

    def __gt__(self, __other: Any) -> bool: ...


_UnionGenericAlias = type(Union[int, str])
_GenericAlias = type(typing.List[int])
_SpecialGenericAlias = type(typing.Sized)
_TypedDictMeta = type(JSONTypedDict)

# The following serves more of a documentary purpose than for static analysis.
# We can't include the above three types in the definition, even thought we'd
# like to, because they are determined at runtime. Note that UnionType and
# others are defined the same way, and it appears that mypy supports only those
# specific cases.
#
type TypeExpression = Union[
    type,
    TypeAliasType,
    UnionType,
    GenericAlias,
]


def check_type(type_expression: TypeExpression, value: Any) -> bool:
    """
    CAUTION: THIS IS A PROOF OF CONCEPT. IT MAY CHANGE OR GO AWAY IN THE FUTURE.

    This function resembles the ``isinstance()`` built-in but additionally
    supports type unions, type aliases, TypedDict and certain generic
    collections (currently any mapping or iterable). TypedDict instances and
    collections are checked recursively, and for every item, which is why this
    function may be slow. If the second argument is a cyclic data structure,
    this function may never return. Another difference to ``isinstance`` is that
    the order of the arguments is swapped: the type comes first.

    >>> check_type(list, [])
    True

    >>> check_type(list, {})
    False

    >>> check_type(list[str], [1])
    False

    >>> check_type(list[str], [''])
    True

    >>> check_type(typing.List[str], [''])
    True

    Intentional deviations from Python's type system: booleans are not
    considered integers and strings are not considered sequences.

    We believe that isinstance(True, int) is a design flaw in Python. It
    results in `[True, 3]` passing as list[int] which causes problems in one of
    the main use cases for this function: validating JSON using TypedDicts. The
    same is the case for strings being sequences.

    Generally speaking, both behaviors are surprising. If a string is a
    sequence, what is it a sequence of? The answer would have to be characters
    but Python doesn't have a dedicated type for that. So instead, Python
    represents the indivdual characters as strings which represents type
    recursion:

    >>> 'x'[0][0][0][0][0][0][0][0][0]
    'x'

    Many other languages consider characters to be unsigned integers. Python
    could have opted to do that, making `ord(s) == s[0]`.

    https://github.com/python/typing/issues/256

    >>> check_type(int, True)
    False

    >>> check_type(Sequence[str], ''), check_type(Sequence[int], '')
    (False, False)

    Note that we need to extend the deviation to superclasses of Sequence, at
    least those that are generic in the type of the members,
    otherwise the empty string would be considered `Iterable[T]` with T being
    any other type. This is because the empty string lacks any elements to be
    check for T. We may want to refine this behavior but that would require a
    static assignability check like typing.assert_type(Iterable[int], '') which
    only works in a type checker like mypy.

    >>> check_type(Iterable[str], ''), check_type(Iterable[int], '')
    (False, False)

    >>> from collections.abc import Sized, Collection

    >>> check_type(Collection[str], ''), check_type(Collection[int], '')
    (False, False)

    However, a string still passes for a non-generic baseclasses of Sequence:

    >>> check_type(Sized, '')
    True
    >>> check_type(typing.Sized, '')
    True

    >>> type X[T] = dict[int, set[T] | list[T|None]]
    >>> x = {1: {'a'}, 2: [None, 'b']}
    >>> check_type(X[str], x)
    True

    >>> check_type(X[int], x)
    False

    Assign a type variable to the value of another type variable …

    … of a different name:

    >>> class D[S, T](TypedDict):
    ...     x: X[S]
    ...     y: T
    >>> check_type(D[str, int], {'x': x, 'y': 42})
    True

    … of the same name:

    >>> class D[T, S](TypedDict):
    ...     x: X[T]
    ...     y: S
    >>> check_type(D[str, int], {'x': x, 'y': 42})
    True

    >>> class C(dict[str, int]): pass
    >>> check_type(C, C({'x': 1}))
    True
    >>> check_type(C, {'x': 1})
    False

    >>> class E(TypedDict):
    ...     x: int
    >>> check_type(E, {'x': 2})
    True
    >>> check_type(E, {'x': 2, 'y': 3})
    False

    >>> class F(TypedDict, total=False):
    ...     x: Required[int]
    ...     y: str
    >>> check_type(F, {'x': 22, 'y': '33'})
    True
    >>> check_type(F, {'x': 22})
    True
    >>> check_type(F, {'x': 22, 'z': 44})
    True

    >>> class G(TypedDict):
    ...     x: int
    ...     y: NotRequired[str]
    >>> check_type(G, {'x': 22, 'y': '33'})
    True
    >>> check_type(G, {'x': 22})
    True
    >>> check_type(G, {'x': 22, 'z': 44})
    False

    """
    return _check_type(type_expression, value, {})


def _check_type(t: TypeExpression | TypeVar,
                x: Any,
                tvs: dict[str, TypeExpression]
                ) -> bool:
    """
    :param t: the expected type of the value
    :param x: the value to check
    :param tvs: the values of any type variables ocurring in the first argument

    Furthermore `ot` is the origin type, `at` and `ats` are argument type(s)
    """
    if isinstance(t, TypeVar):
        return _check_type(tvs[t.__name__], x, tvs)
    elif isinstance(t, TypeAliasType):
        return _check_type(t.__value__, x, tvs)
    elif isinstance(t, _SpecialGenericAlias):
        ot = not_none(get_origin(t))
        return _check_type(ot, x, tvs)
    elif isinstance(t, (UnionType, _UnionGenericAlias)):
        ats = get_args(t)
        return any(_check_type(at, x, tvs) for at in ats)
    elif isinstance(t, (GenericAlias, _GenericAlias)):
        ot, ats = not_none(get_origin(t)), get_args(t)
        if ot in (ReadOnly, Required, NotRequired):
            return _check_type(one(ats), x, tvs)
        else:
            tps = getattr(ot, '__type_params__', ())
            if tps:
                tvs = tvs | {
                    tp.__name__: tvs[at.__name__] if isinstance(at, TypeVar) else at
                    for tp, at in zip(tps, ats, strict=True)
                }
                return _check_type(ot, x, tvs)
            elif _check_type(ot, x, tvs):
                if issubclass(ot, Mapping):
                    assert isinstance(x, Mapping)
                    kt, vt = ats
                    return all(
                        _check_type(kt, k, tvs) and _check_type(vt, v, tvs)
                        for k, v in x.items()
                    )
                elif issubclass(ot, Iterable):
                    assert isinstance(x, Iterable)
                    it = one(ats)
                    return all(_check_type(it, i, tvs) for i in x)
                else:
                    assert False, ('Unsupported generic type', ot)
            else:
                return False
    elif isinstance(t, _TypedDictMeta):
        if isinstance(x, dict):
            for k, vt in t.__annotations__.items():
                try:
                    v = x[k]
                except KeyError:
                    if k in t.__required_keys__:
                        return False
                else:
                    if not _check_type(vt, v, tvs):
                        return False
            if t.__total__ and x.keys() - t.__annotations__.keys():
                return False
            return True
        else:
            return False
    elif isinstance(t, type):
        if isinstance(x, str) and issubclass(t, Iterable) and t is not str:
            # See doctests
            return False
        elif t is int and isinstance(x, bool):
            # See doctests
            return False
        else:
            return isinstance(x, t)
    else:
        assert False, ('Unsupported type', t)


def is_of_type[T](value: Any, typ: type[T]) -> TypeIs[T]:
    return check_type(typ, value)
