from abc import (
    ABCMeta,
    abstractmethod,
)
from itertools import (
    count,
)
import logging
from types import (
    UnionType,
)
from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
    Self,
    TypeAliasType,
    TypeVar,
    TypedDict,
    Union,
    final,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from attr import (
    AttrsInstance,
)
import attrs
from more_itertools import (
    flatten,
    one,
)

from azul import (
    config,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.functions import (
    compose,
)
from azul.lib.json import (
    PolymorphicSerializable,
    Serializable,
)
from azul.lib.types import (
    AnyJSON,
    CompositeJSON,
    JSON,
    JSONArray,
    MutableCompositeJSON,
    MutableJSON,
    MutableJSONArray,
    PrimitiveJSON,
    derived_type_params,
    json_mapping,
    json_str,
    not_none,
    reify,
)

log = logging.getLogger(__name__)


def strict_auto(*args, **kwargs):
    """
    A field that uses the annotated type for validation.

    See :func:`as_annotated` for details
    """
    return attrs.field(*args, validator=as_annotated(), **kwargs)


def as_annotated():
    """
    Returns a validator that verifies that a field's value is of the annotated
    type. Has some limited magic for parameterized types such as typing.Union
    and typing.Optional.

    >>> from azul.lib.types import AnyJSON
    >>> @attrs.define
    ... class Foo:
    ...     x: Optional[bool] = strict_auto()
    ...     y: AnyJSON = strict_auto()

    >>> Foo(x=None, y={}), Foo(x=True, y=[]), Foo(x=False, y='foo')
    (Foo(x=None, y={}), Foo(x=True, y=[]), Foo(x=False, y='foo'))

    >>> # noinspection PyTypeChecker
    >>> Foo(x='foo', y={})
    Traceback (most recent call last):
    ...
    TypeError: ('x', 'foo', (<class 'bool'>, <class 'NoneType'>))

    >>> # noinspection PyTypeChecker
    >>> Foo(x=None, y=set())
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    TypeError: ('y', set(), (<class 'collections.abc.Mapping'>,
    <class 'collections.abc.Sequence'>, <class 'str'>, <class 'int'>,
    <class 'float'>, <class 'bool'>, <class 'NoneType'>))

    Note that you cannot share one return value of this function between more
    than one field.

    >>> validator = as_annotated()
    >>> @attrs.define
    ... class Bar:
    ...     x: int = attrs.field(validator=validator)
    ...     y: str = attrs.field(validator=validator)
    >>> Bar(x=1, y='')
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    AssertionError: R('Validator cannot be shared among fields',
    Attribute(name='x', default=NOTHING, validator=as_annotated(), repr=True,
    eq=True, eq_key=None, order=True, order_key=None, hash=None, init=True,
    metadata=mappingproxy({}), type=<class 'int'>, converter=None,
    kw_only=False, inherited=False, on_setattr=None, alias='x'),
    Attribute(name='y', default=NOTHING, validator=as_annotated(), repr=True,
    eq=True, eq_key=None, order=True, order_key=None, hash=None, init=True,
    metadata=mappingproxy({}), type=<class 'str'>, converter=None,
    kw_only=False, inherited=False, on_setattr=None, alias='y'))

    Unfortunately, this sharing violation is currently detected very late,
    during the first instantiation of a class that reuses a validator.

    >>> validator = as_annotated()
    >>> @attrs.define
    ... class Bar:
    ...     x: int = attrs.field(validator=validator)
    >>> @attrs.define
    ... class Foo:
    ...     y: str = attrs.field(validator=validator)
    >>> Bar(x=1)
    Bar(x=1)
    >>> Foo(y='')
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    AssertionError: R('Validator cannot be shared among fields', ...

    """
    return _AsAnnotated()


class _AsAnnotated:
    _cache: Optional[tuple[attrs.Attribute, Union[type, tuple[type]]]] = None

    def __call__(self, _instance, field, value):
        reified_type = self._reify(field)
        if not isinstance(value, reified_type):
            raise TypeError(field.name, value, reified_type)

    def _reify(self, field):
        # reify() isn't exactly cheap so we'll cache its result
        if self._cache is None:
            reified_types = reify(field.type)
            self._cache = field, reified_types
        else:
            cached_field, reified_types = self._cache
            assert cached_field == field, R(
                'Validator cannot be shared among fields', cached_field, field)
        return reified_types

    def __repr__(self):
        return 'as_annotated()'


type Source = list[str | tuple[str, ...] | Source]

type FromJSON = Callable[[AnyJSON], Any]
type ToJSON = Callable[[Any], AnyJSON]


class SerializableAttrs(Serializable, attrs.AttrsInstance):
    """
    >>> @attrs.frozen(kw_only=True)
    ... class InnerBase(SerializableAttrs):
    ...     x: int

    >>> @attrs.frozen(kw_only=True)
    ... class MiddleInner[T](InnerBase):
    ...     y: T | None

    >>> @attrs.frozen(kw_only=True)
    ... class Inner(MiddleInner[str]): ...

    >>> @attrs.frozen(kw_only=True)
    ... class OuterBase[X, T: InnerBase](SerializableAttrs):
    ...     inner: list[T] | None

    >>> class MiddleOuter[X](OuterBase[X, Inner]): ...

    >>> class Outer(MiddleOuter[float]): ...

    >>> outer = Outer(inner=[Inner(x=1, y='b')])
    >>> outer.to_json()
    {'inner': [{'x': 1, 'y': 'b'}]}

    >>> Outer.from_json(outer.to_json())
    Outer(inner=[Inner(x=1, y='b')])

    >>> Outer(inner=None).to_json()
    {'inner': None}

    >>> Outer.from_json({'inner': None})
    Outer(inner=None)

    >>> Outer.from_json({'inner': [{'x': 'bad', 'y': 'b'}]})
    Traceback (most recent call last):
    ...
    ValueError: ('Invalid type of value', <class 'str'>, 'expecting', <class 'int'>)

    >>> Outer.from_json({'inner': [{'x': 1, 'y': None}]})
    Outer(inner=[Inner(x=1, y=None)])

    A class with custom serialization (float serialized as string):

    >>> @attrs.frozen(kw_only=True)
    ... class CustomBase(SerializableAttrs):
    ...     x: float
    ...
    ...     def to_json(self) -> JSON:
    ...         return super().to_json() | {'x': str(self.x)}
    ...
    ...     @classmethod
    ...     def _from_json(cls, json: JSON) -> dict[str, Any]:
    ...         return dict(super()._from_json(json), x=float(json['x']))

    >>> @attrs.frozen(kw_only=True)
    ... class Custom(CustomBase):
    ...     y: str

    >>> Custom(x=1.23, y='y').to_json()
    {'x': '1.23', 'y': 'y'}

    >>> Custom.from_json({'x': '1.23', 'y': 'y'})
    Custom(x=1.23, y='y')

    >>> @attrs.frozen(kw_only=True)
    ... class Embedded(SerializableAttrs):
    ...     x: JSON

    >>> Embedded(x={'y': 12}).to_json()
    {'x': {'y': 12}}

    >>> @attrs.frozen(kw_only=True)
    ... class WithDicts(SerializableAttrs):
    ...     inners: dict[int, Inner]

    >>> WithDicts(inners={1: Inner(x=1, y='b')}).to_json()
    {'inners': {1: {'x': 1, 'y': 'b'}}}

    >>> WithDicts.from_json({'inners': {1: {'x': 1, 'y': 'b'}}})
    WithDicts(inners={1: Inner(x=1, y='b')})
    """

    @classmethod
    @final
    def from_json(cls, json: AnyJSON) -> Self:
        cls._assert_concrete()
        kwargs = cls._from_json(json_mapping(json))
        return cls(**kwargs)

    @classmethod
    def _from_json(cls, json: JSON) -> dict[str, Any]:
        """
        Return a dictionary with keyword arguments for the constructor. An
        override must call the overridden method via super() but only need to
        populate keyword arguments for the fields defined by the class that
        overrides the method. Typically, the overrides in subclasses will be
        generated automatically but if a subclass explicitly defines an
        override, it will be left alone.
        """
        return {}

    def _to_json(self) -> dict[str, AnyJSON]:
        """
        Typically, the overrides in subclasses will be generated automatically
        but if a subclass explicitly defines an override, it will be left alone.
        """
        return {}

    def to_json(self) -> dict[str, AnyJSON]:
        self._assert_concrete()
        return self._to_json()

    @classmethod
    def _assert_concrete(cls):
        assert not cls._deferred_fields, R(
            'Class has fields of unknown type', cls._deferred_fields)

    def __init_subclass__(cls):
        super().__init_subclass__()
        try:
            fields = attrs.fields(cls)
        except attrs.exceptions.NotAnAttrsClassError:
            pass
        else:
            cls._instrument(fields)

    @classmethod
    def __attrs_init_subclass__(cls):
        cls._instrument(attrs.fields(cls))

    #: The names of fields that we weren't able to generate code for in this
    #: class because at least one of them was annotated with a variable type.
    #: Generic descendants that use free type variables in their attrs field
    #: annotations override this attribute to a non-empty set. The
    #: responsibility to handle deferred fields falls on the descendant that
    #: binds the last remaining free type variable.
    #:
    _deferred_fields: frozenset[str] = frozenset()

    @classmethod
    def _instrument(cls, fields: list[attrs.Attribute]):
        """
        Add overrides for to_json and _from_json to the given class. The
        overrides will handle the serialization and deserialization of the
        fields defined by the class, not those that it inherits. An override
        will only be added if the class doesn't already provide one. This method
        must be idempotent because it may be invoked twice for the same class,
        before and after the attrs decorator did its work. Even for slotted
        classes this method will be invoked twice, albeit the second time on a
        copy of the class.
        """
        # When slots=True (the default for attrs.define), attrs makes a copy of
        # the class so the subclass hook will be invoked twice, once for the
        # original class, and again for the copy. The copy is likely to have
        # additional fields defined so we need to start from scratch and reset
        # any left-overs that would interfere with that.
        #
        if cls._has_custom('_to_json') and cls._has_custom('_from_json'):
            pass
        else:
            if '_deferred_fields' in cls.__dict__:
                del cls._deferred_fields
            owned_fields = [
                field
                for field in fields
                if field.name in cls.__annotations__ or field.name in cls._deferred_fields
            ]
            if owned_fields:
                deferred_fields = cls._make(owned_fields)
                if deferred_fields != cls._deferred_fields:
                    cls._deferred_fields = deferred_fields

    @classmethod
    def _make(cls, fields: list[attrs.Attribute]) -> frozenset[str]:
        try:
            _from_json = cls._make_from_json(fields)
        except cls.Strategy.MustDefer:
            deferred_fields = frozenset(field.name for field in fields)
        else:
            cls._define(_from_json)
            deferred_fields = frozenset()
            to_json = cls._make_to_json(fields)
            cls._define(to_json)
        return deferred_fields

    @classmethod
    def _make_from_json(cls, fields: list[attrs.Attribute]) -> Callable:
        globals = {cls.__name__: cls}
        deserializers = (cls.Deserializer(cls, field, globals) for field in fields)
        source = cls._indent([
            '@classmethod',
            'def _from_json(cls, json):', [
                f'kwargs = super({cls.__name__}, cls)._from_json(json)',
                *flatten(
                    [
                        f'x = json["{deserializer.field.name}"]',
                        *(deserializer.handle('x')),
                        f'kwargs["{deserializer.field.name}"] = x'
                    ]
                    for deserializer in deserializers
                    if deserializer.enabled
                ),
                'return kwargs'
            ]
        ])
        return cls._compile(source, globals)

    @classmethod
    def _make_to_json(cls, fields: list[attrs.Attribute]) -> Callable:
        globals = {cls.__name__: cls}
        serializers = (cls.Serializer(cls, field, globals) for field in fields)
        to_json = cls._indent([
            'def _to_json(self):', [
                # Using the super() shortcut would require messing with the
                # ``__closure__`` attribute of the function, and, we assume,
                # would be slower.
                f'json = super({cls.__name__}, self)._to_json()',
                *flatten(
                    [
                        f'x = self.{serializer.field.name}',
                        f'json["{serializer.field.name}"] = ' + serializer.handle('x')
                    ]
                    for serializer in serializers
                    if serializer.enabled
                ),
                'return json'
            ]
        ])
        return cls._compile(to_json, globals)

    @classmethod
    def _indent(cls, source: Source, level=0):
        """
        Indent and join the given list of source code items. An item can be
        either a line, a tuple of words, or a nested list of items. The
        indentation of lines is based on the nesting of the lists. Lines are
        joined with a newline character, words are joined with a comma.
        """
        return '\n'.join(
            cls._indent(v, level + 1)
            if isinstance(v, list) else
            ' ' * level * 4 + (', '.join(v) if isinstance(v, tuple) else v)
            for v in source
        )

    @classmethod
    def _compile(cls, source: str, globals: dict[str, Any]):
        """
        Compile a function definition from the given source & context
        """
        if config.debug > 2:
            log.debug('Generating code for method in %r with globals %r. '
                      'See next line for body of method.\n%s', cls, globals, source)
        bytecode = compile(source, cls.__module__, 'exec')
        locals: dict[str, Any] = {}
        eval(bytecode, globals, locals)
        function = one(locals.values())
        return function

    _method_marker = '__azul_serializable__'

    @classmethod
    def _has_custom(cls, method_name):
        method = cls.__dict__.get(method_name)
        return method is not None and not hasattr(method, cls._method_marker)

    @classmethod
    def _define(cls, function: Callable) -> None:
        """
        Add the given function as a method of the class to be instrumented
        """
        method_name = function.__name__
        custom = cls._has_custom(method_name)
        # We should never replace a custom definition. However, an
        # instrumentation during attrs' subclass hook must replace
        # the definition from the standard subclass hook.
        if not custom:
            setattr(function, cls._method_marker, None)
            setattr(cls, method_name, function)

    @attrs.frozen
    class Strategy[T](metaclass=ABCMeta):
        cls: type[SerializableAttrs]
        field: attrs.Attribute
        globals: dict[str, Any]
        depth: Iterator[int] = attrs.field(factory=count)

        class MustDefer(Exception):
            pass

        class Custom(TypedDict):
            from_json: FromJSON | None
            to_json: ToJSON | None

        @cached_property
        def custom(self) -> Custom | None:
            return self._metadata('custom', None)

        def _metadata[V](self, key: str, default: V) -> V:
            try:
                return self.field.metadata['azul'][key]
            except KeyError:
                return default

        @cached_property
        def discriminator(self) -> str | None:
            return self._metadata('discriminator', None)

        def handle(self, x: str) -> T:
            if self.custom is None:
                return self._handle(x, self._reify(self.field.type))
            else:
                return self._custom(x)

        def _owner(self) -> type:
            """
            Find the nearest ancestor that introduced the given field
            """
            for base in self.cls.__mro__:
                if self.field.name in base.__annotations__:
                    assert isinstance(base, type)
                    assert issubclass(base, SerializableAttrs)
                    return base
            assert False

        def _reify(self, field_type: Any) -> Any:
            """
            Resolve the type parameters of the given type, or raise
            MustDefer if that's not possible.
            """
            while isinstance(field_type, TypeVar):
                owner = self._owner()
                if owner is self.cls:
                    raise self.MustDefer
                params = derived_type_params(self.cls, root=owner)
                try:
                    field_type = params[field_type]
                except KeyError:
                    raise self.MustDefer
            return field_type

        embedded_json_types = (
            JSON,
            CompositeJSON,
            JSONArray,
            MutableJSON,
            MutableCompositeJSON,
            MutableJSONArray
        )

        def _handle(self, x: str, field_type: Any):
            if field_type in self.embedded_json_types:
                return self._embedded_json(x, one(reify(field_type)))
            elif isinstance(field_type, TypeAliasType):
                field_type = field_type.__value__
            if isinstance(field_type, type):
                if field_type in reify(PrimitiveJSON):
                    return self._primitive(x, field_type)
                elif issubclass(field_type, Serializable):
                    cls_name = field_type.__name__
                    self.globals[cls_name] = field_type
                    is_polymorphic = issubclass(field_type, PolymorphicSerializable)
                    has_discriminator = self.discriminator is not None
                    if is_polymorphic and has_discriminator:
                        return self._polymorphic(x, cls_name)
                    else:
                        return self._serializable(x, cls_name)
            else:
                origin = get_origin(field_type)
                if origin in (Union, UnionType):
                    arg_types = set(get_args(field_type))
                    arg_types.discard(type(None))
                    if len(arg_types) == 1:
                        field_type = self._reify(one(arg_types))
                        return self._optional(x, field_type)
                elif issubclass(origin, list):
                    item_type = one(get_args(field_type))
                    item_type = self._reify(item_type)
                    return self._list(x, item_type)
                elif issubclass(origin, dict):
                    key_type, value_type = map(self._reify, get_args(field_type))
                    return self._dict(x, key_type, value_type)
            raise TypeError('Unserializable field', field_type, self.field)

        @property
        @abstractmethod
        def enabled(self) -> bool:
            raise NotImplementedError

        @abstractmethod
        def _primitive(self, x: str, field_type: type) -> T:
            raise NotImplementedError

        @abstractmethod
        def _embedded_json(self, x: str, field_type: type) -> T:
            raise NotImplementedError

        @abstractmethod
        def _optional(self, x: str, field_type: type) -> T:
            raise NotImplementedError

        @abstractmethod
        def _serializable(self, x: str, cls: str) -> T:
            raise NotImplementedError

        @abstractmethod
        def _polymorphic(self, x: str, base_cls: str) -> T:
            raise NotImplementedError

        @abstractmethod
        def _list(self, x: str, item_type: type) -> T:
            raise NotImplementedError

        @abstractmethod
        def _dict(self, x: str, key_type: type, value_type: type) -> T:
            raise NotImplementedError

        @abstractmethod
        def _custom(self, x: str) -> T:
            raise NotImplementedError

    class Deserializer(Strategy[Source]):

        @property
        def enabled(self) -> bool:
            return self.custom is None or self.custom['from_json'] is not None

        def _optional(self, x: str, field_type: type) -> Source:
            return [
                f'if {x} is not None:', self._handle(x, field_type)
            ]

        def _serializable(self, x: str, cls: str) -> Source:
            return [
                f'{x} = {cls}.from_json({x})'
            ]

        def _polymorphic(self, x: str, base_cls: str) -> Source:
            depth = next(self.depth)
            cls = f'cls{depth}'
            return [
                f'{cls} = {x}["{self.discriminator}"]',
                f'{cls} = {base_cls}.cls_from_json({cls})',
                f'{x} = {cls}.from_json({x})'
            ]

        def _primitive(self, x: str, field_type: type) -> Source:
            return [
                f'if not isinstance({x}, {field_type.__name__}):', [
                    'raise ValueError(', [(
                        '"Invalid type of value"',
                        f'type({x})',
                        '"expecting"',
                        field_type.__name__,
                    )], ')'
                ]
            ]

        def _embedded_json(self, x: str, field_type: type) -> Source:
            self.globals[field_type.__name__] = field_type
            return self._primitive(x, field_type)

        def _list(self, x: str, item_type: type) -> Source:
            depth = next(self.depth)
            l, v = f'l{depth}', f'v{depth}'
            return [
                f'{l} = []',
                f'for {v} in {x}:', [
                    *self._handle(v, item_type),
                    f'{l}.append({v})'
                ],
                f'{x} = {l}'
            ]

        def _dict(self, x: str, key_type: type, value_type: type) -> Source:
            level = next(self.depth)
            d, k, v = f'd{level}', f'k{level}', f'v{level}'
            return [
                f'{d} = {{}}',
                f'for {k},{v} in {x}.items():', [
                    *self._handle(k, key_type),
                    *self._handle(v, value_type),
                    f'{d}[{k}] = {v}'
                ],
                f'{x} = {d}'
            ]

        def _custom(self, x: str) -> Source:
            var_name = self.field.name + '_from_json'
            from_json = not_none(not_none(self.custom)['from_json'])
            self.globals[var_name] = from_json
            return [
                f'{x} = {var_name}({x})'
            ]

    @attrs.frozen(slots=False)
    class Serializer(Strategy[str]):

        def __attrs_post_init__(self):
            def _assert_not_in(key: str, json: dict) -> dict:
                assert key not in json, (key, json)
                return json

            self.globals['_assert_not_in'] = _assert_not_in

        @property
        def enabled(self) -> bool:
            return self.custom is None or self.custom['to_json'] is not None

        def _primitive(self, x: str, field_type: type) -> str:
            return x

        def _embedded_json(self, x: str, field_type: type) -> str:
            return x

        def _optional(self, x: str, field_type: type) -> str:
            return f'{x} if {x} is None else ({self._handle(x, field_type)})'

        def _serializable(self, x: str, cls: str) -> str:
            return f'{x}.to_json()'

        def _polymorphic(self, x: str, base_cls: str) -> str:
            d = self.discriminator
            return f'dict({d}={x}.cls_to_json(), **_assert_not_in("{d}", {x}.to_json()))'

        def _list(self, x: str, item_type: type) -> str:
            depth = next(self.depth)
            v = f'v{depth}'
            v_ = self._handle(v, item_type)
            return f'[({v_}) for {v} in {x}]'

        def _dict(self, x: str, key_type: type, value_type: type) -> str:
            level = next(self.depth)
            k, v = f'k{level}', f'v{level}'
            k_, v_ = self._handle(k, key_type), self._handle(v, value_type)
            return f'{{{k_}: {v_} for {k}, {v} in x.items()}}'

        def _custom(self, x: str) -> str:
            to_json = not_none(not_none(self.custom)['to_json'])
            var_name = self.field.name + '_to_json'
            self.globals[var_name] = to_json
            return f'{var_name}({x})'


class DiscriminatingPolymorphicSerializableAttrs(
    SerializableAttrs,
    PolymorphicSerializable,
    metaclass=ABCMeta
):
    """
    This class provides an alternative serialization format that includes the
    discriminator field, facilitating the polymorphic deserialization of
    subclasses even when they are not being used as polymorphic fields of a
    larger serializable object. Each subclass is responsible for ensuring that
    the name of the discriminator property does not conflict with the names of
    any instance attributes.
    """

    @classmethod
    @abstractmethod
    def discriminator(cls) -> str:
        """
        The name of a property in serialized instances that contains the name
        of the type said instances will be deserialized as.
        """
        raise NotImplementedError

    # The @final decorator in SerializableAttrs is intended to only apply to
    # clients of this module, not to parts of the module.

    @classmethod  # type: ignore[misc]
    def from_json(cls, json: AnyJSON) -> Self:
        json = json_mapping(json)
        try:
            discriminator = json[cls.discriminator()]
        except KeyError:
            return super().from_json(json)
        else:
            subcls = cls.cls_from_json(discriminator)
            kwargs = subcls._from_json(json)
            return subcls(**kwargs)

    def to_json(self) -> dict[str, AnyJSON]:
        json = self._to_json()
        discriminator = self.discriminator()
        assert discriminator not in json, (discriminator, json)
        return {
            discriminator: self.cls_to_json(),
            **json
        }


def serializable[T](field: T | None = None,
                    *,
                    from_json: FromJSON,
                    to_json: ToJSON) -> T:
    """
    Use the provided callables to (de)serialize values of the given field,
    instead of generating them.

    >>> @attrs.frozen
    ... class Foo(SerializableAttrs):
    ...     x: set[str] = serializable(to_json=sorted, from_json=set)

    >>> Foo(x={'b','a'}).to_json()
    {'x': ['a', 'b']}

    >>> Foo.from_json({'x': ['a']})
    Foo(x={'a'})
    """
    custom = SerializableAttrs.Strategy.Custom(from_json=from_json,
                                               to_json=to_json)
    return _set_field_metadata(field, 'custom', custom)


def not_serializable[T](field: T) -> T:
    """
    Skip the given field during (de)serialization. The field should have a
    default value or there should be some other provision for the constructor to
    handle the case that no argument will be passed to it for any field that was
    marked this way.

    >>> @attrs.frozen
    ... class Foo(SerializableAttrs):
    ...     x: int = not_serializable(attrs.field(default=42))

    >>> Foo().to_json()
    {}

    >>> Foo.from_json({})
    Foo(x=42)
    """
    custom = SerializableAttrs.Strategy.Custom(from_json=None,
                                               to_json=None)
    return _set_field_metadata(field, 'custom', custom)


def _set_field_metadata[T](field: T | None, key, value):
    if field is None:
        field = attrs.field()
    # The actual return value of `attrs.field` is of a type that's internal to
    # attrs and unrelated to any types in the public API. The declared return
    # type is the same as the field's type, to facilitate type checking. Hence,
    # there's no satisfactory type bound to declare for `T` or type to assert
    # here.
    assert hasattr(field, 'metadata'), field
    metadata = field.metadata.setdefault('azul', {})
    metadata[key] = value
    return field


def polymorphic[T](field: T | None = None,
                   *,
                   discriminator: str
                   ) -> T:
    """
    Mark an attrs field to use the given name for the discriminator property in
    serialized instances of PolymorphicSerializable that occur in the value of
    that field. The given discriminator property of a serialized instance
    represents the type to use when deserializing that instance again.

    >>> from azul.lib.json import StaticRegisteredPolymorphicSerializable

    >>> class Inner(SerializableAttrs, StaticRegisteredPolymorphicSerializable):
    ...     pass

    >>> @attrs.frozen
    ... class InnerWithInt(Inner):
    ...     x: int

    >>> @attrs.frozen
    ... class InnerWithStr(Inner):
    ...     y: str

    >>> @attrs.frozen(kw_only=True)
    ... class Outer(SerializableAttrs):
    ...     inner: Inner = polymorphic(discriminator='type')
    ...     inners: list[Inner] = polymorphic(discriminator='_cls')

    >>> from azul.lib.doctests import assert_json

    >>> outer = Outer(inner=InnerWithInt(42),
    ...               inners=[InnerWithStr('foo'), InnerWithInt(7)])
    >>> assert_json(outer.to_json())
    {
        "inner": {
            "type": "InnerWithInt",
            "x": 42
        },
        "inners": [
            {
                "_cls": "InnerWithStr",
                "y": "foo"
            },
            {
                "_cls": "InnerWithInt",
                "x": 7
            }
        ]
    }
    >>> Outer.from_json(outer.to_json()) == outer
    True

    If a subclass has a field whose name matches the discriminator,
    serialization of the enclosing object will fail:

    >>> @attrs.frozen
    ... class InnerWithType(Inner):
    ...     type: str

    >>> Outer(inner=InnerWithType(type='foo'), inners=[]).to_json()
    Traceback (most recent call last):
        ...
    AssertionError: ('type', {'type': 'foo'})

    In order to enable polymorphic serialization of the value of a given field,
    the discriminator property needs to be specified explicitly, otherwise the
    serialization framework will resort to the static type of the field.

    >>> @attrs.frozen
    ... class GenericOuter[T: Inner](SerializableAttrs):
    ...     inner: T

    >>> class StaticOuter(GenericOuter[InnerWithInt]):
    ...     pass

    >>> outer = StaticOuter(InnerWithInt(42))
    >>> outer.to_json()
    {'inner': {'x': 42}}

    Despite the fact that ``{'x': 42}`` does not encode any type information,
    ``from_json`` can tell from the static type of the field that {'x': 42}
    should be deserialized as an ``InnerWithInt``.

    >>> StaticOuter.from_json(outer.to_json()).inner
    InnerWithInt(x=42)

    >>> StaticOuter.from_json(outer.to_json()) == outer
    True

    However, when the static type of the field is not concrete, deserialization
    may fail or, like in this case, lose information by creating an instance of
    the parent class instead of the class that was serialized.

    >>> @attrs.frozen
    ... class AbstractOuter(SerializableAttrs):
    ...     inner: Inner

    >>> outer = AbstractOuter(InnerWithInt(42))
    >>> AbstractOuter.from_json(outer.to_json()).inner  # doctest: +ELLIPSIS
    <azul.lib.attrs.Inner object at ...>
    """
    return _set_field_metadata(field, 'discriminator', discriminator)


def devolve[T: AttrsInstance](cls: type[T],
                              inst: AttrsInstance,
                              **changes: Any
                              ) -> T:
    """
    Like attrs.evolve but for an arbitrary class. The most common use case is
    to evolve an instance of a class to an instance of a subclass of that class,
    in which case the keyword arguments must supply values for all the fields
    that the subclass adds. When evolving to an instance of a superclass, any
    instance fields not defined in the superclass will be discarded.

    >>> @attrs.frozen()
    ... class Foo:
    ...     x: int
    ...     _y: str

    >>> @attrs.frozen()
    ... class Bar(Foo):
    ...     z: str
    ...     c: float = attrs.field(default=3.14159, init=False)

    >>> devolve(Bar, Foo(x=42, y='foo'), z='bar')
    Bar(x=42, _y='foo', z='bar', c=3.14159)

    >>> devolve(Foo, Bar(x=42, y='foo', z='bar'), x=24)
    Foo(x=24, _y='foo')

    >>> devolve(Bar, Foo(x=42, y='foo'))
    Traceback (most recent call last):
    ....
    AttributeError: 'Foo' object has no attribute 'z'

    >>> devolve(Bar, Foo(x=42, y='foo'), z='bar', c=2.71828)
    Traceback (most recent call last):
    ...
    TypeError: Bar.__init__() got an unexpected keyword argument 'c'
    """
    fields = attrs.fields(cls)
    for field in fields:
        if field.init:
            init_name = field.alias
            if init_name not in changes:
                changes[init_name] = getattr(inst, field.name)
    return cls(**changes)


def is_uuid(version):
    def validator(_instance, field, value):
        if not isinstance(value, UUID) or value.version != version:
            raise TypeError(f'Not a UUID{version}', field.name, value)

    return validator


def serializable_uuid[T: UUID](field: T | None = None) -> T:
    return serializable(field,
                        from_json=compose(UUID, json_str),
                        to_json=str)
