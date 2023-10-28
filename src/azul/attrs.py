from typing import (
    Optional,
    Tuple,
    Union,
)
from uuid import (
    UUID,
)

import attrs

from azul import (
    require,
)
from azul.types import (
    reify,
)


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

    >>> from azul.types import AnyJSON
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
    TypeError: ('y', set(), (<class 'collections.abc.Sequence'>,
    <class 'collections.abc.Mapping'>, <class 'str'>, <class 'int'>,
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
    azul.RequirementError: ('Validator cannot be shared among fields',
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
    azul.RequirementError: ('Validator cannot be shared among fields', ...

    """
    return _AsAnnotated()


class _AsAnnotated:
    _cache: Optional[Tuple[attrs.Attribute, Union[type, Tuple[type]]]] = None

    def __call__(self, _instance, field, value):
        reified_type = self._reify(field)
        if not isinstance(value, reified_type):
            raise TypeError(field.name, value, reified_type)

    def _reify(self, field):
        # reify() isn't exactly cheap so we'll cache its result
        if self._cache is None:
            reified_type = reify(field.type)
            self._cache = field, reified_type
        else:
            cached_field, reified_type = self._cache
            require(cached_field == field,
                    'Validator cannot be shared among fields', cached_field, field)
        return reified_type

    def __repr__(self):
        return 'as_annotated()'


def is_uuid(version):
    def validator(_instance, field, value):
        if not isinstance(value, UUID) or value.version != version:
            raise TypeError(f'Not a UUID{version}', field.name, value)

    return validator
