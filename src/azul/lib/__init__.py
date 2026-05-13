import functools
from typing import (
    TYPE_CHECKING,
    final,
)

import furl
from more_itertools import (
    one,
)

from azul.lib.caching import (
    CachedProperty,
    lru_cache_per_thread,
)

cached_property = CachedProperty

lru_cache = functools.lru_cache

cache = functools.cache


def cache_per_thread(f, /):
    return lru_cache_per_thread(maxsize=None)(f)


if TYPE_CHECKING:
    mutable_furl = furl._mutable_furl
else:
    mutable_furl = furl.furl


class R:
    """
    R is short for Requirement. We think this abbreviation is justified by how
    frequently this class is used.

    Use an instance of this class as the second argument to `assert` in order to
    express that the assertion fired due to an invalid input to a component of
    the program, rather than a defect *in* the program component itself. A
    program component can be a function, class or module. Individual methods
    typically aren't components. A regular assertion firing constitutes a defect
    inside the component, an unsatisfied requirement constitutes a defect
    outside of it.

    >>> foo = 1
    >>> assert foo > 42, R('Invalid foo', foo)
    Traceback (most recent call last):
    ...
    AssertionError: R('Invalid foo', 1)

    There are two advantages to using `assert` to enforce requirements: One
    advantage is that the second argument to assert is evaluated lazily, thereby
    avoiding potentially expensive operations in case the assert does not fire.

    >>> foo = 43
    >>> assert foo > 42, R('Invalid foo', (foo:=0))
    >>> foo
    43

    The second advantage is that `assert` can help type checkers to infer a more
    narrow type:

    >>> strict = True
    >>> def f(x:int | None) -> bytes:
    ...     if strict:
    ...         assert x is not None, R('x may not be None')
    ...         return x.to_bytes()
    """

    @classmethod
    def caused(cls, e: AssertionError) -> bool:
        """
        Use this method to check if the given exception was raised due to an
        unsatisfied requirement. Typical usage looks as follows:

        >>> try:
        ...     foo = 1
        ...     assert foo > 42, R('Invalid foo', foo)
        ... except AssertionError as e:
        ...     if R.caused(e):
        ...         pass  # handle the unsatisfied requirement
        ...     else:
        ...         raise  # some other type of assertion
        """
        return bool(e.args) and isinstance(e.args[0], cls)

    @classmethod
    def propagate[E:BaseException](cls,
                                   cause: AssertionError,
                                   effect_cls: type[E]
                                   ) -> E:
        """
        Propagate the arguments of an R instance that caused the given exception
        to a new exception of the given type.

        >>> try:
        ...     foo = 1
        ...     assert foo > 42, R('Invalid foo', foo)
        ... except AssertionError as e:
        ...     if R.caused(e):
        ...         raise R.propagate(e, ValueError)
        Traceback (most recent call last):
        ...
        ValueError: ('Invalid foo', 1)

        :param cause: an exception for which :meth:`caused` returns True

        :param effect_cls: the type of exception to propagate to

        :return: an instance of the given type, instantiated with the arguments
                 of the R instance that's the sole argument of the given
                 exception
        """
        args = one(cause.args).args
        return effect_cls(*args)

    def __init__(self, message: str, *args):
        super().__init__()
        self.args = message, *args

    def __repr__(self):
        class_name = type(self).__name__
        match self.args:
            case (message, ):
                return f'{class_name}({message!r})'
            case args:
                return class_name + repr(args)

    @final
    def __eq__(self, other: object):
        return isinstance(other, R) and self.args == other.args


def false() -> bool:
    """
    Use this to disable code while keeping it in scope for type checkers and
    refactorings, but without tripping static detection of "dead" code. The
    disablement is usually temporary (a work around) but may even be permanent,
    in order to, say, document a hypothetical.

    :return: Always ``False``

    >>> if false():
    ...     print('Entering the forbidden zone')
    """
    return False


def true() -> bool:
    """
    See :meth:`false`
    """
    return True
