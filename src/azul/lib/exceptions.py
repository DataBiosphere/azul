from typing import (
    Callable,
    Type,
)


def catch[E:BaseException, R, **P](f: Callable[P, R],
                                   exception_cls: Type[E],
                                   /,
                                   *args: P.args,
                                   **kwargs: P.kwargs
                                   ) -> tuple[E, None] | tuple[None, R]:
    """
    Invoke the given callable. If the callable raises an instance of the
    specified exception class, return that exception, otherwise return the
    result of the callable.

    :param f: The callable to invoke

    :param exception_cls: The class of exceptions to catch

    :param args: Positional arguments to the callable

    :param kwargs: Keyword arguments to the callable

    :return: Either a tuple of None and the return value of the callable or a
             tuple of the exception raised by the callable and None

    >>> catch(int, Exception, '42')
    (None, 42)

    >>> catch(int, Exception, '42', base=16)
    (None, 66)

    >>> catch(int, ValueError, '')
    (ValueError("invalid literal for int() with base 10: ''"), None)

    >>> catch(int, BaseException, '')
    (ValueError("invalid literal for int() with base 10: ''"), None)

    >>> catch(int, NotImplementedError, '')
    Traceback (most recent call last):
    ...
    ValueError: invalid literal for int() with base 10: ''

    >>> catch(int, ValueError, '', base=16)
    (ValueError("invalid literal for int() with base 16: ''"), None)

    >>> catch(int, ValueError, '', base=16)
    (ValueError("invalid literal for int() with base 16: ''"), None)
    """
    try:
        return None, f(*args, **kwargs)
    except exception_cls as e:
        return e, None
