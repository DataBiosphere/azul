from typing import (
    Callable,
    Tuple,
    Type,
    TypeVar,
    Union,
)

E = TypeVar('E', bound=BaseException)
R = TypeVar('R', bound=BaseException)


def catch(f: Callable[..., R],
          *args,
          exception_cls_to_catch: Type[E] = Exception,
          **kwargs
          ) -> Union[Tuple[None, R], Tuple[E, None]]:
    """
    Invoke the given callable. If the callable raises an instance of the
    specified exception class, return that exception, otherwise return the
    result of the callable.

    :param f: the callable to invoke

    :param args: positional arguments to the callable

    :param exception_cls_to_catch: class of exceptions to catch. The name is
           intentionally long in order to avoid collisions with the keyword
           arguments to the callable.

    :param kwargs: keyword arguments to the callable

    :return: Either a tuple of None and the return value of the callable or
             a tuple of the exception raised by the callable and None.

    >>> catch(int, '42')
    (None, 42)

    >>> catch(int, '42', base=16)
    (None, 66)

    >>> catch(int, '')
    (ValueError("invalid literal for int() with base 10: ''"), None)

    >>> catch(int, '', exception_cls_to_catch=ValueError)
    (ValueError("invalid literal for int() with base 10: ''"), None)

    >>> catch(int, '', exception_cls_to_catch=BaseException)
    (ValueError("invalid literal for int() with base 10: ''"), None)

    >>> catch(int, '', exception_cls_to_catch=NotImplementedError)
    Traceback (most recent call last):
    ...
    ValueError: invalid literal for int() with base 10: ''

    >>> catch(int, '', base=16)
    (ValueError("invalid literal for int() with base 16: ''"), None)

    >>> catch(int, '', base=16, exception_cls_to_catch=ValueError)
    (ValueError("invalid literal for int() with base 16: ''"), None)
    """
    try:
        return None, f(*args, **kwargs)
    except exception_cls_to_catch as e:
        return e, None
