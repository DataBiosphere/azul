from collections.abc import (
    Iterable,
    Iterator,
)
from functools import (
    partial,
)
from itertools import (
    dropwhile,
    islice,
)
from operator import (
    ge,
)
import random
from typing import (
    Callable,
    TypeVar,
    cast,
)

from more_itertools import (
    first,
)

from azul.lib import (
    R,
)

T = TypeVar('T')


# noinspection PyPep8Naming
class generable(Iterable[T]):
    """
    Convert a generator into a true iterable, i.e. an iterable that is not an
    iterator i.e., whose ``__iter__`` does not return ``self`` and that does not
    have ``__next__``.

    A generator function:

    >>> def f(n):
    ...     for i in range(n):
    ...         yield i

    It returns an iterator that can only be consumed once:

    >>> g = f(3)
    >>> list(g)
    [0, 1, 2]
    >>> list(g)
    []

    Wrapping the generator function with ``generable`` produces a true iterable
    that can be consumed multiple times:

    >>> g = generable(f, 3)
    >>> list(g)
    [0, 1, 2]
    >>> list(g)
    [0, 1, 2]
    """

    def __init__(self, generator: Callable[..., Iterator[T]], *args, **kwargs):
        self._generator = partial(generator, *args, **kwargs)

    def __iter__(self) -> Iterator[T]:
        return self._generator()


def reservoir_sample(k: int,
                     it: Iterable[T],
                     *,
                     # The cast is safe because the `random` module has a
                     # function for every method in the `random.Random` class.
                     random: random.Random = cast(random.Random, random)
                     ) -> list[T]:
    """
    Return a random choice of a given size from an iterable.

    https://stackoverflow.com/a/35671225/4171119

    >>> r = random.Random(42)

    >>> reservoir_sample(5, '', random=r)
    []

    >>> reservoir_sample(5, 'abcd', random=r)
    ['c', 'b', 'd', 'a']

    >>> reservoir_sample(0, 'abcd', random=r)
    []

    >>> reservoir_sample(5, 'abcdefghijklmnopqrstuvwxyz', random=r)
    ['x', 'l', 'a', 'n', 'b']
    """
    if k == 0:
        return []
    assert k > 0, R('Sample size must not be negative', k)
    it = iter(it)
    sample = list(islice(it, k))
    random.shuffle(sample)
    for i, item in enumerate(it, start=k + 1):
        j = random.randrange(i)
        if j < k:
            sample[j] = item
    return sample


def find_range(value: int | float, upper_bounds: Iterable[int]) -> int:
    """
    The upper bound of the range that the given value falls into, given an
    iterable of the exclusive upper bounds of adjacent ranges, in ascending
    order. The first range has no lower bound. The value must be below the last
    upper bound.

    >>> [
    ...     find_range(value, [50, 100])
    ...     for value in [-1, 0, 1, 49, 50, 75, 99]
    ... ]
    [50, 50, 50, 50, 100, 100, 100]

    >>> find_range(1.5, [1, 2])
    2

    >>> find_range(100, [50, 100])
    Traceback (most recent call last):
    ...
    AssertionError: R('Value is not below the last upper bound', 100)

    An infinite iterable of upper bounds is only consumed up to the first one
    that exceeds the value.

    >>> from itertools import count
    >>> find_range(1000, (2 ** exponent for exponent in count()))
    1024
    """
    bound = first(dropwhile(partial(ge, value), upper_bounds), None)
    assert bound is not None, R('Value is not below the last upper bound', value)
    return bound
