from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    Counter,
    defaultdict,
)
import logging
import sys
from typing import (
    Any,
    Callable,
    Hashable,
    TYPE_CHECKING,
)

from azul import (
    R,
)
from azul.collections import (
    none_safe_key,
)
from azul.indexer.document import (
    EntityType,
)
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
    json_mapping,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from _typeshed import (
        SupportsAdd,
        SupportsDunderGT,
        SupportsDunderLT,
        SupportsRichComparison,
    )


class Accumulator[V, A](metaclass=ABCMeta):
    """
    Accumulates multiple values into a single value, not necessarily of the same
    type.
    """

    def __init__(self):
        self.dropped = 0

    @abstractmethod
    def accumulate(self, value: V | list[V]) -> Any:
        """
        Incorporate the given value into this accumulator. If the value is not
        incorporated (due to e.g. a maximum size constraint), implementations
        should increment :py:attr:`dropped`.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self) -> A:
        """
        Return the accumulated value.
        """
        raise NotImplementedError


class BaseAccumulator[V, A](Accumulator[V, A], metaclass=ABCMeta):
    """
    Handles lists of values by accumulating each item individually. For certain
    types of accumulators this may not be the most efficient way to handle such
    lists.
    """

    def accumulate(self, value: V | list[V]) -> None:
        if isinstance(value, list):
            for value in value:
                self._accumulate(value)
        else:
            self._accumulate(value)

    @abstractmethod
    def _accumulate(self, value: V) -> None:
        raise NotImplementedError


class SumAccumulator[V: 'SupportsAdd'](BaseAccumulator[V | None, V | None]):
    """
    Add values.

    Unlike the built-in sum() function, this accumulator doesn't default to an
    initial value of 0 but defaults to the first accumulated value instead. Also
    unlike sum(), it simply ignores None values.
    """

    def __init__(self, *, initially: V | None = None) -> None:
        """
        :param initially: the initial value for the sum. If None, the first
                          accumulated value that is not None will be used to
                          initialize the sum. Note that if this parameter is
                          None, the return value of get() could be None, too.
        """
        super().__init__()
        self.value = initially

    def _accumulate(self, value: V | None) -> None:
        if value is not None:
            if self.value is None:
                self.value = value
            else:
                self.value += value

    def get(self) -> V | None:
        return self.value


class SetAccumulator[V: Hashable](Accumulator[V, list[V]]):
    """
    Accumulates values into a set, discarding duplicates and, optionally, values
    that would grow the set past the maximum size. The accumulated value is
    returned as a sorted list. The maximum size constraint does not take the
    ordering into account. This accumulator does not return a list of the N
    smallest values, it returns a sorted list of the first N distinct values.
    """

    def __init__(self,
                 max_size: int | None = None,
                 key: Callable[[V], 'SupportsRichComparison'] | None = None
                 ) -> None:
        """
        :param max_size: the maximum number of elements to retain

        :param key: The key to be used for sorting the accumulated set of
                    values. If this value is None, a default None-safe key will
                    be used. With that default key, if any None values were
                    placed in the accumulator, the first element, and only the
                    first element of the returned list will be None.
        """
        super().__init__()
        self.value: set[V] = set()
        self.max_size = max_size
        self.key = none_safe_key(none_last=True) if key is None else key

    def accumulate(self, value: V | list[V]) -> int:
        """
        :return: The number of distinct values that were incorporated. There are
                 two reasons a value may not be incorporated: it was already in
                 the set or the accumulator is full. The latter is reflected in
                 self.dropped

        >>> acc = SetAccumulator(max_size=4)
        >>> acc.accumulate([]), acc.get(), acc.dropped
        (0, [], 0)

        >>> acc.accumulate(1), acc.get(), acc.dropped
        (1, [1], 0)

        >>> acc.accumulate(1), acc.get(), acc.dropped
        (0, [1], 0)

        >>> acc.accumulate(2), acc.get(), acc.dropped
        (1, [1, 2], 0)

        >>> acc.accumulate([1, 2, 3]), acc.get(), acc.dropped
        (1, [1, 2, 3], 0)

        >>> acc.accumulate([1, 2, 3]), acc.get(), acc.dropped
        (0, [1, 2, 3], 0)

        >>> acc.accumulate([3, 4, 5]), acc.get(), acc.dropped
        (1, [1, 2, 3, 4], 1)

        >>> acc.accumulate([5, 6]), acc.get(), acc.dropped
        (0, [1, 2, 3, 4], 3)

        >>> acc.accumulate(1), acc.get(), acc.dropped
        (0, [1, 2, 3, 4], 3)

        >>> acc.accumulate(5), acc.get(), acc.dropped
        (0, [1, 2, 3, 4], 4)

        The ``dropped`` attribute is incremented for each of the 5's below since
        that's what would happen were they incorporated in separate calls.

        >>> acc.accumulate([4, 4, 5, 5]), acc.get(), acc.dropped
        (0, [1, 2, 3, 4], 6)

        >>> acc = SetAccumulator(max_size=0)

        >>> acc.accumulate([]), acc.get(), acc.dropped
        (0, [], 0)

        >>> acc.accumulate(1), acc.get(), acc.dropped
        (0, [], 1)

        >>> acc.accumulate([1, 1]), acc.get(), acc.dropped
        (0, [], 3)

        >>> import random
        >>> l = [random.randint(0, 9) for _ in range(10000)]
        >>> acc = SetAccumulator()
        >>> acc.accumulate(l)
        10

        >>> list(set(acc.get())) == acc.get()
        True

        >>> set(l) == set(acc.get())
        True

        Tuples are treated as scalars. We rely on this behavior when aggregating
        `ValueAndUnit` fields.

        >>> acc = SetAccumulator(max_size=2)
        >>> acc.accumulate((1, 2)), acc.get(), acc.dropped
        (1, [(1, 2)], 0)

        >>> acc.accumulate([(2, 1), (1, 2), ()]), acc.get(), acc.dropped
        (1, [(1, 2), (2, 1)], 1)
        """
        current, max_size = self.value, self.max_size
        initial_len = len(current)
        free_space = sys.maxsize if max_size is None else max_size - initial_len
        assert free_space >= 0
        if isinstance(value, list):
            if len(value) <= free_space:
                # If there is sufficient free space to incorporate all values,
                # even if they're all the same, do so.
                current.update(value)
            else:
                # If there are no duplicates in the argument, we can add as many
                # items from the argument as we have free space for.
                current.update(value[0:free_space])
                value = value[free_space:]
                new_len = len(current)
                # We may still have free space left if there were duplicate
                # items in the slice we just incorporated, or if some of those
                # items had already been incorporated before the slice was.
                num_added = new_len - initial_len
                free_space -= num_added
                assert free_space >= 0
                # We could repeat the above but that could lead to many slices
                # of length one. Instead we'll switch to handling elements
                # individually until we run out of space.
                i = iter(value)
                try:
                    while free_space > 0:
                        current.add(next(i))
                        if new_len != len(current):
                            new_len += 1
                            free_space -= 1
                    # We've run out of space. Report any elements not already
                    # accumulated as dropped.
                    while True:
                        if next(i) not in current:
                            self.dropped += 1
                except StopIteration:
                    pass
        else:
            if free_space > 0:
                current.add(value)
            elif value not in current:
                self.dropped += 1
        final_len = len(current)
        assert max_size is None or final_len <= max_size
        return final_len - initial_len

    def get(self) -> list[V]:
        return sorted(self.value, key=self.key)


class SetOfDictAccumulator(SetAccumulator[JSON | None]):
    """
    A set accumulator that supports mutable mappings as values.

    >>> acc = SetOfDictAccumulator(key=lambda d: d['foo'])
    >>> d = {'foo': 2}
    >>> acc.accumulate(d)
    1

    >>> acc.accumulate(d)
    0

    >>> d = {'foo': 1, 'bar': 1}
    >>> acc.accumulate(d)
    1

    >>> acc.accumulate([d, d])
    0

    >>> acc.get()
    [{'foo': 1, 'bar': 1}, {'foo': 2}]
    """

    def _freeze(self, value: JSON | None) -> JSON | None:
        return None if value is None else json_mapping(freeze(value))

    def _thaw(self, value: JSON | None) -> JSON | None:
        return None if value is None else json_mapping(thaw(value))

    def accumulate(self, value: JSON | None | list[JSON | None]) -> int:
        if isinstance(value, list):
            # `freeze` converts lists to tuples, which the superclass treats as
            # scalars instead of sequences. Passing a list as a tuple would
            # therefore introduce an extraneous level of nesting, as every
            # element in `value` would end up in a single element of the
            # accumulated result.
            value = list(map(self._freeze, value))
        else:
            value = self._freeze(value)
        return super().accumulate(value)

    def get(self) -> list[JSON | None]:
        return [self._thaw(value) for value in super().get()]


if TYPE_CHECKING:
    # @formatter:off (PyCharm puts two blank lines around indented top-level
    # classes, flake8 wants one)
    class HashableAndSupportsDunderLT(SupportsDunderLT,
                                      Hashable,
                                      metaclass=ABCMeta):
        ...

    class HashableAndSupportsDunderGT(SupportsDunderGT,
                                      Hashable,
                                      metaclass=ABCMeta):
        ...

    type HashableAndSortable = (
        HashableAndSupportsDunderGT
        | HashableAndSupportsDunderLT
    )
    # @formatter:on


class DictAccumulator[K: HashableAndSortable, V](Accumulator[V, list[V]]):
    """
    Accumulate values into a dictionary, allowing one unique value per key,
    discarding values that would exceed the maximum number of dictionary keys.
    In a way this is a generalized SetAccumulator. DictAccumulator can replace a
    SetAccumulator by using the identity function for the key.
    """

    def __init__(self,
                 *,
                 max_size: int | None,
                 key: Callable[[V], K]):
        """
        :param max_size: The maximum number of elements to retain. A value of
                         None can be used to specify no maximum.

        :param key: A function returning the key to be used both for storing the
                    accumulated value and sorting the accumulated set of values.
        """
        super().__init__()
        self.max_size = max_size
        self.key = key
        self.value: dict[K, V] = {}

    def accumulate(self, value):
        """
        >>> acc = DictAccumulator(max_size=3, key=lambda s: s.lower())
        >>> acc.accumulate('foo')
        >>> acc.get(), acc.dropped
        (['foo'], 0)

        >>> acc.accumulate('foo')
        >>> acc.get(), acc.dropped
        (['foo'], 0)

        >>> acc.accumulate('Foo')
        Traceback (most recent call last):
        ...
        AssertionError: R('Ambiguos key:', 'foo', 'values:', 'foo', 'Foo')

        >>> acc.accumulate('Bar')
        >>> acc.accumulate('BAZ')
        >>> acc.get(), acc.dropped
        (['Bar', 'BAZ', 'foo'], 0)

        >>> acc.accumulate('spam')
        >>> acc.get(), acc.dropped
        (['Bar', 'BAZ', 'foo'], 1)
        """
        key = self.key(value)
        if self.max_size is None or len(self.value) < self.max_size:
            try:
                old_value = self.value[key]
            except KeyError:
                self.value[key] = value
            else:
                assert old_value == value, R(
                    'Ambiguos key:', key, 'values:', old_value, value)
        elif key not in self.value:
            self.dropped += 1

    def get(self):
        return sorted(self.value.values(), key=self.key)


class FrequencySetAccumulator[V](Accumulator[V, list[V]]):
    """
    An accumulator that accepts any number of values and returns a list with
    at most max_size most frequently occurring values.

    Note the max_size argument only limits the length of the accumulate, the
    overall menory consumption of this accumulator is unbounded.

    >>> acc = FrequencySetAccumulator(max_size=2)
    >>> acc.accumulate('x')
    >>> acc.accumulate(['x','y'])
    >>> acc.accumulate(['x','y','z'])
    >>> acc.get()
    ['x', 'y']

    >>> acc = FrequencySetAccumulator(max_size=0)
    >>> acc.accumulate('x')
    >>> acc.get()
    []
    """

    def __init__(self, *, max_size: int) -> None:
        super().__init__()
        self.value: Counter[V] = Counter()
        self.max_size = max_size

    def accumulate(self, value: V | list[V]) -> None:
        if isinstance(value, (dict, list)):
            self.value.update(value)
        else:
            self.value[value] += 1

    def get(self) -> list[V]:
        self.dropped = max(0, len(self.value) - self.max_size)
        return [item for item, count in self.value.most_common(self.max_size)]


class LastValueAccumulator(Accumulator):
    """
    An accumulator that accepts any number of values and returns the value most
    recently seen.
    """

    def __init__(self) -> None:
        super().__init__()
        self.value = None

    def accumulate(self, value):
        self.value = value

    def get(self):
        return self.value


class SingleValueAccumulator(LastValueAccumulator):
    """
    An accumulator that accepts any number of values given that they all are the
    same value and returns a single value. Occurrence of any value that is
    different than the first accumulated value raises a ValueError.
    """

    def accumulate(self, value):
        if self.value is None:
            super().accumulate(value)
        elif self.value != value:
            raise ValueError('Conflicting values:', self.value, value)


class MinAccumulator(LastValueAccumulator):
    """
    An accumulator that returns the minimal value seen.
    """

    def accumulate(self, value):
        if value is not None and (self.value is None or value < self.value):
            super().accumulate(value)


class MaxAccumulator(LastValueAccumulator):
    """
    An accumulator that returns the maximal value seen.
    """

    def accumulate(self, value):
        if value is not None and (self.value is None or value > self.value):
            super().accumulate(value)


class DistinctAccumulator[K:Hashable, V, A](BaseAccumulator[tuple[K, V], A]):
    """
    An accumulator for (key, value) tuples. Of two pairs with the same key, only
    the value from the first pair will be accumulated. The actual values will be
    accumulated in another accumulator instance specified at construction.

    >>> acc = DistinctAccumulator(SumAccumulator(initially=0), max_size=3)

    Keys can be tuples, too.

    >>> acc.accumulate((('x', 'y'), 3))

    Values associated with a recurring key will not be accumulated.

    >>> acc.accumulate((('x', 'y'), 4))
    >>> acc.accumulate(('a', 20))
    >>> acc.accumulate(('b', 100))

    Accumulation stops at max_size distinct keys.

    >>> acc.accumulate(('c', 1000))
    >>> acc.get()
    123
    """

    def __init__(self,
                 inner: Accumulator[V, A],
                 max_size: int | None = None) -> None:
        super().__init__()
        self.inner = inner
        self.keys: SetAccumulator[K] = SetAccumulator(max_size=max_size)

    def _accumulate(self, value: tuple[K, V]) -> None:
        key, value = value
        if self.keys.accumulate(key):
            self.inner.accumulate(value)

    def get(self) -> A:
        return self.inner.get()


class UniqueValueCountAccumulator[V:Hashable](Accumulator[V, int]):
    """
    Count the number of unique values
    """

    def __init__(self):
        self.inner: SetAccumulator[V] = SetAccumulator()
        super().__init__()

    def accumulate(self, value: V | list[V]) -> Any:
        self.inner.accumulate(value)

    def get(self) -> int:
        return len(self.inner.get())


class EntityAggregator(metaclass=ABCMeta):

    def __init__(self, outer_entity_type: EntityType, entity_type: EntityType):
        self.outer_entity_type = outer_entity_type
        self.entity_type = entity_type

    def _transform_entity(self, entity: JSON) -> JSON:
        return entity

    def _accumulator(self, field: str) -> Accumulator | None:
        """
        Return the Accumulator instance to be used for the given field or None
        if the field should not be accumulated.
        """
        return self._default_accumulator()

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator(max_size=100)

    @abstractmethod
    def aggregate(self, entities: JSONs) -> JSONs:
        raise NotImplementedError


type JSONAccumulator = Accumulator[AnyJSON, AnyJSON]

type Aggregate = dict[str, JSONAccumulator | None]


class SimpleAggregator(EntityAggregator):

    def aggregate(self, entities: JSONs) -> JSONs:
        aggregate: Aggregate = {}
        for entity in entities:
            self._accumulate(aggregate, entity)
        return [self._aggregate(aggregate)] if aggregate else []

    def _accumulate(self, aggregate: Aggregate, entity: JSON) -> None:
        entity = self._transform_entity(entity)
        for field, value in entity.items():
            try:
                accumulator = aggregate[field]
            except KeyError:
                accumulator = self._accumulator(field)
                aggregate[field] = accumulator
            if accumulator is not None:
                accumulator.accumulate(value)

    def _aggregate(self, aggregate: Aggregate) -> JSON:
        result = {}
        for k, accumulator in aggregate.items():
            if accumulator is not None:
                result[k] = accumulator.get()
                if accumulator.dropped > 0:
                    log.warning('Values were dropped %d times while aggregating %s.%s into %s',
                                accumulator.dropped, self.entity_type, k, self.outer_entity_type)
        return result


type GroupKeys = tuple[Hashable, ...]


class GroupingAggregator(SimpleAggregator):

    def aggregate(self, entities: JSONs) -> JSONs:
        aggregates: dict[GroupKeys, Aggregate] = defaultdict(dict)
        for entity in entities:
            group_keys = self._group_keys(entity)
            aggregate = aggregates[group_keys]
            self._accumulate(aggregate, entity)
        return [
            self._aggregate(aggregate)
            for aggregate in aggregates.values()
        ]

    @abstractmethod
    def _group_keys(self, entity) -> GroupKeys:
        raise NotImplementedError
