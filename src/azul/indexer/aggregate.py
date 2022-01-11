from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    Counter,
    defaultdict,
)
import logging
from typing import (
    Any,
    Callable,
    List,
    MutableMapping,
    Optional,
    Tuple,
)

from azul import (
    require,
)
from azul.collections import (
    none_safe_key,
)
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.types import (
    JSON,
    MutableJSONs,
)

logger = logging.getLogger(__name__)

Entities = MutableJSONs


class Accumulator(ABC):
    """
    Accumulates multiple values into a single value, not necessarily of the same type.
    """

    @abstractmethod
    def accumulate(self, value):
        """
        Incorporate the given value into this accumulator.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self):
        """
        Return the accumulated value.
        """
        raise NotImplementedError


class SumAccumulator(Accumulator):
    """
    Add values.

    Unlike the sum() built-in, this accumulator doesn't default to an initial
    value of 0 but defaults to the first accumulated value instead.
    """

    def __init__(self, *, initially=None) -> None:
        """
        :param initially: the initial value for the sum. If None, the first
                          accumulated value that is not None will be used to
                          initialize the sum. Note that if this parameter is
                          None, the return value of close() could be None, too.
        """
        super().__init__()
        self.value = initially

    def accumulate(self, value) -> None:
        if value is not None:
            if self.value is None:
                self.value = value
            else:
                self.value += value

    def get(self):
        return self.value


class SetAccumulator(Accumulator):
    """
    Accumulates values into a set, discarding duplicates and, optionally, values
    that would grow the set past the maximum size. The accumulated value is
    returned as a sorted list. The maximum size constraint does not take the
    ordering into account. This accumulator does not return a list of the N
    smallest values, it returns a sorted list of the first N distinct values.
    """

    def __init__(self, max_size=None, key=None) -> None:
        """
        :param max_size: the maximum number of elements to retain

        :param key: The key to be used for sorting the accumulated set of
                    values. If this value is None, a default None-safe key will
                    be used. With that default key, if any None values were
                    placed in the accumulator, the first element, and only the
                    first element of the returned list will be None.
        """
        super().__init__()
        self.value = set()
        self.max_size = max_size
        self.key = none_safe_key(none_last=True) if key is None else key

    def accumulate(self, value) -> bool:
        """
        :return: True, if the given value was incorporated into the set
        """
        if self.max_size is None or len(self.value) < self.max_size:
            before = len(self.value)
            if isinstance(value, (list, set)):
                self.value.update(value)
            else:
                self.value.add(value)
            after = len(self.value)
            if before < after:
                return True
            elif before == after:
                return False
            else:
                assert False
        else:
            return False

    def get(self) -> List[Any]:
        return sorted(self.value, key=self.key)


class ListAccumulator(Accumulator):
    """
    Accumulate values into a list, optionally discarding values that
    would grow the list past the maximum size, if specified.
    """

    def __init__(self, max_size=None) -> None:
        super().__init__()
        self.value = list()
        self.max_size = max_size

    def accumulate(self, value):
        if self.max_size is None or len(self.value) < self.max_size:
            if isinstance(value, (list, set)):
                self.value.extend(value)
            else:
                self.value.append(value)

    def get(self) -> List[Any]:
        return sorted(self.value)


class SetOfDictAccumulator(SetAccumulator):
    """
    A set accumulator that supports mutable mappings as values.
    """

    def accumulate(self, value) -> bool:
        return super().accumulate(freeze(value))

    def get(self):
        return thaw(super().get())


class DictAccumulator(Accumulator):
    """
    Accumulate values into a dictionary, allowing one unique value per key,
    discarding values that would exceed the maximum number of dictionary keys.
    In a way this is a generalized SetAccumulator. DictAccumulator can replace
    a SetAccumulator by using the identity function (``lambda _: _``) for the key.
    """

    def __init__(self, max_size: Optional[int], key: Callable):
        """
        :param max_size: The maximum number of elements to retain. A value of
                         None can be used to specify no maximum.

        :param key: A function returning the key to be used both for storing the
                    accumulated value and sorting the accumulated set of values.
        """
        self.max_size = max_size
        self.key = key
        self.value = {}

    def accumulate(self, value):
        if self.max_size is None or len(self.value) < self.max_size:
            key = self.key(value)
            try:
                old_value = self.value[key]
            except KeyError:
                self.value[key] = value
            else:
                require(old_value == value, old_value, value)

    def get(self):
        return sorted(self.value.values(), key=self.key)


class FrequencySetAccumulator(Accumulator):
    """
    An accumulator that accepts any number of values and returns a list with length max_size or smaller containing
    the most frequent values accumulated.

    >>> a = FrequencySetAccumulator(2)
    >>> a.accumulate('x')
    >>> a.accumulate(['x','y'])
    >>> a.accumulate({'x','y','z'})
    >>> a.get()
    ['x', 'y']
    >>> a = FrequencySetAccumulator(0)
    >>> a.accumulate('x')
    >>> a.get()
    []
    """

    def __init__(self, max_size) -> None:
        super().__init__()
        self.value = Counter()
        self.max_size = max_size

    def accumulate(self, value) -> None:
        if isinstance(value, (dict, list, set)):
            self.value.update(value)
        else:
            self.value[value] += 1

    def get(self) -> List[Any]:
        return [item for item, count in self.value.most_common(self.max_size)]


class LastValueAccumulator(Accumulator):
    """
    An accumulator that accepts any number of values and returns the value most recently seen.
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
    An accumulator that accepts any number of values given that they all are the same value and returns a single value.
    Occurrence of any value that is different than the first accumulated value raises a ValueError.
    """

    def accumulate(self, value):
        if self.value is None:
            super().accumulate(value)
        elif self.value != value:
            raise ValueError('Conflicting values:', self.value, value)


class OptionalValueAccumulator(LastValueAccumulator):
    """
    An accumulator that accepts at most one value and returns it.
    Occurrence of more than one value, same or different, raises a ValueError.
    """

    def accumulate(self, value):
        if self.value is None:
            super().accumulate(value)
        else:
            raise ValueError('Conflicting values:', self.value, value)


class MandatoryValueAccumulator(OptionalValueAccumulator):
    """
    An accumulator that requires exactly one value and returns it.
    Occurrence of more than one value or no value at all raises a ValueError.
    """

    def get(self):
        if self.value is None:
            raise ValueError('No value')
        else:
            return super().get()


class PriorityOptionalValueAccumulator(OptionalValueAccumulator):
    """
    An OptionalValueAccumulator that accepts (priority, value) tuples and
    returns the value whose priority is equal to the maximum priority observed.
    Occurrence of more than one value per priority raises a ValueError.
    """

    def __init__(self) -> None:
        super().__init__()
        self.priority = None

    def accumulate(self, value):
        priority, value = value
        if self.priority is None or self.priority < priority:
            self.priority = priority
            self.value = None
        if self.priority == priority:
            super().accumulate(value)


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


class DistinctAccumulator(Accumulator):
    """
    An accumulator for (key, value) tuples. Of two pairs with the same key, only the value from the first pair will
    be accumulated. The actual values will be accumulated in another accumulator instance specified at construction.

        >>> a = DistinctAccumulator(SumAccumulator(initially=0), max_size=3)

    Keys can be tuples, too.

        >>> a.accumulate((('x', 'y'), 3))

    Values associated with a recurring key will not be accumulated.

        >>> a.accumulate((('x', 'y'), 4))
        >>> a.accumulate(('a', 20))
        >>> a.accumulate(('b', 100))

    Accumulation stops at max_size distinct keys.

        >>> a.accumulate(('c', 1000))
        >>> a.get()
        123
    """

    def __init__(self, inner: Accumulator, max_size: int = None) -> None:
        self.value = inner
        self.keys = SetAccumulator(max_size=max_size)

    def accumulate(self, value):
        key, value = value
        if self.keys.accumulate(key):
            self.value.accumulate(value)

    def get(self):
        return self.value.get()


class UniqueValueCountAccumulator(Accumulator):
    """
    Count the number of unique values
    """

    def __init__(self):
        super().__init__()
        self.value = SetAccumulator()

    def accumulate(self, value) -> bool:
        """
        :return: True, if the given value increased the count of unique values
        """
        return self.value.accumulate(value)

    def get(self) -> int:
        unique_items = self.value.get()
        return len(unique_items)


class EntityAggregator(ABC):

    def _transform_entity(self, entity: JSON) -> JSON:
        return entity

    def _get_accumulator(self, field: str) -> Optional[Accumulator]:
        """
        Return the Accumulator instance to be used for the given field or None
        if the field should not be accumulated.
        """
        if field == 'submission_date':
            return MinAccumulator()
        elif field in ('update_date', 'last_modified_date'):
            return MaxAccumulator()
        else:
            return self._get_default_accumulator()

    def _get_default_accumulator(self) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)

    @abstractmethod
    def aggregate(self, entities: Entities) -> Entities:
        raise NotImplementedError


class SimpleAggregator(EntityAggregator):

    def aggregate(self, entities: Entities) -> Entities:
        aggregate = {}
        for entity in entities:
            self._accumulate(aggregate, entity)
        return [
            {
                k: accumulator.get()
                for k, accumulator in aggregate.items()
                if accumulator is not None
            }
        ] if aggregate else []

    def _accumulate(self, aggregate: MutableMapping[str, Optional[Accumulator]], entity: JSON):
        entity = self._transform_entity(entity)
        for field_, value in entity.items():
            try:
                accumulator = aggregate[field_]
            except Exception:
                accumulator = self._get_accumulator(field_)
                aggregate[field_] = accumulator
            if accumulator is not None:
                accumulator.accumulate(value)


class GroupingAggregator(SimpleAggregator):

    def aggregate(self, entities: Entities) -> Entities:
        aggregates: MutableMapping[Any, MutableMapping[str, Optional[Accumulator]]] = defaultdict(dict)
        for entity in entities:
            group_keys = self._group_keys(entity)
            aggregate = aggregates[group_keys]
            self._accumulate(aggregate, entity)
        return [
            {
                field: accumulator.get()
                for field, accumulator in aggregate.items()
                if accumulator is not None
            }
            for aggregate in aggregates.values()
        ]

    @abstractmethod
    def _group_keys(self, entity) -> Tuple[Any, ...]:
        raise NotImplementedError
