from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    FrozenSet,
    Iterable,
)

from azul.indexer import (
    BundlePartition,
)
from azul.indexer.aggregate import (
    EntityAggregator,
)
from azul.indexer.document import (
    Contribution,
    FieldTypes,
)


class Transformer(ABC):

    @classmethod
    @abstractmethod
    def entity_type(cls) -> str:
        """
        The type of entity this transformer creates and aggregates
        contributions for.
        """
        raise NotImplementedError

    @classmethod
    def inner_entity_types(cls) -> FrozenSet[str]:
        return frozenset((cls.entity_type(),))

    @classmethod
    @abstractmethod
    def field_types(cls) -> FieldTypes:
        raise NotImplementedError

    @abstractmethod
    def estimate(self, partition: BundlePartition) -> int:
        """
        Return the expected number of contributions that would be returned by
        a call to :meth:`transform()`.
        """

    @abstractmethod
    def transform(self, partition: BundlePartition) -> Iterable[Contribution]:
        """
        Return the contributions by the current bundle to the entities it
        contains metadata about. More than one bundle can contribute to a
        particular entity and any such entity can receive contributions by more
        than one bundle. Only after all bundles have been transformed, can the
        contributions pertaining to a particular entity be aggregated into
        a single index document containing exhaustive metadata about that
        entity.

        :param partition: The partition of the bundle to return contributions
                          for.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_aggregator(cls, entity_type) -> EntityAggregator:
        """
        Returns the aggregator to be used for entities of the given type that
        occur in the document to be aggregated. A document for an entity of
        type X typically contains exactly one entity of type X and multiple
        entities of types other than X.
        """
        raise NotImplementedError
