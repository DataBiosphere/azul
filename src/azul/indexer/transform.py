from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Iterable,
)

from azul.indexer import (
    Bundle,
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
    @abstractmethod
    def field_types(cls) -> FieldTypes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def create(cls, bundle: Bundle, deleted: bool) -> 'Transformer':
        """
        Create a transformer instance for the given bundle.

        :param bundle: the bundle to be transformed
        :param deleted: whether the bundle being indexed was deleted
        """
        raise NotImplementedError

    @abstractmethod
    def transform(self) -> Iterable[Contribution]:
        """
        Return the contributions by the current bundle to the entities it
        contains metadata about. More than one bundle can contribute to a
        particular entity and any such entity can receive contributions by more
        than one bundle. Only after all bundles have been transformed, can the
        contributions pertaining to a particular entity be aggregated into
        a single index document containing exhaustive metadata about that
        entity.
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
