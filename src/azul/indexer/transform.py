from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Iterable,
)

from azul.indexer import Bundle
from azul.indexer.aggregate import EntityAggregator
from azul.indexer.document import (
    Contribution,
    FieldTypes,
)


class Transformer(ABC):

    @abstractmethod
    def field_types(self) -> FieldTypes:
        raise NotImplementedError()

    @abstractmethod
    def transform(self, bundle: Bundle, deleted: bool) -> Iterable[Contribution]:
        """
        Given the metadata for a particular bundle, compute a list of
        contributions to Elasticsearch documents. The contributions constitute
        partial documents, e.g. their `bundles` attribute is a singleton list,
        representing only the contributions by the specified bundle. Before
        the contributions can be persisted, they need to be merged with
        contributions by all other bundles.

        :param bundle: the bundle to be transformed
        :param deleted: Whether the bundle being indexed was deleted
        :return: The document contributions
        """
        raise NotImplementedError()

    @abstractmethod
    def entity_type(self) -> str:
        """
        The type of entity for which this transformer can aggregate documents.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_aggregator(self, entity_type) -> EntityAggregator:
        """
        Returns the aggregator to be used for entities of the given type that
        occur in the document to be aggregated. A document for an entity of
        type X typically contains exactly one entity of type X and multiple
        entities of types other than X.
        """
        raise NotImplementedError()
