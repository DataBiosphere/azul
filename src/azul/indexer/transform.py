from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Optional,
)

import attr

from azul.indexer import (
    Bundle,
    BundlePartition,
)
from azul.indexer.aggregate import (
    EntityAggregator,
)
from azul.indexer.document import (
    Contribution,
    ContributionCoordinates,
    EntityID,
    EntityReference,
    EntityType,
    FieldTypes,
)
from azul.types import (
    JSON,
    MutableJSON,
)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class Transformer(metaclass=ABCMeta):
    bundle: Bundle
    deleted: bool

    @classmethod
    @abstractmethod
    def entity_type(cls) -> EntityType:
        """
        The type of outer entity this transformer creates and aggregates
        contributions for.
        """
        raise NotImplementedError

    @classmethod
    def inner_entity_types(cls) -> frozenset[str]:
        """
        The set of types of inner entities that *do not* require aggregation in
        an aggregate for an entity of this transformer's outer entity type. For
        any *outer* entity of a certain type there is usually just one *inner*
        entity of that same type, eliminating the need to aggregate multiple
        inner entities.
        """
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
    def aggregator(cls, entity_type: EntityType) -> Optional[EntityAggregator]:
        """
        Returns the aggregator to be used for inner entities of the given type
        that occur in contributions to an entity of this transformer's (outer)
        entity type.
        """
        raise NotImplementedError

    def _contribution(self,
                      contents: MutableJSON,
                      entity: EntityReference
                      ) -> Contribution:
        coordinates = ContributionCoordinates(entity=entity,
                                              bundle=self.bundle.fqid.upcast(),
                                              deleted=self.deleted)
        return Contribution(coordinates=coordinates,
                            version=None,
                            source=self.bundle.fqid.source,
                            contents=contents)

    @classmethod
    @abstractmethod
    def inner_entity_id(cls, entity_type: EntityType, entity: JSON) -> EntityID:
        """
        Return the identifier of the given inner entity. Typically, the
        identifier is the value of a particular property of the entity.
        """
        raise NotImplementedError
