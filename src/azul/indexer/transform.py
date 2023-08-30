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
    BundleFQID,
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
    Replica,
    ReplicaCoordinates,
)
from azul.json import (
    json_hash,
)
from azul.types import (
    JSON,
    MutableJSON,
)

Transform = tuple[Optional[Contribution], Optional[Replica]]


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

    @abstractmethod
    def replica_type(self, entity: EntityReference) -> str:
        """
        The type of replica emitted by this transformer.

        See :py:attr:`Replica.replica_type`
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
    def transform(self, partition: BundlePartition) -> Iterable[Transform]:
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

    def _replica(self, contents: MutableJSON, entity: EntityReference) -> Replica:
        coordinates = ReplicaCoordinates(content_hash=json_hash(contents).hexdigest(),
                                         entity=attr.evolve(entity,
                                                            entity_type='replica'))
        return Replica(coordinates=coordinates,
                       version=None,
                       replica_type=self.replica_type(entity),
                       contents=contents,
                       hub_ids=[])

    @classmethod
    @abstractmethod
    def inner_entity_id(cls, entity_type: EntityType, entity: JSON) -> EntityID:
        """
        Return the identifier of the given inner entity. Typically, the
        identifier is the value of a particular property of the entity.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def reconcile_inner_entities(cls,
                                 entity_type: EntityType,
                                 *,
                                 this: tuple[JSON, BundleFQID],
                                 that: tuple[JSON, BundleFQID],
                                 ) -> tuple[JSON, BundleFQID]:
        """
        Given two potentially different copies of an inner entity, return the
        copy that should be incorporated into the aggregate for an outer entity
        of this transformer's entity type. Each copy is accompanied by the FQID
        of the bundle that contributed it. Typically, the copy from the more
        recently updated bundle is returned, but other implementations, such as
        merging the two copies are plausible, too.

        :param entity_type: The type of the entity to reconcile

        :param this: One copy of the entity and the bundle it came from

        :param that: Another copy of the entity and the bundle it came from

        :return: The copy to use and the bundle it came from. The return value
                 may be passed to this method again in case there is yet another
                 copy to reconcile. In that case, the return value will be
                 passed as the ``this`` argument.
        """
        raise NotImplementedError
