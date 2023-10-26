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
        The type of entity this transformer creates and aggregates
        contributions for.
        """
        raise NotImplementedError

    @abstractmethod
    def replica_type(self, entity: EntityReference) -> str:
        """
        The type of replica emitted by this transformer. Related to, but not
        necessarily the same as, the entity type.
        """
        raise NotImplementedError

    @classmethod
    def inner_entity_types(cls) -> frozenset[str]:
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
    def get_aggregator(cls, entity_type: EntityType) -> Optional[EntityAggregator]:
        """
        Returns the aggregator to be used for entities of the given type that
        occur in the document to be aggregated. A document for an entity of
        type X typically contains exactly one entity of type X and multiple
        entities of types other than X.
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
