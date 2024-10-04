from abc import (
    ABC,
)
from typing import (
    AbstractSet,
    Generic,
    Iterable,
    Self,
    TypeVar,
)

import attrs
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
)
from azul.collections import (
    aset,
    none_safe_apply,
)
from azul.indexer import (
    BUNDLE_FQID,
    Bundle,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.types import (
    JSON,
    MutableJSON,
)

# AnVIL snapshots do not use UUIDs for primary/foreign keys. This type alias
# helps us distinguish these keys from the document UUIDs, which are drawn from
# the `datarepo_row_id` column. Note that entities from different tables may
# have the same key, so `KeyReference` should be used when mixing keys from
# different entity types.
Key = str


@attrs.frozen(kw_only=True)
class KeyReference:
    key: Key
    entity_type: EntityType


ENTITY_REF = TypeVar('ENTITY_REF', bound=EntityReference | KeyReference)


@attrs.frozen(kw_only=True, order=False)
class Link(Generic[ENTITY_REF]):
    inputs: AbstractSet[ENTITY_REF] = attrs.field(factory=frozenset,
                                                  converter=frozenset)

    activity: ENTITY_REF | None = attrs.field(default=None)

    outputs: AbstractSet[ENTITY_REF] = attrs.field(factory=frozenset,
                                                   converter=frozenset)

    @property
    def all_entities(self) -> AbstractSet[ENTITY_REF]:
        return self.inputs | self.outputs | aset(self.activity)

    @classmethod
    def from_json(cls, link: JSON) -> Self:
        return cls(inputs=set(map(EntityReference.parse, link['inputs'])),
                   activity=none_safe_apply(EntityReference.parse, link['activity']),
                   outputs=set(map(EntityReference.parse, link['outputs'])))

    def to_json(self) -> MutableJSON:
        return {
            'inputs': sorted(map(str, self.inputs)),
            'activity': none_safe_apply(str, self.activity),
            'outputs': sorted(map(str, self.outputs))
        }

    @classmethod
    def merge(cls, links: Iterable[Self]) -> Self:
        return cls(inputs=frozenset.union(*[link.inputs for link in links]),
                   activity=one({link.activity for link in links}),
                   outputs=frozenset.union(*[link.outputs for link in links]))

    def __lt__(self, other: Self) -> bool:
        return min(self.inputs) < min(other.inputs)


@attrs.define(kw_only=True)
class AnvilBundle(Bundle[BUNDLE_FQID], ABC):
    entities: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)
    links: set[Link[EntityReference]] = attrs.field(factory=set)

    def reject_joiner(self, catalog: CatalogName):
        # FIXME: Optimize joiner rejection and re-enable it for AnVIL
        #        https://github.com/DataBiosphere/azul/issues/5256
        pass

    def to_json(self) -> MutableJSON:
        return {
            'entities': {
                str(entity_ref): entity
                for entity_ref, entity in sorted(self.entities.items())
            },
            'links': [link.to_json() for link in sorted(self.links)]
        }

    @classmethod
    def from_json(cls, fqid: BUNDLE_FQID, json_: JSON) -> Self:
        return cls(
            fqid=fqid,
            entities={
                EntityReference.parse(entity_ref): entity
                for entity_ref, entity in json_['entities'].items()
            },
            links=set(map(Link.from_json, json_['links']))
        )
