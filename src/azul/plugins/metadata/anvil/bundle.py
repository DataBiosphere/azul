from abc import (
    ABC,
)
from typing import (
    AbstractSet,
    Generic,
    Iterable,
    Optional,
    TypeVar,
    Union,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
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


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class KeyReference:
    key: Key
    entity_type: EntityType


ENTITY_REF = TypeVar('ENTITY_REF', bound=Union[EntityReference, KeyReference])


@attr.s(auto_attribs=True, frozen=True, kw_only=True, order=False)
class Link(Generic[ENTITY_REF]):
    inputs: AbstractSet[ENTITY_REF] = attr.ib(factory=frozenset, converter=frozenset)
    activity: Optional[ENTITY_REF] = attr.ib(default=None)
    outputs: AbstractSet[ENTITY_REF] = attr.ib(factory=frozenset, converter=frozenset)

    @property
    def all_entities(self) -> AbstractSet[ENTITY_REF]:
        return self.inputs | self.outputs | (set() if self.activity is None else {self.activity})

    @classmethod
    def from_json(cls, link: JSON) -> 'Link':
        return cls(inputs=set(map(EntityReference.parse, link['inputs'])),
                   activity=None if link['activity'] is None else EntityReference.parse(link['activity']),
                   outputs=set(map(EntityReference.parse, link['outputs'])))

    def to_json(self) -> MutableJSON:
        return {
            'inputs': sorted(map(str, self.inputs)),
            'activity': None if self.activity is None else str(self.activity),
            'outputs': sorted(map(str, self.outputs))
        }

    @classmethod
    def merge(cls, links: Iterable['Link']) -> 'Link':
        return cls(inputs=frozenset.union(*[link.inputs for link in links]),
                   activity=one({link.activity for link in links}),
                   outputs=frozenset.union(*[link.outputs for link in links]))

    def __lt__(self, other: 'Link') -> bool:
        return min(self.inputs) < min(other.inputs)


@attr.s(auto_attribs=True, kw_only=True)
class AnvilBundle(Bundle[BUNDLE_FQID], ABC):
    entities: dict[EntityReference, MutableJSON] = attr.ib(factory=dict)
    links: set[Link[EntityReference]] = attr.ib(factory=set)

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
    def from_json(cls, fqid: BUNDLE_FQID, json_: JSON) -> 'AnvilBundle':
        return cls(
            fqid=fqid,
            entities={
                EntityReference.parse(entity_ref): entity
                for entity_ref, entity in json_['entities'].items()
            },
            links=set(map(Link.from_json, json_['links']))
        )
