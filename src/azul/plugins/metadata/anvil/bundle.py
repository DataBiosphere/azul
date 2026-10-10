from abc import (
    ABCMeta,
)
from collections import (
    defaultdict,
)
from functools import (
    total_ordering,
)
from itertools import (
    chain,
)
from typing import (
    Collection,
    Iterable,
    Iterator,
    Mapping,
    Self,
)

import attrs
from more_itertools import (
    one,
)

from azul.indexer import (
    Bundle,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityReference,
    EntityType,
)
from azul.lib.attrs import (
    SerializableAttrs,
    serializable,
)
from azul.lib.collections import (
    aset,
    none_safe_apply,
)
from azul.lib.types import (
    MutableJSON,
    json_element_mappings,
    json_element_strings,
)

#: A type alias for the values of a table's primary key column, which the AnVIL
#: schema declares to be a string. This type alias distinguishes such a key from
#: an entity ID, which is derived from it. A key is only unique within a single
#: table, so `KeyReference` should be used when mixing keys from different
#: entity types.
#:
type Key = str


@attrs.frozen(kw_only=True, order=True)
class KeyReference(SerializableAttrs):
    key: Key
    entity_type: EntityType


def ref_set_field():
    return serializable(
        from_json=lambda x: frozenset(map(EntityReference.parse,
                                          json_element_strings(x))),
        to_json=lambda x: sorted(map(str, x))
    )


@total_ordering
@attrs.frozen(kw_only=True, order=False)
class Link[REF: (EntityReference, KeyReference)](SerializableAttrs):
    inputs: frozenset[REF] = ref_set_field()
    activity: REF | None = None
    outputs: frozenset[REF] = ref_set_field()

    @property
    def all_entities(self) -> frozenset[REF]:
        return self.inputs | self.outputs | aset(self.activity)

    def __lt__(self, other: Self) -> bool:
        return min(self.inputs) < min(other.inputs)


class EntityLink(Link[EntityReference]):
    pass


class KeyLink(Link[KeyReference]):

    def to_entity_link(self,
                       entities_by_key: Mapping[KeyReference, EntityReference]
                       ) -> EntityLink:
        lookup = entities_by_key.__getitem__
        return EntityLink(inputs=frozenset(map(lookup, self.inputs)),
                          activity=none_safe_apply(lookup, self.activity),
                          outputs=frozenset(map(lookup, self.outputs)))


EntityRefsByType = dict[EntityType, set[EntityReference]]


@attrs.frozen(kw_only=True)
class LinkedEntities(Iterable[EntityReference]):
    origin: EntityReference
    ancestors: EntityRefsByType
    descendants: EntityRefsByType

    def __getitem__(self, item: EntityType) -> set[EntityReference]:
        return self.ancestors[item] | self.descendants[item]

    def __iter__(self) -> Iterator[EntityReference]:
        for entities in self.ancestors.values():
            yield from entities
        for entities in self.descendants.values():
            yield from entities

    @classmethod
    def from_links(cls,
                   origin: EntityReference,
                   links: Collection[EntityLink]
                   ) -> Self:
        return cls(origin=origin,
                   ancestors=cls._search(origin, links, from_='outputs', to='inputs'),
                   descendants=cls._search(origin, links, from_='inputs', to='outputs'))

    @classmethod
    def _search(cls,
                entity_ref: EntityReference,
                links: Collection[EntityLink],
                entities: EntityRefsByType | None = None,
                *,
                from_: str,
                to: str
                ) -> EntityRefsByType:
        entities = defaultdict(set) if entities is None else entities
        if entity_ref.entity_type.endswith('activity'):
            follow = [one(link for link in links if entity_ref == link.activity)]
        else:
            follow = [link for link in links if entity_ref in getattr(link, from_)]
        for link in follow:
            for relative in [link.activity, *getattr(link, to)]:
                if relative is not None:
                    if relative != entity_ref and relative.entity_id not in entities[relative.entity_type]:
                        entities[relative.entity_type].add(relative)
                        cls._search(relative, links, entities, from_=from_, to=to)
        return entities


@attrs.define(kw_only=True)
class AnvilBundle[BUNDLE_FQID: SourcedBundleFQID](Bundle[BUNDLE_FQID],
                                                  metaclass=ABCMeta):
    # The `entity_type` attribute of these keys contains the entities' BigQuery
    # table name (e.g. `anvil_sequencingactivity`), not the entity type used for
    # the contributions (e.g. `activities`). The metadata plugin converts from
    # the former to the latter during transformation.
    entities: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)
    links: set[EntityLink] = serializable(
        attrs.field(factory=set),
        from_json=lambda x: set(map(EntityLink.from_json, json_element_mappings(x))),
        to_json=lambda x: [v.to_json() for v in sorted(x)]
    )
    orphans: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)

    def reject_joiner(self):
        # We can skip the `links` attribute because the only strings it contains
        # are UUIDs and table names
        self._reject_joiner(chain(self.entities.values(), self.orphans.values()))
