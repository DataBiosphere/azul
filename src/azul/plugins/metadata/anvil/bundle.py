from abc import (
    ABC,
)
from collections import (
    defaultdict,
)
from typing import (
    AbstractSet,
    Generic,
    Mapping,
    Self,
    TypeVar,
)

import attrs

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


REF = TypeVar('REF', bound=EntityReference | KeyReference)


@attrs.frozen(kw_only=True, order=False)
class Link(Generic[REF]):
    inputs: AbstractSet[REF] = attrs.field(factory=frozenset,
                                           converter=frozenset)

    activity: REF | None = attrs.field(default=None)

    outputs: AbstractSet[REF] = attrs.field(factory=frozenset,
                                            converter=frozenset)

    @property
    def all_entities(self) -> AbstractSet[REF]:
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
    def group_by_activity(cls, links: set[Self]):
        """
        Merge links that share the same (non-null) activity.
        """
        groups_by_activity: Mapping[KeyReference, set[Self]] = defaultdict(set)
        for link in links:
            if link.activity is not None:
                groups_by_activity[link.activity].add(link)
        for activity, group in groups_by_activity.items():
            if len(group) > 1:
                links -= group
                merged_link = cls(inputs=frozenset.union(*[link.inputs for link in group]),
                                  activity=activity,
                                  outputs=frozenset.union(*[link.outputs for link in group]))
                links.add(merged_link)

    def __lt__(self, other: Self) -> bool:
        return min(self.inputs) < min(other.inputs)


class EntityLink(Link[EntityReference]):
    pass


class KeyLink(Link[KeyReference]):

    def to_entity_link(self,
                       entities_by_key: Mapping[KeyReference, EntityReference]
                       ) -> EntityLink:
        lookup = entities_by_key.__getitem__
        return EntityLink(inputs=set(map(lookup, self.inputs)),
                          activity=none_safe_apply(lookup, self.activity),
                          outputs=set(map(lookup, self.outputs)))


@attrs.define(kw_only=True)
class AnvilBundle(Bundle[BUNDLE_FQID], ABC):
    entities: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)
    links: set[EntityLink] = attrs.field(factory=set)
    orphans: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)

    def reject_joiner(self, catalog: CatalogName):
        # FIXME: Optimize joiner rejection and re-enable it for AnVIL
        #        https://github.com/DataBiosphere/azul/issues/5256
        pass

    def to_json(self) -> MutableJSON:
        def serialize_entities(entities):
            return {
                str(entity_ref): entity
                for entity_ref, entity in sorted(entities.items())
            }

        return {
            'entities': serialize_entities(self.entities),
            'orphans': serialize_entities(self.orphans),
            'links': [link.to_json() for link in sorted(self.links)]
        }

    @classmethod
    def from_json(cls, fqid: BUNDLE_FQID, json_: JSON) -> Self:
        def deserialize_entities(json_entities):
            return {
                EntityReference.parse(entity_ref): entity
                for entity_ref, entity in json_entities.items()
            }

        return cls(
            fqid=fqid,
            entities=deserialize_entities(json_['entities']),
            links=set(map(EntityLink.from_json, json_['links'])),
            orphans=deserialize_entities(json_['orphans'])
        )
