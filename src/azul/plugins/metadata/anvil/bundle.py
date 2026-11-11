from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    defaultdict,
)
from collections.abc import (
    Set,
)
from functools import (
    cached_property,
    total_ordering,
)
from itertools import (
    chain,
    filterfalse,
    product,
)
from typing import (
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Self,
)

import attrs

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
    singleton,
)
from azul.lib.types import (
    MutableJSON,
    json_element_mappings,
    json_element_strings,
)

#: The one snapshot whose files may lack an MD5 digest
#:
#: FIXME: Files from 1000G snapshot in anvildev can't be mirrored
#:        https://github.com/DataBiosphere/azul/issues/7634
#:
snapshot_without_md5s = 'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732'

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
        # A primary bundle may contain an activity none of whose inputs are in
        # the bundle, so the inputs of a link may be empty. Such links sort
        # first.
        if self.inputs and other.inputs:
            return min(self.inputs) < min(other.inputs)
        else:
            return bool(other.inputs)


class EntityLink(Link[EntityReference]):
    pass


class KeyLink(Link[KeyReference]):
    pass


def is_activity(entity: EntityReference | KeyReference) -> bool:
    return entity.entity_type.endswith('activity')


@attrs.define(kw_only=True)
class LinkGraph[REF: (EntityReference, KeyReference)]:
    """
    The links between the entities in a bundle, as a directed graph. The graph
    has an edge from every input of a link to the link's activity, and from
    the activity to every output of the link. A link without an activity
    contributes an edge from every one of its inputs to every one of its
    outputs. Every entity in a link is a node of the graph, and so is every
    entity added explicitly, even if it has no edges.
    """
    parents: dict[REF, set[REF]] = attrs.field(factory=dict)
    children: dict[REF, set[REF]] = attrs.field(factory=dict)

    @classmethod
    def from_links(cls, links: Iterable[Link[REF]]) -> Self:
        self = cls()
        self.add_links(links)
        return self

    @property
    def nodes(self) -> Set[REF]:
        return self.parents.keys()

    def add_node(self, node: REF) -> bool:
        """
        Add the given node to this graph, unless it is already in it. Returns
        True if it wasn't.
        """
        is_new = node not in self.parents
        if is_new:
            self.parents[node] = set()
            self.children[node] = set()
        return is_new

    def add_links(self, links: Iterable[Link[REF]]) -> set[REF]:
        """
        Add the given links to this graph and return the nodes that weren't in
        it before.
        """
        new_nodes = set()
        for link in links:
            for node in link.all_entities:
                if self.add_node(node):
                    new_nodes.add(node)
            if link.activity is None:
                edges = product(link.inputs, link.outputs)
            else:
                edges = chain(product(link.inputs, [link.activity]),
                              product([link.activity], link.outputs))
            for parent, child in edges:
                self.parents[child].add(parent)
                self.children[parent].add(child)
        return new_nodes

    def map[T: (EntityReference, KeyReference)](self, f: Callable[[REF], T]) -> LinkGraph[T]:
        """
        A copy of this graph with every node replaced by the result of the
        given function for that node.
        """

        def map_edges(edges: dict[REF, set[REF]]) -> dict[T, set[T]]:
            return {f(node): set(map(f, relatives)) for node, relatives in edges.items()}

        return LinkGraph(parents=map_edges(self.parents),
                         children=map_edges(self.children))

    def to_links(self, link_cls: type[Link[REF]]) -> Iterator[Link[REF]]:
        """
        The links this graph was built from, one per activity, as instances of
        the given class. The edges that don't involve an activity are grouped
        into one link per input, which may group them differently than the
        original links did. Only a subclass of Link that binds its type
        variable, like EntityLink, can be serialized.
        """
        for node, children in self.children.items():
            if is_activity(node):
                yield link_cls(inputs=frozenset(self.parents[node]),
                               activity=node,
                               outputs=frozenset(children))
            else:
                outputs = frozenset(filterfalse(is_activity, children))
                if outputs:
                    yield link_cls(inputs=singleton(node), outputs=outputs)

    def ancestors(self, node: REF) -> Mapping[EntityType, Set[REF]]:
        """
        The nodes upstream of the given node, by entity type
        """
        return self._closure(node, self.parents)

    def descendants(self, node: REF) -> Mapping[EntityType, Set[REF]]:
        """
        The nodes downstream of the given node, by entity type
        """
        return self._closure(node, self.children)

    def _closure(self,
                 node: REF,
                 relatives: Mapping[REF, Set[REF]]
                 ) -> Mapping[EntityType, Set[REF]]:
        closure: dict[EntityType, set[REF]] = defaultdict(set)
        visited, pending = {node}, [node]
        while pending:
            for relative in relatives.get(pending.pop(), frozenset()):
                if relative not in visited:
                    visited.add(relative)
                    closure[relative.entity_type].add(relative)
                    pending.append(relative)
        return closure


KeyGraph = LinkGraph[KeyReference]

EntityGraph = LinkGraph[EntityReference]

EntityRefsByType = Mapping[EntityType, Set[EntityReference]]


@attrs.frozen(kw_only=True)
class LinkedEntities(Iterable[EntityReference]):
    origin: EntityReference
    graph: EntityGraph

    @cached_property
    def ancestors(self) -> EntityRefsByType:
        return self.graph.ancestors(self.origin)

    @cached_property
    def descendants(self) -> EntityRefsByType:
        return self.graph.descendants(self.origin)

    def __getitem__(self, item: EntityType) -> Set[EntityReference]:
        empty: frozenset[EntityReference] = frozenset()
        return self.ancestors.get(item, empty) | self.descendants.get(item, empty)

    def __iter__(self) -> Iterator[EntityReference]:
        for entities in self.ancestors.values():
            yield from entities
        for entities in self.descendants.values():
            yield from entities


@attrs.define(kw_only=True)
class AnvilBundle[BUNDLE_FQID: SourcedBundleFQID](Bundle[BUNDLE_FQID],
                                                  metaclass=ABCMeta):
    # The `entity_type` attribute of these keys contains the entities' BigQuery
    # table name (e.g. `anvil_sequencingactivity`), not the entity type used for
    # the contributions (e.g. `activities`). The metadata plugin converts from
    # the former to the latter during transformation.
    entities: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)
    links: EntityGraph = serializable(
        attrs.field(factory=EntityGraph),
        from_json=lambda x: EntityGraph.from_links(map(EntityLink.from_json,
                                                       json_element_mappings(x))),
        to_json=lambda x: [v.to_json() for v in sorted(x.to_links(EntityLink))]
    )
    orphans: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)

    @property
    @abstractmethod
    def bundle_entity(self) -> EntityReference | None:
        """
        A reference to the bundle entity, or None if the bundle is batched
        """
        raise NotImplementedError

    def reject_joiner(self):
        # We can skip the `links` attribute because the only strings it contains
        # are UUIDs and table names
        self._reject_joiner(chain(self.entities.values(), self.orphans.values()))
