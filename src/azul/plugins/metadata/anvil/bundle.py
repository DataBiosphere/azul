from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Set,
)
from functools import (
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
    serialized_as,
)
from azul.lib.collections import (
    aset,
    singleton,
)
from azul.lib.json import (
    Serializable,
)
from azul.lib.types import (
    AnyJSON,
    JSONs,
    MutableJSON,
    json_element_mappings,
    json_element_strings,
    json_str,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schemas,
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
#: table.
#:
type Key = str


def ref_set_field():
    return serializable(
        from_json=lambda x: frozenset(map(EntityReference.parse,
                                          json_element_strings(x))),
        to_json=lambda x: sorted(map(str, x))
    )


@total_ordering
@attrs.frozen(kw_only=True, order=False)
class EntityLink(SerializableAttrs):
    inputs: frozenset[EntityReference] = ref_set_field()
    activity: EntityReference | None = None
    outputs: frozenset[EntityReference] = ref_set_field()

    @property
    def all_entities(self) -> frozenset[EntityReference]:
        return self.inputs | self.outputs | aset(self.activity)

    def __lt__(self, other: Self) -> bool:
        # A primary bundle may contain an activity none of whose inputs are in
        # the bundle, so the inputs of a link may be empty. Such links sort
        # first.
        if self.inputs and other.inputs:
            return min(self.inputs) < min(other.inputs)
        else:
            return bool(other.inputs)


type EntityRefsByType = Mapping[EntityType, Set[EntityReference]]

type MutableEntityRefsByType = dict[EntityType, set[EntityReference]]

type GraphEdges = dict[EntityReference, set[EntityReference]]

#: The version of the AnVIL schema that this package was written against
#:
anvil_schema_version = 6

anvil_schema = anvil_schemas[anvil_schema_version]

#: The names of the tables whose rows are activities. The AnVIL schema
#: identifies them only by the suffix of their name.
#:
activity_tables: Set[EntityType] = frozenset(
    name
    for table in json_element_mappings(anvil_schema['tables'])
    if (name := json_str(table['name'])).endswith('activity')
)


def is_activity(entity: EntityReference) -> bool:
    return entity.entity_type in activity_tables


@attrs.define(kw_only=True)
class EntityGraph(Serializable):
    """
    The links between the entities in a bundle, as a directed graph. The graph
    has an edge from every input of a link to the link's activity, and from
    the activity to every output of the link. A link without an activity
    contributes an edge from every one of its inputs to every one of its
    outputs. Every entity in a link is a node of the graph, and so is every
    entity added explicitly, even if it has no edges.
    """
    parents: GraphEdges = attrs.field(factory=dict)
    children: GraphEdges = attrs.field(factory=dict)

    @classmethod
    def from_links(cls, links: Iterable[EntityLink]) -> Self:
        self = cls()
        self.add_links(links)
        return self

    @classmethod
    def from_json(cls, json: AnyJSON) -> Self:
        return cls.from_links(map(EntityLink.from_json, json_element_mappings(json)))

    def to_json(self) -> JSONs:
        return [link.to_json() for link in sorted(self.to_links())]

    @property
    def nodes(self) -> Set[EntityReference]:
        return self.parents.keys()

    def add_node(self, node: EntityReference) -> bool:
        """
        Add the given node to this graph, unless it is already in it. Returns
        True if it wasn't.
        """
        is_new = node not in self.parents
        if is_new:
            self.parents[node] = set()
            self.children[node] = set()
        return is_new

    def add_links(self, links: Iterable[EntityLink]) -> set[EntityReference]:
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

    def map(self, f: Callable[[EntityReference], EntityReference]) -> Self:
        """
        A copy of this graph with every node replaced by the result of the
        given function for that node.
        """

        def map_edges(edges: GraphEdges) -> GraphEdges:
            return {f(node): set(map(f, relatives)) for node, relatives in edges.items()}

        return type(self)(parents=map_edges(self.parents),
                          children=map_edges(self.children))

    def to_links(self) -> Iterator[EntityLink]:
        """
        The links this graph was built from, one per activity. The edges that
        don't involve an activity are grouped into one link per input, which
        may group them differently than the original links did.
        """
        for node, children in self.children.items():
            if is_activity(node):
                yield EntityLink(inputs=frozenset(self.parents[node]),
                                 activity=node,
                                 outputs=frozenset(children))
            else:
                outputs = frozenset(filterfalse(is_activity, children))
                if outputs:
                    yield EntityLink(inputs=singleton(node), outputs=outputs)

    def ancestors(self, node: EntityReference) -> EntityRefsByType:
        """
        The nodes upstream of the given node, by entity type
        """
        return self._closure(node, self.parents)

    def descendants(self, node: EntityReference) -> EntityRefsByType:
        """
        The nodes downstream of the given node, by entity type
        """
        return self._closure(node, self.children)

    def lineage(self, node: EntityReference) -> EntityRefsByType:
        """
        The nodes upstream or downstream of the given node, by entity type
        """
        return self._closure(node, self.children, self._closure(node, self.parents))

    def _closure(self,
                 node: EntityReference,
                 edges: GraphEdges,
                 closure: MutableEntityRefsByType | None = None
                 ) -> MutableEntityRefsByType:
        closure = {} if closure is None else closure
        visited, pending = {node}, [node]
        while pending:
            for relative in edges.get(pending.pop(), frozenset()):
                if relative not in visited:
                    visited.add(relative)
                    relatives = closure.setdefault(relative.entity_type, set())
                    relatives.add(relative)
                    pending.append(relative)
        return closure


@attrs.define(kw_only=True)
class AnvilBundle[BUNDLE_FQID: SourcedBundleFQID](Bundle[BUNDLE_FQID],
                                                  metaclass=ABCMeta):
    # The `entity_type` attribute of these keys contains the entities' BigQuery
    # table name (e.g. `anvil_sequencingactivity`), not the entity type used for
    # the contributions (e.g. `activities`). The metadata plugin converts from
    # the former to the latter during transformation.
    entities: dict[EntityReference, MutableJSON] = attrs.field(factory=dict)
    graph: EntityGraph = serialized_as(attrs.field(factory=EntityGraph),
                                       name='links')
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
