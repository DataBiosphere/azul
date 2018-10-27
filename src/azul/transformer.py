from abc import ABC, ABCMeta, abstractmethod
from collections import defaultdict
import itertools
import logging
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple, MutableMapping, Any

from dataclasses import dataclass, field

from azul import config
from azul.json_freeze import freeze, thaw
from azul.types import JSON

logger = logging.getLogger(__name__)


@dataclass
class Bundle:
    uuid: str
    version: str
    contents: Optional[JSON] = field(default_factory=dict)

    def to_json(self) -> JSON:
        json = dict(uuid=self.uuid, version=self.version)
        # To save space in the document source we suppress the `content` entry if it is None
        if self.contents is not None:
            json['contents'] = self.contents
        return json

    @classmethod
    def from_json(cls, json) -> 'Bundle':
        return cls(uuid=json['uuid'], version=json['version'], contents=json.get('contents'))

    @property
    def deleted(self) -> bool:
        return self.contents.get('deleted', False)

    def delete(self):
        self.contents = {'deleted': True}


DocumentCoordinates = Tuple[str, str]


@dataclass
class ElasticSearchDocument:
    entity_type: str
    entity_id: str
    bundles: List[Bundle]
    document_type: str = "doc"
    document_version: int = 1
    contents: Optional[JSON] = None

    def to_json(self) -> JSON:
        return dict(entity_type=self.entity_type,
                    entity_id=self.entity_id,
                    bundles=[b.to_json() for b in self.bundles],
                    document_type=self.document_type,
                    document_version=self.document_version,
                    contents=self.contents)

    @classmethod
    def from_json(cls, json: JSON):
        return cls(entity_type=json['entity_type'],
                   entity_id=json['entity_id'],
                   bundles=list(map(Bundle.from_json, json['bundles'])),
                   document_type=json['document_type'],
                   document_version=json['document_version'],
                   contents=json['contents'])

    @property
    def original(self) -> bool:
        return self.contents is None

    @property
    def aggregate(self) -> bool:
        return not self.original

    @property
    def document_id(self) -> str:
        return self.entity_id

    @property
    def document_index(self) -> str:
        return config.es_index_name(self.entity_type, aggregate=self.aggregate)

    @property
    def coordinates(self) -> DocumentCoordinates:
        return self.document_index, self.document_id

    @property
    def original_coordinates(self) -> DocumentCoordinates:
        return config.es_index_name(self.entity_type, aggregate=False), self.document_id

    @property
    def aggregate_coordinates(self) -> DocumentCoordinates:
        return config.es_index_name(self.entity_type, aggregate=True), self.document_id

    def to_source(self) -> JSON:
        source = {
            "entity_id": self.entity_id,
            "bundles": [b.to_json() for b in self.bundles]
        }
        if self.aggregate:
            source["contents"] = self.contents
        return source

    @classmethod
    def from_index(cls, hit: JSON):
        source = hit["_source"]
        entity_type, aggregate = config.parse_es_index_name(hit['_index'])
        self = cls(entity_type=entity_type,
                   entity_id=source['entity_id'],
                   bundles=[Bundle.from_json(b) for b in source['bundles']],
                   document_version=hit.get("_version", 0),
                   contents=source.get("contents"))
        assert self.aggregate == aggregate
        return self

    def update_with(self, other: 'ElasticSearchDocument') -> bool:
        """
        Merge updates from another instance into this one. Typically, `self` represents a persistent document loaded
        from an index while `other` contains contributions from incoming bundles.

        :returns: True, if this document was modified and should be written back to the index
        """
        assert self._is_compatible_with(other)
        modified, self.bundles = self._merge_bundles(self.bundles, other.bundles)
        self.document_version = self.document_version + 1
        return modified

    def _is_compatible_with(self, other):
        return (self.document_id == other.document_id and
                self.document_index == other.document_index and
                self.document_type == other.document_type and
                self.entity_type == other.entity_type)

    @staticmethod
    def _merge_bundles(current: List[Bundle], updates: List[Bundle]) -> Tuple[bool, List[Bundle]]:
        """
        Merge two bundle contribution lists into one. Bundles from either list are matched based on their UUID,
        version and content. Bundles absent from either argument will be placed in the result. Of two bundles with
        the same UUID the one with the higher version will be placed into the result. If UUID and version are the
        same but the contents differ (as defined by `__eq__`), the update (from the second argument) is selected. All
        three being equal, the bundle from the first argument will selected.

        :returns: the merged list of bundles and a boolean indicating whether at least one update was selected. Note
        that bundles in the result may occur in a different order compared to the first argument, even if this
        boolean is False.

        >>> merge = ElasticSearchDocument._merge_bundles
        >>> B = Bundle

        Bs without a match in the other list are chosen:

            >>> merge([B(uuid='0', version='0'),                          ],
            ...       [                          B(uuid='2', version='0') ])
            (True, [Bundle(uuid='2', version='0', contents={}), Bundle(uuid='0', version='0', contents={})])

        If the updates are a subset of the first argument, no changes are made, and the first argument is returned.

            >>> merge([B(uuid='0', version='0'), B(uuid='2', version='0')],
            ...       [                          B(uuid='2', version='0')])
            (False, [Bundle(uuid='2', version='0', contents={}), Bundle(uuid='0', version='0', contents={})])

        If the updates are a superset of the first argument, the updates are returned.

            >>> merge([                          B(uuid='2', version='0')],
            ...       [B(uuid='0', version='0'), B(uuid='2', version='0')])
            (True, [Bundle(uuid='0', version='0', contents={}), Bundle(uuid='2', version='0', contents={})])

        If the UUID matches, the more recent bundle version is chosen:

            >>> merge([B(uuid='0', version='0'), B(uuid='2', version='1')],
            ...       [B(uuid='0', version='1'), B(uuid='2', version='0')])
            (True, [Bundle(uuid='0', version='1', contents={}), Bundle(uuid='2', version='1', contents={})])

        Ties (identical UUID and version) are broken by favoring the bundle from the second argument as long as it is
        different:

            >>> merge([B(uuid='1', version='0', contents={'x':1})],
            ...       [B(uuid='1', version='0', contents={'x':2})])
            (True, [Bundle(uuid='1', version='0', contents={'x': 2})])

        Everything being equal, the first argument is favored and no changes are reported:

            >>> merge([B(uuid='1', version='0', contents={'x':1})],
            ...       [B(uuid='1', version='0', contents={'x':1})])
            (False, [Bundle(uuid='1', version='0', contents={'x': 1})])

        A more complicated case:

            >>> merge([B(uuid='0', version='0'), B(uuid='1', version='0', contents={'x':1}), B(uuid='2', version='0')],
            ...       [                          B(uuid='1', version='0', contents={'x':2}), B(uuid='2', version='1')])
            ... # doctest: +NORMALIZE_WHITESPACE
            (True,
            [Bundle(uuid='1', version='0', contents={'x': 2}),
            Bundle(uuid='2', version='1', contents={}),
            Bundle(uuid='0', version='0', contents={})])
        """
        modified = False
        current_by_id = {bundle.uuid: bundle for bundle in current}
        assert len(current_by_id) == len(current)
        bundles = {}
        for update in updates:
            try:
                cur_bundle = current_by_id.pop(update.uuid)
            except KeyError:
                modified, bundle = True, update
            else:
                if cur_bundle.version < update.version:
                    modified, bundle = True, update
                elif cur_bundle.version > update.version:
                    bundle = cur_bundle
                else:
                    if update == cur_bundle:
                        bundle = cur_bundle
                    else:
                        modified, bundle = True, update
            assert bundles.setdefault(update.uuid, bundle) is bundle
        for bundle in current_by_id.values():
            assert bundles.setdefault(bundle.uuid, bundle) is bundle
        return modified, list(bundles.values())

    def consolidate(self, others: Iterable['ElasticSearchDocument']):
        """
        Combine bundle contributions from multiple other instances into this one. All involved instances must
        represent the same metadata entity or an exception wioll be raised. The bundle contributions from all
        involved instances must be disjunctive or an exception will be raised. See `:py:methd:`update_with` for a way
        to reconcile two instances with non-disjunctive bundle contributions.
        """
        assert all(self._is_compatible_with(other) for other in others)
        bundles = {}
        for bundle in itertools.chain(self.bundles, *(other.bundles for other in others)):
            assert bundles.setdefault((bundle.uuid, bundle.version), bundle) == bundle, \
                f"Conflicting contributions for bundle {bundle.uuid}, version {bundle.version}"
        self.bundles = list(bundles.values())


class Transformer(ABC):

    @abstractmethod
    def create_documents(self,
                         uuid: str,
                         version: str,
                         manifest: List[JSON],
                         metadata_files: Mapping[str, JSON]
                         ) -> Sequence[ElasticSearchDocument]:
        """
        Given the metadata for a particular bundle, compute a list of contributions to Elasticsearch documents. The
        contributions constitute partial documents, e.g. their `bundles` attribute is a singleton list, representing
        only the contributions by the specified bundle. Before the contributions can be persisted, they need to be
        merged with contributions by all other bundles.

        :param uuid: The UUID of the bundle to create documents for
        :param version: The version of the bundle to create documents for
        :param manifest:  The bundle manifest entries for all data and metadata files in the bundle
        :param metadata_files: The contents of all metadata files in the bundle
        :return: The document contributions
        """
        raise NotImplementedError()

    @abstractmethod
    def aggregate_document(self, document: ElasticSearchDocument) -> Optional[ElasticSearchDocument]:
        """
        Given a
        """
        return None


Entities = List[JSON]

EntityID = str

BundleVersion = str


class Accumulator(ABC):
    @abstractmethod
    def accumulate(self, value):
        """
        Incorporate the given value into this accumulator.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self):
        """
        Return the accumulated value.
        """
        raise NotImplementedError


class SumAccumulator(Accumulator):

    def __init__(self, initially=None) -> None:
        """
        :param initially: the initial value for the sum. If None, the first accumulated value that is not None will
                          be used to initialize the sum. Note that if this parameter is None, the return value of
                          close() could be None, too.
        """
        super().__init__()
        self.value = initially

    def accumulate(self, value) -> None:
        if value is not None:
            if self.value is None:
                self.value = value
            else:
                self.value += value

    def get(self):
        return self.value


class SetAccumulator(Accumulator):

    def __init__(self, max_size=None) -> None:
        super().__init__()
        self.value = set()
        self.max_size = max_size

    def accumulate(self, value) -> bool:
        """
        :return: True, if the given value was incorporated into the set
        """
        if self.max_size is None or len(self.value) < self.max_size:
            before = len(self.value)
            if isinstance(value, (list, set)):
                self.value.update(value)
            else:
                self.value.add(value)
            after = len(self.value)
            if before < after:
                return True
            elif before == after:
                return False
            else:
                assert False
        else:
            return False

    def get(self) -> List[Any]:
        return list(self.value)


class ListAccumulator(Accumulator):

    def __init__(self, max_size=None) -> None:
        super().__init__()
        self.value = list()
        self.max_size = max_size

    def accumulate(self, value):
        if self.max_size is None or len(self.value) < self.max_size:
            if isinstance(value, (list, set)):
                self.value.extend(value)
            else:
                self.value.append(value)

    def get(self) -> List[Any]:
        return list(self.value)


class SetOfDictAccumulator(SetAccumulator):

    def accumulate(self, value) -> bool:
        return super().accumulate(freeze(value))

    def get(self):
        return thaw(super().get())


class LastValueAccumulator(Accumulator):

    def __init__(self) -> None:
        super().__init__()
        self.value = None

    def accumulate(self, value):
        self.value = value

    def get(self):
        return self.value


class FirstValueAccumulator(LastValueAccumulator):

    def accumulate(self, value):
        if self.value is not None:
            raise ValueError('Conflicting values:', self.value, value)
        else:
            super().accumulate(value)


class OneValueAccumulator(FirstValueAccumulator):

    def get(self):
        if self.value is None:
            raise ValueError('No value')
        else:
            return super().get()


class MinAccumulator(LastValueAccumulator):

    def accumulate(self, value):
        if value is not None and (self.value is None or value < self.value):
            super().accumulate(value)


class MaxAccumulator(LastValueAccumulator):

    def accumulate(self, value):
        if value is not None and (self.value is None or value > self.value):
            super().accumulate(value)


class DistinctAccumulator(Accumulator):
    """
    An accumulator for (key, value) tuples. Of two pairs with the same key, only the value from the first pair will
    be accumulated. The actual values will be accumulated in another accumulator instance specified at construction.

        >>> a = DistinctAccumulator(SumAccumulator(0), max_size=3)

    Keys can be tuples, too.

        >>> a.accumulate((('x', 'y'), 3))

    Values associated with a recurring key will not be accumulated.

        >>> a.accumulate((('x', 'y'), 4))
        >>> a.accumulate(('a', 20))
        >>> a.accumulate(('b', 100))

    Accumulation stops at max_size distinct keys.

        >>> a.accumulate(('c', 1000))
        >>> a.get()
        123
    """

    def __init__(self, inner: Accumulator, max_size: int = None) -> None:
        self.value = inner
        self.keys = SetAccumulator(max_size=max_size)

    def accumulate(self, value):
        key, value = value
        if self.keys.accumulate(key):
            self.value.accumulate(value)

    def get(self):
        return self.value.get()


class EntityAggregator(ABC):

    def _transform_entity(self, entity: JSON) -> JSON:
        return entity

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        """
        Return the Accumulator instance to be used for the given field or None if the field should not be accumulated.
        """
        return SetAccumulator()

    @abstractmethod
    def aggregate(self, entities: Entities) -> Entities:
        raise NotImplementedError


class SimpleAggregator(EntityAggregator):

    def aggregate(self, entities: Entities) -> Entities:
        aggregate = {}
        for entity in entities:
            self._accumulate(aggregate, entity)
        return [
            {
                k: accumulator.get()
                for k, accumulator in aggregate.items()
                if accumulator is not None
            }
        ]

    def _accumulate(self, aggregate, entity):
        entity = self._transform_entity(entity)
        for field, value in entity.items():
            try:
                accumulator = aggregate[field]
            except:
                accumulator = self._get_accumulator(field)
                aggregate[field] = accumulator
            if accumulator is not None:
                accumulator.accumulate(value)


class GroupingAggregator(SimpleAggregator):

    def aggregate(self, entities: Entities) -> Entities:
        aggregates: MutableMapping[Any, MutableMapping[str, Optional[Accumulator]]] = defaultdict(dict)
        for entity in entities:
            aggregate = aggregates[self._group_key(entity)]
            self._accumulate(aggregate, entity)
        return [
            {
                field: accumulator.get()
                for field, accumulator in aggregate.items()
                if accumulator is not None
            }
            for aggregate in aggregates.values()
        ]

    @abstractmethod
    def _group_key(self, entity) -> Any:
        raise NotImplementedError


class AggregatingTransformer(Transformer, metaclass=ABCMeta):

    @abstractmethod
    def entity_type(self) -> str:
        """
        The type of entity for which this transformer can aggregate documents.
        """
        raise NotImplementedError

    def get_aggregator(self, entity_type) -> EntityAggregator:
        """
        Returns the aggregator to be used for entities of the given type that occur in the document to be aggregated.
        A document for an entity of type X typically contains exactly one entity of type X and multiple entities of
        types other than X.
        """
        return SimpleAggregator()

    def aggregate_document(self, document: ElasticSearchDocument) -> ElasticSearchDocument:
        if document.entity_type == self.entity_type():
            contents = self._select_latest(document)
            for entity_type in contents.keys():
                if entity_type == self.entity_type():
                    assert len(contents[entity_type]) == 1
                else:
                    aggregator = self.get_aggregator(entity_type)
                    contents[entity_type] = aggregator.aggregate(contents[entity_type])
            bundles = [Bundle(uuid=bundle.uuid, version=bundle.version, contents=None)
                       for bundle in document.bundles if not bundle.deleted]
            assert bool(contents) == bool(bundles)
            return ElasticSearchDocument(entity_type=document.entity_type,
                                         entity_id=document.entity_id,
                                         bundles=bundles,
                                         document_type=document.document_type,
                                         document_version=document.document_version,
                                         contents=contents)
        else:
            return super().aggregate_document(document)

    def _select_latest(self, document) -> MutableMapping[str, Entities]:
        """
        Collect the latest version of each entity from the document. If two or more bundles contribute copies of the
        same entity, potentially with different contents, the copy from the newest bundle will be chosen.
        """
        collated_contents: MutableMapping[str, MutableMapping[EntityID, Tuple[BundleVersion, JSON]]] = defaultdict(dict)
        for bundle in document.bundles:
            for entity_type, entities in bundle.contents.items():
                if entity_type == 'deleted':
                    assert entities is True
                else:
                    collated_entities = collated_contents[entity_type]
                    for entity in entities:
                        entity_id = entity['document_id']
                        bundle_version, collated_entity = collated_entities.get(entity_id, ('', {}))
                        if bundle_version < bundle.version:
                            collated_entities[entity_id] = bundle.version, entity
        contents = {entity_type: [entity for entity_id, (_, entity) in entities.items()]
                    for entity_type, entities in collated_contents.items()}
        return contents
