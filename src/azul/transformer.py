from abc import (
    ABC,
    ABCMeta,
    abstractmethod,
)
from collections import (
    Counter,
    defaultdict,
)
from enum import (
    Enum,
    auto,
)
import logging
import sys
from typing import (
    Any,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from dataclasses import (
    dataclass,
    fields,
    field,
)
from humancellatlas.data.metadata import api
from more_itertools import one

from azul import config
from azul.collections import none_safe_key
from azul.json_freeze import (
    freeze,
    thaw,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
)

logger = logging.getLogger(__name__)

Entities = List[JSON]

EntityID = str

BundleVersion = str

BundleUUID = str

EntityType = str


class EntityReference(NamedTuple):
    entity_type: EntityType
    entity_id: EntityID


class DocumentCoordinates(NamedTuple):
    document_index: str
    document_id: str


FieldType = Union[type, None]

FieldTypes4 = Union[Mapping[str, FieldType], FieldType]
FieldTypes3 = Union[Mapping[str, FieldTypes4], FieldType]
FieldTypes2 = Union[Mapping[str, FieldTypes3], FieldType]
FieldTypes1 = Union[Mapping[str, FieldTypes2], FieldType]
FieldTypes = Mapping[str, FieldTypes1]


class VersionType(Enum):
    # No versioning; document is created or overwritten as needed
    none = auto()

    # Writing a document fails with 409 conflict if one with the same ID already
    # exists in the index
    create_only = auto()

    # Use the Elasticsearch "internal" versioning type
    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-index_.html#_version_types
    internal = auto()


@dataclass
class Document:
    entity: EntityReference
    type: ClassVar[str] = 'doc'
    version_type: VersionType = field(init=False)
    version: Optional[int]
    contents: Optional[JSON]

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            'entity_id': str,
            'entity_version': str,
            'contents': field_types
        }

    @classmethod
    def translate_field(cls, value: AnyJSON, field_type: FieldType, forward: bool = True) -> Any:
        """
        Translate a single value for insert into or after fetching from Elasticsearch.

        :param value: A value to translate
        :param field_type: The type of the field value
        :param forward: If we should translate forward or backward (aka un-translate)
        :return: The translated value
        """
        # Note that the replacement values for `None` used for each data type
        # ensure that `None` values are placed at the end of a sorted list.
        null_string = '~null'
        # Maximum int that can be represented as a 64-bit int and double IEEE
        # floating point number. This prevents loss when converting between the two.
        null_int = sys.maxsize - 1023
        assert null_int == int(float(null_int))
        bool_translation_forward = {False: 0, True: 1, None: null_int}
        bool_translation_backward = {0: False, 1: True, null_int: None}

        if field_type is bool:
            if forward:
                return bool_translation_forward[value]
            else:
                return bool_translation_backward[value]
        elif field_type is int or field_type is float:
            if forward:
                if value is None:
                    return null_int
            else:
                if value == null_int:
                    return None
            return value
        elif field_type is str:
            if forward:
                if value is None:
                    return null_string
            else:
                if value == null_string:
                    return None
            return value
        elif field_type is dict:
            return value
        elif field_type is api.UUID4:
            return value
        elif field_type is None:
            return value
        else:
            raise ValueError(f'Unknown field_type value {field_type}')

    @classmethod
    def translate_fields(cls,
                         doc: AnyJSON,
                         field_types: Union[FieldType, FieldTypes],
                         forward: bool = True) -> AnyMutableJSON:
        """
        Traverse a document to translate field values for insert into Elasticsearch, or to translate back
        response data. This is done to support None/null values since Elasticsearch does not index these values.
        Values that are empty lists ([]) and lists of None ([None]) are both forward converted to [null_string]

        :param doc: A document dict of values
        :param field_types: A mapping of field paths to field type
        :param forward: If we should translate forward or backward (aka un-translate)
        :return: A copy of the original document with values translated according to their type
        """
        if field_types is None:
            return doc
        elif isinstance(doc, dict):
            new_dict = {}
            for key, val in doc.items():
                # Shadow copy fields should only be present during a reverse translation and we skip over to remove them
                if key.endswith('_'):
                    assert not forward
                else:
                    try:
                        field_type = field_types[key]
                    except KeyError:
                        raise KeyError(f'Key {key} not defined in field_types')
                    except TypeError:
                        raise TypeError(f'Key {key} not defined in field_types')
                    new_dict[key] = cls.translate_fields(val, field_type, forward=forward)
                    if forward and field_type in (int, float):
                        # Add a non-translated shadow copy of this field's numeric value for sum aggregations
                        new_dict[key + '_'] = val
            return new_dict
        elif isinstance(doc, list):
            # Translate an empty list to a list containing a single None value
            # (and then further translate that None value according to the field
            # type), but do so only at the field level (to avoid case of
            # contents['organoids'] == []).
            if doc or isinstance(field_types, dict):
                return [cls.translate_fields(val, field_types, forward=forward) for val in doc]
            else:
                assert forward
                return cls.translate_fields([None], field_types)
        else:
            return cls.translate_field(doc, field_types, forward=forward)

    @classmethod
    def index_name(cls, entity_type: EntityType) -> str:
        return config.es_index_name(entity_type, aggregate=issubclass(cls, Aggregate))

    @classmethod
    def entity_type(cls, index_name: str) -> EntityType:
        entity_type, is_aggregate = config.parse_es_index_name(index_name)
        assert is_aggregate == issubclass(cls, Aggregate)
        return entity_type

    @property
    def document_id(self) -> str:
        return self.entity.entity_id

    @property
    def document_index(self) -> str:
        return self.index_name(self.entity.entity_type)

    @property
    def coordinates(self) -> DocumentCoordinates:
        return DocumentCoordinates(document_index=self.document_index,
                                   document_id=self.document_id)

    def to_source(self) -> JSON:
        return dict(entity_id=self.entity.entity_id,
                    contents=self.contents)

    def to_dict(self) -> JSON:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def _from_source(cls, source: JSON) -> Mapping[str, Any]:
        return {}

    @classmethod
    def mandatory_source_fields(cls) -> List[str]:
        """
        A list of field paths into the source of each document that are expected to be present. Subclasses that
        override _from_source() should override this method, too, such that the list returned by this method mentions
        the name of every field expected by _from_source().
        """
        return ['entity_id']

    @classmethod
    def from_index(cls, field_types, hit: JSON) -> 'Document':
        source = cls.translate_fields(hit['_source'], field_types, forward=False)
        # noinspection PyArgumentList
        # https://youtrack.jetbrains.com/issue/PY-28506
        self = cls(entity=EntityReference(entity_type=cls.entity_type(hit['_index']),
                                          entity_id=source['entity_id']),
                   version=hit.get('_version', 0),
                   contents=source.get('contents'),
                   **cls._from_source(source))
        return self

    def to_index(self, field_types, bulk=False) -> dict:
        """
        Build request parameters from the document for indexing

        :param field_types: A mapping of field paths to field type
        :param bulk: If bulk indexing
        :return: Request parameters for indexing
        """
        delete = self.delete
        result = {
            '_index' if bulk else 'index': self.document_index,
            '_type' if bulk else 'doc_type': self.type,
            **({} if delete else {'_source' if bulk else 'body': self.translate_fields(self.to_source(), field_types)}),
            '_id' if bulk else 'id': self.document_id
        }
        if self.version_type is VersionType.none:
            assert self.version is None
            if bulk:
                result['_op_type'] = 'delete' if delete else 'index'
            else:
                # For non-bulk updates, the op-type is determined by which
                # client method is invoked.
                pass
        elif self.version_type is VersionType.create_only:
            assert self.version is None
            if bulk:
                result['_op_type'] = 'delete' if delete else 'create'
            else:
                result['op_type'] = 'create'
        elif self.version_type is VersionType.internal:
            if bulk:
                result['_op_type'] = 'delete' if delete else ('create' if self.version is None else 'index')
            elif not delete:
                if self.version is None:
                    result['op_type'] = 'create'
            else:
                # For non-bulk updates 'delete' is not a possible op-type.
                # Instead, self.delete controls which client method is invoked.
                pass
            result['_version_type' if bulk else 'version_type'] = 'internal'
            result['_version' if bulk else 'version'] = self.version
        else:
            assert False
        return result

    @property
    def delete(self):
        return False


@dataclass
class Contribution(Document):
    bundle_uuid: BundleUUID
    bundle_version: BundleVersion
    bundle_deleted: bool

    def __post_init__(self):
        # The version_type attribute will change to VersionType.none if writing
        # to Elasticsearch fails with 409. The reason we provide a default for
        # version_type at the class level is due to limitations with @dataclass.
        self.version_type = VersionType.create_only

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'bundle_uuid': None,  # No translation needed on this str field, field will never be None
            'bundle_version': None,  # No translation needed on this str field, field will never be None
            'bundle_deleted': None,  # No translation needed on this bool field, field will never be None
        }

    @property
    def document_id(self) -> str:
        return self.make_document_id(self.entity.entity_id, self.bundle_uuid, self.bundle_version, self.bundle_deleted)

    @classmethod
    def make_document_id(cls,
                         entity_id: EntityID,
                         bundle_uuid: BundleUUID,
                         bundle_version: BundleVersion,
                         bundle_deleted: bool):
        document_id = entity_id, bundle_uuid, bundle_version, 'deleted' if bundle_deleted else 'exists'
        return '_'.join(document_id)

    @classmethod
    def _from_source(cls, source: JSON) -> Mapping[str, Any]:
        return dict(super()._from_source(source),
                    bundle_uuid=source['bundle_uuid'],
                    bundle_version=source['bundle_version'],
                    bundle_deleted=source['bundle_deleted'])

    @classmethod
    def mandatory_source_fields(cls) -> List[str]:
        return super().mandatory_source_fields() + ['bundle_uuid', 'bundle_version', 'bundle_deleted']

    def to_source(self):
        return dict(super().to_source(),
                    bundle_uuid=self.bundle_uuid,
                    bundle_version=self.bundle_version,
                    bundle_deleted=self.bundle_deleted)


@dataclass
class Aggregate(Document):
    bundles: Optional[List[JSON]]
    num_contributions: int

    def __post_init__(self):
        # Cannot provide a default for version_type at the class level due to
        # limitations with @dataclass.
        self.version_type = VersionType.internal

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'num_contributions': None,  # Exclude count value field from translations, field will never be None
            'bundles': None  # Exclude bundle uuid and version values from translations
        }

    @classmethod
    def _from_source(cls, source: JSON) -> Mapping[str, Any]:
        return dict(super()._from_source(source),
                    num_contributions=source['num_contributions'],
                    bundles=source.get('bundles'))

    @classmethod
    def mandatory_source_fields(cls) -> List[str]:
        return super().mandatory_source_fields() + ['num_contributions']

    def to_source(self) -> JSON:
        return dict(super().to_source(),
                    num_contributions=self.num_contributions,
                    bundles=self.bundles)

    @property
    def delete(self):
        # Aggregates are deleted when their contents goes blank
        return super().delete or not self.contents


class Transformer(ABC):

    @classmethod
    @abstractmethod
    def field_types(cls) -> FieldTypes:
        raise NotImplementedError()

    @abstractmethod
    def transform(self,
                  uuid: BundleUUID,
                  version: BundleVersion,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]) -> Iterable[Contribution]:
        """
        Given the metadata for a particular bundle, compute a list of contributions to Elasticsearch documents. The
        contributions constitute partial documents, e.g. their `bundles` attribute is a singleton list, representing
        only the contributions by the specified bundle. Before the contributions can be persisted, they need to be
        merged with contributions by all other bundles.

        :param uuid: The UUID of the bundle to create documents for
        :param version: The version of the bundle to create documents for
        :param deleted: Whether or not the bundle being indexed is a deleted bundle
        :param manifest:  The bundle manifest entries for all data and metadata files in the bundle
        :param metadata_files: The contents of all metadata files in the bundle
        :return: The document contributions
        """
        raise NotImplementedError()


class Accumulator(ABC):
    """
    Accumulates multiple values into a single value, not necessarily of the same type.
    """

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
    """
    Add values.

    Unlike the sum() built-in, this accumulator doesn't default to an initial
    value of 0 but defaults to the first accumulated value instead.
    """

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
    """
    Accumulates values into a set, discarding duplicates and, optionally, values
    that would grow the set past the maximum size. The accumulated value is
    returned as a sorted list. The maximum size constraint does not take the
    ordering into account. This accumulator does not return a list of the N
    smallest values, it returns a sorted list of the first N distinct values.
    """

    def __init__(self, max_size=None, key=None) -> None:
        """
        :param max_size: the maximum number of elemens to retain

        :param key: The key to be used for sorting the accumulated set of
                    values. If this value is None, a default None-safe key will
                    be used. With that default key, if any None values were
                    placed in the accumulator, the first element, and only the
                    first element of the returned list will be None.
        """
        super().__init__()
        self.value = set()
        self.max_size = max_size
        self.key = none_safe_key(none_last=True) if key is None else key

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
        return sorted(self.value, key=self.key)


class ListAccumulator(Accumulator):
    """
    Accumulate values into a list, optionally discarding values that
    would grow the list past the maximum size, if specified.
    """

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
        return sorted(self.value)


class SetOfDictAccumulator(SetAccumulator):
    """
    A set accumulator that supports mutable mappings as values.
    """

    def accumulate(self, value) -> bool:
        return super().accumulate(freeze(value))

    def get(self):
        return thaw(super().get())


class FrequencySetAccumulator(Accumulator):
    """
    An accumulator that accepts any number of values and returns a list with length max_size or smaller containing
    the most frequent values accumulated.

    >>> a = FrequencySetAccumulator(2)
    >>> a.accumulate('x')
    >>> a.accumulate(['x','y'])
    >>> a.accumulate({'x','y','z'})
    >>> a.get()
    ['x', 'y']
    >>> a = FrequencySetAccumulator(0)
    >>> a.accumulate('x')
    >>> a.get()
    []
    """

    def __init__(self, max_size) -> None:
        super().__init__()
        self.value = Counter()
        self.max_size = max_size

    def accumulate(self, value) -> None:
        if isinstance(value, (dict, list, set)):
            self.value.update(value)
        else:
            self.value[value] += 1

    def get(self) -> List[Any]:
        return [item for item, count in self.value.most_common(self.max_size)]


class LastValueAccumulator(Accumulator):
    """
    An accumulator that accepts any number of values and returns the value most recently seen.
    """

    def __init__(self) -> None:
        super().__init__()
        self.value = None

    def accumulate(self, value):
        self.value = value

    def get(self):
        return self.value


class SingleValueAccumulator(LastValueAccumulator):
    """
    An accumulator that accepts any number of values given that they all are the same value and returns a single value.
    Occurrence of any value that is different than the first accumulated value raises a ValueError.
    """

    def accumulate(self, value):
        if self.value is None:
            super().accumulate(value)
        elif self.value != value:
            raise ValueError('Conflicting values:', self.value, value)


class OptionalValueAccumulator(LastValueAccumulator):
    """
    An accumulator that accepts at most one value and returns it.
    Occurrence of more than one value, same or different, raises a ValueError.
    """

    def accumulate(self, value):
        if self.value is None:
            super().accumulate(value)
        else:
            raise ValueError('Conflicting values:', self.value, value)


class MandatoryValueAccumulator(OptionalValueAccumulator):
    """
    An accumulator that requires exactly one value and returns it.
    Occurrence of more than one value or no value at all raises a ValueError.
    """

    def get(self):
        if self.value is None:
            raise ValueError('No value')
        else:
            return super().get()


class PriorityOptionalValueAccumulator(OptionalValueAccumulator):
    """
    An OptionalValueAccumulator that accepts (priority, value) tuples and
    returns the value whose priority is equal to the maximum priority observed.
    Occurrence of more than one value per priority raises a ValueError.
    """

    def __init__(self) -> None:
        super().__init__()
        self.priority = None

    def accumulate(self, value):
        priority, value = value
        if self.priority is None or self.priority < priority:
            self.priority = priority
            self.value = None
        if self.priority == priority:
            super().accumulate(value)


class MinAccumulator(LastValueAccumulator):
    """
    An accumulator that returns the minimal value seen.
    """

    def accumulate(self, value):
        if value is not None and (self.value is None or value < self.value):
            super().accumulate(value)


class MaxAccumulator(LastValueAccumulator):
    """
    An accumulator that returns the maximal value seen.
    """

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


class UniqueValueCountAccumulator(Accumulator):
    """
    Count the number of unique values
    """

    def __init__(self):
        super().__init__()
        self.value = SetAccumulator()

    def accumulate(self, value) -> bool:
        """
        :return: True, if the given value increased the count of unique values
        """
        return self.value.accumulate(value)

    def get(self) -> int:
        unique_items = self.value.get()
        return len(unique_items)


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
        ] if aggregate else []

    def _accumulate(self, aggregate: MutableMapping[str, Optional[Accumulator]], entity: JSON):
        entity = self._transform_entity(entity)
        for field_, value in entity.items():
            try:
                accumulator = aggregate[field_]
            except Exception:
                accumulator = self._get_accumulator(field_)
                aggregate[field_] = accumulator
            if accumulator is not None:
                accumulator.accumulate(value)


class GroupingAggregator(SimpleAggregator):

    def aggregate(self, entities: Entities) -> Entities:
        aggregates: MutableMapping[Any, MutableMapping[str, Optional[Accumulator]]] = defaultdict(dict)
        for entity in entities:
            group_key = self._group_keys(entity)
            if isinstance(group_key, (list, set)):
                group_key = frozenset(group_key)
            aggregate = aggregates[group_key]
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
    def _group_keys(self, entity) -> Iterable[Any]:
        raise NotImplementedError


CollatedEntities = MutableMapping[EntityID, Tuple[BundleUUID, BundleVersion, JSON]]


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

    def aggregate(self, contributions: List[Contribution]) -> JSON:
        contents = self._select_latest(contributions)
        aggregate_contents = {}
        for entity_type, entities in contents.items():
            if entity_type == self.entity_type():
                assert len(entities) == 1
            else:
                aggregator = self.get_aggregator(entity_type)
                if aggregator is not None:
                    entities = aggregator.aggregate(contents[entity_type])
            aggregate_contents[entity_type] = entities
        return aggregate_contents

    def _select_latest(self, contributions: Sequence[Contribution]) -> MutableMapping[EntityType, Entities]:
        """
        Collect the latest version of each inner entity from multiple given documents.

        If two or more contributions contain copies of the same inner entity, potentially with different contents, the
        copy from the contribution with the latest bundle version will be selected.
        """
        if len(contributions) == 1:
            return one(contributions).contents
        else:
            contents: MutableMapping[EntityType, CollatedEntities] = defaultdict(dict)
            for contribution in contributions:
                for entity_type, entities in contribution.contents.items():
                    collated_entities = contents[entity_type]
                    entity: JSON
                    for entity in entities:
                        entity_id = entity['document_id']  # FIXME: the key 'document_id' is HCA specific
                        cur_bundle_uuid, cur_bundle_version, cur_entity = \
                            collated_entities.get(entity_id, (None, '', None))
                        if cur_entity is not None and entity.keys() != cur_entity.keys():
                            symmetric_difference = set(entity.keys()).symmetric_difference(cur_entity)
                            logger.warning('Document shape of `%s` entity `%s` does not match between bundles '
                                           '%s, version %s and %s, version %s: %s',
                                           entity_type, entity_id,
                                           cur_bundle_uuid, cur_bundle_version,
                                           contribution.bundle_uuid,
                                           contribution.bundle_version,
                                           symmetric_difference)
                        if cur_bundle_version < contribution.bundle_version:
                            collated_entities[entity_id] = contribution.bundle_uuid, contribution.bundle_version, entity
            return {
                entity_type: [entity for _, _, entity in entities.values()]
                for entity_type, entities in contents.items()
            }
