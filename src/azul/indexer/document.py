from abc import (
    ABCMeta,
    abstractmethod,
)
from dataclasses import (
    dataclass,
    field,
    fields,
)
from enum import (
    Enum,
    auto,
)
import sys
from typing import (
    Any,
    ClassVar,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import attr
from humancellatlas.data.metadata import (
    api,
)

from azul import (
    CatalogName,
    IndexName,
    config,
)
from azul.indexer import (
    BundleFQID,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
)

EntityID = str
EntityType = str


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class EntityReference:
    entity_type: EntityType
    entity_id: EntityID

    def __str__(self) -> str:
        return f'{self.entity_type}/{self.entity_id}'


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class CataloguedEntityReference(EntityReference):
    catalog: CatalogName

    def __str__(self) -> str:
        return f'{self.catalog}/{super().__str__()}'

    @classmethod
    def for_entity(cls, catalog: CatalogName, entity: EntityReference):
        return cls(catalog=catalog,
                   entity_type=entity.entity_type,
                   entity_id=entity.entity_id)


E = TypeVar('E', bound=EntityReference)


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class DocumentCoordinates(Generic[E], metaclass=ABCMeta):
    """
    Document coordinates of contributions. Contributions produced by
    transformers don't specify a catalog, the catalog is supplied when the
    contributions are written to the index and it is guaranteed to be the same
    for all contributions produced in response to one notification. When
    contributions are read back during aggregation, they specify a catalog, the
    catalog they were read from. Because of that duality this class has to
    be generic in E, the type of EntityReference.
    """
    type: ClassVar[str] = 'doc'
    entity: E
    aggregate: bool

    @property
    def index_name(self) -> str:
        """
        The fully qualifed name of the Elasticsearch index for a document with
        these coordinates. Only call this if these coordinates use a catalogued
        entity reference. You can use `.with_catalog()` to create one.
        """
        assert isinstance(self.entity, CataloguedEntityReference)
        return config.es_index_name(catalog=self.entity.catalog,
                                    entity_type=self.entity.entity_type,
                                    aggregate=self.aggregate)

    @property
    @abstractmethod
    def document_id(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_hit(cls, hit: JSON) -> 'DocumentCoordinates[CataloguedEntityReference]':
        index_name = config.parse_es_index_name(hit['_index'])
        document_id = hit['_id']
        subcls = AggregateCoordinates if index_name.aggregate else ContributionCoordinates
        assert issubclass(subcls, cls)
        return subcls._from_index(index_name, document_id)

    @classmethod
    @abstractmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> 'DocumentCoordinates[CataloguedEntityReference]':
        raise NotImplementedError

    def with_catalog(self, catalog: Optional[CatalogName]) -> 'DocumentCoordinates[CataloguedEntityReference]':
        """
        Return coordinates for the given catalog. Only works for instances that
        have no catalog or ones having the same catalog in which case `self` is
        returned.
        """
        if isinstance(self.entity, CataloguedEntityReference):
            if catalog is not None:
                assert self.entity.catalog == catalog, (self.entity.catalog, catalog)
            return self
        else:
            assert catalog is not None
            return attr.evolve(self, entity=CataloguedEntityReference.for_entity(catalog, self.entity))


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class ContributionCoordinates(DocumentCoordinates[E], Generic[E]):
    aggregate: bool = attr.ib(init=False, default=False)
    bundle: BundleFQID
    deleted: bool

    @property
    def document_id(self) -> str:
        return '_'.join((
            self.entity.entity_id,
            self.bundle.uuid,
            self.bundle.version,
            'deleted' if self.deleted else 'exists'
        ))

    @classmethod
    def _from_index(cls, index_name: IndexName,
                    document_id: str) -> 'ContributionCoordinates[CataloguedEntityReference]':
        entity_type = index_name.entity_type
        assert index_name.aggregate is False
        entity_id, bundle_uuid, bundle_version, deleted = document_id.split('_')
        if deleted == 'deleted':
            deleted = True
        elif deleted == 'exists':
            deleted = False
        else:
            assert False, deleted
        entity = CataloguedEntityReference(catalog=index_name.catalog,
                                           entity_type=entity_type,
                                           entity_id=entity_id)
        return cls(entity=entity,
                   bundle=BundleFQID(uuid=bundle_uuid, version=bundle_version),
                   deleted=deleted)

    def __str__(self) -> str:
        return ' '.join((
            'deletion of' if self.deleted else 'contribution to',
            str(self.entity),
            'by bundle', self.bundle.uuid, 'at', self.bundle.version
        ))


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class AggregateCoordinates(DocumentCoordinates[CataloguedEntityReference]):
    """
    Document coordinates for aggregates. Aggregate coordinates always carry a
    catalog.
    """
    aggregate: bool = attr.ib(init=False, default=True)

    @classmethod
    def _from_index(cls, index_name: IndexName, document_id: str) -> 'AggregateCoordinates':
        entity_type = index_name.entity_type
        assert index_name.aggregate is True
        return cls(entity=CataloguedEntityReference(catalog=index_name.catalog,
                                                    entity_type=entity_type,
                                                    entity_id=document_id))

    def __attrs_post_init__(self):
        assert isinstance(self.entity, CataloguedEntityReference), type(self.entity)

    @property
    def document_id(self) -> str:
        return self.entity.entity_id

    def __str__(self) -> str:
        return f'aggregate for {self.entity}'


FieldType = Union[type, None]
FieldTypes4 = Union[Mapping[str, FieldType], FieldType]
FieldTypes3 = Union[Mapping[str, FieldTypes4], FieldType]
FieldTypes2 = Union[Mapping[str, FieldTypes3], FieldType]
FieldTypes1 = Union[Mapping[str, FieldTypes2], FieldType]
FieldTypes = Mapping[str, FieldTypes1]
CataloguedFieldTypes = Mapping[CatalogName, FieldTypes]


class VersionType(Enum):
    # No versioning; document is created or overwritten as needed
    none = auto()

    # Writing a document fails with 409 conflict if one with the same ID already
    # exists in the index
    create_only = auto()

    # Use the Elasticsearch "internal" versioning type
    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docs-index_.html#_version_types
    internal = auto()


C = TypeVar('C', bound=DocumentCoordinates)


@dataclass
class Document(Generic[C]):
    type: ClassVar[str] = DocumentCoordinates.type

    coordinates: C
    version_type: VersionType = field(init=False)
    version: Optional[int]
    contents: Optional[JSON]

    @property
    def entity(self) -> EntityReference:
        return self.coordinates.entity

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
                         *,
                         forward: bool,
                         path: Tuple[str, ...] = ()
                         ) -> AnyMutableJSON:
        """
        Traverse a document to translate field values for insert into
        Elasticsearch, or to translate back response data. This is done to
        support None/null values since Elasticsearch does not index these
        values. Values that are empty lists ([]) and lists of None ([None]) are
        both forward converted to [null_string]

        :param doc: A document dict of values

        :param field_types: A mapping of field paths to field type

        :param forward: If True, substitute None values with their respective
                        Elasticsearch placeholder.

        :param path: Used internally during document traversal to capture the
                     current path into the document as a tuple of keys.

        :return: A copy of the original document with values translated
                 according to their type.
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
                    new_dict[key] = cls.translate_fields(val, field_type, forward=forward, path=(*path, key))
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
                return [cls.translate_fields(val, field_types, forward=forward, path=path) for val in doc]
            else:
                assert len(doc) == 0 and isinstance(doc, list) and isinstance(field_types, type)
                assert forward, path
                return cls.translate_fields([None], field_types, forward=forward, path=path)
        else:
            return cls.translate_field(doc, field_types, forward=forward)

    def to_source(self) -> JSON:
        return dict(entity_id=self.coordinates.entity.entity_id,
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
    def from_index(cls, field_types: CataloguedFieldTypes, hit: JSON) -> 'Document':
        coordinates = DocumentCoordinates.from_hit(hit)
        if 'contents' in hit['_source']:
            file: JSON
            content_descriptions = [
                file['content_description']
                for file in hit['_source']['contents']['files']
            ]
            assert [] not in content_descriptions, 'Found empty list as content_description value'
        source = cls.translate_fields(hit['_source'], field_types[coordinates.entity.catalog], forward=False)
        # noinspection PyArgumentList
        # https://youtrack.jetbrains.com/issue/PY-28506
        self = cls(coordinates=coordinates,
                   version=hit.get('_version', 0),
                   contents=source.get('contents'),
                   **cls._from_source(source))
        return self

    def to_index(self, catalog: Optional[CatalogName], field_types: CataloguedFieldTypes, bulk: bool = False) -> JSON:
        """
        Build request parameters from the document for indexing

        :param catalog: An optional catalog name. If None, this document's
                        coordinates must supply it. Otherwise this document's
                        coordinates must supply the same catalog or none at all.
        :param field_types: A mapping of field paths to field type
        :param bulk: If bulk indexing
        :return: Request parameters for indexing
        """
        delete = self.delete
        coordinates = self.coordinates.with_catalog(catalog)
        result = {
            '_index' if bulk else 'index': coordinates.index_name,
            '_type' if bulk else 'doc_type': self.coordinates.type,
            **(
                {}
                if delete else
                {
                    '_source' if bulk else 'body':
                        self.translate_fields(doc=self.to_source(),
                                              field_types=field_types[coordinates.entity.catalog],
                                              forward=True)
                }
            ),
            '_id' if bulk else 'id': self.coordinates.document_id
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
class Contribution(Document[ContributionCoordinates[E]]):

    def __post_init__(self):
        assert isinstance(self.coordinates, ContributionCoordinates)
        assert self.coordinates.aggregate is False
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

    @classmethod
    def mandatory_source_fields(cls) -> List[str]:
        return super().mandatory_source_fields() + ['bundle_uuid', 'bundle_version', 'bundle_deleted']

    def to_source(self):
        return dict(super().to_source(),
                    bundle_uuid=self.coordinates.bundle.uuid,
                    bundle_version=self.coordinates.bundle.version,
                    bundle_deleted=self.coordinates.deleted)


@dataclass
class Aggregate(Document[AggregateCoordinates]):
    bundles: Optional[List[JSON]]
    num_contributions: int

    def __post_init__(self):
        assert isinstance(self.coordinates, AggregateCoordinates)
        assert self.coordinates.aggregate is True
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


CataloguedContribution = Contribution[CataloguedEntityReference]
