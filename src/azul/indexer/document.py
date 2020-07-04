from enum import (
    Enum,
    auto,
)
import sys
from typing import (
    Any,
    ClassVar,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Union,
)

from dataclasses import (
    dataclass,
    field,
    fields,
)
from humancellatlas.data.metadata import api

from azul import config
from azul.indexer import (
    BundleUUID,
    BundleVersion,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
)

EntityID = str
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
                         forward: bool = True,
                         path: list = None) -> AnyMutableJSON:
        """
        Traverse a document to translate field values for insert into Elasticsearch, or to translate back
        response data. This is done to support None/null values since Elasticsearch does not index these values.
        Values that are empty lists ([]) and lists of None ([None]) are both forward converted to [null_string]

        :param doc: A document dict of values
        :param field_types: A mapping of field paths to field type
        :param forward: If we should translate forward or backward (aka un-translate)
        :param path:
        :return: A copy of the original document with values translated according to their type
        """
        if field_types is None:
            return doc
        elif isinstance(doc, dict):
            new_dict = {}
            for key, val in doc.items():
                if path is None:
                    path = []
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
                    new_dict[key] = cls.translate_fields(val, field_type, forward=forward, path=path + [key])
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
                return cls.translate_fields([None], field_types, path=path)
        else:
            return cls.translate_field(doc, field_types, forward=forward)

    @classmethod
    def index_name(cls, entity_type: EntityType) -> str:
        return config.es_index_name(entity_type, aggregate=issubclass(cls, Aggregate))

    @classmethod
    def entity_type(cls, index_name: str) -> EntityType:
        index_name = config.parse_es_index_name(index_name)
        assert index_name.aggregate == issubclass(cls, Aggregate)
        return index_name.entity_type

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
        if 'contents' in hit['_source']:
            file: JSON
            content_descriptions = [
                file['content_description']
                for file in hit['_source']['contents']['files']
            ]
            assert [] not in content_descriptions, 'Found empty list as content_description value'
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
