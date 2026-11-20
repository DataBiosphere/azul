from abc import (
    ABCMeta,
    abstractmethod,
)
from enum import (
    Enum,
)
import re
from typing import (
    ClassVar,
    Self,
    overload,
)

from attrs import (
    define,
    evolve,
    frozen,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.field_type import (
    CataloguedFieldTypes,
    FieldType,
    FieldTypes,
    null_str,
    pass_thru_bool,
    pass_thru_int,
    pass_thru_json,
    pass_thru_str,
)
from azul.indexer import (
    BundleFQID,
)
from azul.lib import (
    R,
)
from azul.lib.enums import (
    auto,
)
from azul.lib.json import (
    Parseable,
)
from azul.lib.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
    MutableJSON,
    json_int,
    json_mapping,
    json_sequence,
    json_str,
    optional,
)
from azul.source import (
    SourceRef,
)

type EntityID = str
type EntityType = str


@frozen(kw_only=True, order=True)
class EntityReference(Parseable):
    entity_type: EntityType
    entity_id: EntityID

    def __str__(self) -> str:
        return f'{self.entity_type}/{self.entity_id}'

    @classmethod
    def parse(cls, s: str) -> Self:
        entity_type, entity_id = s.split('/')
        return cls(entity_type=entity_type, entity_id=entity_id)


@frozen(kw_only=True)
class CataloguedEntityReference(EntityReference):
    catalog: CatalogName

    def __str__(self) -> str:
        return f'{self.catalog}/{super().__str__()}'

    @classmethod
    def for_entity(cls, catalog: CatalogName, entity: EntityReference):
        return cls(catalog=catalog,
                   entity_type=entity.entity_type,
                   entity_id=entity.entity_id)


class DocumentType(Enum):
    contribution = 'contribution'
    aggregate = 'aggregate'
    replica = 'replica'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}.{self._name_}>'


@frozen(kw_only=True)
class IndexName:
    """
    The name of an OpenSearch index used by an Azul deployment, parsed into
    its components. The index naming scheme underwent a number of changes during
    the evolution of Azul. The different naming schemes are captured in a
    `version` component. Note that the first version of the index name syntax
    did not carry an explicit version. The resulting ambiguity requires entity
    types to not match the version regex below.
    """
    #: Every index name starts with this prefix
    prefix: ClassVar[str] = 'azul'

    #: The version of the index naming scheme
    version: int

    #: The name of the deployment the index belongs to
    deployment: str

    #: The catalog the index belongs to
    catalog: CatalogName

    #: An additional qualifier to distinguish between indices of the same
    #: `doc_type`. For indices containing contribution or aggregate documents,
    #: for example, this is the name of the type of entity the documents contain
    #: metadata about.
    qualifier: str

    #: Whether the documents in the index are contributions, aggregates, or
    #: replicas
    doc_type: DocumentType

    index_name_version_re: ClassVar[re.Pattern] = re.compile(r'v(\d+)')

    def __attrs_post_init__(self):
        """
        >>> IndexName(version=2,
        ...           deployment='dev',
        ...           catalog='main',
        ...           qualifier='foo_bar',
        ...           doc_type=DocumentType.contribution)
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName(version=1,
        ...           deployment='',
        ...           catalog='',
        ...           qualifier='',
        ...           doc_type=DocumentType.contribution)
        Traceback (most recent call last):
        ...
        AssertionError: R('Version must be 2', 1)

        >>> IndexName(version=2,
        ...           deployment='dev',
        ...           catalog=None,  # noqa
        ...           qualifier='foo',
        ...           doc_type=DocumentType.contribution)
        Traceback (most recent call last):
        ...
        AssertionError: R('Catalog name is required', None)

        >>> IndexName(version=2,
        ...           deployment='_',
        ...           catalog='foo',
        ...           qualifier='bar',
        ...           doc_type=DocumentType.contribution)
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R("Deployment name '_' is too short, too long
        or contains invalid characters.")

        >>> IndexName(version=2,
        ...           deployment='dev',
        ...           catalog='_',
        ...           qualifier='bar',
        ...           doc_type=DocumentType.contribution)
        Traceback (most recent call last):
        ...
        AssertionError: R('Catalog name is invalid', '_')

        >>> IndexName(version=2,
        ...           deployment='dev',
        ...           catalog='foo',
        ...           qualifier='_',
        ...           doc_type=DocumentType.contribution)
        ... # doctest: +NORMALIZE_WHITESPACE
        Traceback (most recent call last):
        ...
        AssertionError: R("qualifier is either too short, too long
        or contains invalid characters: '_'")

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='hca',
        ...               qualifier='foo',
        ...               doc_type=DocumentType.replica))
        Traceback (most recent call last):
        ...
        AssertionError: R('Unexpected replica qualifier', 'foo')
        """
        config.validate_prefix(self.prefix)
        assert self.version == 2, R('Version must be 2', self.version)
        config.validate_deployment_name(self.deployment)
        assert self.catalog is not None, R('Catalog name is required', self.catalog)
        config.Catalog.validate_name(self.catalog)
        config.validate_qualifier(self.qualifier)
        if self.doc_type is DocumentType.replica:
            # To shorten the string representation of replica index names, we
            # expect the qualifier and document type to be the same string.
            assert self.qualifier == self.doc_type.value, R(
                'Unexpected replica qualifier', self.qualifier)
        assert '_' not in self.prefix, self.prefix
        assert '_' not in self.deployment, self.deployment
        assert self.catalog is None or '_' not in self.catalog, self.catalog

    def validate(self):
        assert self.deployment == config.deployment_stage, R(
            'Index name does not use current deployment',
            self, config.deployment_stage)

    @classmethod
    def create(cls,
               *,
               catalog: CatalogName,
               qualifier: str,
               doc_type: DocumentType
               ) -> Self:
        return cls(version=2,
                   deployment=config.deployment_stage,
                   catalog=catalog,
                   qualifier=qualifier,
                   doc_type=doc_type)

    @classmethod
    def parse(cls, index_name: str) -> Self:
        """
        Parse the name of an index from any deployment and any version of Azul.

        >>> IndexName.parse('azul_dev')
        Traceback (most recent call last):
        ...
        AssertionError: R('Too few index name elements', ['azul', 'dev'])

        >>> IndexName.parse('azul_foo_dev')
        Traceback (most recent call last):
        ...
        AssertionError: R('Version is required')

        >>> IndexName.parse('azl_v2_dev_main_foo')
        Traceback (most recent call last):
        ...
        AssertionError: R('Unexpected prefix', 'azul', 'azl')

        >>> IndexName.parse('azul_v2_dev_main_foo')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='foo',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_v2_dev_main_foo_aggregate')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='foo',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar_aggregate')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='foo_bar',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_staging_hca_foo_bar_aggregate')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='staging',
                  catalog='hca',
                  qualifier='foo_bar',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_dev_main_replica')
        ... # doctest: +NORMALIZE_WHITESPACE
        IndexName(version=2,
                  deployment='dev',
                  catalog='main',
                  qualifier='replica',
                  doc_type=<DocumentType.replica>)

        >>> IndexName.parse('azul_v2_staging__foo_bar__aggregate')
        ... # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        AssertionError: R("qualifier ... 'foo_bar_'")

        >>> IndexName.parse('azul_v3_bla')
        Traceback (most recent call last):
        ...
        AssertionError: R('Version must be 2', 3)
        """
        index_name = index_name.split('_')
        assert len(index_name) > 2, R('Too few index name elements', index_name)
        prefix, *index_name = index_name
        assert prefix == cls.prefix, R('Unexpected prefix', cls.prefix, prefix)
        version = cls.index_name_version_re.fullmatch(index_name[0])
        assert version is not None, R('Version is required')
        _, *index_name = index_name
        version = int(version.group(1))
        assert version == 2, R('Version must be 2', version)
        deployment, catalog, *index_name = index_name
        if index_name[-1] == DocumentType.aggregate.value:
            *index_name, _ = index_name
            doc_type = DocumentType.aggregate
        elif index_name == [DocumentType.replica.value]:
            doc_type = DocumentType.replica
        else:
            doc_type = DocumentType.contribution
        qualifier = '_'.join(index_name)
        config.validate_qualifier(qualifier)
        self = cls(version=version,
                   deployment=deployment,
                   catalog=catalog,
                   qualifier=qualifier,
                   doc_type=doc_type)
        return self

    def __str__(self) -> str:
        """
        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               qualifier='foo',
        ...               doc_type=DocumentType.contribution))
        'azul_v2_dev_main_foo'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               qualifier='foo',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_dev_main_foo_aggregate'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               qualifier='foo_bar',
        ...               doc_type=DocumentType.contribution))
        'azul_v2_dev_main_foo_bar'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               qualifier='foo_bar',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_dev_main_foo_bar_aggregate'

        >>> str(IndexName(version=2,
        ...               deployment='staging',
        ...               catalog='hca',
        ...               qualifier='foo_bar',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_staging_hca_foo_bar_aggregate'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='hca',
        ...               qualifier='replica',
        ...               doc_type=DocumentType.replica))
        'azul_v2_dev_hca_replica'
        """
        if self.doc_type is DocumentType.aggregate:
            doc_type = ['aggregate']
        elif self.doc_type is DocumentType.contribution:
            doc_type = []
        elif self.doc_type is DocumentType.replica:
            assert self.qualifier == self.doc_type.value
            doc_type = []
        else:
            assert False, self.doc_type
        assert self.version == 2, self
        assert self.catalog is not None, R('Catalog is required')
        return '_'.join([
            self.prefix,
            f'v{self.version}',
            self.deployment,
            self.catalog,
            self.qualifier,
            *doc_type,
        ])


type CataloguedDocumentCoordinates = DocumentCoordinates[CataloguedEntityReference]


@frozen(kw_only=True)
class DocumentCoordinates[E: EntityReference](metaclass=ABCMeta):
    """
    The coordinates of a document ultimately define two strings: 1) the name of
    the OpenSearch index that contains the document and 2) the unique ID by
    which it can be retrieved from that index. Both of these strings are
    composed of smaller elements information, e.g., a reference to the entity
    the document contains metadata about and the type of the document. Concrete
    subclasses typically add more such elements to be encoded in their index
    names and document IDs.
    """

    doc_type: ClassVar[DocumentType]

    entity: E

    @property
    def index_name(self) -> str:
        """
        The fully qualified name of the OpenSearch index for a document with
        these coordinates. Only call this if these coordinates use a catalogued
        entity reference. You can use `.with_catalog()` to create one.
        """
        assert isinstance(self.entity, CataloguedEntityReference)
        return str(IndexName.create(catalog=self.entity.catalog,
                                    qualifier=self.index_qualifier,
                                    doc_type=self.doc_type))

    @property
    def index_qualifier(self):
        return self.entity.entity_type

    @property
    @abstractmethod
    def document_id(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_hit(cls, hit: JSON) -> CataloguedDocumentCoordinates:
        index_name = IndexName.parse(json_str(hit['_index']))
        index_name.validate()
        document_id = json_str(hit['_id'])
        subcls: type[CataloguedDocumentCoordinates]
        if index_name.doc_type is DocumentType.contribution:
            subcls = ContributionCoordinates
        elif index_name.doc_type is DocumentType.aggregate:
            subcls = AggregateCoordinates
        elif index_name.doc_type is DocumentType.replica:
            subcls = ReplicaCoordinates
        else:
            assert False, index_name.doc_type
        assert issubclass(subcls, DocumentCoordinates)
        return subcls._from_index(index_name, document_id)

    @classmethod
    @abstractmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> CataloguedDocumentCoordinates:
        raise NotImplementedError

    def with_catalog(self: DocumentCoordinates,
                     catalog: CatalogName | None
                     ) -> CataloguedDocumentCoordinates:
        """
        Return coordinates for the given catalog. Only works for instances that
        have no catalog or ones having the same catalog in which case ``self``
        is returned.
        """
        if isinstance(self.entity, CataloguedEntityReference):
            if catalog is not None:
                assert self.entity.catalog == catalog, (self.entity.catalog, catalog)
            return self
        else:
            assert catalog is not None
            entity = CataloguedEntityReference.for_entity(catalog, self.entity)
            return evolve(self, entity=entity)


type CataloguedContributionCoordinates = ContributionCoordinates[CataloguedEntityReference]


@frozen(kw_only=True)
class ContributionCoordinates[E: EntityReference](DocumentCoordinates[E]):
    """
    Coordinates of contribution documents. Contributions originate from a
    subgraph ("bundle") and represent either the addition of metadata to an
    entity or the removal of metadata from an entity.

    Contributions produced by transformers don't specify a catalog. The catalog
    is supplied when the contributions are written to the index and it is
    guaranteed to be the same for all contributions produced in response to one
    notification. When contributions are read back during aggregation, they
    specify a catalog, the catalog they were read from. Because of that duality
    this class has to be generic in E, the type of EntityReference.
    """

    doc_type: ClassVar[DocumentType] = DocumentType.contribution

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
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> CataloguedContributionCoordinates:
        entity_type = index_name.qualifier
        assert index_name.doc_type is DocumentType.contribution
        deleted: str | bool
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
        bundle = BundleFQID(uuid=bundle_uuid, version=bundle_version)
        return ContributionCoordinates(entity=entity, bundle=bundle, deleted=deleted)

    def __str__(self) -> str:
        return ' '.join((
            'deletion of' if self.deleted else 'contribution to',
            str(self.entity),
            'by bundle', self.bundle.uuid, 'at', self.bundle.version
        ))


@frozen(kw_only=True)
class AggregateCoordinates(DocumentCoordinates[CataloguedEntityReference]):
    """
    Coordinates of aggregate documents. Aggregate coordinates always carry a
    catalog.
    """

    doc_type: ClassVar[DocumentType] = DocumentType.aggregate

    @classmethod
    def _from_index(cls, index_name: IndexName, document_id: str) -> Self:
        entity_type = index_name.qualifier
        assert index_name.doc_type is DocumentType.aggregate
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


type CataloguedReplicaCoordinates = ReplicaCoordinates[CataloguedEntityReference]


@frozen(kw_only=True)
class ReplicaCoordinates[E: EntityReference](DocumentCoordinates[E]):
    """
    Coordinates of replica documents. Replicas are content-addressed, so these
    coordinates depend not only on the entity reference, but on the contents of
    the underlying metadata document.
    """

    doc_type: ClassVar[DocumentType] = DocumentType.replica

    #: A hash of the replica's JSON document
    content_hash: str

    # Overrides the property in the base class. We need this to be statically
    # accessible through the class.
    index_qualifier: ClassVar[str] = 'replica'

    # The current v2 index name encoding depends on this
    assert index_qualifier == doc_type.value

    @property
    def document_id(self) -> str:
        return '_'.join((
            self.entity.entity_type,
            self.entity.entity_id,
            self.content_hash
        ))

    @classmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> CataloguedReplicaCoordinates:
        assert index_name.doc_type is DocumentType.replica, index_name
        assert index_name.qualifier == cls.index_qualifier, index_name
        # entity_type, the first component, may contain underscores
        entity_type, entity_id, content_hash = document_id.rsplit('_', 2)
        entity = CataloguedEntityReference(catalog=index_name.catalog,
                                           entity_type=entity_type,
                                           entity_id=entity_id)
        return ReplicaCoordinates(content_hash=content_hash, entity=entity)

    def __str__(self) -> str:
        return f'replica of {self.entity}'


#: The elements of a field path (see below)
#:
FieldPathElement = str

#: The path to a field in an index document. The first (last) element in the
#: path is the key into the outermost (innermost) dictionary in that document.
#:
FieldPath = tuple[FieldPathElement, ...]

InternalVersion = tuple[int, int]


class OpType(Enum):
    #: Write the document to the index, overwriting it if it already exists
    index = auto()

    #: Write the document to the index or fail if it already exists
    create = auto()

    #: Remove the document from the index or fail if it does not exist
    delete = auto()

    #: Modify a document in the index via a scripted update or create it if it
    #: does not exist
    update = auto()


@define(kw_only=True)
class Document[C: DocumentCoordinates](metaclass=ABCMeta):
    needs_translation: ClassVar[bool] = True

    coordinates: C

    version: InternalVersion | None

    # In the index, the `contents` property is always present and never null in
    # documents. In instances of the Aggregate subclass, this attribute is None
    # when they were created from documents that were retrieved from the
    # index while intentionally excluding that property for efficiency. In
    # instances of the Contribution subclass, this attribute is never None.
    #
    contents: JSON | None

    @property
    def entity(self) -> EntityReference:
        return self.coordinates.entity

    @property
    @abstractmethod
    def op_type(self) -> OpType:
        """
        Get the ES client method to use when writing this document to the index.
        """
        raise NotImplementedError

    @op_type.setter
    @abstractmethod
    def op_type(self, value: OpType):
        """
        Set the ES client method to use when writing this document to the index.
        This setter is optional, concrete classes may raise NotImplementedError
        in their implementations and callers must gracefully handle that case.
        """
        raise NotImplementedError

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            'entity_id': null_str,
            'contents': field_types
        }

    @classmethod
    @overload
    def translate_fields(cls,
                         doc: JSON,
                         field_types: FieldType | FieldTypes,
                         *,
                         forward: bool,
                         allowed_paths: list[FieldPath] | None = None
                         ) -> MutableJSON:
        ...

    @classmethod
    @overload
    def translate_fields(cls,
                         doc: AnyJSON,
                         field_types: FieldType | FieldTypes,
                         *,
                         forward: bool,
                         allowed_paths: list[FieldPath] | None = None,
                         path: FieldPath
                         ) -> AnyMutableJSON:
        ...

    @classmethod
    def translate_fields(cls,
                         doc: AnyJSON,
                         field_types: FieldType | FieldTypes,
                         *,
                         forward: bool,
                         allowed_paths: list[FieldPath] | None = None,
                         path: FieldPath = ()
                         ) -> AnyMutableJSON:
        """
        Traverse a document to translate field values for insert into
        OpenSearch, or to translate back response data. This is done to
        support None/null values since OpenSearch does not index these
        values. Values that are empty lists ([]) and lists of None ([None]) are
        both forward converted to [null_string]

        :param doc: A document dict of values

        :param field_types: A mapping of field paths to field type

        :param forward: If True, substitute None values with their respective
                        OpenSearch placeholder.

        :param allowed_paths: A list of field paths expected to be present in
                              the resulting document. If an unexpected field is
                              found, an AssertionError will be raised.

        :param path: Used internally during document traversal to capture the
                     current path into the document as a tuple of keys.

        :return: A copy of the original document with values translated
                 according to their type.
        """
        if isinstance(field_types, dict):
            if isinstance(doc, dict):
                new_doc = {}
                for key, val in doc.items():
                    if key.endswith('_'):
                        # Shadow copy fields should only be present during a reverse
                        # translation and we skip over to remove them.
                        assert not forward, path
                    else:
                        try:
                            field_type = field_types[key]
                        except KeyError:
                            raise KeyError(f'Key {key!r} not defined in field_types')
                        except TypeError:
                            raise TypeError(f'Key {key!r} not defined in field_types')
                        new_doc[key] = cls.translate_fields(val,
                                                            field_type,
                                                            forward=forward,
                                                            allowed_paths=allowed_paths,
                                                            path=(*path, key))
                        if forward and isinstance(field_type, FieldType) and field_type.shadowed:
                            # Add a non-translated shadow copy of this field's
                            # numeric value for sum aggregations
                            new_doc[key + '_'] = val
                return new_doc
            elif isinstance(doc, list):
                return [
                    cls.translate_fields(val,
                                         field_types,
                                         forward=forward,
                                         allowed_paths=allowed_paths,
                                         path=path)
                    for val in doc
                ]
            else:
                assert False, (path, type(doc))
        else:
            if isinstance(field_types, list):
                # FIXME: Assert that a non-list field_type implies a non-list
                #        doc (only possible for contributions).
                #        https://github.com/DataBiosphere/azul/issues/2689
                assert isinstance(doc, list), (doc, path)

                field_types = one(field_types)
            if isinstance(field_types, FieldType):
                field_type = field_types
            else:
                assert False, (path, type(field_types))
            if allowed_paths is not None:
                # An allowed path may be a prefix instead of a complete path,
                # as is the case for `contents.files.related_files`
                assert path in allowed_paths or path[:-1] in allowed_paths, (path, allowed_paths)
            if forward:
                if isinstance(doc, list):
                    if not doc and field_type.allow_sorting_by_empty_lists:
                        # Translate an empty list to a list containing a single
                        # None value (and then further translate that None value
                        # according to the field type) so ES doesn't discard it.
                        # That way, documents with fields that are empty lists
                        # are placed at the beginning (end) of an ascending
                        # (descending) sort. PassTrough fields like
                        # contents.metadata should not undergo this transformation.
                        doc = [None]
                    return [field_type.to_index(value) for value in doc]
                else:
                    return field_type.to_index(doc)
            else:
                if isinstance(doc, list):
                    assert doc or not field_type.allow_sorting_by_empty_lists
                    return [field_type.from_index(value) for value in doc]
                else:
                    return field_type.from_index(doc)

    def to_json(self) -> JSON:
        assert self.contents is not None, self
        return dict(entity_id=self.coordinates.entity.entity_id,
                    contents=self.contents)

    @classmethod
    def from_json(cls,
                  *,
                  coordinates: C,
                  document: JSON,
                  version: InternalVersion | None,
                  **kwargs,
                  ) -> Self:
        self = cls(coordinates=coordinates,
                   version=version,
                   contents=optional(json_mapping, document.get('contents')),
                   **kwargs)
        assert document['entity_id'] == self.entity.entity_id
        return self

    @classmethod
    def mandatory_source_fields(cls) -> list[str]:
        """
        A list of dot-separated field paths into the source of each document
        that :meth:`from_json` expects to be present. Subclasses that override
        that method should also override this one.
        """
        return ['entity_id']

    @classmethod
    def from_index(cls,
                   field_types: CataloguedFieldTypes,
                   hit: JSON,
                   *,
                   coordinates: CataloguedDocumentCoordinates | None = None
                   ) -> Self:
        if coordinates is None:
            coordinates = DocumentCoordinates.from_hit(hit)
        document = json_mapping(hit['_source'])
        if cls.needs_translation:
            document = cls.translate_fields(document,
                                            field_types[coordinates.entity.catalog],
                                            forward=False)
        try:
            version = json_int(hit['_seq_no']), json_int(hit['_primary_term'])
        except KeyError:
            assert '_seq_no' not in hit
            assert '_primary_term' not in hit
            version = None

        assert isinstance(coordinates, cls.coordinate_cls())

        return cls.from_json(coordinates=coordinates,
                             document=json_mapping(document),
                             version=version)

    @classmethod
    @abstractmethod
    def coordinate_cls(cls) -> type[C]:
        pass

    def to_index(self,
                 catalog: CatalogName | None,
                 field_types: CataloguedFieldTypes
                 ) -> JSON:
        """
        Prepare a request to write this document to the index. The return value
        is a dictionary with keyword arguments to the ES client method selected
        by the :meth:`op_type` property.

        :param catalog: An optional catalog name. If None, this document's
                        coordinates must supply it. Otherwise this document's
                        coordinates must supply the same catalog or none at all.

        :param field_types: A mapping of field paths to field type

        :return: Request parameters for indexing
        """
        coordinates = self.coordinates.with_catalog(catalog)
        result: dict[str, AnyJSON] = {
            'index': coordinates.index_name,
            'id': self.coordinates.document_id
        }
        if self.op_type is not OpType.delete:
            result['body'] = self._body(field_types[coordinates.entity.catalog])
        if self.version is not None:
            result['if_seq_no'], result['if_primary_term'] = self.version
        if self.op_type is OpType.update:
            result['params'] = {'retry_on_conflict': 3}
        return result

    def _body(self, field_types: FieldTypes) -> JSON:
        body = self.to_json()
        if self.needs_translation:
            body = self.translate_fields(doc=body,
                                         field_types=field_types,
                                         forward=True)
        return body


@define(kw_only=True)
class Contribution[E: EntityReference](Document[ContributionCoordinates[E]]):

    @classmethod
    def coordinate_cls(cls) -> type[ContributionCoordinates[E]]:
        return ContributionCoordinates

    # This narrows the type declared in the superclass. See comment there.
    contents: JSON
    source: SourceRef

    #: The op_type attribute will change to OpType.index if writing
    #: to OpenSearch fails with 409
    _op_type: OpType = OpType.create

    @property
    def op_type(self) -> OpType:
        return self._op_type

    @op_type.setter
    def op_type(self, op_type: OpType):
        self._op_type = op_type

    def __attrs_post_init__(self):
        assert self.contents is not None
        assert isinstance(self.coordinates, ContributionCoordinates)
        assert self.coordinates.doc_type is DocumentType.contribution

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'document_id': null_str,
            # Aggregates have `sources` instead, as a singleton list of JSON
            # objects. The field types of contributions and aggregates are
            # merged, so the two names must differ.
            'source': pass_thru_json,
            # These pass-through fields will never be None
            'bundle_uuid': pass_thru_str,
            'bundle_version': pass_thru_str,
            'bundle_deleted': pass_thru_bool
        }

    @classmethod
    def from_json(cls,
                  *,
                  coordinates: ContributionCoordinates[E],
                  document: JSON,
                  version: InternalVersion | None,
                  **kwargs
                  ) -> Self:
        self = super().from_json(coordinates=coordinates,
                                 document=document,
                                 version=version,
                                 source=SourceRef.from_json(document['source']),
                                 **kwargs)
        assert self.coordinates.document_id == document['document_id']
        assert self.coordinates.bundle.uuid == document['bundle_uuid']
        assert self.coordinates.bundle.version == document['bundle_version']
        assert self.coordinates.deleted == document['bundle_deleted']
        return self

    @classmethod
    def mandatory_source_fields(cls) -> list[str]:
        return super().mandatory_source_fields() + [
            'contents',
            'document_id',
            'source',
            'bundle_uuid',
            'bundle_version',
            'bundle_deleted'
        ]

    def to_json(self):
        return dict(super().to_json(),
                    document_id=self.coordinates.document_id,
                    source=self.source.to_json(),
                    bundle_uuid=self.coordinates.bundle.uuid,
                    bundle_version=self.coordinates.bundle.version,
                    bundle_deleted=self.coordinates.deleted)


type CataloguedContribution = Contribution[CataloguedEntityReference]


@define(kw_only=True)
class Aggregate(Document[AggregateCoordinates]):
    sources: set[SourceRef]
    bundles: list[BundleFQID] | None
    num_contributions: int

    def __attrs_post_init__(self):
        assert isinstance(self.coordinates, AggregateCoordinates)
        assert self.coordinates.doc_type is DocumentType.aggregate

    @classmethod
    def coordinate_cls(cls) -> type[AggregateCoordinates]:
        return AggregateCoordinates

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'num_contributions': pass_thru_int,
            'sources': SourceRef.field_types(),
            'bundles': BundleFQID.field_types()
        }

    @classmethod
    def from_json(cls,
                  *,
                  coordinates: AggregateCoordinates,
                  document: JSON,
                  version: InternalVersion | None,
                  **kwargs
                  ) -> Self:
        sources: set[SourceRef] = set(map(SourceRef.from_json,
                                          json_sequence(document['sources'])))
        bundles = optional(json_sequence, document.get('bundles'))
        bundles = None if bundles is None else list(map(BundleFQID.from_json, bundles))
        num_contributions = json_int(document['num_contributions'])
        self = super().from_json(coordinates=coordinates,
                                 document=document,
                                 version=version,
                                 num_contributions=num_contributions,
                                 sources=sources,
                                 bundles=bundles)
        assert isinstance(self, Aggregate)
        return self

    @classmethod
    def mandatory_source_fields(cls) -> list[str]:
        return super().mandatory_source_fields() + [
            'num_contributions',
            *(f'sources.{f}' for f in SourceRef.field_types().keys())
        ]

    def to_json(self) -> JSON:
        sources = [source.to_json() for source in self.sources]
        if self.bundles is None:
            bundles = None
        else:
            bundles = [bundle.to_json() for bundle in self.bundles]
        return dict(super().to_json(),
                    num_contributions=self.num_contributions,
                    sources=sources,
                    bundles=bundles)

    @property
    def op_type(self) -> OpType:
        if self.contents:
            return OpType.create if self.version is None else OpType.index
        else:
            # Aggregates are deleted when their contents goes blank
            return OpType.delete

    @op_type.setter
    def op_type(self, value: OpType):
        raise NotImplementedError


@define(kw_only=True)
class Replica[E: EntityReference](Document[ReplicaCoordinates[E]]):
    """
    A verbatim copy of a metadata document
    """

    #: The type of replica, i.e., what sort of metadata document from the
    #: underlying data repository we are storing a copy of. In practice, this is
    #: the same as `self.coordinates.entity.entity_type`, but this isn't
    #: necessarily the case.
    #:
    #: Typically, all replicas of the same type have similar shapes, just like
    #: contributions for entities of the same type. However, mixing replicas of
    #: different types results in an index containing documents of heterogeneous
    #: shapes. Document heterogeneity is a problem for ES, but we deal with it
    #: by disabling the ES index mapping, essentially turning off the reverse
    #: index that ES normally builds from these documents and using the index
    #: only to store and retrieve the documents by their coordinates.
    replica_type: str

    contents: JSON

    source: SourceRef

    hub_ids: list[EntityID]

    needs_translation: ClassVar[bool] = False

    def __attrs_post_init__(self):
        assert isinstance(self.coordinates, ReplicaCoordinates)
        assert self.coordinates.doc_type is DocumentType.replica

    @classmethod
    def coordinate_cls(cls) -> type[ReplicaCoordinates]:
        return ReplicaCoordinates

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        # Replicas do not undergo translation
        raise NotImplementedError

    def to_json(self) -> JSON:
        return dict(super().to_json(),
                    source=self.source.to_json(),
                    replica_type=self.replica_type,
                    # Ensure that index contents is deterministic for unit tests
                    hub_ids=sorted(set(self.hub_ids)))

    @property
    def op_type(self) -> OpType:
        return OpType.update

    @op_type.setter
    def op_type(self, value: OpType):
        raise NotImplementedError

    def _body(self, field_types: FieldTypes) -> JSON:
        return {
            'script': {
                'source': '''
                        Stream stream = Stream.concat(ctx._source.hub_ids.stream(),
                                                      params.hub_ids.stream());
                        ctx._source.hub_ids = stream.sorted().distinct().collect(Collectors.toList());
                    ''',
                'params': {
                    'hub_ids': self.hub_ids
                }
            },
            'upsert': super()._body(field_types)
        }
