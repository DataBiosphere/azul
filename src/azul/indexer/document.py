from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Mapping,
)
from datetime import (
    datetime,
    timezone,
)
from enum import (
    Enum,
)
import re
import sys
from typing import (
    ClassVar,
    Generic,
    Optional,
    Self,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
    get_args,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
    require,
)
from azul.enums import (
    auto,
)
from azul.indexer import (
    BundleFQID,
    BundleFQIDJSON,
    SimpleSourceSpec,
    SourceJSON,
    SourceRef,
)
from azul.openapi import (
    schema,
)
from azul.time import (
    format_dcp2_datetime,
    parse_dcp2_datetime,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
    JSON,
    PrimitiveJSON,
    reify,
)

EntityID = str
EntityType = str


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class EntityReference:
    entity_type: EntityType
    entity_id: EntityID

    def __str__(self) -> str:
        return f'{self.entity_type}/{self.entity_id}'

    @classmethod
    def parse(cls, s: str) -> Self:
        entity_type, entity_id = s.split('/')
        return cls(entity_type=entity_type, entity_id=entity_id)


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


class DocumentType(Enum):
    contribution = 'contribution'
    aggregate = 'aggregate'
    replica = 'replica'

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}.{self._name_}>'


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class IndexName:
    """
    The name of an Elasticsearch index used by an Azul deployment, parsed into
    its components. The index naming scheme underwent a number of changes during
    the evolution of Azul. The different naming schemes are captured in a
    `version` component. Note that the first version of the index name syntax
    did not carry an explicit version. The resulting ambiguity requires entity
    types to not match the version regex below.
    """
    #: Every index name starts with this prefix
    prefix: str = 'azul'

    #: The version of the index naming scheme
    version: int

    #: The name of the deployment the index belongs to
    deployment: str

    #: The catalog the index belongs to or None for v1 indices.
    catalog: Optional[CatalogName] = attr.ib(default=None)

    #: The type of entities this index contains metadata about
    entity_type: str

    #: Whether the documents in the index are contributions, aggregates, or
    #: replicas
    doc_type: DocumentType = DocumentType.contribution

    index_name_version_re: ClassVar[re.Pattern] = re.compile(r'v(\d+)')

    def __attrs_post_init__(self):
        """
        >>> IndexName(prefix='azul',
        ...           version=1,
        ...           deployment='dev',
        ...           entity_type='foo_bar') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName(prefix='azul',
        ...           version=1,
        ...           deployment='dev',
        ...           catalog=None,
        ...           entity_type='foo_bar')  # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName(prefix='azul',
        ...           version=2,
        ...           deployment='dev',
        ...           catalog='main',
        ...           entity_type='foo_bar') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName(prefix='azul', version=1, deployment='dev', catalog='hca', entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 1 prohibits a catalog name ('hca').

        >>> IndexName(prefix='azul', version=2, deployment='dev', entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 2 requires a catalog name (None).

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog=None, entity_type='foo')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Version 2 requires a catalog name (None).

        >>> IndexName(prefix='_', version=2, deployment='dev', catalog='foo', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Prefix '_' is to short, too long or contains invalid characters.

        >>> IndexName(prefix='azul', version=2, deployment='_', catalog='foo', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: Deployment name '_' is to short, too long or contains invalid characters.

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog='_', entity_type='bar')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ('Catalog name is invalid', '_')

        >>> IndexName(prefix='azul', version=2, deployment='dev', catalog='foo', entity_type='_')
        Traceback (most recent call last):
        ...
        azul.RequirementError: entity_type is either too short, too long or contains invalid characters: '_'
        """
        config.validate_prefix(self.prefix)
        require(self.version > 0, f'Version must be at least 1, not {self.version}.')
        config.validate_deployment_name(self.deployment)
        if self.version == 1:
            require(self.catalog is None,
                    f'Version {self.version} prohibits a catalog name ({self.catalog!r}).')
        else:
            require(self.catalog is not None,
                    f'Version {self.version} requires a catalog name ({self.catalog!r}).')
            config.Catalog.validate_name(self.catalog)
        config.validate_entity_type(self.entity_type)
        if self.doc_type is DocumentType.replica:
            assert self.entity_type == 'replica', self.entity_type
        assert '_' not in self.prefix, self.prefix
        assert '_' not in self.deployment, self.deployment
        assert self.catalog is None or '_' not in self.catalog, self.catalog

    def validate(self):
        require(self.deployment == config.deployment_stage,
                'Index name does not use current deployment', self, config.deployment_stage)
        require(self.prefix == config.index_prefix,
                'Index name has invalid prefix', self, config.index_prefix)

    @classmethod
    def create(cls,
               *,
               catalog: CatalogName,
               entity_type: str,
               doc_type: 'DocumentType'
               ) -> Self:
        return cls(prefix=config.index_prefix,
                   version=2,
                   deployment=config.deployment_stage,
                   catalog=catalog,
                   entity_type=entity_type,
                   doc_type=doc_type)

    @classmethod
    def parse(cls, index_name: str) -> Self:
        """
        Parse the name of an index from any deployment and any version of Azul.

        >>> IndexName.parse('azul_foo_dev') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_foo_aggregate_dev') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_foo_bar_dev') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_foo_bar_aggregate_dev') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo_bar',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('good_foo_dev') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='good',
                  version=1,
                  deployment='dev',
                  catalog=None,
                  entity_type='foo',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_dev')
        Traceback (most recent call last):
        ...
        azul.RequirementError: ['azul', 'dev']

        >>> IndexName.parse('azul_aggregate_dev') # doctest: +ELLIPSIS
        Traceback (most recent call last):
        ...
        azul.RequirementError: entity_type ... ''

        >>> IndexName.parse('azul_v2_dev_main_foo') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='foo',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_v2_dev_main_foo_aggregate') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='foo',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='foo_bar',
                  doc_type=<DocumentType.contribution>)

        >>> IndexName.parse('azul_v2_dev_main_foo_bar_aggregate') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='foo_bar',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_staging_hca_foo_bar_aggregate') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='staging',
                  catalog='hca',
                  entity_type='foo_bar',
                  doc_type=<DocumentType.aggregate>)

        >>> IndexName.parse('azul_v2_dev_main_replica') # doctest: +NORMALIZE_WHITESPACE
        IndexName(prefix='azul',
                  version=2,
                  deployment='dev',
                  catalog='main',
                  entity_type='replica',
                  doc_type=<DocumentType.replica>)

        >>> IndexName.parse('azul_v2_staging__foo_bar__aggregate') # doctest: +ELLIPSIS
        Traceback (most recent call last):
            ...
        azul.RequirementError: entity_type ... 'foo_bar_'

        >>> IndexName.parse('azul_v3_bla')
        Traceback (most recent call last):
        ...
        azul.RequirementError: 3

        """
        index_name = index_name.split('_')
        require(len(index_name) > 2, index_name)
        prefix, *index_name = index_name
        version = cls.index_name_version_re.fullmatch(index_name[0])
        if version:
            _, *index_name = index_name
            version = int(version.group(1))
            require(version == 2, version)
            deployment, catalog, *index_name = index_name
        else:
            version = 1
            catalog = None
            *index_name, deployment = index_name
        if index_name[-1] == 'aggregate':
            *index_name, _ = index_name
            doc_type = DocumentType.aggregate
        elif index_name == ['replica']:
            doc_type = DocumentType.replica
        else:
            doc_type = DocumentType.contribution
        entity_type = '_'.join(index_name)
        config.validate_entity_type(entity_type)
        self = cls(prefix=prefix,
                   version=version,
                   deployment=deployment,
                   catalog=catalog,
                   entity_type=entity_type,
                   doc_type=doc_type)
        return self

    def __str__(self) -> str:
        """
        >>> str(IndexName(version=1, deployment='dev', entity_type='foo'))
        'azul_foo_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo', doc_type=DocumentType.aggregate))
        'azul_foo_aggregate_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo_bar'))
        'azul_foo_bar_dev'

        >>> str(IndexName(version=1, deployment='dev', entity_type='foo_bar', doc_type=DocumentType.aggregate))
        'azul_foo_bar_aggregate_dev'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo'))
        'azul_v2_dev_main_foo'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               entity_type='foo',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_dev_main_foo_aggregate'

        >>> str(IndexName(version=2, deployment='dev', catalog='main', entity_type='foo_bar'))
        'azul_v2_dev_main_foo_bar'

        >>> str(IndexName(version=2,
        ...               deployment='dev',
        ...               catalog='main',
        ...               entity_type='foo_bar',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_dev_main_foo_bar_aggregate'

        >>> str(IndexName(version=2,
        ...               deployment='staging',
        ...               catalog='hca',
        ...               entity_type='foo_bar',
        ...               doc_type=DocumentType.aggregate))
        'azul_v2_staging_hca_foo_bar_aggregate'
        """
        aggregate = ['aggregate'] if self.doc_type is DocumentType.aggregate else []
        if self.version == 1:
            require(self.catalog is None)
            return '_'.join([
                self.prefix,
                self.entity_type,
                *aggregate,
                self.deployment
            ])
        elif self.version == 2:
            require(self.catalog is not None, self.catalog)
            return '_'.join([
                self.prefix,
                f'v{self.version}',
                self.deployment,
                self.catalog,
                self.entity_type,
                *aggregate,
            ])
        else:
            assert False, self.version


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
    entity: E
    doc_type: DocumentType

    @property
    def index_name(self) -> str:
        """
        The fully qualified name of the Elasticsearch index for a document with
        these coordinates. Only call this if these coordinates use a catalogued
        entity reference. You can use `.with_catalog()` to create one.
        """
        assert isinstance(self.entity, CataloguedEntityReference)
        return str(IndexName.create(catalog=self.entity.catalog,
                                    entity_type=self.entity.entity_type,
                                    doc_type=self.doc_type))

    @property
    @abstractmethod
    def document_id(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_hit(cls,
                 hit: JSON
                 ) -> 'DocumentCoordinates[CataloguedEntityReference]':
        index_name = IndexName.parse(hit['_index'])
        index_name.validate()
        document_id = hit['_id']
        if index_name.doc_type is DocumentType.contribution:
            subcls = ContributionCoordinates
        elif index_name.doc_type is DocumentType.aggregate:
            subcls = AggregateCoordinates
        elif index_name.doc_type is DocumentType.replica:
            subcls = ReplicaCoordinates
        else:
            assert False, index_name.doc_type
        assert issubclass(subcls, cls)
        return subcls._from_index(index_name, document_id)

    @classmethod
    @abstractmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> 'DocumentCoordinates[CataloguedEntityReference]':
        raise NotImplementedError

    def with_catalog(self,
                     catalog: Optional[CatalogName]
                     ) -> 'DocumentCoordinates[CataloguedEntityReference]':
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
    doc_type: DocumentType = attr.ib(init=False, default=DocumentType.contribution)
    bundle: BundleFQID
    deleted: bool

    def __attrs_post_init__(self):
        # If we were to allow instances of subclasses, we'd risk breaking
        # equality and hashing semantics. It is impossible to correctly
        # implement the transitivity property of equality between instances of
        # type and subtype without ignoring the additional attributes added by
        # the subtype. Consider types T and S where S is a subtype of T.
        # Transitivity requires that s1 == s2 for any two instances s1 and s2
        # of S for which s1 == t and s2 == t, where t is any instance of T. The
        # subtype instances s1 and s2 can only be equal to t if they match in
        # all attributes that T defines. The additional attributes introduced
        # by S must be ignored, even when comparing s1 and s2, otherwise s1 and
        # s2 might turn out to be unequal. In this particular case that is not
        # desirable: we do want to be able to compare instances of subtypes of
        # BundleFQID without ignoring any of their attributes.
        concrete_type = type(self.bundle)
        assert concrete_type is BundleFQID, concrete_type

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
                    ) -> 'ContributionCoordinates[CataloguedEntityReference]':
        entity_type = index_name.entity_type
        assert index_name.doc_type is DocumentType.contribution
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
    doc_type: DocumentType = attr.ib(init=False, default=DocumentType.aggregate)

    @classmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> Self:
        entity_type = index_name.entity_type
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


@attr.s(frozen=True, auto_attribs=True, kw_only=True, slots=True)
class ReplicaCoordinates(DocumentCoordinates[E], Generic[E]):
    """
    Document coordinates for replicas. Replicas are content-addressed, so
    these these coordinates depend not only on the entity reference, but on the
    contents of the underlying metadata document.
    """

    doc_type: DocumentType = attr.ib(init=False, default=DocumentType.replica)

    #: A hash of the replica's JSON document
    content_hash: str

    @property
    def document_id(self) -> str:
        return f'{self.entity.entity_id}_{self.content_hash}'

    @classmethod
    def _from_index(cls,
                    index_name: IndexName,
                    document_id: str
                    ) -> 'ReplicaCoordinates[CataloguedEntityReference]':
        assert index_name.doc_type is DocumentType.replica, index_name
        entity_id, content_hash = document_id.split('_')
        return cls(content_hash=content_hash,
                   entity=CataloguedEntityReference(catalog=index_name.catalog,
                                                    entity_type='replica',
                                                    entity_id=entity_id))

    def __str__(self) -> str:
        return f'replica of {self.entity.entity_id}'


# The native type of the field in documents as they are being created by a
# transformer or processed by an aggregator.
N = TypeVar('N')

# The type of the field in a document just before it's being written to the
# index. Think "translated type".
T = TypeVar('T', bound=AnyJSON)

P = TypeVar('P', bound=PrimitiveJSON)

Range = tuple[P, P]


class FieldType(Generic[N, T], metaclass=ABCMeta):
    shadowed: bool = False
    es_sort_mode: str = 'min'
    allow_sorting_by_empty_lists: bool = True

    def __init__(self, native_type: Type[N], translated_type: Type[T]):
        self.native_type = native_type
        self.translated_type = translated_type

    @property
    @abstractmethod
    def es_type(self) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def to_index(self, value: N) -> T:
        raise NotImplementedError

    @abstractmethod
    def from_index(self, value: T) -> N:
        raise NotImplementedError

    def to_tsv(self, value: N) -> str:
        return '' if value is None else str(value)

    @property
    def api_schema(self) -> JSON:
        """
        The JSONSchema describing fields of this type in OpenAPI specifications.
        """
        return schema.make_type(self.native_type)

    def from_api(self, value: AnyJSON) -> N:
        """
        Convert a deserialized JSON value occurring as an input to a REST API
        to the native representation of values of this field type.

        The default implementation assumes that the REST API representation
        of the value is already of the native type, and just returns the
        argument. Subclasses must override this if the native and API
        representations differ. An API representation of a field only occurs
        in inputs to a REST API. Outputs like the body of a response use the
        native representation.
        """
        assert isinstance(value, reify(self.native_type))
        return value

    @property
    def supported_filter_relations(self) -> tuple[str, ...]:
        """
        The filter relations in which fields of this type can be used as a
        left-handside. By default, this class only supports equality. A scalar
        field type would override this method to include the `within` relation.
        """
        return 'is',

    def api_filter_schema(self, relation: str) -> JSON:
        """
        The JSONSchema describing the right-handside operand of the given filter
        relation in OpenAPI specifications when the left-handside operand is a
        field of this type.
        """
        assert relation in self.supported_filter_relations, relation
        api_type = self.api_schema
        if relation == 'is':
            return api_type
        elif relation == 'within':
            return self._api_range_schema(api_type)
        else:
            assert False, relation

    def _api_range_schema(self, api_schema: JSON) -> JSON:
        return schema.array(api_schema, minItems=2, maxItems=2)

    def _api_range_to_index(self, value: Range[T]) -> JSON:
        return {'gte': value[0], 'lte': value[1]}

    def _from_api_range(self, value: AnyJSON) -> Range[T]:
        assert isinstance(value, (list, tuple)) and len(value) == 2, value
        gte, lte = value
        return self.from_api(gte), self.from_api(lte)

    def filter(self, relation: str, values: list[AnyJSON]) -> list[T]:
        if relation == 'within':
            return list(map(self._api_range_to_index, map(self._from_api_range, values)))
        else:
            return list(map(self.to_index, values))


class PassThrough(Generic[T], FieldType[T, T]):
    allow_sorting_by_empty_lists = False

    def __init__(self, translated_type: Type[T], *, es_type: Optional[str]):
        super().__init__(translated_type, translated_type)
        self._es_type = es_type

    @property
    def es_type(self) -> str:
        return self._es_type

    def to_index(self, value: T) -> T:
        return value

    def from_index(self, value: T) -> T:
        return value


# FIXME: change the es_type for JSON to `nested`
#        https://github.com/DataBiosphere/azul/issues/2621
pass_thru_json: PassThrough[JSON] = PassThrough(JSON, es_type=None)


class NumericPassThrough(PassThrough[T]):

    @property
    def supported_filter_relations(self) -> tuple[str, ...]:
        return *super().supported_filter_relations, 'within'

    def from_api(self, value: AnyJSON) -> T:
        """
        1.0 is a valid JSONSchema `integer`

        >>> pass_thru_int.from_api(1.0)
        1

        1 is a valid JSONSchema `number`

        >>> pass_thru_float.from_api(1)
        1.0

        1.1 is not a valid JSONSchema `integer`

        >>> pass_thru_int.from_api(1.1)
        Traceback (most recent call last):
            ...
        AssertionError: 1.1

        1.1 is a valid JSONSchema `float`

        >>> pass_thru_float.from_api(1.1)
        1.1
        """
        native_value = self.native_type(value)
        assert native_value == value, value
        return native_value


pass_thru_str = PassThrough(str, es_type='keyword')
pass_thru_int = NumericPassThrough(int, es_type='long')
pass_thru_float = NumericPassThrough(float, es_type='double')
pass_thru_bool = PassThrough(bool, es_type='boolean')


class Nullable(FieldType[Optional[N], T]):

    def __init__(self, native_type: Type[N], translated_type: Type[T]) -> None:
        super().__init__(Optional[native_type], translated_type)

    @property
    def optional_type(self):
        native_type, none_type = get_args(self.native_type)
        assert none_type is type(None)  # noqa: E721
        return native_type

    @abstractmethod
    def to_index(self, value: N) -> T:
        raise NotImplementedError

    @abstractmethod
    def from_index(self, value: T) -> N:
        raise NotImplementedError

    @property
    def api_schema(self) -> JSON:
        return schema.nullable(schema.make_type(self.optional_type))


class NullableScalar(Nullable[N, T], metaclass=ABCMeta):

    def api_filter_schema(self, relation: str) -> JSON:
        if relation == 'within':
            # The LHS operand of a range relation can't be null
            api_type = schema.make_type(self.optional_type)
            return self._api_range_schema(api_type)
        else:
            return super().api_filter_schema(relation)

    @property
    def supported_filter_relations(self) -> tuple[str, ...]:
        return *super().supported_filter_relations, 'within'


class NullableString(Nullable[str, str]):
    # Note that the replacement values for `None` used for each data type
    # ensure that `None` values are placed at the end of a sorted list.
    null_string = '~null'
    es_type = 'keyword'

    def __init__(self):
        super().__init__(str, str)

    def to_index(self, value: Optional[str]) -> str:
        return self.null_string if value is None else value

    def from_index(self, value: str) -> Optional[str]:
        return None if value == self.null_string else value


null_str = NullableString()

# While Elasticsearch distinguishes between integers and floating point numbers
# in its index, JSON does not. Since all payloads to and from Elasticsearch are
# serialized as JSON we have to be prepared to get 1 back when we write 1.0.

JSONNumber = Union[int, float]

U = TypeVar('U', bound=Union[bool, int, float])


class NullableNumber(Generic[U], NullableScalar[U, JSONNumber]):
    shadowed = True
    # Maximum int that can be represented as a 64-bit int and double IEEE
    # floating point number. This prevents loss when converting between the two.
    null_value = sys.maxsize - 1023
    assert null_value == int(float(null_value))

    def __init__(self, native_type: Type[U], es_type: str) -> None:
        assert issubclass(native_type, get_args(JSONNumber))
        super().__init__(native_type, JSONNumber)
        self._es_type = es_type

    @property
    def es_type(self) -> Optional[str]:
        return self._es_type

    def to_index(self, value: Optional[U]) -> JSONNumber:
        if value is None:
            return self.null_value
        else:
            assert value < self.null_value, (value, self.null_value)
            return value

    def from_index(self, value: JSONNumber) -> Optional[U]:
        if value == self.null_value:
            return None
        else:
            return self.optional_type(value)

    def from_api(self, value: AnyJSON) -> N:
        """
        1.0 is a valid JSONSchema `integer`

        >>> null_int.from_api(1.0)
        1

        1 is a valid JSONSchema `number`

        >>> pass_thru_float.from_api(1)
        1.0

        1.1 is not a valid JSONSchema `integer`

        >>> null_int.from_api(1.1)
        Traceback (most recent call last):
            ...
        AssertionError: 1.1

        1.1 is a valid JSONSchema `float`

        >>> pass_thru_float.from_api(1.1)
        1.1
        """
        native_value = self.optional_type(value)
        assert native_value == value, value
        return native_value


null_int = NullableNumber(int, 'long')

null_float = NullableNumber(float, 'double')


class NullableBool(NullableNumber[bool]):
    shadowed = False

    def __init__(self):
        super().__init__(bool, 'boolean')

    def to_index(self, value: Optional[bool]) -> JSONNumber:
        value = {False: 0, True: 1, None: None}[value]
        return super().to_index(value)

    def from_index(self, value: JSONNumber) -> Optional[bool]:
        value = super().from_index(value)
        return {0: False, 1: True, None: None}[value]

    @property
    def supported_filter_relations(self) -> tuple[str, ...]:
        return 'is',  # no point in supporting range relation


null_bool = NullableBool()


class NullableDateTime(Nullable[str, str]):
    es_type = 'date'
    null = format_dcp2_datetime(datetime(9999, 1, 1, tzinfo=timezone.utc))

    def to_index(self, value: Optional[str]) -> str:
        if value is None:
            return self.null
        else:
            parse_dcp2_datetime(value)
            return value

    def from_index(self, value: str) -> Optional[str]:
        if value == self.null:
            return None
        else:
            return value


null_datetime: NullableDateTime = NullableDateTime(str, str)


class Nested(PassThrough[JSON]):
    properties: Mapping[str, FieldType]

    def __init__(self, **properties):
        super().__init__(JSON, es_type='nested')
        self.properties = properties

    def api_filter_schema(self, relation: str) -> JSON:
        assert relation == 'is'
        properties, required = {}, []
        for field, field_type in self.properties.items():
            properties[field] = field_type.api_filter_schema(relation)
            if not isinstance(field_type, Nullable):
                required.append(field)
        kwargs = dict(additionalProperties=False)
        if required:
            kwargs['required'] = required
        return schema.object_type(properties, **kwargs)

    def filter(self, relation: str, values: list[JSON]) -> list[JSON]:
        nested_object = one(values)
        assert isinstance(nested_object, dict)
        query_filters = {}
        for nested_field, nested_value in nested_object.items():
            nested_type = self.properties[nested_field]
            to_index = nested_type.to_index
            value = one(values)[nested_field]
            query_filters[nested_field] = to_index(value)
        return [query_filters]


class ClosedRange(Generic[P], FieldType[Range[P], JSON]):

    def __init__(self, ends_type: FieldType[P, P]):
        super().__init__(Range[P], JSON)
        self.ends_type = ends_type

    @property
    def es_type(self) -> Optional[str]:
        return None

    def to_index(self, value: Range[P]) -> JSON:
        return self._api_range_to_index(value)

    def from_index(self, value: JSON) -> Range[P]:
        return value['gte'], value['lte']

    @property
    def api_schema(self):
        return self._api_range_schema(self.ends_type.api_schema)

    @property
    def supported_filter_relations(self) -> tuple[str, ...]:
        return 'is', 'within', 'contains', 'intersects'

    def api_filter_schema(self, relation: str) -> JSON:
        if relation == 'contains':
            # A range can contain a range or a value
            return schema.union(self.ends_type.api_schema, self.api_schema)
        else:
            return self.api_schema

    def from_api(self, value: AnyJSON) -> Range[P]:
        return self.ends_type._from_api_range(value)

    def filter(self, relation: str, values: list[AnyJSON]) -> list[JSON]:
        result = []
        for value in values:
            if isinstance(value, list):
                pass
            elif relation == 'contains' and isinstance(value, reify(PrimitiveJSON)):
                value = [value, value]
            else:
                assert False, (relation, value)
            result.append(self.to_index(self.from_api(value)))
        return result


FieldTypes4 = Union[Mapping[str, FieldType], Sequence[FieldType], FieldType]
FieldTypes3 = Union[Mapping[str, FieldTypes4], Sequence[FieldType], FieldType]
FieldTypes2 = Union[Mapping[str, FieldTypes3], Sequence[FieldType], FieldType]
FieldTypes1 = Union[Mapping[str, FieldTypes2], Sequence[FieldType], FieldType]
FieldTypes = Mapping[str, FieldTypes1]
CataloguedFieldTypes = Mapping[CatalogName, FieldTypes]


class VersionType(Enum):
    # No versioning; document is created or overwritten as needed
    none = auto()

    # Writing a document fails with 409 conflict if one with the same ID already
    # exists in the index
    create_only = auto()

    # Use the Elasticsearch "internal" versioning type
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docs-index_.html#_version_types
    internal = auto()


InternalVersion = tuple[int, int]

C = TypeVar('C', bound=DocumentCoordinates)


@attr.s(frozen=False, kw_only=True, auto_attribs=True)
class Document(Generic[C]):
    needs_seq_no_primary_term: ClassVar[bool] = False
    needs_translation: ClassVar[bool] = True

    coordinates: C
    version_type: VersionType = VersionType.none

    # For VersionType.internal, version is a tuple composed of the sequence
    # number and primary term. For VersionType.none and .create_only, it is
    # None.
    # https://www.elastic.co/guide/en/elasticsearch/reference/7.9/docs-bulk.html#bulk-api-response-body
    version: Optional[InternalVersion]

    # In the index, the `contents` property is always present and never null in
    # documents. In instances of the Aggregate subclass, this attribute is None
    # when they were created from documents that were retrieved from the
    # index while intentionally excluding that property for efficiency. In
    # instances of the Contribution subclass, this attribute is never None.
    #
    contents: Optional[JSON]

    @property
    def entity(self) -> EntityReference:
        return self.coordinates.entity

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            'entity_id': null_str,
            'contents': field_types
        }

    @classmethod
    def translate_fields(cls,
                         doc: AnyJSON,
                         field_types: Union[FieldType, FieldTypes],
                         *,
                         forward: bool,
                         path: tuple[str, ...] = ()
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
                        new_doc[key] = cls.translate_fields(val, field_type, forward=forward, path=(*path, key))
                        if forward and isinstance(field_type, FieldType) and field_type.shadowed:
                            # Add a non-translated shadow copy of this field's
                            # numeric value for sum aggregations
                            new_doc[key + '_'] = val
                return new_doc
            elif isinstance(doc, list):
                return [cls.translate_fields(val, field_types, forward=forward, path=path) for val in doc]
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
                  version: Optional[InternalVersion],
                  **kwargs,
                  ) -> Self:
        # noinspection PyArgumentList
        # https://youtrack.jetbrains.com/issue/PY-28506
        self = cls(coordinates=coordinates,
                   version=version,
                   contents=document.get('contents'),
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
                   coordinates: Optional[DocumentCoordinates[CataloguedEntityReference]] = None
                   ) -> Self:
        if coordinates is None:
            coordinates = DocumentCoordinates.from_hit(hit)
        document = hit['_source']
        if cls.needs_translation:
            document = cls.translate_fields(document,
                                            field_types[coordinates.entity.catalog],
                                            forward=False)
        if cls.needs_seq_no_primary_term:
            try:
                version = (hit['_seq_no'], hit['_primary_term'])
            except KeyError:
                assert '_seq_no' not in hit
                assert '_primary_term' not in hit
                version = None
        else:
            version = None

        return cls.from_json(coordinates=coordinates,
                             document=document,
                             version=version)

    def to_index(self,
                 catalog: Optional[CatalogName],
                 field_types: CataloguedFieldTypes,
                 bulk: bool = False
                 ) -> JSON:
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
            **(
                {}
                if delete else
                {
                    '_source' if bulk else 'body':
                        self.translate_fields(doc=self.to_json(),
                                              field_types=field_types[coordinates.entity.catalog],
                                              forward=True)
                        if self.needs_translation else
                        self.to_json()
                }
            ),
            '_id' if bulk else 'id': self.coordinates.document_id
        }
        if self.version_type is VersionType.none:
            assert not self.needs_seq_no_primary_term
            if bulk:
                result['_op_type'] = 'delete' if delete else 'index'
            else:
                # For non-bulk updates, the op-type is determined by which
                # client method is invoked.
                pass
        elif self.version_type is VersionType.create_only:
            assert not self.needs_seq_no_primary_term
            if bulk:
                if delete:
                    result['_op_type'] = 'delete'
                    result['if_seq_no'], result['if_primary_term'] = self.version
                else:
                    result['_op_type'] = 'create'
            else:
                assert not delete
                result['op_type'] = 'create'
        elif self.version_type is VersionType.internal:
            assert self.needs_seq_no_primary_term
            if bulk:
                result['_op_type'] = 'delete' if delete else ('create' if self.version is None else 'index')
            elif not delete:
                if self.version is None:
                    result['op_type'] = 'create'
            else:
                # For non-bulk updates 'delete' is not a possible op-type.
                # Instead, self.delete controls which client method is invoked.
                pass
            if self.version is not None:
                # For internal versioning, self.version is None for new documents
                result['if_seq_no'], result['if_primary_term'] = self.version
        else:
            assert False
        return result

    @property
    def delete(self):
        return False


class DocumentSource(SourceRef[SimpleSourceSpec, SourceRef]):
    pass


@attr.s(frozen=False, kw_only=True, auto_attribs=True)
class Contribution(Document[ContributionCoordinates[E]]):
    # This narrows the type declared in the superclass. See comment there.
    contents: JSON
    source: DocumentSource

    #: The version_type attribute will change to VersionType.none if writing
    #: to Elasticsearch fails with 409
    version_type: VersionType = VersionType.create_only

    def __attrs_post_init__(self):
        assert self.contents is not None
        assert isinstance(self.coordinates, ContributionCoordinates)
        assert self.coordinates.doc_type is DocumentType.contribution

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'document_id': null_str,
            'source': pass_thru_json,
            # These pass-through fields will never be None
            'bundle_uuid': pass_thru_str,
            'bundle_version': pass_thru_str,
            'bundle_deleted': pass_thru_bool
        }

    @classmethod
    def from_json(cls,
                  *,
                  coordinates: C,
                  document: JSON,
                  version: Optional[InternalVersion],
                  **kwargs
                  ) -> Self:
        self = super().from_json(coordinates=coordinates,
                                 document=document,
                                 version=version,
                                 source=DocumentSource.from_json(cast(SourceJSON, document['source'])),
                                 **kwargs)
        assert isinstance(self, Contribution)
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


@attr.s(frozen=False, kw_only=True, auto_attribs=True)
class Aggregate(Document[AggregateCoordinates]):
    version_type: VersionType = VersionType.internal
    sources: set[DocumentSource]
    bundles: Optional[list[BundleFQIDJSON]]
    num_contributions: int
    needs_seq_no_primary_term: ClassVar[bool] = True

    def __attrs_post_init__(self):
        assert isinstance(self.coordinates, AggregateCoordinates)
        assert self.coordinates.doc_type is DocumentType.aggregate

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(field_types),
            'num_contributions': pass_thru_int,
            'sources': {
                'id': pass_thru_str,
                'spec': pass_thru_str
            },
            'bundles': {
                'uuid': pass_thru_str,
                'version': pass_thru_str,
            }
        }

    @classmethod
    def from_json(cls,
                  *,
                  coordinates: C,
                  document: JSON,
                  version: Optional[InternalVersion],
                  **kwargs
                  ) -> Self:
        self = super().from_json(coordinates=coordinates,
                                 document=document,
                                 version=version,
                                 num_contributions=document['num_contributions'],
                                 sources=set(map(DocumentSource.from_json,
                                                 cast(list[SourceJSON], document['sources']))),
                                 bundles=document.get('bundles'))
        assert isinstance(self, Aggregate)
        return self

    @classmethod
    def mandatory_source_fields(cls) -> list[str]:
        return super().mandatory_source_fields() + [
            'num_contributions',
            'sources.id',
            'sources.spec'
        ]

    def to_json(self) -> JSON:
        return dict(super().to_json(),
                    num_contributions=self.num_contributions,
                    sources=[source.to_json() for source in self.sources],
                    bundles=self.bundles)

    @property
    def delete(self):
        # Aggregates are deleted when their contents goes blank
        return super().delete or not self.contents


@attr.s(frozen=False, kw_only=True, auto_attribs=True)
class Replica(Document[ReplicaCoordinates[E]]):
    """
    A verbatim copy of a metadata document
    """

    #: The type of replica, i.e., what sort of metadata document from the
    #: underlying data repository we are storing a copy of. Conceptually related
    #: to the entity type, but its value may be different from the entity type.
    #: For example, AnVIL replicas use the name of the data table that contains
    #: the entity, e.g. 'anvil_file', instead of just 'file'.
    #:
    #: We can't model replica types as entity types because we want to hold all
    #: replicas in a single index per catalog to facilitate quick retrieval.
    #: Typically, all replicas of the same type have similar shapes, just like
    #: contributions for entities of the same type. However, mixing replicas of
    #: different types results in an index containing documents of heterogeneous
    #: shapes. Document heterogeneity is a problem for ES, but we deal with it
    #: by disabling the ES index mapping, essentially turning off the reverse
    #: index that ES normally builds from these documents and using the index
    #: only to store and retrieve the documents by their coordinates.
    replica_type: str

    contents: JSON

    hub_ids: list[EntityID]

    #: The version_type attribute will change to VersionType.none if writing
    #: to Elasticsearch fails with 409
    version_type: VersionType = VersionType.create_only

    needs_translation: ClassVar[bool] = False

    def __attrs_post_init__(self):
        assert isinstance(self.coordinates, ReplicaCoordinates)
        assert self.coordinates.doc_type is DocumentType.replica
        assert self.entity.entity_type == 'replica', self.entity

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return {
            **super().field_types(pass_thru_json),
            'replica_type': pass_thru_str,
            'hub_ids': pass_thru_str
        }

    def to_json(self) -> JSON:
        return dict(super().to_json(),
                    replica_type=self.replica_type,
                    hub_ids=self.hub_ids)


CataloguedContribution = Contribution[CataloguedEntityReference]
