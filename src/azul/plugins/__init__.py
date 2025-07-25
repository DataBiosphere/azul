from abc import (
    ABCMeta,
    abstractmethod,
)
from enum import (
    Enum,
)
import importlib
from inspect import (
    isabstract,
)
from typing import (
    AbstractSet,
    Callable,
    ClassVar,
    Iterable,
    Mapping,
    Self,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
    TypedDict,
    cast,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    cached_property,
    config,
)
from azul.attrs import (
    SerializableAttrs,
)
from azul.chalice import (
    Authentication,
)
from azul.digests import (
    Digest,
)
from azul.drs import (
    DRSClient,
)
from azul.indexer import (
    Bundle,
    Prefix,
    SourceRef,
    SourceSpec,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    Aggregate,
    DocumentType,
    EntityType,
    FieldPath,
    FieldPathElement,
    IndexName,
)
from azul.indexer.transform import (
    ReplicaTransformer,
    Transformer,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
    derived_type_params,
    json_str,
)
from azul.uuids import (
    validate_uuid_prefix,
)

if TYPE_CHECKING:
    from azul.service.elasticsearch_service import (
        AggregationStage,
        FilterStage,
    )
    # These are only needed for type hints and would otherwise introduce a
    # circular import since the service layer heavily depends on the plugin.
    from azul.service.repository_service import (
        SearchResponseStage,
        SummaryResponseStage,
    )

FieldName = str

FieldMapping = Mapping[FieldName, FieldPath]

ColumnMapping = Mapping[FieldPathElement, FieldName | None]
ManifestConfig = Mapping[FieldPath, ColumnMapping]
MutableColumnMapping = dict[FieldPathElement, FieldName]
MutableManifestConfig = dict[FieldPath, MutableColumnMapping]

DottedFieldPath = str
FieldGlobs = list[DottedFieldPath]


def dotted(path_or_element: FieldPathElement | FieldPath,
           *elements: FieldPathElement
           ) -> DottedFieldPath:
    dot = '.'
    if isinstance(path_or_element, FieldPathElement):
        # The dotted('field') case is pointless, so we won't special-case it
        return dot.join((path_or_element, *elements))
    elif elements:
        return dot.join((*path_or_element, *elements))
    else:
        return dot.join(path_or_element)


class DocumentSlice(TypedDict, total=False):
    """
    Also known in Elasticsearch land as a *source filter*, but that phrase has
    a different meaning in Azul.

    https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-fields.html#source-filtering
    """
    includes: FieldGlobs
    excludes: FieldGlobs


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class Sorting:
    field_name: FieldName
    descending: bool = attr.ib(default=False)
    max_page_size: int = 1000

    @property
    def order(self) -> str:
        return 'desc' if self.descending else 'asc'


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class SpecialFields:
    """
    Azul defines a number of fields in each /index/{entity_type} response that
    are synthetic (not directly taken from the metadata) and/or are used
    internally. Their naming is inconsistent between metadata plugin
    implementations. This class encapsulates the naming of these fields so that
    we don't need to litter the source with strings literals and conditionals.

    It is an incomplete abstraction in that it does not express the name of the
    inner entity the field is a property of in the /index/{entity_type}
    response. In that way, the values of the attributes of instances of this
    class are more akin to a facet name, rather than a field name. However, not
    every field represented here is actually a facet.
    """
    accessible: ClassVar[FieldName] = 'accessible'
    source_id: FieldName
    source_spec: FieldName
    bundle_uuid: FieldName
    bundle_version: FieldName
    root_entity_id: FieldName


class ManifestFormat(Enum):
    compact = 'compact'
    terra_pfb = 'terra.pfb'
    curl = 'curl'
    verbatim_jsonl = 'verbatim.jsonl'
    verbatim_pfb = 'verbatim.pfb'


class Plugin[BUNDLE: Bundle](metaclass=ABCMeta):
    """
    A base class for Azul plugins. Concrete plugins shouldn't inherit this
    class directly but one of the subclasses of this class. This class just
    defines the mechanism for loading concrete plugins classes and doesn't
    specify any interface to the concrete plugin itself.
    """

    @classmethod
    def load(cls, catalog: CatalogName) -> type[Self]:
        """
        Load and return one of the concrete subclasses of the class this method
        is called on. Which concrete class is returned depends on how the
        catalog is configured. Different catalogs can use different combinations
        of concrete plugin implementations.

        :param catalog: the name of the catalog for which to load the plugin
        """
        assert cls != Plugin, f'Must use a subclass of {cls.__name__}'
        assert isabstract(cls), f'Must use an abstract subclass of {cls.__name__}'
        plugin_type_name = cls._plugin_type_name()
        plugin_package_name = config.catalogs[catalog].plugins[plugin_type_name].name
        return cls._load(plugin_type_name, plugin_package_name)

    @classmethod
    def types(cls) -> Sequence[type[Self]]:
        return cls.__subclasses__()

    @classmethod
    def type_for_name(cls, plugin_type_name: str) -> type[Self]:
        """
        Return the plugin type for the given name.

        Note that the returned class is still abstract. To get a concrete
        implementation of a particular plugin type, call the :meth:`.load`
        method of the class returned by this method. The need to call this
        method is uncommon. Depending on the purpose, say, interacting with
        the repository, a client usually knows the abstract type of plugin
        they'd like to use i.e., :class:`RepositoryPlugin`. The only thing
        they don't know is which concrete implementation of that class to
        use, as that depends on the catalog.
        """
        for subclass in cls.types():
            if subclass.type_name() == plugin_type_name:
                return subclass
        raise ValueError('No such plugin type', plugin_type_name)

    @classmethod
    @abstractmethod
    def type_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def bundle_cls(cls,
                   plugin_package_name: str
                   ) -> type[BUNDLE]:
        plugin_type_name = cls._plugin_type_name()
        plugin_cls = cls._load(plugin_type_name, plugin_package_name)
        bundle_cls = derived_type_params(plugin_cls, root=Plugin)[BUNDLE]
        assert isinstance(bundle_cls, type)
        assert issubclass(bundle_cls, Bundle), bundle_cls
        return cast(type[BUNDLE], bundle_cls)

    @classmethod
    def _plugin_type_name(cls) -> str:
        assert cls != Plugin, f'Must use a subclass of {cls.__name__}'
        assert isabstract(cls) != Plugin, f'Must use an abstract subclass of {cls.__name__}'
        plugin_type_name = cls.type_name()
        return plugin_type_name

    @classmethod
    def _load(cls, plugin_type_name: str, plugin_package_name: str) -> type[Self]:
        plugin_package_path = f'{__name__}.{plugin_type_name}.{plugin_package_name}'
        plugin_module = importlib.import_module(plugin_package_path)
        plugin_cls = getattr(plugin_module, 'Plugin')
        assert issubclass(plugin_cls, cls)
        return plugin_cls


class MetadataPlugin[BUNDLE: Bundle](Plugin[BUNDLE]):

    @classmethod
    def type_name(cls) -> str:
        return 'metadata'

    # If the need arises to parameterize instances of a concrete plugin class,
    # add the parameters to create() and make it abstract.

    @classmethod
    def create(cls) -> Self:
        return cls()

    @abstractmethod
    def transformer_types(self) -> Iterable[type[Transformer]]:
        raise NotImplementedError

    @abstractmethod
    def transformers(self,
                     bundle: BUNDLE,
                     *,
                     delete: bool
                     ) -> Iterable[Transformer]:
        """
        Instantiate all transformer classes.

        :param bundle: the bundle to initialize the transformers with

        :param delete: whether the bundle was deleted
        """
        raise NotImplementedError

    def aggregate_class(self) -> type[Aggregate]:
        """
        Returns the concrete class to use for representing aggregate documents
        in the indexer.
        """
        return Aggregate

    @property
    def string_mapping(self):
        return {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 256
                }
            }
        }

    range_mapping = {
        # A float (single precision IEEE-754) can represent all integers up to
        # 16,777,216. If we used float values for organism ages in seconds, we
        # would not be able to accurately represent an organism age of
        # 16,777,217 seconds. That is 194 days and 15617 seconds.
        # A double precision IEEE-754 representation loses accuracy at
        # 9,007,199,254,740,993 which is more than 285616415 years.

        # Note that Python's float uses double precision IEEE-754.
        # (https://docs.python.org/3/tutorial/floatingpoint.html#representation-error)
        'type': 'double_range'
    }

    def mapping(self, index_name: IndexName) -> MutableJSON:
        return {
            'numeric_detection': False,
            'properties': {
                # Declare the primary key since it's used as the tiebreaker when
                # sorting. We used to use _uid for that but that's gone in ES 7 and
                # _id can't be used for sorting:
                #
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking-changes-7.0.html#uid-meta-field-removed
                #
                # https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html
                #
                # > The _id field is restricted from use in aggregations, sorting,
                # > and scripting. In case sorting or aggregating on the _id field
                # > is required, it is advised to duplicate the content of the _id
                # > field into another field that has doc_values enabled.
                #
                'entity_id': self.string_mapping,
                **(
                    {
                        'contents': {
                            # All replicas are stored in a single index per catalog,
                            # regardless of entity type, resulting in heterogeneous
                            # documents. Additionally, we don't want ES re-ordering
                            # arrays or dictionary items within replica documents.
                            # Therefore, we disable the mapping for their contents.
                            'type': 'object',
                            'enabled': False
                        }
                    }
                    if index_name.doc_type is DocumentType.replica else
                    {}
                )
            },
            'dynamic_templates': [
                {
                    'strings_as_text': {
                        'match_mapping_type': 'string',
                        'mapping': self.string_mapping
                    }
                },
                {
                    'other_types_with_keyword': {
                        'match_mapping_type': '*',
                        'mapping': {
                            'type': '{dynamic_type}',
                            'fields': {
                                'keyword': {
                                    'type': '{dynamic_type}'
                                }
                            }
                        }
                    }
                }
            ]
        }

    @property
    @abstractmethod
    def exposed_indices(self) -> dict[EntityType, Sorting]:
        """
        The indices for which the service provides an `/index/…` endpoint.
        The return value maps the outer entity type of each exposed index to the
        default values of the request parameters that control the paging and
        ordering of hits returned by the corresponding endpoint.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def manifest_formats(self) -> Sequence[ManifestFormat]:
        """
        The supported formats for generating a manifest. The first value will be
        used as a default if no format is explicitly specified.
        """
        raise NotImplementedError

    #: See :meth:`_field_mapping`
    _FieldMapping2 = Mapping[FieldPathElement, FieldName]
    _FieldMapping1 = Mapping[FieldPathElement, FieldName | _FieldMapping2]
    _FieldMapping = Mapping[FieldPathElement, FieldName | _FieldMapping1]

    @cached_property
    def field_mapping(self) -> FieldMapping:
        """
        Maps a field's name in the service response to the field's path in
        Elasticsearch index documents.
        """

        def invert(v: MetadataPlugin._FieldMapping,
                   *path: FieldPathElement
                   ) -> Iterable[tuple[FieldName, FieldPath]]:
            if isinstance(v, dict):
                for k, v in v.items():
                    assert isinstance(k, FieldPathElement)
                    yield from invert(v, *path, k)
            elif isinstance(v, FieldName):
                yield v, path
            else:
                assert False, v

        inversion: dict[FieldName, FieldPath] = {}
        for v, path in invert(self._field_mapping):
            other_path = inversion.setdefault(v, path)
            assert other_path == path, (
                f'Field {v!r} has conflicting paths', path, other_path
            )
        return inversion

    @property
    @abstractmethod
    def _field_mapping(self) -> _FieldMapping:
        """
        An inverted and more compact representation of the field mapping. It is
        made up of nested dictionaries where each key is an element in a field's
        path whereas the corresponding value is either the field's name, if the
        key represents the element in the path, or a dictionary otherwise.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def special_fields(self) -> SpecialFields:
        """
        See :py:class:`SpecialFields`.
        """
        raise NotImplementedError

    @property
    def root_entity_type(self) -> str:
        """
        The type of entity that sits at the root of the entity graph, and that
        all other entities are directly or indirectly associated with.
        Typically, entities of other types are thought of as *belonging to* the
        root entity and this relationship is implied rather than made explicit
        via a foreign key or some other manifestation of a graph connection. The
        mere presence of a `project` entity in a TDR snapshot for HCA, for
        example, implies that all other entities in that snapshot *belong* to
        that project.
        """
        raise NotImplementedError

    @property
    def hot_entity_types(self) -> Iterable[str]:
        """
        The types of inner entities that do not explicitly track their hubs in
        replica documents in order to avoid a large list of hub references in
        the replica document, and to avoid contention when updating that list
        during indexing. This will always include the root type.
        """
        replica_transformer_type = one(
            t for t in self.transformer_types()
            if issubclass(t, ReplicaTransformer)
        )
        hot_entity_types = replica_transformer_type.hot_entity_types().values()
        assert self.root_entity_type in hot_entity_types
        return hot_entity_types

    @property
    def facets(self) -> Sequence[str]:
        return [self.special_fields.source_id]

    @property
    @abstractmethod
    def manifest_config(self) -> ManifestConfig:
        raise NotImplementedError

    def verbatim_pfb_entity_id(self, replica: JSON) -> str:
        return json_str(replica['entity_id'])

    def verbatim_pfb_schema(self, replicas: list[JSON]) -> list[JSON]:
        """
        Generate the azul-specific parts of the PFB schema for the verbatim
        manifest. The default, metadata-agnostic implementation loads all
        replica documents into memory and dynamically generates a schema based
        on their observed shapes. This results in inconsistencies in the schema
        depending on the manifest contents, so subclasses should override this
        method if their metadata adheres to an authoritative schema that can be
        known in advance.

        :param replicas: The replica documents to be described by the PFB schema

        :return: a list of PFB entity schemas describing the replicas
        """
        from azul.service import (
            avro_pfb,
        )
        return avro_pfb.pfb_schema_from_replicas(replicas)

    def verbatim_pfb_relations(self, replica: JSON) -> list[tuple[str, str]]:
        """
        A list of the replicas that the given replica references/depends on,
        represented as (replica_type, entity_id) pairs.
        """
        return []

    def verbatim_pfb_links(self, replica_type: str) -> MutableJSONs:
        """
        Express the relationships of the given replica type as PFB links
        (https://uc-cdis.github.io/pypfb/#link).
        """
        return []

    @abstractmethod
    def document_slice(self, entity_type: str) -> DocumentSlice | None:
        raise NotImplementedError

    @property
    @abstractmethod
    def summary_response_stage(self) -> 'type[SummaryResponseStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def search_response_stage(self) -> 'type[SearchResponseStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def summary_aggregation_stage(self) -> 'type[AggregationStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def aggregation_stage(self) -> 'type[AggregationStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def filter_stage(self) -> 'type[FilterStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def file_class(self) -> type['File']:
        raise NotImplementedError


class RepositoryPlugin[BUNDLE: Bundle,
                       SOURCE_SPEC: SourceSpec,
                       SOURCE_REF: SourceRef,
                       BUNDLE_FQID: SourcedBundleFQID](
    Plugin[BUNDLE]
):

    @classmethod
    def type_name(cls) -> str:
        return 'repository'

    @classmethod
    @abstractmethod
    def create(cls, catalog: CatalogName) -> Self:
        """
        Return a plugin instance suitable for populating the given catalog.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def sources(self) -> AbstractSet[SOURCE_SPEC]:
        """
        The names of the sources the plugin is configured to read metadata from.
        """
        raise NotImplementedError

    def _assert_source(self, source: SOURCE_REF):
        """
        Assert that the given source is present in the plugin configuration.
        """
        assert source.spec.prefix is not None, source
        for configured_spec in self.sources:
            if configured_spec == source.spec:
                break
            # Most configured sources lack an explicit prefix
            elif configured_spec.eq_ignoring_prefix(source.spec):
                assert configured_spec.prefix is None, (configured_spec, source)
                break
            else:
                continue
        else:
            assert False, (self.sources, source)

    def _assert_partition(self, source: SOURCE_REF, prefix: str):
        """
        Assert that the given partition is a valid derivation of the given
        source's configured prefix.
        """
        validate_uuid_prefix(prefix)
        assert prefix in source.spec.prefix, (source.spec, prefix)

    @abstractmethod
    def list_sources(self,
                     authentication: Authentication | None
                     ) -> Iterable[SOURCE_REF]:
        """
        The sources the plugin is configured to read metadata from that are
        accessible using the provided authentication. Retrieving this
        information may require a round-trip to the underlying repository.
        Implementations should raise PermissionError if the provided
        authentication is insufficient to access the repository.
        """
        raise NotImplementedError

    def list_source_ids(self,
                        authentication: Authentication | None
                        ) -> set[str]:
        """
        List source IDs in the underlying repository that are accessible using
        the provided authentication. Sources may be included even if they are
        not configured to be read from. Subclasses should override this method
        if it can be implemented more efficiently than `list_sources`.

        Retrieving this information may require a round-trip to the underlying
        repository. Implementations should raise PermissionError if the provided
        authentication is insufficient to access the repository.
        """
        return {source.id for source in self.list_sources(authentication)}

    @cached_property
    def _generic_params(self) -> dict[TypeVar, type]:
        params = derived_type_params(type(self), root=RepositoryPlugin)
        assert all(isinstance(p, type) for p in params.values()), params
        return cast(dict[TypeVar, type], params)

    @property
    def source_ref_cls(self) -> type[SOURCE_REF]:
        ref_cls = self._generic_params[SOURCE_REF]
        assert isinstance(ref_cls, type), ref_cls
        assert issubclass(ref_cls, SourceRef), ref_cls
        return cast(type[SOURCE_REF], ref_cls)

    @property
    def bundle_fqid_cls(self) -> type[BUNDLE_FQID]:
        fqid_cls = self._generic_params[BUNDLE_FQID]
        assert isinstance(fqid_cls, type), fqid_cls
        assert issubclass(fqid_cls, SourcedBundleFQID), fqid_cls
        return cast(type[BUNDLE_FQID], fqid_cls)

    def resolve_source(self, spec: str) -> SOURCE_REF:
        """
        Return an instance of :class:`SourceRef` for the repository source
        matching the given specification or raise an exception if no such source
        exists.
        """
        ref_cls = self.source_ref_cls
        spec = ref_cls.spec_cls().parse(spec)
        id = self._lookup_source_id(spec)
        return ref_cls(id=id, spec=spec)

    @abstractmethod
    def _lookup_source_id(self, spec: SOURCE_SPEC) -> str:
        """
        Return the ID of the repository source with the specified name or raise
        an exception if no such source exists.
        """
        raise NotImplementedError

    @abstractmethod
    def count_bundles(self, source: SOURCE_SPEC) -> int:
        """
        The total number of subgraphs in the given source. The source's prefix
        may be None, indicating that the source hasn't been partitioned yet and
        that this method should count all bundles in the source.
        """
        raise NotImplementedError

    @abstractmethod
    def count_files(self, source: SOURCE_SPEC) -> int:
        """
        The total number of files in the given source. The source's prefix
        may be None, indicating that the source hasn't been partitioned yet and
        that this method should count all files in the source.
        """
        raise NotImplementedError

    def partition_source_for_indexing(self,
                                      catalog: CatalogName,
                                      source: SOURCE_REF
                                      ) -> SOURCE_REF:
        """
        If the source already has a prefix, return the source. Otherwise, return
        an updated copy of the source with a heuristically computed prefix that
        should be appropriate for indexing in the given catalog.
        """
        return self._partition_source(catalog, source, self.count_bundles)

    def partition_source_for_mirroring(self,
                                       catalog: CatalogName,
                                       source: SOURCE_REF
                                       ) -> SOURCE_REF:
        """
        If the source already has a prefix, return the source. Otherwise, return
        an updated copy of the source with a heuristically computed prefix that
        should be appropriate for mirroring in the given catalog.
        """
        return self._partition_source(catalog, source, self.count_files)

    def _partition_source(self,
                          catalog: CatalogName,
                          source: SOURCE_REF,
                          counter: Callable[[SOURCE_SPEC], int]
                          ) -> SOURCE_REF:
        if source.spec.prefix is None:
            count = counter(source.spec)
            is_main = config.deployment.is_main
            is_it = catalog in config.integration_test_catalogs
            # We use the "lesser" heuristic during IT to keep the cost and
            # performance of the tests within reasonable limits
            if is_main and not is_it:
                prefix = Prefix.for_main_deployment(count)
            else:
                prefix = Prefix.for_lesser_deployment(count)
            return source.with_prefix(prefix)
        else:
            return source

    @abstractmethod
    def list_bundles(self,
                     source: SOURCE_REF,
                     prefix: str
                     ) -> list[BUNDLE_FQID]:
        """
        List the bundles in the given source whose UUID starts with the given
        prefix.

        :param source: a reference to the repository source that contains the
                       bundles to list

        :param prefix: a string that should be no more than eight lower-case
                       hexadecimal characters
        """

        raise NotImplementedError

    @abstractmethod
    def fetch_bundle(self, bundle_fqid: BUNDLE_FQID) -> BUNDLE:
        """
        Fetch the given bundle.

        :param bundle_fqid: The fully qualified ID of the bundle to fetch,
                            including its source.
        """
        raise NotImplementedError

    @abstractmethod
    def list_files(self, source: SOURCE_REF, prefix: str) -> list['File']:
        """
        List the files in the given source whose digest value starts with the
        given prefix.

        :param source: A reference to the repository source that contains the
                       files to list

        :param prefix: A string of lower-case hexadecimal characters
        """

        raise NotImplementedError

    @abstractmethod
    def drs_client(self,
                   authentication: Authentication | None = None
                   ) -> DRSClient:
        """
        Returns a DRS client that uses the given authentication with requests to
        the DRS server. If a concrete subclass doesn't support authentication,
        it should assert that the argument is ``None``.
        """
        raise NotImplementedError

    @abstractmethod
    def file_download_class(self) -> type['RepositoryFileDownload']:
        raise NotImplementedError

    @abstractmethod
    def validate_version(self, version: str) -> None:
        """
        Raise ValueError if the given version string is invalid.
        """
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class File(SerializableAttrs, metaclass=ABCMeta):
    """
    A reference to a data file in the repository.
    """

    #: The UUID of the data file. Some plugins use the same UUID for the
    #: file's metadata document.
    uuid: str

    #: The name of the file on the user's disk.
    name: str

    #: Optional version of the file. Defaults to the most recent version.
    version: str | None

    #: The DRS URI of the file in the repository from which to download the
    #: file.
    #:
    #: https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_drs_ids
    #:
    #: Repository plugins that populate the DRS URI (``azul.indexer.Bundle.
    #: drs_uri``) usually require this to be set. Plugins that don't will
    #: ignore this.
    drs_uri: str | None

    #: The file's size on disk, if known.
    size: int | None = None

    #: The file's MIME content type, if known
    content_type: str | None = None

    @classmethod
    @abstractmethod
    def from_hit(cls, hit: JSON) -> Self:
        """
        Instantiate this class from an entity aggregate document retrieved from
        Elasticsearch.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def digest(self) -> Digest:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True)
class RepositoryFileDownload(metaclass=ABCMeta):
    #: The file being downloaded
    file: File

    #: True if the download of a file requires its DRS URI
    needs_drs_uri: ClassVar[bool] = False

    #: The name of the replica to download the file from. Defaults to the name
    #: of the default replica. The set of valid replica names depends on the
    #: repository, but each repository must support the default replica.
    replica: str | None

    #: A token to capture download state in. Should be `None` when the download
    #: is first requested.
    token: str | None

    @abstractmethod
    def update(self,
               plugin: RepositoryPlugin,
               authentication: Authentication | None
               ) -> None:
        """
        Initiate the preparation of a URL from which the file can be downloaded.
        Set any attributes that are None to their default values. If a download
        is already being prepared, update those attributes and set the
        `retry_after` property. If the download has been prepared, set the
        `location` property.

        :param plugin: The plugin for the repository from which the file is to
                       be downloaded.

        :param authentication: The authentication provided with the download
                               request.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def location(self) -> str | None:
        """
        The final URL from which the file contents can be downloaded.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def retry_after(self) -> int | None:
        """
        A number of seconds to wait before calling `update` again.
        """
