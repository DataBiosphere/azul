from abc import (
    ABC,
    abstractmethod,
)
import importlib
from inspect import (
    isabstract,
)
from typing import (
    AbstractSet,
    ClassVar,
    Generic,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    TypedDict,
    Union,
    get_args,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    cached_property,
    config,
    require,
)
from azul.chalice import (
    Authentication,
)
from azul.drs import (
    DRSClient,
)
from azul.http import (
    http_client,
)
from azul.indexer import (
    Bundle,
    SOURCE_REF,
    SOURCE_SPEC,
    SourceRef,
    SourceSpec,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    Aggregate,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.types import (
    JSON,
    JSONs,
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
FieldPathElement = str
FieldPath = Tuple[FieldPathElement, ...]

FieldMapping = Mapping[FieldName, FieldPath]

ColumnMapping = Mapping[FieldPathElement, FieldName]
ManifestConfig = Mapping[FieldPath, ColumnMapping]
MutableColumnMapping = MutableMapping[FieldPathElement, FieldName]
MutableManifestConfig = MutableMapping[FieldPath, MutableColumnMapping]

DottedFieldPath = str
FieldGlobs = List[DottedFieldPath]


def dotted(path_or_element: Union[FieldPathElement, FieldPath],
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
    Also known in Elasticsearch land as a *source filter*, but those two words
    have different meaning in Azul.

    https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-fields.html#source-filtering
    """
    includes: FieldGlobs
    excludes: FieldGlobs


T = TypeVar('T', bound='Plugin')


class Plugin(ABC):
    """
    A base class for Azul plugins. Concrete plugins shouldn't inherit this
    class directly but one of the subclasses of this class. This class just
    defines the mechanism for loading concrete plugins classes and doesn't
    specify any interface to the concrete plugin itself.
    """

    @classmethod
    def load(cls: Type[T], catalog: CatalogName) -> Type[T]:
        """
        Load and return one of the concrete subclasses of the class this method
        is called on. Which concrete class is returned depends on how the
        catalog is configured. Different catalogs can use different combinations
        of concrete plugin implementations.

        :param catalog: the name of the catalog for which to load the plugin
        """
        assert cls != Plugin, f'Must use a subclass of {cls.__name__}'
        assert isabstract(cls) != Plugin, f'Must use an abstract subclass of {cls.__name__}'
        plugin_type_name = cls.type_name()
        plugin_package_name = config.catalogs[catalog].plugins[plugin_type_name].name
        plugin_package_path = f'{__name__}.{plugin_type_name}.{plugin_package_name}'
        plugin_module = importlib.import_module(plugin_package_path)
        plugin_cls = getattr(plugin_module, 'Plugin')
        assert issubclass(plugin_cls, cls)
        return plugin_cls

    @classmethod
    def types(cls) -> Sequence[Type['Plugin']]:
        return cls.__subclasses__()

    @classmethod
    def type_for_name(cls, plugin_type_name: str) -> Type[T]:
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


class MetadataPlugin(Plugin):

    @classmethod
    def type_name(cls) -> str:
        return 'metadata'

    # If the need arises to parameterize instances of a concrete plugin class,
    # add the parameters to create() and make it abstract.

    @classmethod
    def create(cls) -> 'MetadataPlugin':
        return cls()

    @abstractmethod
    def transformer_types(self) -> Iterable[Type[Transformer]]:
        raise NotImplementedError

    @abstractmethod
    def transformers(self,
                     bundle: Bundle,
                     *,
                     delete: bool
                     ) -> Iterable[Transformer]:
        """
        Instantiate all transformer classes.

        :param bundle: the bundle to initialize the transformers with

        :param delete: whether the bundle was deleted
        """
        raise NotImplementedError

    def aggregate_class(self) -> Type[Aggregate]:
        """
        Returns the concrete class to use for representing aggregate documents
        in the indexer.
        """
        return Aggregate

    @abstractmethod
    def mapping(self) -> JSON:
        raise NotImplementedError

    #: See :meth:`_field_mapping`
    _FieldMapping2 = Mapping[FieldPathElement, FieldName]
    _FieldMapping1 = Mapping[FieldPathElement, Union[FieldName, _FieldMapping2]]
    _FieldMapping = Mapping[FieldPathElement, Union[FieldName, _FieldMapping1]]

    @cached_property
    def field_mapping(self) -> FieldMapping:
        """
        Maps a field's name in the service response to the field's path in
        Elasticsearch index documents.
        """

        def invert(v: MetadataPlugin._FieldMapping,
                   *path: FieldPathElement
                   ) -> Iterable[Tuple[FieldName, FieldPath]]:
            if isinstance(v, dict):
                for k, v in v.items():
                    assert isinstance(k, FieldPathElement)
                    yield from invert(v, *path, k)
            elif isinstance(v, FieldName):
                yield v, path
            else:
                assert False

        return dict(invert(self._field_mapping))

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
    def source_id_field(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def facets(self) -> Sequence[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def manifest(self) -> ManifestConfig:
        raise NotImplementedError

    @abstractmethod
    def document_slice(self, entity_type: str) -> Optional[DocumentSlice]:
        raise NotImplementedError

    @property
    @abstractmethod
    def summary_response_stage(self) -> 'Type[SummaryResponseStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def search_response_stage(self) -> 'Type[SearchResponseStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def summary_aggregation_stage(self) -> 'Type[AggregationStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def aggregation_stage(self) -> 'Type[AggregationStage]':
        raise NotImplementedError

    @property
    @abstractmethod
    def filter_stage(self) -> 'Type[FilterStage]':
        raise NotImplementedError


class RepositoryPlugin(Generic[SOURCE_SPEC, SOURCE_REF], Plugin):

    @classmethod
    def type_name(cls) -> str:
        return 'repository'

    @classmethod
    @abstractmethod
    def create(cls, catalog: CatalogName) -> 'RepositoryPlugin':
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
        assert source.spec in self.sources, (self.sources, source)

    @abstractmethod
    def list_sources(self,
                     authentication: Optional[Authentication]
                     ) -> Iterable[SOURCE_REF]:
        """
        The sources the plugin is configured to read metadata from.
        Retrieving this information may require a round-trip to the underlying
        repository. Implementations should raise PermissionError if the provided
        authentication is insufficient to access the repository.
        """
        raise NotImplementedError

    @cached_property
    def _source_ref_cls(self) -> Type[SOURCE_REF]:
        cls = type(self)
        base_cls = one(getattr(cls, '__orig_bases__'))
        spec_cls, ref_cls = get_args(base_cls)
        require(issubclass(spec_cls, SourceSpec))
        require(issubclass(ref_cls, SourceRef))
        assert ref_cls.spec_cls() is spec_cls
        return ref_cls

    def source_from_json(self, ref: JSON) -> SOURCE_REF:
        """
        Instantiate a :class:`SourceRef` from its JSON representation. The
        expected input format matches the output format of `SourceRef.to_json`.
        """
        return self._source_ref_cls.from_json(ref)

    def resolve_source(self, spec: str) -> SOURCE_REF:
        """
        Return an instance of :class:`SourceRef` for the repository source
        matching the given specification or raise an exception if no such source
        exists.
        """
        ref_cls = self._source_ref_cls
        spec = ref_cls.spec_cls().parse(spec)
        id = self.lookup_source_id(spec)
        return ref_cls(id=id, spec=spec)

    def verify_source(self, ref: SOURCE_REF) -> None:
        """
        Verify that the source's ID matches that defined in the
        repository for the source's spec.
        """
        actual_id = self.lookup_source_id(ref.spec)
        require(ref.id == actual_id, 'Source ID changed unexpectedly', ref, actual_id)

    @abstractmethod
    def lookup_source_id(self, spec: SOURCE_SPEC) -> str:
        """
        Return the ID of the repository source with the specified name or raise
        an exception if no such source exists.
        """
        raise NotImplementedError

    @abstractmethod
    def list_bundles(self,
                     source: SOURCE_REF,
                     prefix: str
                     ) -> List[SourcedBundleFQID[SOURCE_REF]]:
        """
        List the bundles in the given source whose UUID starts with the given
        prefix.

        :param source: a reference to the repository source that contains the
                       bundles to list

        :param prefix: appended to the common prefix of the provided source's
                       spec to produce a string that should be no more than
                       eight lower-case hexadecimal characters
        """

        raise NotImplementedError

    def list_partitions(self, source: SOURCE_REF) -> Optional[Mapping[str, int]]:
        """
        Return the number of bundles in each partition of the given source, or
        return None if that information cannot be retrieved inexpensively. Each
        key in the returned mapping is the full prefix of a partition, including
        the common prefix if one is configured.

        Subclasses may optionally implement this method to facilitate
        integration test coverage of the partition sizes of their sources.
        """
        return None

    @abstractmethod
    def fetch_bundle(self, bundle_fqid: SourcedBundleFQID[SOURCE_REF]) -> Bundle:
        """
        Fetch the given bundle.

        :param bundle_fqid: The fully qualified ID of the bundle to fetch,
                            including its source.
        """
        raise NotImplementedError

    @abstractmethod
    def portal_db(self) -> JSONs:
        """
        Returns integrations data object
        """
        raise NotImplementedError

    def drs_client(self,
                   authentication: Optional[Authentication] = None
                   ) -> DRSClient:
        return DRSClient(http_client=http_client())

    @abstractmethod
    def drs_uri(self, drs_path: Optional[str]) -> Optional[str]:
        """
        Given the file-specifc suffix of a DRS URI for a data file, return the
        complete DRS URI.

        This method is typically called by the service.
        """
        raise NotImplementedError

    @abstractmethod
    def file_download_class(self) -> Type['RepositoryFileDownload']:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True)
class RepositoryFileDownload(ABC):
    #: The UUID of the file to be downloaded
    file_uuid: str

    #: The name of the file on the user's disk.
    file_name: str

    #: Optional version of the file. Defaults to the most recent version.
    file_version: Optional[str]

    #: The DRS path of the file in the repository from which to download the
    #: file. A DRS path is the path component of a DRS URI. Same as a DRS ID:
    #:
    #: https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_drs_ids
    #:
    #: Repository plugins that populate the DRS path (``azul.indexer.Bundle.
    #: drs_path``) usually require this to be set. Plugins that don't will
    #: ignore this.
    drs_path: Optional[str]

    #: True if the download of a file requires its DRS path
    needs_drs_path: ClassVar[bool] = False

    #: The name of the replica to download the file from. Defaults to the name
    #: of the default replica. The set of valid replica names depends on the
    #: repository, but each repository must support the default replica.
    replica: Optional[str]

    #: A token to capture download state in. Should be `None` when the download
    #: is first requested.
    token: Optional[str]

    # This stub is only needed to aid PyCharm's type inference. Without this,
    # the following constructor invocation
    #
    # cls : Type[RepositoryFileDownload] = ...
    # cls(file_uuid=..., file_name=...)
    #
    # will cause a warning. I suspect this is a bug in PyCharm:
    #
    # https://youtrack.jetbrains.com/issue/PY-44728
    #
    # noinspection PyDataclass
    # noinspection PyUnusedLocal
    def __init__(self,
                 file_uuid: str,
                 file_name: str,
                 file_version: Optional[str],
                 drs_path: Optional[str],
                 replica: Optional[str],
                 token: Optional[str]) -> None: ...

    @abstractmethod
    def update(self,
               plugin: RepositoryPlugin,
               authentication: Optional[Authentication]
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
    def location(self) -> Optional[str]:
        """
        The final URL from which the file contents can be downloaded.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def retry_after(self) -> Optional[int]:
        """
        A number of seconds to wait before calling `update` again.
        """
