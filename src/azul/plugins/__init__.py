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
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

import attr

from azul import (
    CatalogName,
    config,
)
from azul.drs import (
    DRSClient,
)
from azul.http import (
    http_client,
)
from azul.indexer import (
    Bundle,
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

ColumnMapping = Mapping[str, str]
MutableColumnMapping = MutableMapping[str, str]
ManifestConfig = Mapping[str, ColumnMapping]
MutableManifestConfig = MutableMapping[str, MutableColumnMapping]
Translation = Mapping[str, str]


class ServiceConfig(NamedTuple):
    # Except otherwise noted the attributes were previously held in a JSON file
    # called `request_config.json`
    translation: Translation
    autocomplete_translation: Mapping[str, Mapping[str, str]]
    manifest: ManifestConfig
    cart_item: Mapping[str, Sequence[str]]
    facets: Sequence[str]
    # This used to be defined in a JSON file called `autocomplete_mapping_config.json`
    autocomplete_mapping_config: Mapping[str, Mapping[str, Union[str, Sequence[str]]]]
    # This used to be defined in a text file called `order_config`
    order_config: Sequence[str]


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
        plugin_package_name = config.plugin_name(catalog, plugin_type_name)
        plugin_package_path = f'{__name__}.{plugin_type_name}.{plugin_package_name}'
        plugin_module = importlib.import_module(plugin_package_path)
        plugin_cls = getattr(plugin_module, 'Plugin')
        assert issubclass(plugin_cls, cls)
        return plugin_cls

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
        for subclass in cls.__subclasses__():
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
    def mapping(self) -> JSON:
        raise NotImplementedError

    @abstractmethod
    def transformers(self) -> Iterable[Type[Transformer]]:
        raise NotImplementedError

    @abstractmethod
    def service_config(self) -> ServiceConfig:
        """
        Returns service configuration in a legacy format.
        """
        raise NotImplementedError

    def aggregate_class(self) -> Type[Aggregate]:
        """
        Returns the concrete class to use for representing aggregate documents
        in the indexer.
        """
        return Aggregate


class RepositoryPlugin(Plugin):

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
    def sources(self) -> AbstractSet[str]:
        """
        The sources the plugin is configured to read metadata from.
        """
        raise NotImplementedError

    @abstractmethod
    def list_bundles(self, source: str, prefix: str) -> List[SourcedBundleFQID]:
        """
        List the bundles in the given source whose UUID starts with the given
        prefix.

        :param source: a string referencing the repository source to list the
                       bundles in

        :param prefix: a string of a most eight lower-case hexacdecimal
                       characters

        :return:
        """

        raise NotImplementedError

    @abstractmethod
    def fetch_bundle(self, bundle_fqid: SourcedBundleFQID) -> Bundle:
        """
        Fetch the given bundle from the given repository source.

        :param source: a string referencing the source to fetch the bundle from

        :param bundle_fqid: The fully qualified ID of the bundle to fetch
        """
        raise NotImplementedError

    @abstractmethod
    def dss_subscription_query(self, prefix: str) -> JSON:
        """
        The query to use for subscribing Azul to bundle additions in the DSS.
        This query will also be used for listing bundles in the DSS during
        reindexing.

        :param prefix: a prefix that restricts the set of bundles to subscribe
                       to. This parameter is used to subset or partition the set
                       of bundles in the DSS. The returned query should only
                       match bundles whose UUID starts with the given prefix.
        """
        raise NotImplementedError

    @abstractmethod
    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        """
        The query to use for subscribing Azul to bundle deletions in the DSS.

        :param prefix: a prefix that restricts the set of bundles to subscribe to. This parameter is used to subset
                       or partition the set of bundles in the DSS. The returned query should only match bundles whose
                       UUID starts with the given prefix.
        """
        raise NotImplementedError

    @abstractmethod
    def portal_db(self) -> JSONs:
        """
        Returns integrations data object
        """
        raise NotImplementedError

    def drs_client(self) -> DRSClient:
        return DRSClient(http_client=http_client())

    @abstractmethod
    def drs_uri(self, drs_path: str) -> str:
        """
        Given the file-specifc suffix of a DRS URI for a data file, return the
        complete DRS URI.

        This method is typically called by the service.
        """
        raise NotImplementedError

    @abstractmethod
    def direct_file_url(self,
                        file_uuid: str,
                        *,
                        file_version: Optional[str] = None,
                        replica: Optional[str] = None,
                        ) -> Optional[str]:
        """
        A URL pointing at the specified (or latest) version of the specified
        file in the underlying repository, or `None` if no such URL is
        available.
        """
        raise NotImplementedError

    @abstractmethod
    def file_download_class(self) -> Type['RepositoryFileDownload']:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True)
class RepositoryFileDownload(ABC):
    file_uuid: str
    """
    The UUID of the file to be downloaded
    """
    file_name: str
    """
    The name of the file on the user's disk.
    """
    file_version: Optional[str]
    """
    Optional version of the file. Defaults to the most recent version.
    """
    drs_path: Optional[str]
    """
    The DRS path of the file in the repository from which to download
    the file. A DRS path is the path component of a DRS URI. Same as a DRS ID:

    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/#_drs_ids

    Repository plugins that populate the DRS path (azul.indexer.Bundle.drs_path)
    usually require this to be set. Plugins that don't will ignore this.
    """
    replica: Optional[str]
    """
    The name of the replica to download the file from. Defaults to the name of
    the default replica. The set of valid replica names depends on the
    repository, but each repository must support the default replica.
    """

    token: Optional[str]
    """
    A token to capture download state in. Should be `None` when the download is
    first requested.
    """

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
    def __init__(self,
                 file_uuid: str,
                 file_name: str,
                 file_version: Optional[str],
                 drs_path: Optional[str],
                 replica: Optional[str],
                 token: Optional[str]) -> None: ...

    @abstractmethod
    def update(self, plugin: RepositoryPlugin) -> None:
        """
        Initiate the preparation of a URL from which the file can be downloaded.
        Set any attributes that are None to their default values. If a download
        is already being prepared, update those attributes and set the
        `retry_after` property. If the download has been prepared, set the
        `location` property.

        :param plugin: The plugin for the repository from which the file is to
                       be downloaded.
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
