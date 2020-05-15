from abc import (
    ABC,
    abstractmethod,
)
import importlib
from inspect import isabstract
from typing import (
    Iterable,
    Mapping,
    MutableMapping,
    NamedTuple,
    Sequence,
    Type,
    TypeVar,
    Union,
)

from azul import config
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.indexer.transform import Transformer
from azul.types import (
    JSON,
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
    def load(cls: Type[T]) -> Type[T]:
        """
        Load and return one of the concrete subclasses of the class this method
        is called on.
        """
        assert cls != Plugin, f'Must use a subclass of {cls.__name__}'
        assert isabstract(cls) != Plugin, f'Must use an abstract subclass of {cls.__name__}'
        plugin_type_name = cls._name()
        plugin_package_name = config.plugin_name(plugin_type_name)
        plugin_package_path = f'{__name__}.{plugin_type_name}.{plugin_package_name}'
        plugin_module = importlib.import_module(plugin_package_path)
        plugin_cls = plugin_module.Plugin
        assert issubclass(plugin_cls, cls)
        return plugin_cls

    @classmethod
    @abstractmethod
    def _name(cls) -> str:
        raise NotImplementedError()


class MetadataPlugin(Plugin):

    @classmethod
    def _name(cls) -> str:
        return 'metadata'

    # If the need arises to parameterize instances of a concrete plugin class,
    # add the parameters to create() and make it abstract.

    @classmethod
    def create(cls) -> 'MetadataPlugin':
        return cls()

    @abstractmethod
    def mapping(self) -> JSON:
        raise NotImplementedError()

    @abstractmethod
    def transformers(self) -> Iterable[Type[Transformer]]:
        raise NotImplementedError()

    @abstractmethod
    def service_config(self) -> ServiceConfig:
        """
        Returns service configuration in a legacy format.
        """
        raise NotImplementedError()


class RepositoryPlugin(Plugin):

    @classmethod
    def _name(cls) -> str:
        return 'repository'

    # If the need arises to parameterize instances of a concrete plugin class,
    # add the parameters to create() and make it abstract.

    @classmethod
    def create(cls) -> 'RepositoryPlugin':
        return cls()

    @abstractmethod
    def fetch_bundle(self, bundle_fqid: BundleFQID) -> Bundle:
        raise NotImplementedError()

    @abstractmethod
    def dss_subscription_query(self, prefix: str) -> JSON:
        """
        The query to use for subscribing Azul to bundle additions in the DSS. This query will also be used for
        listing bundles in the DSS during reindexing.

        :param prefix: a prefix that restricts the set of bundles to subscribe to. This parameter is used to subset
                       or partition the set of bundles in the DSS. The returned query should only match bundles whose
                       UUID starts with the given prefix.
        """
        raise NotImplementedError()

    @abstractmethod
    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        """
        The query to use for subscribing Azul to bundle deletions in the DSS.

        :param prefix: a prefix that restricts the set of bundles to subscribe to. This parameter is used to subset
                       or partition the set of bundles in the DSS. The returned query should only match bundles whose
                       UUID starts with the given prefix.
        """
        raise NotImplementedError()

    @abstractmethod
    def portal_db(self) -> Sequence[JSON]:
        """
        Returns integrations data object
        """
        raise NotImplementedError()
