from abc import (
    ABC,
    abstractmethod,
)
from functools import lru_cache
import importlib
from inspect import isabstract
from typing import (
    Mapping,
    MutableMapping,
    NamedTuple,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from azul import config
from azul.indexer.index_service import IndexService
from azul.indexer.transformer import (
    Document,
    FieldType,
    FieldTypes,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
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
    The base class for Azul plugins.

    To obtain a plugin instance at runtime, Azul will dynamically load the
    module `azul.plugins.metadata.$AZUL_PROJECT` where `AZUL_PROJECT` is an
    environment variable. Once the module is loaded, Azul will retrieve
    `Plugin` attribute of that module. The value of that attribute is expected
    to be a concrete subclass of this class. Finally, Azul will invoke the
    constructor of that class in order to obtain an actual instance of the
    plugin. The constructor will be invoked without arguments.
    """

    @classmethod
    def load(cls: Type[T]) -> T:
        """
        Load and return the Azul plugin configured via the `AZUL_PROJECT`
        environment variable.

        A plugin is an instance of a concrete subclass of the `Plugin` class.
        """
        assert cls != Plugin, f'Must use an subclass of {cls.__name__}'
        assert isabstract(cls) != Plugin, f'Must use an abstract subclass of {cls.__name__}'
        plugin_type_name = cls.name()
        plugin_package_name = config.plugin_name(plugin_type_name)
        plugin_package_path = f'{__name__}.{plugin_type_name}.{plugin_package_name}'
        plugin_module = importlib.import_module(plugin_package_path)
        plugin_cls = plugin_module.Plugin
        assert issubclass(plugin_cls, cls)
        return plugin_cls()

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        raise NotImplementedError()


class MetadataPlugin(Plugin):

    @classmethod
    def name(cls) -> str:
        return 'metadata'

    @abstractmethod
    def indexer_class(self) -> Type[IndexService]:
        raise NotImplementedError()

    @lru_cache(maxsize=None)
    def field_type(self, path: Tuple[str, ...]) -> FieldType:
        """
        Get the field type of a field specified by the full field name split on '.'
        :param path: A tuple of keys to traverse down the field_types dict
        """
        field_types = self.field_types()
        for p in path:
            try:
                field_types = field_types[p]
            except KeyError:
                raise KeyError(f'Path {path} not represented in field_types')
            except TypeError:
                raise TypeError(f'Path {path} not represented in field_types')
            if field_types is None:
                return None
        return field_types

    def field_types(self) -> FieldTypes:
        return self.indexer_class().field_types()

    def translate_fields(self, doc: AnyJSON, forward: bool = True) -> AnyMutableJSON:
        return Document.translate_fields(doc, self.field_types(), forward)

    @abstractmethod
    def service_config(self) -> ServiceConfig:
        """
        Returns service configuration in a legacy format.
        """
        raise NotImplementedError()


class RepositoryPlugin(Plugin):

    @classmethod
    def name(cls) -> str:
        return 'repository'

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
