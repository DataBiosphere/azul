from abc import ABC, abstractmethod
import importlib
from typing import Type, Sequence, NamedTuple, Mapping, Union

from azul import config
from azul.indexer import BaseIndexer
from azul.types import JSON


class ServiceConfig(NamedTuple):
    translation: Mapping[str, str]
    autocomplete_translation: Mapping[str, Mapping[str, str]]
    manifest: Mapping[str, Mapping[str, str]]
    cart_item: Mapping[str, Sequence[str]]
    facets: Sequence[str]
    autocomplete_mapping_config: Mapping[str, Mapping[str, Union[str, Sequence[str]]]]
    order_config: Sequence[str]


class Plugin(ABC):
    """
    The base class for Azul plugins.

    To obtain a plugin instance at runtime, Azul will dynamically load the module `azul.project.$AZUL_PROJECT` where
    `AZUL_PROJECT` is an environment variable. Once the module is loaded, Azul will retrieve `Plugin` attribute of
    that module. The value of that attribute is expected to be a concrete subclass of this class. Finally,
    Azul will invoke the constructor of that class in order to obtain an actual instance of the plugin. The
    constructor will be invoked without arguments.
    """

    @abstractmethod
    def indexer_class(self) -> Type[BaseIndexer]:
        raise NotImplementedError()

    def field_types(self):
        return self.indexer_class().field_types()

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
    def service_config(self) -> ServiceConfig:
        """
        Returns service configuration in a legacy format. This used to be defined in a JSON file called
        request_config.json, hence the name.
        """
        raise NotImplementedError()

    def autocomplete_mapping_config(self) -> JSON:
        """
        Returns service autocomplete mapping configuration in a legacy format. This used to be defined in a JSON file
        called `autocomplete_mapping_config.json`, hence the name.
        """
        return self.service_config().autocomplete_mapping_config

    def order_config(self) -> Sequence[str]:
        """
        Returns service order configuration in a legacy format. This used to be defined in a text file
        called `order_config`, hence the name.
        """
        return self.service_config().order_config

    @classmethod
    def load(cls) -> 'Plugin':
        """
        Load and return the Azul plugin configured via the `AZUL_PROJECT` environment variable.

        A plugin is an instance of a concrete subclass of the `Plugin` class.
        """
        plugin_module = importlib.import_module(config.plugin_name)
        plugin_cls = plugin_module.Plugin
        assert issubclass(plugin_cls, cls)
        return plugin_cls()
