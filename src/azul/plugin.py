from abc import ABC, abstractmethod
import importlib
from typing import Type

from azul import config
from azul.indexer import BaseIndexer
from azul.types import JSON


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

    def dss_deletion_subscription_query(self, prefix: str) -> JSON:
        """
        The query to use for subscribing Azul to bundle deletions in the DSS.

        :param prefix: a prefix that restricts the set of bundles to subscribe to. This parameter is used to subset
                       or partition the set of bundles in the DSS. The returned query should only match bundles whose
                       UUID starts with the given prefix.
        """
        raise NotImplementedError()

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
