import attr

from azul import (
    CatalogName,
    JSON,
    cache,
    config,
)
from azul.openapi import (
    schema,
)
from azul.plugins import (
    MetadataPlugin,
    Plugin,
    RepositoryPlugin,
)
from azul.service import (
    Controller,
)


class CatalogController(Controller):

    # The custom return type annotation is an experiment. Please don't adopt
    # this just yet elsewhere in the program.

    def list_catalogs(self) -> schema.object(
        default_catalog=str,
        catalogs=schema.object(
            additional_properties=schema.object(
                atlas=str,
                internal=bool,
                plugins=schema.object(
                    additional_properties=schema.object(
                        name=str,
                        sources=schema.optional(schema.array(str)),
                        indices=schema.optional(schema.object(
                            additional_properties=schema.object(
                                default_sort=str,
                                default_order=str
                            )
                        )),
                    ),
                )
            )
        )
    ):
        return {
            'default_catalog': config.default_catalog,
            'catalogs': {
                catalog.name: {
                    'internal': catalog.internal,
                    'atlas': catalog.atlas,
                    'plugins': {
                        plugin_type: {
                            **attr.asdict(plugin),
                            **self._plugin_config(plugin_type, catalog.name)
                        }
                        for plugin_type, plugin in catalog.plugins.items()
                    }
                }
                for catalog in config.catalogs.values()
            }
        }

    @cache
    def _plugin_config(self, plugin_base_cls: str, catalog: CatalogName) -> JSON:
        plugin_base_cls = Plugin.type_for_name(plugin_base_cls)
        plugin_cls = plugin_base_cls.load(catalog)
        if issubclass(plugin_base_cls, RepositoryPlugin):
            plugin = plugin_cls.create(catalog)
            return {
                'sources': list(map(str, plugin.sources))
            }
        elif issubclass(plugin_base_cls, MetadataPlugin):
            plugin = plugin_cls.create()
            return {
                'indices': {
                    entity_type: {
                        'default_sort': sorting.field_name,
                        'default_order': sorting.order
                    }
                    for entity_type, sorting in plugin.exposed_indices.items()
                }
            }
        else:
            assert False, plugin_base_cls
