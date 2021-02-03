from azul import (
    cache,
    config,
)
from azul.openapi import (
    schema,
)
from azul.plugins import (
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
                plugins=schema.array(
                    schema.object(
                        name=str,
                        type=str,
                        additional_properties=True
                    )
                )
            )
        )
    ):
        return {
            'default_catalog': config.default_catalog,
            'catalogs': {
                catalog.name: {
                    'internal': catalog.is_internal,
                    'atlas': catalog.atlas,
                    'plugins': [
                        {
                            'type': plugin_type,
                            'name': plugin,
                            **self._plugin_config(plugin_type, catalog.name)
                        }
                        for plugin_type, plugin in catalog.plugins.items()
                    ]
                }
                for catalog in config.catalogs.values()
            }
        }

    @cache
    def _plugin_config(self, plugin_type: str, catalog: str):
        plugin_type = Plugin.type_for_name(plugin_type)
        if plugin_type == RepositoryPlugin:
            return {
                'sources': list(plugin_type.load(catalog).create(catalog).sources)
            }
        else:
            return {}
