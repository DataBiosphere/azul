from inspect import (
    signature,
)
from typing import (
    Annotated,
    Any,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.lib import (
    cache,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
)
from azul.openapi import (
    responses,
    schema,
)
from azul.plugins import (
    MetadataPlugin,
    Plugin,
    RepositoryPlugin,
)
from azul.service.controller import (
    ServiceController,
)


class CatalogController(ServiceController):

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/index/catalogs',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'List all available catalogs.',
                'tags': ['Index'],
                'responses': {
                    '200': {
                        'description': fd('''
                            The name of the default catalog and a list of all available
                            catalogs. For each catalog, the response includes the name
                            of the atlas the catalog belongs to, a flag indicating
                            whether the catalog is for internal use only as well as the
                            names and types of plugins currently active for the catalog.
                            For some plugins, the response includes additional
                            configuration properties, such as the sources used by the
                            repository plugin to populate the catalog or the set of
                            available [indices][1].

                            [1]: #operations-Index-get_index__entity_type_
                        '''),
                        **responses.json_content(
                            # The custom return type annotation is an experiment. Please
                            # don't adopt this just yet elsewhere in the program.
                            one(signature(self.list_catalogs).return_annotation.__metadata__)
                        )
                    }
                }
            }
        )
        def get_index_catalogs():
            return self.list_catalogs()

        return locals()

    # The custom return type annotation is an experiment. Please don't adopt
    # this just yet elsewhere in the program.

    def list_catalogs(self) -> Annotated[JSON, schema.object(
        default_catalog=str,
        catalogs=schema.object(
            additionalProperties=schema.object(
                atlas=str,
                internal=bool,
                mirror_limit=schema.optional(int),
                plugins=schema.object(
                    additionalProperties=schema.object(
                        name=str,
                        sources=schema.optional(schema.array(str)),
                        indices=schema.optional(schema.object(
                            additionalProperties=schema.object(
                                default_sort=str,
                                default_order=str
                            )
                        )),
                    ),
                )
            )
        )
    )]:
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
    def _plugin_config(self, plugin_type_name: str, catalog: CatalogName) -> JSON:
        plugin_base_cls: type[Plugin] = Plugin.type_for_name(plugin_type_name)
        plugin_cls = plugin_base_cls.load(catalog)
        if issubclass(plugin_cls, RepositoryPlugin):
            repository_plugin = plugin_cls.create(catalog)
            return {
                'sources': [
                    {
                        'sourceSpec': source_spec.to_json(),
                        'sourceConfig': source_config.to_json()
                    }
                    for source_spec, source_config in repository_plugin.sources.items()
                ]
            }
        elif issubclass(plugin_cls, MetadataPlugin):
            metadata_plugin = plugin_cls.create()
            return {
                'indices': {
                    entity_type: {
                        'default_sort': sorting.field_name,
                        'default_order': sorting.order
                    }
                    for entity_type, sorting in metadata_plugin.exposed_indices.items()
                }
            }
        else:
            assert False, plugin_cls
