from collections.abc import (
    Iterable,
)
from typing import (
    Mapping,
    Sequence,
    Type,
)

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.field_type import (
    CataloguedFieldTypes,
    FieldType,
    FieldTypes,
    FieldTypes1,
    Nested,
    pass_thru_bool,
)
from azul.indexer.document import (
    Aggregate,
    Contribution,
    Document,
    FieldPath,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.lib import (
    cache,
)
from azul.lib.collections import (
    deep_dict_merge,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
    json_dict,
)
from azul.plugins import (
    FieldName,
    MetadataPlugin,
)


class DocumentService:

    @cache
    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return MetadataPlugin.load(catalog).create()

    @cache
    def aggregate_class(self, catalog: CatalogName) -> Type[Aggregate]:
        return self.metadata_plugin(catalog).aggregate_class()

    @property
    def always_limit_access(self) -> bool:
        """
        True if access restrictions are enforced unconditionally. False, if the
        filter stage is allowed to weaken them, e.g., based on the entity type.
        """
        return True

    def transformer_types(self,
                          catalog: CatalogName
                          ) -> Iterable[Type[Transformer]]:
        return self.metadata_plugin(catalog).transformer_types()

    @cache
    def entity_types(self, catalog: CatalogName) -> list[str]:
        return [
            transformer_cls.entity_type()
            for transformer_cls in self.transformer_types(catalog)
        ]

    @cache
    def field_type(self, catalog: CatalogName, path: FieldPath) -> FieldType:
        """
        Get the type of the field at the given document path.

        :param catalog: The catalog to operate on. Different catalogs may use
                        different field types.

        :param path: A tuple of keys to traverse document.
        """
        field_types: FieldTypes | FieldTypes1 = self.field_types(catalog)
        elements = iter(path)
        while isinstance(field_types, Mapping):
            field_types = field_types[next(elements)]
        if isinstance(field_types, Sequence):
            field_types = one(field_types)
        if isinstance(field_types, Nested):
            element = next(elements, None)
            if element is not None:
                assert element == field_types.agg_property, (element, field_types)
                field_types = field_types.properties[element]
        assert isinstance(field_types, FieldType), (path, field_types)
        element = next(elements, None)
        assert element is None, (element, field_types)
        return field_types

    def field_types(self, catalog: CatalogName) -> FieldTypes:
        """
        Returns a mapping of fields to field types

        :return: dict with nested keys matching OpenSearch fields and values
                 with the field's type
        """
        field_types = deep_dict_merge.from_iterable(
            transformer_cls.field_types()
            for transformer_cls in self.transformer_types(catalog)
        )
        aggregate_cls = self.aggregate_class(catalog)
        return deep_dict_merge(
            Contribution.field_types(field_types),
            aggregate_cls.field_types(field_types)
            # Replicas are intentionally omitted here because their contents
            # does not undergo translation
        )

    @cache
    def mapped_field_types(self, catalog: CatalogName) -> Mapping[FieldName, FieldType]:
        """
        Returns the field type for each supported sort and filter field, keyed
        to the name of the field as provided by clients. Unlike field_types(),
        this is a flat mapping and includes synthetic fields like 'accessible'
        that lack an entry in the plugin's field_mapping.
        """
        plugin = self.metadata_plugin(catalog)
        result = {}
        for field, path in plugin.field_mapping.items():
            field_type = self.field_type(catalog, path)
            result[field] = field_type
        accessible_field = plugin.special_fields.accessible.name
        assert accessible_field not in result, result
        result[accessible_field] = pass_thru_bool
        return result

    def catalogued_field_types(self) -> CataloguedFieldTypes:
        return {
            catalog: self.field_types(catalog)
            for catalog in config.catalogs
        }

    def translate_fields(self,
                         catalog: CatalogName,
                         doc: JSON,
                         *,
                         forward: bool,
                         allowed_paths: list[FieldPath] | None = None
                         ) -> MutableJSON:
        return json_dict(Document.translate_fields(doc,
                                                   self.field_types(catalog),
                                                   forward=forward,
                                                   allowed_paths=allowed_paths))
