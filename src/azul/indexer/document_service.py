from collections.abc import (
    Iterable,
)
from typing import (
    Type,
)

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    cache,
    config,
)
from azul.collections import (
    deep_dict_merge,
)
from azul.indexer.document import (
    Aggregate,
    CataloguedFieldTypes,
    Contribution,
    Document,
    FieldType,
    FieldTypes,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins import (
    FieldPath,
    MetadataPlugin,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
)


class DocumentService:

    @cache
    def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
        return MetadataPlugin.load(catalog).create()

    @cache
    def aggregate_class(self, catalog: CatalogName) -> Type[Aggregate]:
        return self.metadata_plugin(catalog).aggregate_class()

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
        field_types = self.field_types(catalog)
        for element in path:
            try:
                field_types = field_types[element]
            except (KeyError, TypeError) as e:
                raise type(e)('Path not represented in field_types', path)
        if isinstance(field_types, list):
            field_types = one(field_types)
        return field_types

    def field_types(self, catalog: CatalogName) -> FieldTypes:
        """
        Returns a mapping of fields to field types

        :return: dict with nested keys matching Elasticsearch fields and values
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

    def catalogued_field_types(self) -> CataloguedFieldTypes:
        return {
            catalog: self.field_types(catalog)
            for catalog in config.catalogs
        }

    def translate_fields(self,
                         catalog: CatalogName,
                         doc: AnyJSON,
                         *,
                         forward: bool
                         ) -> AnyMutableJSON:
        return Document.translate_fields(doc, self.field_types(catalog), forward=forward)
