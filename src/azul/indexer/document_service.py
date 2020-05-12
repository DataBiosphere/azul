from functools import lru_cache
from typing import Tuple

from boltons.cacheutils import cachedproperty
from azul.indexer.transformer import (
    Aggregate,
    Contribution,
    Document,
    FieldType,
    FieldTypes,
)
from azul.plugins import MetadataPlugin
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
)


class DocumentService:

    @cachedproperty
    def metadata_plugin(self):
        return MetadataPlugin.load()

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
        """
        Returns a mapping of fields to field types

        :return: dict with nested keys matching Elasticsearch fields and values with the field's type
        """
        field_types = {}
        for transformer in self.metadata_plugin.transformers():
            field_types.update(transformer.field_types())
        return {
            **Contribution.field_types(field_types),
            **Aggregate.field_types(field_types)
        }

    def translate_fields(self, doc: AnyJSON, forward: bool = True) -> AnyMutableJSON:
        return Document.translate_fields(doc, self.field_types(), forward)
