from abc import ABC
from typing import Any, Iterable, Mapping

from azul import config
from azul.transformer import Transformer


class BaseIndexProperties(ABC):

    @property
    def dss_url(self) -> str:
        return ""

    @property
    def mapping(self) -> Mapping[str, Any]:
        return {}

    @property
    def settings(self) -> Mapping[str, Any]:
        return {}

    @property
    def transformers(self) -> Iterable[Transformer]:
        return [Transformer()]

    @property
    def entities(self) -> Iterable[str]:
        return [""]

    @property
    def index_names(self) -> Iterable[str]:
        entities = self.entities
        return [config.es_index_name(entity) for entity in entities]
