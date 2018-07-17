from abc import ABC
from elasticsearch import Elasticsearch
import os
from typing import Iterable, Mapping, Any
from azul.transformer import Transformer


class BaseIndexProperties(ABC):
    @property
    def elastic_search_client(self) -> Elasticsearch:
        return Elasticsearch()

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
        environment = os.getenv("STAGE_ENVIRONMENT", "dev")
        return ["browser_{}_{}".format(entity, environment) for entity in entities]
