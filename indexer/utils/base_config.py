from abc import ABC
from elasticsearch import Elasticsearch
from typing import Mapping, Any


class IndexProperties(ABC):
    @property
    def elastic_search_client(self) -> Elasticsearch:
        return Elasticsearch()

    @property
    def mapping(self) -> Mapping[str, Any]:
        return {}

    @property
    def settings(self) -> Mapping[str, Any]:
        return {}
