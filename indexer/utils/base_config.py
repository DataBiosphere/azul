from abc import ABC
from typing import Mapping, Any

from elasticsearch import Elasticsearch


class IndexProperties(ABC):
    def __init__(self, elasticsearch_host: str, elasticsearch_port: str) -> None:
        pass

    @property
    def elastic_search_client(self) -> Elasticsearch:
        return Elasticsearch()

    @property
    def mapping(self) -> Mapping[str, Any]:
        return {}

    @property
    def settings(self) -> Mapping[str, Any]:
        return {}