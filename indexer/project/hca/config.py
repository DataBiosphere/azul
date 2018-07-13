from functools import lru_cache
from typing import Any, Iterable, Mapping, Optional, Tuple

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from utils.base_config import BaseIndexProperties
from utils.deployment import aws
from utils.transformer import Transformer


class IndexProperties(BaseIndexProperties):
    """Index properties for HCA"""

    def __init__(self, dss_url: str, es_endpoint: Tuple[str, int]) -> None:
        self._dss_url = dss_url
        self._es_endpoint = es_endpoint

        self._es_mapping = {
            "dynamic_templates": [
                {
                    "strings": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text",
                            "analyzer": "autocomplete",
                            "search_analyzer": "standard",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        }
                    }
                }
            ]
        }
        self._es_settings = {
            "analysis": {
                "filter": {
                    "autocomplete_filter": {
                        "type": "ngram",
                        "min_gram": 1,
                        "max_gram": 36
                    }
                },
                "analyzer": {
                    "autocomplete": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": [
                            "lowercase",
                            "autocomplete_filter"
                        ]
                    }
                }
            }
        }

    @property
    def dss_url(self) -> str:
        return self._dss_url

    @property
    def elastic_search_client(self) -> Elasticsearch:
        host, port = self._es_endpoint
        return self._elastic_search_client(host, port, 60)

    # Stolen from https://github.com/HumanCellAtlas/data-store/blob/master/dss/index/es/__init__.py#L66

    @lru_cache(maxsize=32)
    def _elastic_search_client(self, host, port, timeout):

        common_params = dict(hosts=[dict(host=host, port=port)],
                             timeout=timeout)
        if host.endswith(".amazonaws.com"):
            session = boto3.session.Session()
            current_credentials = session.get_credentials().get_frozen_credentials()
            es_auth = AWS4Auth(current_credentials.access_key,
                               current_credentials.secret_key,
                               session.region_name,
                               "es",
                               session_token=current_credentials.token)
            client = Elasticsearch(use_ssl=True,
                                   verify_certs=True,
                                   connection_class=RequestsHttpConnection,
                                   http_auth=es_auth,
                                   **common_params)
        else:
            client = Elasticsearch(use_ssl=False,
                                   **common_params)
        return client

    @property
    def mapping(self) -> Mapping[str, Any]:
        return self._es_mapping

    @property
    def settings(self) -> Mapping[str, Any]:
        return self._es_settings

    @property
    def transformers(self) -> Iterable[Transformer]:
        from project.hca.transformers import FileTransformer, SpecimenTransformer
        transformers = [FileTransformer(), SpecimenTransformer()]
        return transformers

    @property
    def entities(self) -> Iterable[str]:
        return ["files", "specimens", "projects"]
