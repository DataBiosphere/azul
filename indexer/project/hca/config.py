from aws_requests_auth import boto_utils
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import os
import sys
from typing import Iterable, Mapping, Any

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'chalicelib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from utils.base_config import BaseIndexProperties
from utils.transformer import Transformer


class IndexProperties(BaseIndexProperties):
    """Index properties for HCA"""

    def __init__(self, dss_url: str,
                 elasticsearch_host: str, elasticsearch_port: str) -> None:
        """Initialize properties."""
        self._dss_url = dss_url
        self._es_host = elasticsearch_host
        self._es_port = elasticsearch_port
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
        if self._es_host.endswith('.es.amazonaws.com'):
            # need to have the AWS CLI and $aws configure
            awsauth = AWSRequestsAuth(
                aws_host=self._es_host,
                aws_region='us-west-2',
                aws_service='es',
                **boto_utils.get_credentials()
            )
            # use the requests connection_class and pass in our custom
            # auth class
            es = Elasticsearch(
                hosts=[{'host': self._es_host, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
        else:
            # default auth for testing purposes
            es = Elasticsearch([{'host': self._es_host,
                                 'port': self._es_port}])
        return es

    @property
    def mapping(self) -> Mapping[str, Any]:
        return self._es_mapping

    @property
    def settings(self) -> Mapping[str, Any]:
        return self._es_settings

    @property
    def transformers(self) -> Iterable[Transformer]:
        from project.hca.transformers import FileTransformer,\
            SpecimenTransformer, ProjectTransformer
        transformers = [FileTransformer,
                        SpecimenTransformer, ProjectTransformer]
        return transformers

