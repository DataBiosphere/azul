from aws_requests_auth import boto_utils
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
from typing import Mapping, Any
from utils.base_config import IndexProperties


class HCAIndexProperties(IndexProperties):
    """Index properties for HCA"""

    def __init__(self, elasticsearch_host: str, elasticsearch_port: str) -> None:
        """Initialize properties."""
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
