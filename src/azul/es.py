import logging
from functools import lru_cache

from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection

from azul import config
from azul.deployment import aws

logger = logging.getLogger(__name__)


class ESClientFactory:

    @classmethod
    def get(cls):
        host, port = config.es_endpoint
        return cls._create_client(host, port, config.es_timeout)

    @classmethod
    @lru_cache(maxsize=32)
    def _create_client(cls, host, port, timeout):
        logger.debug(f'Creating ES client [{host}:{port}]')
        common_params = dict(hosts=[dict(host=host, port=port)], timeout=timeout)
        if host.endswith(".amazonaws.com"):
            aws_auth = BotoAWSRequestsAuth(aws_host=host, aws_region=aws.region_name, aws_service='es')
            return Elasticsearch(http_auth=aws_auth, use_ssl=True, verify_certs=True,
                                 connection_class=RequestsHttpConnection, **common_params)
        else:
            return Elasticsearch(**common_params)
