from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth import boto_utils
from aws_requests_auth.aws_auth import AWSRequestsAuth

from azul import config


def es_client():
    host = config.es_endpoint[0]
    port = config.es_endpoint[1]
    ssl_required = True if port == 443 else False
    if ssl_required:
        aws_auth = AWSRequestsAuth(aws_host=host,
                                   aws_region='us-east-1',
                                   aws_service='es',
                                   **boto_utils.get_credentials())
        return Elasticsearch(hosts=[{'host': host, 'port': port}],
                             http_auth=aws_auth,
                             use_ssl=True,
                             verify_certs=True,
                             connection_class=RequestsHttpConnection)
    else:
        return Elasticsearch(hosts=[{'host': host, 'port': port}],
                             connection_class=RequestsHttpConnection)
