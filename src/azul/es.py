import logging

from aws_requests_auth.boto_utils import (
    BotoAWSRequestsAuth,
)
from elasticsearch import (
    Elasticsearch,
    RequestsHttpConnection,
)

from azul import (
    config,
    lru_cache,
)
from azul.deployment import (
    aws,
)

logger = logging.getLogger(__name__)


class CachedBotoAWSRequestsAuth(BotoAWSRequestsAuth):

    def __init__(self, *args, **kwags):
        super().__init__(*args, **kwags)
        # We use the botocore session from Boto3 since it is pre-configured by
        # envhook.py to use cached credentials for the AssumeRoleProvider. This
        # avoids repeated entry of MFA tokens when running this code locally.
        # noinspection PyProtectedMember
        self._refreshable_credentials = aws.boto3_session.get_credentials()


class ESClientFactory:

    @classmethod
    def get(cls) -> Elasticsearch:
        host, port = aws.es_endpoint
        return cls._create_client(host, port, config.es_timeout)

    @classmethod
    @lru_cache(maxsize=32)
    def _create_client(cls, host, port, timeout):
        logger.debug(f'Creating ES client [{host}:{port}]')
        # Implicit retries don't make much sense in conjunction with optimistic locking (versioning). Consider a
        # write request that times out in ELB with a 504 while the upstream ES node actually finishes the request.
        # Retrying that individual write request will fail with a 409. Instead of retrying just the write request,
        # the entire read-modify-write transaction needs to be retried. In order to be in full control of error
        # handling, we disable the implicit retries via max_retries=0.
        common_params = dict(hosts=[dict(host=host, port=port)],
                             timeout=timeout,
                             max_retries=0)
        if host.endswith(".amazonaws.com"):
            aws_auth = CachedBotoAWSRequestsAuth(aws_host=host,
                                                 aws_region=aws.region_name,
                                                 aws_service='es')
            return Elasticsearch(http_auth=aws_auth,
                                 use_ssl=True,
                                 verify_certs=True,
                                 connection_class=RequestsHttpConnection, **common_params)
        else:
            return Elasticsearch(**common_params)
