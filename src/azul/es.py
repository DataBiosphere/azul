import logging
from typing import (
    Any,
    Collection,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import (
    urlencode,
)

from aws_requests_auth.boto_utils import (
    BotoAWSRequestsAuth,
)
from elasticsearch import (
    Connection,
    Elasticsearch,
    RequestsHttpConnection,
    Urllib3HttpConnection,
)

from azul import (
    config,
    lru_cache,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    es_log,
    http_body_log_message,
)

log = logging.getLogger(__name__)


class CachedBotoAWSRequestsAuth(BotoAWSRequestsAuth):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # We use the botocore session from Boto3 since it is pre-configured by
        # envhook.py to use cached credentials for the AssumeRoleProvider. This
        # avoids repeated entry of MFA tokens when running this code locally.
        # noinspection PyProtectedMember
        self._refreshable_credentials = aws.boto3_session.get_credentials()


class AzulConnection(Connection):
    """
    Improves the request logging by the Elasticsearch client library with
    respect to performance and utility. Most importantly, this class logs a
    request *before* it is made, not just when a response is received. At INFO
    level, only the beginning of a request or response body is logged. At DEBUG
    level the complete body is logged. Also eliminates expensive decoding at
    INFO level by logging the request body as a raw ``bytes`` literal. At DEBUG
    level, the *decoded* (and complete) body is logged as a string literal.
    """

    def perform_request(self,
                        method: str,
                        url: str,
                        params: Optional[Mapping[str, Any]] = None,
                        body: Optional[bytes] = None,
                        timeout: Optional[Union[int, float]] = None,
                        ignore: Collection[int] = (),
                        headers: Optional[Mapping[str, str]] = None
                        ) -> Tuple[int, Mapping[str, str], str]:
        self._log_request(method, self._full_url(url, params), headers, body)
        return super().perform_request(method, url, params, body, timeout, ignore, headers)

    def log_request_success(self,
                            method: str,
                            full_url: str,
                            path: str,
                            body: Optional[bytes],
                            status_code: int,
                            response: str,
                            duration: float
                            ) -> None:
        self._log_response(logging.INFO, status_code, duration, full_url, method, response)
        self._log_trace(method, path, body, status_code, response, duration)

    def log_request_fail(self,
                         method: str,
                         full_url: str,
                         path: str,
                         body: Optional[bytes],
                         duration: float,
                         status_code: Optional[int] = None,
                         response: Optional[str] = None,
                         exception: Optional[Exception] = None
                         ) -> None:
        self._log_response(logging.INFO if method == 'HEAD' and status_code == 404 else logging.WARN,
                           status_code, duration, full_url, method, response, exception)
        self._log_trace(method, path, body, status_code, response, duration)

    # Duplicates functionality in the ``perform_request`` method of the base
    # class so that our override of that method can log it speculatively. We
    # also log the full URL *actually* used by the base class when the response
    # is received, since it is only then that it is passed to our overrides of
    # ``log_request_success`` and ``log_request_fail``.

    def _full_url(self, url: str, params: Optional[Mapping[str, Any]]) -> str:
        full_url = self.host + self.url_prefix + url
        if params:
            full_url = f'{full_url}?{urlencode(params)}'
        return full_url

    def _log_request(self, method, full_url, headers, body):
        es_log.info('Making %s request to %s', method, full_url)
        es_log.debug('… with request headers %r', headers)
        es_log.info(http_body_log_message(es_log, 'request', body))

    def _log_response(self,
                      log_level: int,
                      status_code: Optional[int],
                      duration: float,
                      full_url: str,
                      method: str,
                      response: Optional[str],
                      exception=None
                      ) -> None:
        status_code = 'no' if status_code is None else status_code
        # Note that here we log the full URL actually used, see _full_url above
        es_log.log(log_level, 'Got %s response after %.3fs from %s to %s',
                   status_code, duration, method, full_url, exc_info=exception)
        es_log.log(log_level, http_body_log_message(es_log, 'response', response))


class AzulRequestsHttpConnection(AzulConnection, RequestsHttpConnection):
    pass


class AzulUrllib3HttpConnection(AzulConnection, Urllib3HttpConnection):
    pass


class ESClientFactory:

    @classmethod
    def get(cls) -> Elasticsearch:
        host, port = aws.es_endpoint
        return cls._create_client(host, port, config.es_timeout)

    @classmethod
    @lru_cache(maxsize=32)
    def _create_client(cls, host, port, timeout):
        log.debug(f'Creating ES client [{host}:{port}]')
        # Implicit retries don't make much sense in conjunction with optimistic
        # locking (versioning). Consider a write request that times out in ELB
        # with a 504 while the upstream ES node actually finishes the request.
        # Retrying that individual write request will fail with a 409. Instead
        # of retrying just the write request, the entire read-modify-write
        # transaction needs to be retried. In order to be in full control of
        # error handling, we disable the implicit retries via max_retries=0.
        common_params = dict(hosts=[dict(host=host, port=port)],
                             timeout=timeout,
                             max_retries=0)
        if host.endswith('.amazonaws.com'):
            aws_auth = CachedBotoAWSRequestsAuth(aws_host=host,
                                                 aws_region=aws.region_name,
                                                 aws_service='es')
            return Elasticsearch(http_auth=aws_auth,
                                 use_ssl=True,
                                 verify_certs=True,
                                 connection_class=AzulRequestsHttpConnection,
                                 **common_params)
        else:
            return Elasticsearch(connection_class=AzulUrllib3HttpConnection,
                                 **common_params)
