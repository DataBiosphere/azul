import logging
from typing import (
    Any,
    Collection,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
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
from elasticsearch.transport import (
    Transport,
)
import requests
import requests.auth
import urllib3.request

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
        es_log.info(http_body_log_message('request',
                                          body,
                                          verbatim=self._log_body_verbatim))

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
        es_log.log(log_level, http_body_log_message('response',
                                                    response,
                                                    verbatim=self._log_body_verbatim))

    @property
    def _log_body_verbatim(self):
        return es_log.isEnabledFor(logging.DEBUG)


class AzulRequestsHttpConnection(AzulConnection, RequestsHttpConnection):
    pass


class AWSAuthHttpClient(urllib3.request.RequestMethods):
    """
    Decorates a urllib3 HTTPConnectionPool instance so that requests are
    signed with AWS's Signature Version 4 flavor of HMAC.
    """

    def __init__(self,
                 pool: urllib3.HTTPConnectionPool,
                 http_auth: BotoAWSRequestsAuth):
        super().__init__()
        self._inner = pool
        self._http_auth = http_auth

    def urlopen(self,
                method: str,
                url: str,
                body: bytes | None = None,
                headers: Mapping[str, str] | None = None,
                **kwargs
                ) -> urllib3.HTTPResponse:
        # self._http_auth is an instance of BotoAWSRequestsAuth, a subclass of
        # AuthBase from the Requests library. To use that instance with urllib3
        # directly, we need to prepare a Requests request object, sign it with
        # self._http_auth and pass the resulting signature header to urllib3's
        # urlopen() method.
        request = requests.PreparedRequest()
        request.method = method
        # Because urllib3 connection pools are host-specific, URLs passed to a
        # connection pool's urlencode() must be relative and path-absolute. And
        # while PreparedRequest.prepare() requires an absolute URL, we can sneak
        # a relative one in by setting the attribute directly. This neatly
        # avoids having to compose an absolute URL and the URL-encoding
        # ambiguities that entails. The Elasticsearch client, for example,
        # encodes colons in absolute paths even though the leading slash in such
        # a path makes that unnecessary. These ambiguities could lead to an
        # invalid signature. The AWS signature algorithm only looks at path and
        # query of URLs.
        assert url.startswith('/'), url
        request.url = url
        request.headers = headers
        request.body = body
        request = self._http_auth(request)
        # Note that the various urlopen() implementations in urllib3 declare the
        # `body` argument with a default value, making it a keyword argument,
        # the ES client passes it as a positional. If this were ever to change,
        # this method would get a duplicate of the `body` argument as part of
        # `kwargs`, resulting in a TypeError.
        return self._inner.urlopen(method, url, body, headers=request.headers, **kwargs)

    def close(self):
        self._inner.close()


class AzulUrllib3HttpConnection(AzulConnection, Urllib3HttpConnection):

    def __init__(self, *args, http_auth: BotoAWSRequestsAuth = None, **kwargs):
        super().__init__(*args, **kwargs)
        if http_auth is not None:
            # We can't extend the pool class because we don't control the
            # instantiation. We therefore have to decorate the pool instance.
            # Looking at the source of Urllib3HttpConnection we notice that only
            # the methods `urlopen()` and `close()` are called. This means that
            # the decorating class doesn't need to implement (or extend) a full
            # HTTPConnectionPool, only the much slimmer RequestMethods.
            client = AWSAuthHttpClient(self.pool, http_auth)
            # We still need the cast because the stub declares `self.pool` to be
            # an instance of HTTPConnectionPool.
            self.pool = cast(urllib3.HTTPConnectionPool, client)


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
                             max_retries=0,
                             transport_class=ProductAgnosticTransport)
        if host.endswith('.amazonaws.com'):
            aws_auth = CachedBotoAWSRequestsAuth(aws_host=host,
                                                 aws_region=aws.region_name,
                                                 aws_service='es')
            return Elasticsearch(http_auth=aws_auth,
                                 use_ssl=True,
                                 verify_certs=True,
                                 connection_class=AzulUrllib3HttpConnection,
                                 **common_params)
        else:
            return Elasticsearch(connection_class=AzulUrllib3HttpConnection,
                                 **common_params)


class ProductAgnosticTransport(Transport):
    """
    A transport class that disables client-side product verification. This
    bypasses the check that would otherwise prevent us from using ES v7.15+ with
    OpenSearch.
    """

    def _do_verify_elasticsearch(self, *_args, **__kwargs):
        self._verified_elasticsearch = True
