import logging
import sys
from time import (
    time,
)
from typing import (
    Self,
)

import certifi
from furl import (
    furl,
)
import urllib3
import urllib3.exceptions
import urllib3.request

from azul import (
    cached_property,
    config,
)
from azul.logging import (
    http_body_log_message,
)


class HttpClientDecorator(urllib3.request.RequestMethods):
    """
    A convenience base class for implementations of the RequestMethods interface
    that decorate some other instance of an implementation of that interface.
    """

    def __init__(self,
                 inner: urllib3.request.RequestMethods,
                 headers: dict | None = None):
        # We'd use attrs but for some unknown reason that doesn't play well
        # with the superclass constructor.
        super().__init__(headers)
        self._inner = inner

    def urlopen(self, *args, **kwargs):
        return self._inner.urlopen(*args, **kwargs)


class LoggingHttpClient(HttpClientDecorator):
    """
    An HTTP client that logs every request and response to the given logger.
    Request and response bodies will be logged at DEBUG level, and only a prefix
    will be logged. Request and response headers will be logged at DEBUG level.
    Additionally, AZUL_DEBUG must be at least 2 for request headers to be logged
    at all, in order to protect any credentials contained therein.
    """

    def __init__(self,
                 log: logging.Logger,
                 inner: urllib3.request.RequestMethods,
                 headers: dict | None = None):
        super().__init__(inner, headers)
        self._log = log

    def urlopen(self, method, url, body=None, **kwargs) -> urllib3.HTTPResponse:
        log = self._log
        log.info('Making %s request to %r', method, url)
        if config.debug > 1:
            log.debug('… with keyword args %r', kwargs)
        log.debug(http_body_log_message('request', body))
        start = time()
        response = super().urlopen(method, url, body=body, **kwargs)
        duration = time() - start
        assert isinstance(response, urllib3.HTTPResponse), type(response)
        log.info('Got %s response after %.3fs from %s to %s',
                 response.status, duration, method, url)
        log.debug('… with response headers %r', response.headers)
        if response.isclosed():
            log.debug(http_body_log_message('response', response.data))
        else:
            log.debug('… with a streamed response body')
        return response


def http_client(log: logging.Logger | None = None) -> urllib3.request.RequestMethods:
    pool_manager = urllib3.PoolManager(ca_certs=certifi.where())
    if log is None:
        return pool_manager
    else:
        return LoggingHttpClient(inner=pool_manager, log=log)


class LimitedTimeoutException(Exception):

    def __init__(self, url: furl, timeout: float):
        super().__init__(f'No response from {url} within {timeout} seconds')


class TooManyRequestsException(Exception):

    def __init__(self, url: furl):
        super().__init__(f'Maximum request rate exceeded for {url}')


class LimitedRetry(urllib3.Retry):
    """
    First, set up the fixtures:

    >>> from urllib3.exceptions import ReadTimeoutError
    >>> from urllib3.connectionpool import ConnectionPool
    >>> from typing import cast
    >>> from time import sleep
    >>> pool = cast(ConnectionPool, None)
    >>> error = ReadTimeoutError(pool=pool, url='', message='')

    With zero retries …

    >>> r = LimitedRetry.create(retries=0)

    … there still is one tentative retry on read:

    >>> r.connect, r.read, r.redirect, r.status, r.other
    (0, 1, 0, 0, 0)

    A fresh instance is not exhausted:

    >>> r.is_exhausted()
    False

    After a read error, that tentative retry is consumed …

    >>> r = r.increment(method='GET', error=error)
    >>> r.connect, r.read, r.redirect, r.status, r.other
    (0, 0, 0, 0, 0)

    … but since less than 10 ms have passed, the instance is not yet exhausted:

    >>> r.is_exhausted()
    False

    Exhaustion sets in only after a longer delay:

    >>> sleep(.02)
    >>> r.is_exhausted()
    True
    """
    start: float
    retries: int

    @classmethod
    def create(cls, *, retries: int) -> Self:
        # No retries on redirects, limited retries on server failures and I/O
        # errors such as refused or dropped connections. The latter are actually
        # very likely if connections from the pool are reused after a long
        # period of being idle. That's why we need at least one retry on read …
        self = cls(total=None,
                   connect=retries,
                   read=retries + 1,
                   redirect=0,
                   status=retries,
                   other=retries,
                   status_forcelist={500, 502, 503})
        self.start = time()
        self.retries = retries
        return self

    def is_exhausted(self):
        # … but only if the first read attempt failed quickly, in under 10ms.
        # Otherwise, read errors that don't result from a stale pool connection
        # could exceed the overall timeout by as much as 100%. The point of zero
        # retries is to guarantee that the timeout is not exceeded.
        return (
            super().is_exhausted()
            or self.retries == 0 and time() - self.start > .01
        )

    def new(self, **kwargs) -> Self:
        # This is a copy constructor that's used to create a new instance with
        # decremented retry counters. The `is_exhausted` method will be called
        # on the copy in order to determine if another attempt should be made.
        other = super().new(**kwargs)
        other.start = self.start
        other.retries = self.retries
        return other


class LimitedRetryHttpClient(HttpClientDecorator):

    @property
    def _timing_is_restricted(self) -> bool:
        return config.lambda_is_handling_api_gateway_request

    @property
    def timeout(self) -> float:
        return 5 if self._timing_is_restricted else 20

    @property
    def retries(self) -> int:
        return 0 if self._timing_is_restricted else 2

    def urlopen(self, method, url, **kwargs):
        timeout, retries = self.timeout, self.retries
        retry = LimitedRetry.create(retries=retries)
        try:
            return super().urlopen(method, url, retries=retry, timeout=timeout, **kwargs)
        except (urllib3.exceptions.TimeoutError, urllib3.exceptions.MaxRetryError):
            raise LimitedTimeoutException(url, timeout)


class Propagate429HttpClient(HttpClientDecorator):

    def urlopen(self, method, url, **kwargs):
        response = super().urlopen(method, url, **kwargs)
        if response.status == 429:
            raise TooManyRequestsException(url)
        else:
            return response


class HasCachedHttpClient:
    """
    A convenience mixin that provides a cached instance property referring to an
    HTTP client. The client uses a connection pool and logs all requests to the
    logger of the module defining the concrete subclass. The module is expected
    to have a variable called ``log`` referencing a ``logging.Logger`` instance.
    """

    @cached_property
    def _http_client(self) -> urllib3.request.RequestMethods:
        return self._create_http_client()

    def _create_http_client(self) -> urllib3.request.RequestMethods:
        """
        Subclasses can override this method to replace, wrap or modify the HTTP
        client instance returned by this method.
        """
        log = getattr(sys.modules[type(self).__module__], 'log')
        assert isinstance(log, logging.Logger), type(log)
        return http_client(log)
