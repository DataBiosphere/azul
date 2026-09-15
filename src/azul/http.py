from collections.abc import (
    Mapping,
)
import logging
import sys
import time
from typing import (
    Any,
    ClassVar,
    Self,
)

import certifi
from furl import (
    furl,
)
import urllib3
import urllib3._request_methods
import urllib3.connection
import urllib3.connectionpool
import urllib3.exceptions

from azul import (
    config,
)
from azul.lib import (
    R,
    cache,
    cached_property,
)
from azul.lib.strings import (
    redact,
)
from azul.logging import (
    http_body_log_message,
)

HttpClient = urllib3._request_methods.RequestMethods


class HttpClientDecorator(HttpClient):
    """
    A convenience base class for implementations of the RequestMethods interface
    that decorate some other instance of an implementation of that interface.
    """

    def __init__(self,
                 inner: HttpClient,
                 headers: dict | None = None):
        # We'd use attrs but for some unknown reason that doesn't play well
        # with the superclass constructor.
        super().__init__(headers)
        self._inner = inner

    def urlopen(self, *args, **kwargs) -> urllib3.BaseHTTPResponse:
        return self._inner.urlopen(*args, **kwargs)

    def delegate[T: HttpClient](self, cls: type[T]) -> T | None:
        inner = self._inner
        while True:
            if isinstance(inner, cls):
                return inner
            elif isinstance(inner, HttpClientDecorator):
                inner = inner._inner
            else:
                return None


def redact_headers(headers: Mapping[str, str]) -> list[tuple[str, str]]:
    # urllib3's HTTPHeaderDict.items() can yield multiple entries for the
    # same key, or a key only different in case
    return [
        (k, redact_header(k, v))
        for k, v in headers.items()
    ]


def redact_header(name: str, value: str) -> str:
    result = redact(value)
    if result == value:
        # Our standard, pattern-based approach didn't redact anything …
        if name.lower() == 'authorization':
            # … but the header most definitely contains a secret. We don't know
            # what type of secret we're dealing with, so we redact it entirely.
            return 'REDACTED'
        else:
            return value
    else:
        return result


class LoggingHttpClient(HttpClientDecorator):
    """
    An HTTP client that logs every request and response to the given logger.
    Request and response bodies will be logged at DEBUG level, and only a prefix
    will be logged. Request and response headers will be logged at DEBUG level.
    Additionally, AZUL_DEBUG must be at least 2 for request headers to be logged
    at all, in order to protect any credentials contained therein.
    """

    def __init__(self,
                 inner: HttpClient,
                 log: logging.Logger,
                 *,
                 headers: dict | None = None):
        super().__init__(inner, headers)
        self._log = log

        # As a request is being prepared by the various layers of urllib3,
        # requests headers may being added, in addition to the ones supplied by
        # the client. To ensure that all headers are logged, we'd therefore need
        # to log them at the innermost layer. To get at that layer we need to
        # dynamically subclass the connection pool class for each of the two
        # schemes and make the pool manager use those subclasses when creating
        # new pools. The dynamic subclasses inherit a static mixin that actually
        # logs the headers. We need to use subclassing because we don't have any
        # connection pool instances yet and the only place where we can stash
        # the logger instance is in a class attribute. There is one subclass per
        # scheme and logger instance, and since there is typically one logger
        # instance per client module, the class duplication is manageable at two
        # subclasses per client module. The alternative approach would have been
        # to monkey patch the ``_new_pool`` factory method in the pool manager
        # instance but I felt the subclassing approach is more transparent. The
        # subclass name is prefixed with the logger name.
        #
        pool_manager = self.delegate(urllib3.PoolManager)
        attribute_name = 'pool_classes_by_scheme'
        # Use setattr to appease mypy, as the stubs don't declare the attribute.
        attribute_value = getattr(pool_manager, attribute_name)
        setattr(pool_manager, attribute_name, attribute_value | {
            scheme: self._pool_cls(log, scheme)
            for scheme in ['http', 'https']
        })

    @classmethod
    @cache
    def _pool_cls(cls, log: logging.Logger, scheme: str) -> type:
        proto = scheme.upper()
        return type(
            f'{log.name}.Logging{proto}ConnectionPool',
            (_LoggingConnectionPool, getattr(urllib3, f'{proto}ConnectionPool')),
            {'_log': log}
        )

    def urlopen(self, method, url, *args, body=None, **kwargs) -> urllib3.HTTPResponse:
        log = self._log
        redacted_url = redact(url)
        log.info('Making %s request to %r', method, redacted_url)
        log.info(http_body_log_message('request', body))
        start = time.monotonic()
        response = super().urlopen(method, url, *args, body=body, **kwargs)
        duration = time.monotonic() - start
        assert isinstance(response, urllib3.HTTPResponse), type(response)
        log.info('Got %s response after %.3fs from %s to %s',
                 response.status, duration, method, redacted_url)
        log.info('… with response headers %r',
                 redact_headers(response.headers))
        if response.isclosed():
            log.info(http_body_log_message('response', response.data))
        else:
            log.info('… with a streamed response body')
        return response

    def log(self, message: str, *args):
        self._log.info(message, *args)


class _LoggingConnectionPool(urllib3.connectionpool.HTTPConnectionPool):
    _log: ClassVar[logging.Logger]

    def _make_request(self, *args, **kwargs) -> Any:
        log = self._log
        headers = kwargs.get('headers')
        if headers is None or len(headers) == 0:
            log.info('… without request headers')
        else:
            log.info('… with request headers %r',
                     redact_headers(headers))
        # The stubs for urllib3 v1.x don't declare any protected methods
        return super()._make_request(*args, **kwargs)  # type: ignore[misc]


class DisableCrossHostRedirectClient(HttpClientDecorator):
    """
    A client that disables the "custom cross-host redirect logic" (quoting the
    docstring here) employed by :meth:`urllib3.PoolManager.urlopen` by default.
    To enable the logic, simply pass ``redirect=True`` to the urlopen() method.
    """

    def urlopen(self, method, url, *args, **kwargs) -> urllib3.BaseHTTPResponse:
        kwargs.setdefault('redirect', False)
        return super().urlopen(method, url, *args, **kwargs)


class AcceptEncodingClient(HttpClientDecorator):
    """
    A client that informs servers that it accepts compressed responses via the
    ``accept-encoding`` request header. The client will not overwrite or modify
    any user-provided values for that header.
    """

    @classmethod
    @cache
    def accept_encoding_header(cls) -> str:
        # The exact set of encodings that urllib can natively handle depends on
        # whether the current python executable was compiled with support for
        # brotli and/or zstd.
        headers_str = urllib3.make_headers(accept_encoding=True)['accept-encoding']
        headers_list = headers_str.split(',')
        assert 'gzip' in headers_list, headers_list
        assert 'deflate' in headers_list, headers_list
        return headers_str

    def urlopen(self, method, url, *args, **kwargs) -> urllib3.BaseHTTPResponse:
        headers = kwargs.get('headers', {})
        headers.setdefault('accept-encoding', self.accept_encoding_header())
        kwargs['headers'] = headers
        return super().urlopen(method, url, *args, **kwargs)


def http_client(log: logging.Logger | None = None) -> HttpClient:
    client = urllib3.PoolManager(ca_certs=certifi.where())
    client = DisableCrossHostRedirectClient(client)
    client = AcceptEncodingClient(client)
    if log is not None:
        client = LoggingHttpClient(client, log)
    return StatusRetryHttpClient(client)


class HTTPStatusError(Exception):

    def __init__(self, url: str | None, status: int, reason: str | None = None):
        # The URL is intentionally passed last, as it tends to be long and we
        # don't want it to displace the other two arguments in the log line.
        super().__init__('Unexpected response status', status, reason, url)


def raise_on_status(response: urllib3.BaseHTTPResponse) -> None:
    if not 200 <= response.status < 400:
        raise HTTPStatusError(response.url, response.status, response.reason)


class DefaultRetryHttpClient(HttpClientDecorator):

    def __init__(self,
                 inner: HttpClient,
                 retries: urllib3.util.Retry,
                 headers: dict | None = None):
        super().__init__(inner, headers)
        self.retries = retries

    def urlopen(self,
                method: str,
                url: str,
                *args,
                **kwargs
                ) -> urllib3.BaseHTTPResponse:
        assert 'retries' not in kwargs, R("Argument 'retries' is disallowed")
        response = super().urlopen(method,
                                   url,
                                   *args,
                                   retries=self.retries,
                                   **kwargs)
        return response


class LimitedTimeoutException(Exception):

    def __init__(self, url: furl, timeout: float):
        super().__init__(f'No response from {url} within {timeout} seconds')


class TooManyRequestsException(Exception):

    def __init__(self, url: furl):
        super().__init__(f'Maximum request rate exceeded for {url}')


class _LimitedRetry(urllib3.Retry):
    """
    Implementation of urllib3's retry strategy for LimitedRetryHttpClient.

    First, set up the fixtures:

    >>> from urllib3.exceptions import ReadTimeoutError
    >>> from urllib3.connectionpool import ConnectionPool
    >>> from typing import cast
    >>> pool = cast(ConnectionPool, None)
    >>> error = ReadTimeoutError(pool=pool, url='', message='')

    With zero retries …

    >>> r = _LimitedRetry.create(retries=0, timeout=5)

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

    >>> time.sleep(.02)
    >>> r.is_exhausted()
    True
    """
    start: float
    retries: int
    timeout: float

    @classmethod
    def create(cls, *, retries: int, timeout: float) -> Self:
        # No retries on redirects, limited retries on server failures and I/O
        # errors such as refused or dropped connections. The latter are actually
        # very likely if connections from the pool are reused after a long
        # period of being idle. That's why we need at least one retry on read …
        self = cls(total=None,
                   connect=retries,
                   read=retries + 1,
                   redirect=0,
                   raise_on_redirect=True,
                   status=retries,
                   other=retries,
                   status_forcelist={500, 502, 503},
                   raise_on_status=True)
        self.start = time.monotonic()
        self.retries = retries
        self.timeout = timeout
        return self

    def is_exhausted(self):
        # … but only if the first read attempt failed quickly, in under 10ms.
        # Otherwise, read errors that don't result from a stale pool connection
        # could exceed the overall timeout by as much as 100%. The point of zero
        # retries is to guarantee that the timeout is not exceeded.
        if super().is_exhausted():
            return True
        else:
            elapsed = time.monotonic() - self.start
            return self.retries == 0 and elapsed > .01 or elapsed >= self.timeout

    def new(self, **kwargs) -> Self:
        # This is a copy constructor that's used to create a new instance with
        # decremented retry counters. The `is_exhausted` method will be called
        # on the copy in order to determine if another attempt should be made.
        other = super().new(**kwargs)
        other.start = self.start
        other.retries = self.retries
        other.timeout = self.timeout
        return other


class LimitedRetryHttpClient(HttpClientDecorator):
    _default_timeout_margin: ClassVar[float] = 10

    @property
    def _timing_is_restricted(self) -> bool:
        return config.lambda_is_handling_api_gateway_request

    def _timeout(self, margin: float) -> float:
        if self._timing_is_restricted:
            return 5
        elif config.lambda_context is None:
            return 20
        else:
            remaining = config.lambda_context.get_remaining_time_in_millis() / 1000
            return max(5, remaining - margin)

    @property
    def retries(self) -> int:
        return 0 if self._timing_is_restricted else 2

    def urlopen(self, method, url, *args, **kwargs) -> urllib3.BaseHTTPResponse:
        margin = kwargs.pop('timeout_margin', self._default_timeout_margin)
        timeout, retries = self._timeout(margin), self.retries
        assert 'retries' not in kwargs, R("Argument 'retries' is disallowed")
        retry = _LimitedRetry.create(retries=retries, timeout=timeout)
        try:
            response = super().urlopen(method,
                                       url,
                                       *args,
                                       retries=retry,
                                       timeout=timeout / (1 + retries),
                                       **kwargs)
        except (urllib3.exceptions.TimeoutError, urllib3.exceptions.MaxRetryError):
            # Any wrapped instance of LoggingHttpClient may not have had a
            # chance to log anything the response, so we hope that the exception
            # captures enough information about the cause.
            logging.warning('Exception during request or response', exc_info=True)
            raise LimitedTimeoutException(url, timeout)
        else:
            if response.status in retry.status_forcelist:
                raise LimitedTimeoutException(url, timeout)
            else:
                return response


class Propagate429HttpClient(HttpClientDecorator):

    def urlopen(self, method, url, *args, **kwargs) -> urllib3.BaseHTTPResponse:
        response = super().urlopen(method, url, *args, **kwargs)
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
    def _http_client(self) -> HttpClient:
        return self._create_http_client()

    def _create_http_client(self) -> HttpClient:
        """
        Subclasses can override this method to replace, wrap or modify the HTTP
        client instance returned by this method.
        """
        log = getattr(sys.modules[type(self).__module__], 'log')
        assert isinstance(log, logging.Logger), type(log)
        return http_client(log)


class StatusRetryHttpClient(HttpClientDecorator):
    """
    An HTTP client that repeats the request until 1) the response status is not
    one of a specified set of statuses that represent an error, and 2) the
    number of repeat requests, aka *retries*, exceeds a specified value.

    This class attempts to emulate urllib3's built-in retry logic to the extend
    that the author understood it (it is rather complex).

    This class imposes additional restrictions on the arguments to the
    :py:meth:`urlopen` method, and the convenience methods that call it. See
    :py:meth:`urlopen` for details.
    """

    redirect_statuses = frozenset(urllib3.HTTPResponse.REDIRECT_STATUSES)

    retry_after_statuses = frozenset(urllib3.Retry.RETRY_AFTER_STATUS_CODES)

    @property
    def default_retries(self) -> urllib3.Retry:
        # Despite the class docstring claiming that Retry instances "can be
        # safely reused", all their attributes are mutable, so that claim
        # describes a convention and is not explicitly enforced. We therefore
        # defensively create a new instance each time one is requested.
        return urllib3.Retry(total=None,
                             connect=2,
                             read=2,
                             redirect=0,
                             raise_on_redirect=False,
                             status=5,
                             raise_on_status=True,
                             status_forcelist={429, 500, 502, 503, 504})

    def urlopen(self,
                method: str,
                url: str,
                *args,
                retries: urllib3.Retry | None = None,
                **kwargs
                ) -> urllib3.BaseHTTPResponse:
        """
        The ``retries`` argument, if specified, must be ``None`` or an instance
        of ``urllib3.Retry`` that has the ``status`` attribute set to an integer
        value. If the ``retries.status_forcelist`` attribute is not ``None``,
        its value must not intersect with the set of statuses that urllib3
        treats as redirects (``urllib3.HTTPResponse.REDIRECT_STATUSES``).

        If ``retries`` is ``None``, the return value of :meth:`default_retries`
        is used instead. That value statisfies the above constraints but it is
        notably different from the default value for the ``retries`` argument to
        urllib3's ``urlopen()`` method.
        """
        if retries is None:
            retries = self.default_retries

        assert isinstance(retries, urllib3.Retry), R(
            "Argument 'retries' must be an instance of urllib3.Retry",
            type(retries))

        assert isinstance(retries.status, int) and retries.status >= 0, R(
            "Argument 'retries.status' must be an non-negative integer",
            retries.status)
        num_retries = retries.status

        statuses = frozenset(retries.status_forcelist) or self.retry_after_statuses
        assert bool(statuses), R(
            "Argument 'retries.status_forcelist' must not be empty",
            statuses)
        if statuses & self.redirect_statuses:
            assert not bool(retries.redirect), R(
                "Redirects must be disabled if 'retries.status_forcelist' "
                "contains one or more redirect status codes.",
                statuses, self.redirect_statuses)

        logging_client = self.delegate(LoggingHttpClient)
        methods = retries.allowed_methods
        assert methods is not None
        retryable = methods is False or method in methods
        inner_retries = retries.new(status=0,
                                    status_forcelist=None,
                                    respect_retry_after_header=False)
        while True:
            response = super().urlopen(method, url, *args, retries=inner_retries, **kwargs)
            if retryable and response.status in statuses:
                if 0 < num_retries:
                    num_retries -= 1
                    if retries.respect_retry_after_header:
                        try:
                            retry_after = int(response.headers['Retry-After'])
                        except KeyError:
                            pass
                        else:
                            if logging_client is not None:
                                logging_client.log('Sleeping %ds to honor Retry-After header', retry_after)
                            time.sleep(retry_after)
                else:
                    if retries.raise_on_status:
                        pool = getattr(response, '_pool')
                        raise urllib3.exceptions.MaxRetryError(pool, url)
                    else:
                        return response
            else:
                return response


def parse_header(name: str, value: str) -> tuple[str, dict[str, str]]:
    """
    Parse a MIME-related HTTP header, like ``content-type`` or
    ``content-disposition`` into the mandatory part of the header's value and a
    dictionary with an entry for each optional parameter in that value.

    >>> parse_header('content-type', 'text/html; charset=utf-8')
    ('text/html', {'charset': 'utf-8'})

    >>> parse_header('content-type', 'application/json; charset=utf-8; foo=bar')
    ('application/json', {'charset': 'utf-8', 'foo': 'bar'})

    >>> parse_header('content-type', 'text/html')
    ('text/html', {})

    >>> parse_header('content-disposition', 'attachment; filename="document.pdf"')
    ('attachment', {'filename': 'document.pdf'})

    >>> parse_header('content-disposition', 'attachment; name="foo.pdf"; name="bar.pdf"')
    Traceback (most recent call last):
    ...
    AssertionError: R('Duplicate parameters', [('name', 'foo.pdf'), ('name', 'bar.pdf')])

    >>> parse_header('content-disposition', '')
    Traceback (most recent call last):
    ...
    AssertionError: R('Empty arguments are disallowed', 'content-disposition', '')

    >>> parse_header('content-type', 'text:charset=utf-8')
    Traceback (most recent call last):
    ...
    AssertionError: R('Unparsable header format', 'text:charset=utf-8')
    """
    assert '' not in (name, value), R(
        'Empty arguments are disallowed', name, value)
    from email.message import (
        Message,
    )
    m = Message()
    m[name] = value
    params = m.get_params(header=name)
    assert isinstance(params, list)
    key, delimiter = params.pop(0)
    assert delimiter == '', R('Unparsable header format', value)
    params_dict = dict(params)
    assert len(params_dict) == len(params), R('Duplicate parameters', params)
    return key, params_dict
