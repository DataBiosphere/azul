import logging
from time import (
    time,
)
from typing import (
    Union,
)

import certifi
from furl import (
    furl,
)
import urllib3

from azul import (
    cached_property,
)
from azul.strings import (
    trunc_ellipses,
)

log = logging.getLogger(__name__)


class RetryAfter301(urllib3.Retry):
    """
    A retry policy that honors the Retry-After header on 301 status responses
    """
    RETRY_AFTER_STATUS_CODES = urllib3.Retry.RETRY_AFTER_STATUS_CODES | {301}


class HTTPClient:

    def _raw_http(self):
        return urllib3.PoolManager(ca_certs=certifi.where())

    @cached_property
    def _http(self) -> urllib3.PoolManager:
        return self._raw_http()

    def request(self, method: str, url: Union[str, furl], **kwargs) -> urllib3.HTTPResponse:
        self._log_request(method, url, kwargs)
        start = time()
        response = self._http.request(method, str(url), **kwargs)
        assert isinstance(response, urllib3.HTTPResponse), type(response)
        self._log_response(start, response)
        return response

    def _log_request(self, method: str, url: str, kwargs):
        kwargs_pattern = ', '.join(k + '=%r' for k in kwargs.keys())
        log.info(f'%s.request(%r, %s, {kwargs_pattern})',
                 type(self).__name__, method, url, *kwargs.values())

    def _log_response(self, start: float, response: urllib3.HTTPResponse):
        log.info('%s.request(…) -> %.3fs %r %s',
                 type(self).__name__,
                 time() - start,
                 response.status,
                 # Keep data stream intact when preload_content=False
                 repr(trunc_ellipses(response.data, 256))
                 if response.closed else
                 '<streamed response>')
