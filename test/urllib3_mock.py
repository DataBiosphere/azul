from collections import (
    defaultdict,
    deque,
)
import json
from unittest.mock import (
    PropertyMock,
    patch,
)

from urllib3 import (
    BaseHTTPResponse,
    HTTPResponse,
)

from azul.http import (
    HttpClient,
)
from azul.lib import (
    mutable_furl,
)
from azul.lib.types import (
    JSON,
)

type _QueuedResponses = dict[tuple[str, str], deque[BaseHTTPResponse]]


class Urllib3Mock:
    """
    Context manager that patches the ``_http_client`` property of one or more
    target classes with a mock client that returns previously queued mock
    responses. Each distinct combination of HTTP method and URL has a separate
    queue. When the patched target's mock client makes a request matching one of
    those combinations, the mock responses are returned in the order they were
    queued in.
    """

    def __init__(self, *targets: type) -> None:
        """
        Create the context manager instance that patches the given targets on
        entry, and restores them on exit.
        """
        self._responses: _QueuedResponses = defaultdict(deque)
        self._client = _MockHttpClient(self._responses)
        self._patches = deque(
            patch.object(target,
                         '_http_client',
                         new=PropertyMock(return_value=self._client))
            for target in targets
        )

    def add(self,
            *,
            method: str,
            url: str,
            status: int,
            headers: dict[str, str] | None = None,
            body: bytes | str | JSON = b'',
            reason: str | None = None,
            ) -> None:
        """
        Queue a mock response for the given combination of HTTP method and URL.
        If multiple responses are queued for the same combination, they are
        returned in the order they were queued in.

        :param method: the request method, e.g. 'GET'

        :param url: the request URL

        :param status: the status of the returned response

        :param headers: the headers of the returned response

        :param body: the body of the returned response

        :param reason: optional text to follow the numeric response status
        """
        if headers is None:
            headers = {}
        if isinstance(body, dict):
            body = json.dumps(body)
            headers['Content-Type'] = 'application/json'
        if isinstance(body, str):
            body = body.encode()
        assert isinstance(body, bytes), type(body)
        response = HTTPResponse(body=body,
                                headers=headers,
                                status=status,
                                reason=reason,
                                request_method=method,
                                request_url=url,
                                preload_content=True)
        key = (method, _normalize_url(url))
        self._responses[key].append(response)

    def __enter__(self):
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *_args):
        for p in reversed(self._patches):
            p.stop()
        self._responses.clear()


class _MockHttpClient(HttpClient):

    def __init__(self, responses: _QueuedResponses) -> None:
        super().__init__()
        self._responses = responses

    def urlopen(self, method: str, url: str, *args, **kwargs) -> BaseHTTPResponse:
        key = (method, _normalize_url(url))
        responses = self._responses[key]
        assert responses, f'No responses queued for {key!r}'
        return responses.popleft()


def _normalize_url(url: str) -> str:
    url = mutable_furl(url)
    url.set(args=dict(sorted(url.args.items())))
    return str(url)
