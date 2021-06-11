import requests
from requests.adapters import (
    HTTPAdapter,
)
import urllib3.util.retry


class _RetryAfterPolicy(urllib3.util.retry.Retry):

    def __init__(self, *args, **kwargs):
        super(_RetryAfterPolicy, self).__init__(*args, **kwargs)
        self.RETRY_AFTER_STATUS_CODES = frozenset({301} | self.RETRY_AFTER_STATUS_CODES)


def requests_session_with_retry_after() -> requests.Session:
    """
    Return a `requests` session object that's set up to implicitly handle the
    RetryAfter redirects returned by various Azul service endpoints.
    """
    adapter = HTTPAdapter(max_retries=_RetryAfterPolicy())
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
