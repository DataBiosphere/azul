import certifi
import urllib3


def http_client() -> urllib3.PoolManager:
    return urllib3.PoolManager(ca_certs=certifi.where())


class RetryAfter301(urllib3.Retry):
    """
    A retry policy that honors the Retry-After header on 301 status responses
    """
    RETRY_AFTER_STATUS_CODES = urllib3.Retry.RETRY_AFTER_STATUS_CODES | {301}
