import certifi
import urllib3


def http_client() -> urllib3.PoolManager:
    return urllib3.PoolManager(ca_certs=certifi.where())
