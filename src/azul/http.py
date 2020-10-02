import certifi
import urllib3
from urllib3.request import (
    RequestMethods,
)


def http_client() -> RequestMethods:
    return urllib3.PoolManager(ca_certs=certifi.where())
