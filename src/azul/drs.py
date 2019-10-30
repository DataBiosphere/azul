from typing import (
    Optional,
    Tuple,
)
from urllib.parse import (
    urlunsplit,
    SplitResult,
    urlencode,
    urlsplit,
)

from azul import config


def object_url(file_uuid: str,
               file_version: Optional[str] = None,
               base_url: Optional[str] = None) -> str:
    """
    The DRS URL for a given DSS file UUID and version. The return value will point at the bare-bones DRS data object
    endpoint in the web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the DSS file version of the file

    :param base_url: an optional service endpoint, e.g for local test servers. If absent, the service endpoint for the
                     current deployment will be used.
    """
    scheme, netloc = _endpoint(base_url)
    return urlunsplit(SplitResult(scheme='drs',
                                  netloc=netloc,
                                  path=file_uuid,
                                  query=_url_query(file_version),
                                  fragment=''))


def http_object_url(file_uuid: str,
                    file_version: Optional[str] = None,
                    base_url: Optional[str] = None) -> str:
    """
    The HTTP(S) URL for a given DSS file UUID and version. The return value will point at the bare-bones DRS data
    object endpoint in the web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the DSS file version of the file

    :param base_url: an optional service endpoint, e.g for local test servers. If absent, the service endpoint for the
                     current deployment will be used.
    """
    scheme, netloc = _endpoint(base_url)
    return urlunsplit(SplitResult(scheme=scheme,
                                  netloc=netloc,
                                  path=drs_http_object_path(file_uuid),
                                  query=_url_query(file_version),
                                  fragment=''))


def drs_http_object_path(file_uuid: str) -> str:
    return f'/ga4gh/dos/v1/dataobjects/{file_uuid}'


def _endpoint(base_url: Optional[str]) -> Tuple[str, str]:
    if base_url is None:
        base_url = config.drs_endpoint()
    base_url = urlsplit(base_url)
    return base_url.scheme, base_url.netloc


def _url_query(file_version: Optional[str]) -> str:
    return urlencode({'version': file_version} if file_version else {})
