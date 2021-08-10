from collections import (
    namedtuple,
)
from enum import (
    Enum,
)
import json
import logging
import time
from typing import (
    Mapping,
    Optional,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    one,
)
import urllib3

from azul import (
    RequirementError,
    reject,
    require,
)

log = logging.getLogger(__name__)


def drs_object_uri(host: str, path: str, **params: str) -> str:
    assert ':' not in host, host
    return furl(scheme='drs',
                netloc=host,
                path=path,
                args=params,
                ).url


def drs_object_url_path(object_id: str, access_id: str = None) -> str:
    """
    >>> drs_object_url_path('abc')
    '/ga4gh/drs/v1/objects/abc'

    >>> drs_object_url_path('abc', access_id='123')
    '/ga4gh/drs/v1/objects/abc/access/123'
    """
    drs_url = '/ga4gh/drs/v1/objects'
    return '/'.join((drs_url, object_id, *(('access', access_id) if access_id else ())))


def dos_object_url_path(object_id: str) -> str:
    return f'/ga4gh/dos/v1/dataobjects/{object_id}'


class AccessMethod(namedtuple('AccessMethod', 'scheme replica'), Enum):
    https = 'https', 'aws'
    gs = 'gs', 'gcp'

    def __str__(self) -> str:
        return self.name


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Access:
    method: AccessMethod
    url: str
    headers: Optional[Mapping[str, str]] = None


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class DRSClient:
    http_client: urllib3.PoolManager

    def get_object(self,
                   drs_uri: str,
                   access_method: AccessMethod = AccessMethod.https
                   ) -> Access:
        """
        Returns access to the content of the data object identified by the
        given URI. The scheme of the URL in the returned access object depends
        on the access method specified.
        """
        return self._get_object(drs_uri, access_method)

    def _uri_to_url(self, drs_uri: str, access_id: Optional[str] = None) -> str:
        """
        Translate a DRS URI into a DRS URL. All query params included in the DRS
        URI (eg '{drs_uri}?version=123') will be carried over to the DRS URL.
        Only hostname-based DRS URIs (drs://<hostname>/<id>) are supported while
        compact, identifier-based URIs (drs://[provider_code/]namespace:accession)
        are not.
        """
        parsed = furl(drs_uri)
        scheme = 'drs'
        require(parsed.scheme == scheme,
                f'The URI {drs_uri!r} does not have the {scheme!r} scheme')
        # "The colon character is not allowed in a hostname-based DRS URI".
        # https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#_drs_uris
        # It is worth noting that compact identifier-based URI can be hard to
        # parse when following RFC3986, with the 'namespace:accession' part
        # matching either the heir-part or path production depending if the
        # optional provider code and following slash is included.
        reject(':' in parsed.netloc or ':' in str(parsed.path),
               f'The DRS URI {drs_uri!r} is not hostname-based')
        parsed.scheme = 'https'
        object_id = one(parsed.path.segments)
        parsed.path.set(drs_object_url_path(object_id, access_id))
        return parsed.url

    def _get_object(self, drs_uri: str, access_method: AccessMethod) -> Access:
        url = self._uri_to_url(drs_uri)
        while True:
            response = self._request(url)
            if response.status == 200:
                # Bundles are not supported therefore we can expect 'access_methods'
                response = json.loads(response.data)
                access_methods = response['access_methods']
                method = one(m for m in access_methods if m['type'] == access_method.scheme)
                access_url = method.get('access_url')
                access_id = method.get('access_id')
                if access_url is not None and access_id is not None:
                    # TDR quirkily uses the GS access method to provide both a
                    # GS access URL *and* an access ID that produces an HTTPS
                    # signed URL
                    #
                    # https://github.com/ga4gh/data-repository-service-schemas/issues/360
                    # https://github.com/ga4gh/data-repository-service-schemas/issues/361
                    require(access_method is AccessMethod.gs, access_method)
                    return self._get_object_access(drs_uri, access_id, AccessMethod.https)
                elif access_id is not None:
                    return self._get_object_access(drs_uri, access_id, access_method)
                elif access_url is not None:
                    require(furl(access_url['url']).scheme == access_method.scheme)
                    return Access(method=access_method,
                                  url=access_url['url'])
                else:
                    raise RequirementError("'access_url' and 'access_id' are both missing")
            elif response.status == 202:
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response)

    def _get_object_access(self,
                           drs_uri: str,
                           access_id: str,
                           access_method: AccessMethod
                           ) -> Access:
        url = self._uri_to_url(drs_uri, access_id=access_id)
        while True:
            response = self._request(url)
            if response.status == 200:
                response = json.loads(response.data)
                require(furl(response['url']).scheme == access_method.scheme)
                return Access(method=access_method,
                              url=response['url'],
                              headers=response.get('headers'))
            elif response.status == 202:
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response)

    def _request(self, url: str) -> urllib3.HTTPResponse:
        log.info('GET %s', url)
        return self.http_client.request('GET', url, redirect=False)


class DRSError(Exception):

    def __init__(self, response: urllib3.HTTPResponse) -> None:
        super().__init__(response.status, response.data)
