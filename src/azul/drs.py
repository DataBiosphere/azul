from collections import (
    namedtuple,
)
from enum import (
    Enum,
)
import time
from typing import (
    Optional,
    Union,
)

from furl import (
    furl,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    reject,
    require,
)


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


class DRSClient:

    def get_object(self,
                   drs_uri: str,
                   access_id: Optional[str] = None,
                   access_method: AccessMethod = AccessMethod.https
                   ) -> str:
        """
        Resolve a DRS data object to a URL. The scheme of the returned URL
        depends on the access method specified.
        """
        if access_id is None:
            return self._get_object(drs_uri, access_method)
        else:
            return self._get_object_access(drs_uri, access_id, access_method)

    @classmethod
    def drs_uri_to_url(cls, drs_uri: str, access_id: Optional[str] = None) -> str:
        """
        Translate a DRS URI into a DRS URL. All query params included in the DRS
        URI (eg '{drs_uri}?version=123') will be carried over to the DRS URL.
        Only hostname-based DRS URIs (drs://<hostname>/<id>) are supported while
        compact, identifier-based URIs (drs://[provider_code/]namespace:accession)
        are not.
        """
        parsed = furl(drs_uri)
        require(parsed.scheme == 'drs',
                f'The DRS URI {drs_uri} does not have the correct scheme')
        # "The colon character is not allowed in a hostname-based DRS URI".
        # https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#_drs_uris
        # It is worth noting that compact identifier-based URI can be hard to
        # parse when following RFC3986, with the 'namespace:accession' part
        # matching either the heir-part or path production depending if the
        # optional provider code and following slash is included.
        reject(':' in parsed.netloc or ':' in str(parsed.path),
               f'The value {drs_uri} is not a valid hostname-based DRS URI')
        parsed.scheme = 'https'
        object_id = one(parsed.path.segments)
        parsed.path.set(drs_object_url_path(object_id, access_id))
        return parsed.url

    def _get_object(self, drs_uri: str, access_method: AccessMethod) -> str:
        url = self.drs_uri_to_url(drs_uri)
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                # Bundles are not supported therefore we can expect 'access_methods'
                access_methods = response.json()['access_methods']
                method = one(m for m in access_methods if m['type'] == access_method.scheme)
                if 'access_id' in method:
                    return self._get_object_access(drs_uri, method['access_id'], access_method)
                elif 'access_url' in method:
                    return method['access_url']['url']
                else:
                    assert False
            elif response.status_code == 202:
                # note the busy wait
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response.status_code, response.text)

    def _get_object_access(self,
                           drs_uri: str,
                           access_id: str,
                           access_method: AccessMethod
                           ) -> Union[requests.Response, str]:
        """
        For AccessMethod.gs, the gs:// URL is returned. Otherwise for
        AccessMethod.https, the object's bytes will be returned.
        """
        url = self.drs_uri_to_url(drs_uri, access_id=access_id)
        while True:
            response = requests.get(url, allow_redirects=False)
            if response.status_code == 200:
                # we have an access URL
                response_json = response.json()
                url = response_json['url']
                headers = response_json.get('headers')
                if access_method is AccessMethod.gs:
                    return url
                elif access_method is AccessMethod.https:
                    return requests.get(url, headers=headers)
                else:
                    assert False
            elif response.status_code == 202:
                # note the busy wait
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response.status_code, response.text)


class DRSError(Exception):

    def __init__(self, status_code, msg):
        self.status_code = status_code
        self.msg = msg
