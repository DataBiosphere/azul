import base64
from datetime import datetime
import time
from typing import (
    Optional,
    Tuple,
    Mapping,
)

from furl import furl
import requests

from azul import (
    config,
    dss,
)


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
    return furl(
        scheme='drs',
        netloc=netloc,
        path=file_uuid,
        args=_url_query(file_version)
    ).url


def dos_http_object_url(file_uuid: str,
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
    return furl(
        scheme=scheme,
        netloc=netloc,
        path=dos_http_object_path(file_uuid),
        args=_url_query(file_version)
    ).url


def http_object_url(file_uuid: str,
                    file_version: Optional[str] = None,
                    base_url: Optional[str] = None,
                    access_id: Optional[str] = None) -> str:
    """
    The HTTP(S) URL for a given DSS file UUID and version. The return value will
    point at the bare-bones DRS data object endpoint in the web service.

    :param file_uuid: the DSS file UUID of the file
    :param file_version: the DSS file version of the file
    :param base_url: an optional service endpoint, e.g for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.
    :param access_id: access id will be included in the URL if this parameter is
                      supplied
    """
    scheme, netloc = _endpoint(base_url)
    return furl(
        scheme=scheme,
        netloc=netloc,
        path=drs_http_object_path(file_uuid, access_id=access_id),
        args=_url_query(file_version),
    ).url


def drs_http_object_path(file_uuid: str, access_id: str = None) -> str:
    """
    >>> drs_http_object_path('abc')
    '/ga4gh/drs/v1/objects/abc'

    >>> drs_http_object_path('abc', access_id='123')
    '/ga4gh/drs/v1/objects/abc/access/123'
    """
    drs_url = f'/ga4gh/drs/v1/objects'
    return '/'.join((drs_url, file_uuid, *(('access', access_id) if access_id else ())))


def dos_http_object_path(file_uuid: str) -> str:
    return f'/ga4gh/dos/v1/dataobjects/{file_uuid}'


def _endpoint(base_url: Optional[str]) -> Tuple[str, str]:
    if base_url is None:
        base_url = config.drs_endpoint()
    base_url = furl(base_url)
    return base_url.scheme, base_url.netloc


def _url_query(file_version: Optional[str]) -> Mapping[str, str]:
    return {'version': file_version} if file_version else {}


def encode_access_id(token_str: str) -> str:
    """
    Encode a given token as an access ID using URL-safe base64 without padding.

    Standard base64 pads the result with equal signs (`=`). Those would need to
    be URL-encoded when used in the query portion of a URL:

    >>> base64.urlsafe_b64encode(b'boogie street')
    b'Ym9vZ2llIHN0cmVldA=='

    This function strips that padding. The padding is redundant as long as the
    length of the encoded string is known at the time of decoding. With URL
    query parameters this is always the case.

    >>> encode_access_id('boogie street')
    'Ym9vZ2llIHN0cmVldA'

    >>> decode_access_id(encode_access_id('boogie street'))
    'boogie street'
    """
    access_id = token_str.encode()
    access_id = base64.urlsafe_b64encode(access_id)
    return access_id.rstrip(b'=').decode()


def decode_access_id(access_id: str) -> str:
    token = access_id.encode('ascii')  # Base64 is a subset of ASCII
    padding = b'=' * (-len(token) % 4)
    token = base64.urlsafe_b64decode(token + padding)
    return token.decode()


def access_id_drs_object(object_uuid, access_id, version=None):
    return {
        **drs_object(object_uuid, version=version),
        'access_methods': [
            {'access_id': access_id}
        ]
    }


def access_url_drs_object(object_uuid, url, version=None):
    return {
        **drs_object(object_uuid, version=version),
        'access_methods': [
            {
                'access_url': {'url': url},
                'type': 'https'
            }
        ]
    }


def drs_object(object_uuid, version=None):
    version = (f'&version={version}' if version is not None else '')
    url = f'{config.dss_endpoint}/files/{object_uuid}?replica=aws' + version
    headers = requests.head(url).headers
    version = headers['x-dss-version']
    return {
        'checksums': [
            {'sha1': headers['x-dss-sha1']},
            {'sha-256': headers['x-dss-sha256']}
        ],
        'created_time': timestamp(version),
        'id': object_uuid,
        'self_uri': object_url(object_uuid, version),
        'size': headers['x-dss-size'],
        'version': version
    }


def timestamp(version):
    """
    Convert a DSS version into a proper, RFC3339 compliant timestamp.

    >>> timestamp('2019-08-01T211621.345939Z')
    '2019-08-01T21:16:21.345939Z'

    >>> timestamp('2019-08-01T211621:345939Z')
    Traceback (most recent call last):
    ...
    ValueError: time data '2019-08-01T211621:345939Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'
    """
    return datetime.strptime(version, dss.version_format).isoformat() + 'Z'


def access_url(url):
    return {'url': url}


class Client:

    def __init__(self, url: str):
        self.url = furl(url)

    def get_object(self, object_id: str, access_id: str = None) -> requests.Response:
        if access_id is None:
            return self._get_object(object_id)
        else:
            return self._get_object_access(object_id, access_id)

    def _get_object(self, object_id: str) -> requests.Response:
        url = self.url / object_id
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                # Bundles are not supported therefore we can expect 'access_methods'
                access_methods = response.json()['access_methods']
                assert len(access_methods) == 1, 'Only one access method is supported currently'
                for access_method in access_methods:
                    if 'access_id' in access_method:
                        return self._get_object_access(object_id, access_method['access_id'])
                    elif 'access_url' in access_method:
                        # We only support https as an access type
                        assert access_method['type'] == 'https'
                        access_url_ = access_method['access_url']['url']
                        return requests.get(access_url_)
                    else:
                        assert False
            elif response.status_code == 202:
                # note the busy wait
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response.status_code, response.text)

    def _get_object_access(self, object_id: str, access_id: str) -> requests.Response:
        url = self.url / object_id / 'access' / access_id
        while True:
            response = requests.get(url, allow_redirects=False)
            if response.status_code == 200:
                # we have an access URL
                response_json = response.json()
                url = response_json['url']
                headers = response_json.get('headers')
                return requests.get(url, headers=headers)
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
