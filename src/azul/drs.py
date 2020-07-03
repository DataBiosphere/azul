from ast import literal_eval
import base64
from collections import namedtuple
from dataclasses import (
    dataclass,
    field,
)
from datetime import datetime
from enum import Enum
import time
from typing import (
    List,
    Optional,
    Tuple,
    Mapping,
    Union,
)

from furl import furl
from more_itertools import one
import requests

from azul import (
    config,
    dss,
)
from azul.types import (
    JSON,
    MutableJSON,
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
    drs_url = '/ga4gh/drs/v1/objects'
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


def encode_access_id(token_str: str, replica: str) -> str:
    """
    Encode a given token as an access ID using URL-safe base64 without padding.

    Standard base64 pads the result with equal signs (`=`). Those would need to
    be URL-encoded when used in the query portion of a URL:

    >>> base64.urlsafe_b64encode(b"('back on boogie street', 'aws')")
    b'KCdiYWNrIG9uIGJvb2dpZSBzdHJlZXQnLCAnYXdzJyk='

    This function strips that padding. The padding is redundant as long as the
    length of the encoded string is known at the time of decoding. With URL
    query parameters this is always the case.

    >>> encode_access_id('back on boogie street', 'aws')
    'KCdiYWNrIG9uIGJvb2dpZSBzdHJlZXQnLCAnYXdzJyk'

    >>> decode_access_id(encode_access_id('back on boogie street', 'aws'))
    ('back on boogie street', 'aws')
    """
    access_id = repr((token_str, replica)).encode()
    access_id = base64.urlsafe_b64encode(access_id)
    return access_id.rstrip(b'=').decode()


def decode_access_id(access_id: str) -> str:
    token = access_id.encode('ascii')  # Base64 is a subset of ASCII
    padding = b'=' * (-len(token) % 4)
    token = base64.urlsafe_b64decode(token + padding)
    return literal_eval(token.decode())


class AccessMethod(namedtuple('AccessMethod', 'scheme replica'), Enum):
    https = 'https', 'aws'
    gs = 'gs', 'gcp'

    def __str__(self) -> str:
        return self.name


@dataclass
class DRSObject:
    """"
    Used to build up a https://ga4gh.github.io/data-repository-service-schemas/docs/#_drsobject
    """
    uuid: str
    version: Optional[str] = None
    access_methods: List[MutableJSON] = field(default_factory=list)

    def add_access_method(self,
                          access_method: AccessMethod, *,
                          url: Optional[str] = None,
                          access_id: Optional[str] = None):
        """
        We only currently use `url_type`s of 'https' and 'gs'. Only one of `url`
        and `access_id` should be specified.
        """
        assert url is None or access_id is None
        self.access_methods.append({
            'type': access_method.scheme,
            **({} if access_id is None else {'access_id': access_id}),
            **({} if url is None else {'access_url': {'url': url}}),
        })

    def to_json(self) -> JSON:
        version = (f'&version={self.version}' if self.version is not None else '')
        url = f'{config.dss_endpoint}/files/{self.uuid}?replica=aws' + version
        headers = requests.head(url).headers
        version = headers['x-dss-version']
        if self.version is not None:
            assert version == self.version
        return {
            **{
                'checksums': [
                    {'sha1': headers['x-dss-sha1']},
                    {'sha-256': headers['x-dss-sha256']}
                ],
                'created_time': timestamp(version),
                'id': self.uuid,
                'self_uri': object_url(self.uuid, version),
                'size': headers['x-dss-size'],
                'version': version
            },
            'access_methods': self.access_methods
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

    def get_object(self,
                   object_id: str,
                   access_id: Optional[str] = None,
                   access_method: AccessMethod = AccessMethod.https
                   ) -> Union[requests.Response, str]:
        """
        Get a DRS object.

        For AccessMethod.https, the request response containing the object will
        be returned.

        For AccessMethod.gs, the gs:// url for the object will be returned.
        """
        if access_id is None:
            return self._get_object(object_id, access_method=access_method)
        else:
            return self._get_object_access(object_id, access_id, access_method=access_method)

    def _get_object(self, object_id: str, access_method: AccessMethod) -> Union[requests.Response, str]:
        url = self.url / object_id
        while True:
            response = requests.get(url)
            if response.status_code == 200:
                # Bundles are not supported therefore we can expect 'access_methods'
                access_methods = response.json()['access_methods']
                method = one(m for m in access_methods if m['type'] == access_method.scheme)
                if 'access_id' in method:
                    return self._get_object_access(object_id, method['access_id'],
                                                   access_method=access_method)
                elif 'access_url' in method:
                    access_url_ = method['access_url']['url']
                    if method['type'] == AccessMethod.https.scheme:
                        return requests.get(access_url_)
                    elif method['type'] == AccessMethod.gs.scheme:
                        return access_url_
                    else:
                        assert False
                else:
                    assert False
            elif response.status_code == 202:
                # note the busy wait
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSError(response.status_code, response.text)

    def _get_object_access(self,
                           object_id: str,
                           access_id: str,
                           access_method: AccessMethod
                           ) -> Union[requests.Response, str]:
        """
        For AccessMethod.gs, the gs:// URL is returned. Otherwise for
        AccessMethod.https, the object's bytes will be returned.
        """
        url = self.url / object_id / 'access' / access_id
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
