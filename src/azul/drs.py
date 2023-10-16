from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    namedtuple,
)
from collections.abc import (
    Mapping,
    Sequence,
)
from enum import (
    Enum,
)
import json
import logging
import time
from typing import (
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
    cache,
    cached_property,
    mutable_furl,
    reject,
    require,
)
from azul.http import (
    HTTPClient,
)
from azul.types import (
    MutableJSON,
    MutableJSONs,
)

log = logging.getLogger(__name__)


def drs_object_uri(*,
                   base_url: furl,
                   path: Sequence[str],
                   params: Mapping[str, str]
                   ) -> mutable_furl:
    assert ':' not in base_url.netloc
    return furl(url=base_url, scheme='drs', path=path, args=params)


def drs_object_url_path(*, object_id: str, access_id: str = None) -> str:
    """
    >>> drs_object_url_path(object_id='abc')
    '/ga4gh/drs/v1/objects/abc'

    >>> drs_object_url_path(object_id='abc', access_id='123')
    '/ga4gh/drs/v1/objects/abc/access/123'
    """
    drs_url = '/ga4gh/drs/v1/objects'
    return '/'.join((
        drs_url,
        object_id,
        *(('access', access_id) if access_id else ())
    ))


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


class DRSURI(metaclass=ABCMeta):

    @classmethod
    def parse(cls, drs_uri: str) -> 'DRSURI':
        prefix = 'drs://'
        require(drs_uri.startswith(prefix), drs_uri)
        # "The colon character is not allowed in a hostname-based DRS URI".
        #
        # https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#_drs_uris
        #
        subcls = CompactDRSURI if drs_uri.find(':', len(prefix)) >= 0 else RegularDRSURI
        return subcls.parse(drs_uri)

    @abstractmethod
    def to_url(self, client: 'DRSClient', access_id: Optional[str] = None) -> str:
        """
        Translate the DRS URI into a DRS URL. All query params included in the
        DRS URI (eg '{drs_uri}?version=123') will be carried over to the DRS URL.
        """
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True, frozen=True, slots=True)
class RegularDRSURI(DRSURI):
    uri: furl

    def __attrs_post_init__(self):
        assert self.uri.scheme == 'drs', self.uri

    @classmethod
    def parse(cls, drs_uri: str) -> 'RegularDRSURI':
        return cls(uri=furl(drs_uri))

    def to_url(self, client: 'DRSClient', access_id: Optional[str] = None) -> str:
        url = self.uri.copy().set(scheme='https')
        url.set(path=drs_object_url_path(object_id=one(self.uri.path.segments),
                                         access_id=access_id))
        return str(url)


@attr.s(auto_attribs=True, kw_only=True, frozen=True, slots=True)
class CompactDRSURI(DRSURI):
    """
    So-called DRS "URIs" [1] for Compact Identifiers [2] are NOT URIs according
    to RFC 3986 [3] so we can't use off-the-shelf URI parsers.

    [1] https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.3.0/docs/

    [2] https://www.nature.com/articles/sdata201829

    [3] https://datatracker.ietf.org/doc/html/rfc3986
    """
    namespace: str
    accession: str

    def __attrs_post_init__(self):
        assert '/' not in self.namespace and '?' not in self.accession, self

    @classmethod
    def parse(cls, drs_uri: str) -> 'CompactDRSURI':
        scheme, netloc = drs_uri.split('://', 1)
        # Compact identifier-based URIs can be hard to parse when following
        # RFC3986, with the 'namespace:accession' part matching either the
        # heir-part or path production depending if the optional provider code
        # and following slash is included.
        #
        # https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#compact-identifier-based-drs-uris
        #
        prefix, accession = netloc.split(':', 1)
        reject('/' in prefix,
               'Compact identifiers with provider codes are not supported', drs_uri)
        reject('?' in accession,
               'Compact identifiers must not contain query parameters', drs_uri)
        return cls(namespace=prefix,
                   accession=accession)

    def to_url(self, client: 'DRSClient', access_id: Optional[str] = None) -> str:
        url = client.id_client.resolve(self.namespace, self.accession)
        # The URL pattern registered at identifiers.org ought to replicate the
        # DRS spec, but we have to re-create the path using the spec because the
        # registered pattern does not support embedding the access ID.
        require(str(url.path) == drs_object_url_path(object_id=self.accession),
                'Unexpected DRS URL format', url)
        url.set(path=drs_object_url_path(object_id=self.accession, access_id=access_id))
        return str(url)


class IdentifiersDotOrgClient(HTTPClient):

    def resolve(self, prefix: str, accession: str) -> mutable_furl:
        namespace_id = self._prefix_to_namespace(prefix)
        log.info('Resolved prefix %r to namespace ID %r', prefix, namespace_id)
        resource_name, url_pattern = self._namespace_to_host(namespace_id)
        log.info('Obtained URL pattern %r from resource %r', url_pattern, resource_name)
        placeholder = '{$id}'
        require(placeholder in url_pattern, url_pattern)
        url = url_pattern.replace(placeholder, accession)
        return furl(url)

    _api_url = 'https://registry.api.identifiers.org/restApi/'

    @cache
    def _prefix_to_namespace(self, prefix: str) -> str:
        prefix_info = self._api_request('namespaces/search/findByPrefix', prefix=prefix)
        return furl(prefix_info['_links']['self']['href']).path.segments[-1]

    @cache
    def _namespace_to_host(self, namespace_id: str) -> tuple[str, str]:
        namespace_info = self._api_request('resources/search/findAllByNamespaceId',
                                           id=namespace_id)
        resources: MutableJSONs = namespace_info['_embedded']['resources']
        resource = one(resources)
        return resource['name'], resource['urlPattern']

    def _api_request(self, path: str, **args) -> MutableJSON:
        url = furl(self._api_url).add(path=path, args=args)
        response = self.request('GET', url)
        require(response.status == 200)
        return json.loads(response.data)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class DRSClient:
    http_client: HTTPClient

    @cached_property
    def id_client(self) -> IdentifiersDotOrgClient:
        return IdentifiersDotOrgClient()

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

    def _get_object(self, drs_uri: str, access_method: AccessMethod) -> Access:
        url = DRSURI.parse(drs_uri).to_url(self)
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
                    # We can't convert the signed URL into a furl object since
                    # the path can contain `%3A` which furl converts to `:`
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
        url = DRSURI.parse(drs_uri).to_url(self, access_id)
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
        return self.http_client.request('GET', url, redirect=False)


class DRSError(Exception):

    def __init__(self, response: urllib3.HTTPResponse) -> None:
        super().__init__(response.status, response.data)
