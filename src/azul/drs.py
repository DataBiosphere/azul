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
import re
import time
from typing import (
    ClassVar,
    Self,
)
import urllib.parse

import attr
from furl import (
    furl,
)
from more_itertools import (
    one,
)
import urllib3

from azul.http import (
    HasCachedHttpClient,
    HttpClient,
    LimitedRetryHttpClient,
    Propagate429HttpClient,
)
from azul.lib import (
    R,
    cache,
    mutable_furl,
)
from azul.lib.types import (
    MutableJSON,
    json_dict,
    json_list,
    json_str,
    not_none,
)

log = logging.getLogger(__name__)


def drs_object_uri(*,
                   base_url: furl,
                   path: Sequence[str],
                   params: Mapping[str, str]
                   ) -> mutable_furl:
    assert ':' not in not_none(base_url.netloc)
    return mutable_furl(url=base_url, scheme='drs', path=path, args=params)


def drs_object_url_path(*, object_id: str, access_id: str | None = None) -> str:
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


class AccessMethod(namedtuple('AccessMethod', 'scheme replica'), Enum):
    https = 'https', 'aws'
    gs = 'gs', 'gcp'

    def __str__(self) -> str:
        return self.name


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Access:
    method: AccessMethod
    url: str
    headers: Mapping[str, str] | None = None


class DRSURI(metaclass=ABCMeta):

    @classmethod
    def parse(cls, drs_uri: str) -> DRSURI:
        """
        A data repository service URI as defined by the GA4GH alliance.

        https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.5.0/docs/

        A straight-forward hostname-based DRS URI. Note the normalized server
        name:

        >>> DRSURI.parse('drs://SERVER/ID')
        HostBasedDRSURI(server='server', object_id='ID')

        A hostname-based URI with a percent-encoded question mark in the URL:

        >>> DRSURI.parse('drs://SERVER/ID1%3fID2')
        HostBasedDRSURI(server='server', object_id='ID1?ID2')

        A hostname-based URI with a redundantly percent-encoded character in the
        ID:

        >>> DRSURI.parse('drs://SERVER/I%44')
        HostBasedDRSURI(server='server', object_id='ID')

        A real-world, compact identifier-based LungMAP DRS URI.

        >>> DRSURI.parse('drs://dg.4503:44c5fa8e-c465-4187-8565-734b3ac0a32d')
        ... # doctest: +NORMALIZE_WHITESPACE
        CompactDRSURI(provider_code=None,
                      namespace='dg.4503',
                      accession='44c5fa8e-c465-4187-8565-734b3ac0a32d')

        A compact identifier-based DRS URI without a provider code.

        >>> DRSURI.parse('drs://NS:LP')
        CompactDRSURI(provider_code=None, namespace='NS', accession='LP')

        A compact identifier-based DRS URI whose prefix contains a provider
        code. Note that this could also be interpreted as a hostname-based DRS
        URI at server 'PC' and ID 'NS:LP'.

        >>> DRSURI.parse('drs://PC/NS:LP')
        CompactDRSURI(provider_code='PC', namespace='NS', accession='LP')

        A more insidious version of the above. We still treat this as a compact
        identifier-based DRS URI:

        >>> DRSURI.parse('drs://pc.edu/NS:LP')
        CompactDRSURI(provider_code='pc.edu', namespace='NS', accession='LP')

        However, as soon as the colon is removed from the local part, we start
        treating the URI as hostname-based:

        >>> DRSURI.parse('drs://pc.edu/NSLP')
        HostBasedDRSURI(server='pc.edu', object_id='NSLP')

        A compact identifier-based DRS URI whose local part has a colon:

        >>> DRSURI.parse('drs://NS:LP1:LP2')
        CompactDRSURI(provider_code=None, namespace='NS', accession='LP1:LP2')

        A more complicated compact identifier-based DRS URI with provider code
        and a percent-encoded question mark. The question mark has to be
        encoded, otherwise identifiers.org would discard it, or reject the
        request:

        >>> DRSURI.parse('drs://PC/NS1.NS2:LP1%3fLP2')
        CompactDRSURI(provider_code='PC', namespace='NS1.NS2', accession='LP1?LP2')

        Too many slashes in the prefix of a compact identifier-based DRS URI,
        and a disallowed slash in the accession of a hostname-based URI.

        >>> DRSURI.parse('foo://a/b')
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid scheme', 'foo://a/b')

        >>> DRSURI.parse('drs://a/b/c:d')
        Traceback (most recent call last):
        ...
        AssertionError: R('Invalid path', 'drs://a/b/c:d')

        >>> DRSURI.parse('drs://a/b?d')
        Traceback (most recent call last):
        ...
        AssertionError: R('Query arguments are disallowed in a DRS URI')

        >>> DRSURI.parse('drs://a/b#d')
        Traceback (most recent call last):
        ...
        AssertionError: R('Fragment is disallowed in a DRS URI')

        """
        try:
            return CompactDRSURI.parse(drs_uri)
        except AssertionError as e:
            if R.caused(e):
                return HostBasedDRSURI.parse(drs_uri)
            else:
                raise


@attr.s(auto_attribs=True, kw_only=True, frozen=True, slots=True)
class HostBasedDRSURI(DRSURI):
    """
    A hostname-based DRS URI. When DRS URIs were first standardized, this was
    the only type defined in the standard.
    """

    server: str
    object_id: str

    @classmethod
    def parse(cls, drs_uri: str) -> Self:
        parsed_uri = furl(drs_uri)
        assert parsed_uri.scheme == 'drs', R('Invalid scheme', drs_uri)
        assert not parsed_uri.args, R('Query arguments are disallowed in a DRS URI')
        assert not parsed_uri.fragment, R('Fragment is disallowed in a DRS URI')
        path = parsed_uri.path.segments
        assert len(path) == 1, R('Invalid path', drs_uri)
        return cls(server=not_none(parsed_uri.netloc), object_id=path[0])

    def to_url(self) -> furl:
        path = drs_object_url_path(object_id=self.object_id)
        return furl(scheme='https', netloc=self.server, path=path)


@attr.s(auto_attribs=True, kw_only=True, frozen=True, slots=True)
class CompactDRSURI(DRSURI):
    """
    A DRS URI that represents Compact Identifiers [1]. These were introduced in
    a later revision of the standard. Note that DRS URIs of this type are not
    URIs according to RFC 3986 [2] so we can't use off-the-shelf URI parsers.
    The accession part of compact identifiers allows many more characters than
    the port number of the netloc part defined in the RFC. Another complication
    is that the slash separating the optional provider code introduces an
    ambiguity when detecting the type of DRS URI to parse. See the doctests in
    the parent class for details.

    [1] https://www.nature.com/articles/sdata201829

    [2] https://datatracker.ietf.org/doc/html/rfc3986
    """
    provider_code: str | None = None
    namespace: str
    accession: str

    # We'll use the regex from HCA's file descriptor schema to detect this type.
    #
    # https://github.com/HumanCellAtlas/metadata-schema/blob/4800b29226bfa3d2bed3ad2e0b9240903ba40c32/json_schema/system/file_descriptor.json#L114C22-L114C62
    #
    regex: ClassVar[re.Pattern]
    regex = re.compile(r'^drs://([A-Za-z0-9._]+/)?[A-Za-z0-9._]+:.+$')

    @classmethod
    def parse(cls, drs_uri: str) -> Self:
        if cls.regex.match(drs_uri) is None:
            assert False, R('Not a compact identifier-based URI')
        prefix, accession = drs_uri[6:].split(':', 1)
        provider_code: str | None
        match prefix.split('/'):
            case [provider_code, namespace]:
                provider_code = cls._decode(provider_code)
            case [namespace]:
                provider_code = None
            case _:
                assert False, drs_uri
        return cls(provider_code=provider_code,
                   namespace=cls._decode(namespace),
                   accession=cls._decode(accession))

    @classmethod
    def _decode(cls, s: str) -> str:
        return urllib.parse.unquote(s, errors='strict')

    def to_url(self, id_client: IdentifiersDotOrgClient) -> furl:
        if self.provider_code is not None:
            raise NotImplementedError(
                'Resolving compact identifier-based DRS URIs with '
                'provider codes is currently not supported', self
            )
        url = id_client.resolve(self.namespace, self.accession)
        # The URL pattern registered at identifiers.org ought to replicate the
        # DRS spec. If the response to a request to the returned URL includes an
        # access ID, another request must be made to the returned URL followed
        # by the string `/access/` and the ID.
        assert str(url.path) == drs_object_url_path(object_id=self.accession), R(
            'Format of resolved URL is incompatible with the DRS specification', url)
        return url


class _BaseClient(HasCachedHttpClient):

    def _create_http_client(self) -> HttpClient:
        return Propagate429HttpClient(
            LimitedRetryHttpClient(
                super()._create_http_client()
            )
        )


class DRSClient(metaclass=ABCMeta):

    @abstractmethod
    def drs_object(self, drs_url: furl) -> DRSObject:
        raise NotImplementedError


class UnauthenticatedDRSClient(DRSClient, _BaseClient):
    """
    A generic DRS client that does not send authentication to the server.
    """

    def drs_object(self, drs_url: furl) -> DRSObject:
        return DRSObject(url=drs_url,
                         http_client=self._http_client)


class IdentifiersDotOrgClient(_BaseClient):

    def resolve(self, prefix: str, accession: str) -> mutable_furl:
        namespace_id = self._prefix_to_namespace(prefix)
        log.info('Resolved prefix %r to namespace ID %r', prefix, namespace_id)
        resource_name, url_pattern = self._namespace_to_host(namespace_id)
        log.info('Obtained URL pattern %r from resource %r', url_pattern, resource_name)
        placeholder = '{$id}'
        assert placeholder in url_pattern, R(
            'Missing accession placeholder in URL pattern', url_pattern)
        url = url_pattern.replace(placeholder, accession)
        return mutable_furl(url)

    _api_url = 'https://registry.api.identifiers.org/restApi/'

    @cache
    def _prefix_to_namespace(self, prefix: str) -> str:
        prefix_info = self._api_request('namespaces/search/findByPrefix', prefix=prefix)
        href = json_str(json_dict(json_dict(prefix_info['_links'])['self'])['href'])
        return furl(href).path.segments[-1]

    @cache
    def _namespace_to_host(self, namespace_id: str) -> tuple[str, str]:
        namespace_info = self._api_request('resources/search/findAllByNamespaceId',
                                           id=namespace_id)
        resources = json_list(json_dict(namespace_info['_embedded'])['resources'])
        resource = json_dict(one(resources))
        return json_str(resource['name']), json_str(resource['urlPattern'])

    def _api_request(self, path: str, **args) -> MutableJSON:
        url = mutable_furl(self._api_url).add(path=path, args=args)
        response = self._http_client.request('GET', str(url))
        if response.status == 200:
            return json.loads(response.data)
        else:
            raise DRSStatusException(url, response)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class DRSObject:
    _http_client: HttpClient
    _url: furl

    def get(self, access_method: AccessMethod = AccessMethod.https) -> Access:
        """
        Returns access to the content of the data object identified by the
        given URI. The scheme of the URL in the returned access object depends
        on the access method specified.
        """
        return self._get(access_method)

    def _get(self, access_method: AccessMethod) -> Access:
        url = self._url
        while True:
            response = self._request(url)
            if response.status == 200:
                # Bundles are not supported therefore we can expect 'access_methods'
                response_data = json_dict(json.loads(response.data))
                access_methods = map(json_dict, json_list(response_data['access_methods']))
                method = one(m for m in access_methods if m['type'] == access_method.scheme)
                access_url = json_dict(method.get('access_url'))
                access_id = json_str(method.get('access_id'))
                if access_url is not None and access_id is not None:
                    # TDR quirkily uses the GS access method to provide both a
                    # GS access URL *and* an access ID that produces an HTTPS
                    # signed URL
                    #
                    # https://github.com/ga4gh/data-repository-service-schemas/issues/360
                    # https://github.com/ga4gh/data-repository-service-schemas/issues/361
                    assert access_method is AccessMethod.gs, R(
                        'Unexpected access method', access_method)
                    return self._get_access(access_id, AccessMethod.https)
                elif access_id is not None:
                    return self._get_access(access_id, access_method)
                elif access_url is not None:
                    scheme = furl(access_url['url']).scheme
                    assert scheme == access_method.scheme, R(
                        'Unexpected access URL scheme', scheme)
                    # We can't convert the signed URL into a furl object since
                    # the path can contain `%3A` which furl converts to `:`
                    return Access(method=access_method,
                                  url=access_url['url'])
                else:
                    assert False, R("'access_url' and 'access_id' are both missing")
            elif response.status == 202:
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            else:
                raise DRSStatusException(url, response)

    def _get_access(self, access_id: str, access_method: AccessMethod) -> Access:
        url = self._url.copy()
        url.path.add(['access', access_id])
        while True:
            response = self._request(url)
            if response.status == 200:
                response_data = json_dict(json.loads(response.data))
                scheme = furl(json_str(response_data['url'])).scheme
                assert scheme == access_method.scheme, R(
                    'Unexpected access URL scheme', scheme)
                access_url = json_str(response_data['url'])
                headers = response_data.get('headers')
                if headers is None:
                    access_headers = None
                else:
                    access_headers = {k: json_str(v) for k, v in json_dict(headers).items()}
                return Access(method=access_method, url=access_url, headers=access_headers)
            elif response.status == 202:
                wait_time = int(response.headers['retry-after'])
                time.sleep(wait_time)
            elif response.status == 400 and b'requires an x-user-project' in response.data:
                raise DRSRequesterPaysRequired(url, response)
            else:
                raise DRSStatusException(url, response)

    def _request(self, url: furl) -> urllib3.BaseHTTPResponse:
        return self._http_client.request('GET', str(url), redirect=False)


class DRSStatusException(Exception):

    def __init__(self, url: furl, response: urllib3.BaseHTTPResponse) -> None:
        super().__init__(f'Unexpected response from {url}',
                         response.status, response.data)


class DRSRequesterPaysRequired(Exception):

    def __init__(self, url: furl, response: urllib3.BaseHTTPResponse) -> None:
        super().__init__(f'DRS server requires requester-pays for {url}',
                         response.status, response.data)
