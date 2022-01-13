from ast import (
    literal_eval,
)
import base64
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
)
import time
from typing import (
    List,
    Mapping,
    Optional,
    Tuple,
)
import urllib.parse

from chalice import (
    ChaliceViewError,
    Response,
)
from deprecated import (
    deprecated,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    CatalogName,
    config,
    dss,
)
from azul.drs import (
    AccessMethod,
    dos_object_url_path,
    drs_object_uri,
    drs_object_url_path,
)
from azul.openapi import (
    responses,
    schema,
)
from azul.service.repository_service import (
    RepositoryService,
)
from azul.service.source_controller import (
    SourceController,
)
from azul.types import (
    JSON,
    MutableJSON,
)


class DRSController(SourceController):

    def _access_url(self, url):
        return {'url': url}

    @classmethod
    def get_object_response_schema(cls):
        return responses.json_content(
            schema.object(
                created_time=str,
                id=str,
                self_uri=str,
                size=str,
                version=str,
                checksums=schema.object(sha1=str, **{'sha-256': str}),
                access_methods=schema.array(schema.object(
                    access_url=schema.optional(schema.object(url=str)),
                    type=schema.optional(str),
                    access_id=schema.optional(str)
                ))
            )
        )

    def get_object(self, file_uuid, query_params):
        drs_object = DRSObject(file_uuid, version=query_params.get('version'))
        for access_method in AccessMethod:
            # We only want direct URLs for Google
            extra_params = dict(query_params, directurl=access_method.replica == 'gcp')
            response = self.dss_get_file(file_uuid, access_method.replica, **extra_params)
            if response.status_code == 301:
                retry_url = response.headers['location']
                query = urllib.parse.urlparse(retry_url).query
                query = urllib.parse.parse_qs(query, strict_parsing=True)
                token = one(query['token'])
                # We use the encoded token string as the key for our access ID.
                access_id = encode_access_id(token, access_method.replica)
                drs_object.add_access_method(access_method, access_id=access_id)
            elif response.status_code == 302:
                retry_url = response.headers['location']
                if access_method.replica == 'gcp':
                    assert retry_url.startswith('gs:')
                drs_object.add_access_method(access_method, url=retry_url)
            else:
                # For errors, just proxy DSS response
                return Response(response.text, status_code=response.status_code)
        return Response(drs_object.to_json())

    def get_object_access(self, access_id, file_uuid, query_params):
        try:
            token, replica = decode_access_id(access_id)
        except ValueError:
            return Response('Invalid DRS access ID', status_code=400)
        else:
            # Using the same token as before is OK. The DSS only starts a new
            # checkout if the token is absent. Otherwise the token undergoes
            # minimal validation and receives an update to the `attempts` key
            # (which is not used for anything besides perhaps diagnostics).
            response = self.dss_get_file(file_uuid, replica, **{
                **query_params,
                'directurl': replica == 'gcp',
                'token': token
            })
            if response.status_code == 301:
                headers = {'retry-after': response.headers['retry-after']}
                # DRS says no body for 202 responses
                return Response(body='', status_code=202, headers=headers)
            elif response.status_code == 302:
                retry_url = response.headers['location']
                return Response(self._access_url(retry_url))
            else:
                # For errors, just proxy DSS response
                return Response(response.text, status_code=response.status_code)

    def dss_get_file(self, file_uuid, replica, **kwargs):
        dss_params = {
            'replica': replica,
            **kwargs
        }
        url = config.dss_endpoint + '/files/' + file_uuid
        return requests.get(url, params=dss_params, allow_redirects=False)

    @deprecated('DOS support will be removed')
    def dos_get_object(self, catalog, file_uuid, file_version, authentication):
        service = RepositoryService()
        file = service.get_data_file(catalog=catalog,
                                     file_uuid=file_uuid,
                                     file_version=file_version,
                                     filters=self.get_filters(catalog, authentication, None))
        if file is not None:
            data_obj = self.file_to_drs(catalog, file)
            assert data_obj['id'] == file_uuid
            assert file_version is None or data_obj['version'] == file_version
            return Response({'data_object': data_obj}, status_code=200)
        else:
            return Response({'msg': "Data object not found."}, status_code=404)

    @deprecated('DOS support will be removed')
    def _dos_gs_url(self, file_uuid, version):
        url = config.dss_endpoint + '/files/' + urllib.parse.quote(file_uuid, safe='')
        params = dict({'file_version': version} if version else {},
                      directurl=True,
                      replica='gcp')
        while True:
            if self.lambda_context.get_remaining_time_in_millis() / 1000 > 3:
                dss_response = requests.get(url, params=params, allow_redirects=False)
                if dss_response.status_code == 302:
                    url = dss_response.next.url
                    assert url.startswith('gs')
                    return url
                elif dss_response.status_code == 301:
                    url = dss_response.next.url
                    remaining_lambda_seconds = self.lambda_context.get_remaining_time_in_millis() / 1000
                    server_side_sleep = min(1,
                                            max(remaining_lambda_seconds - config.api_gateway_timeout_padding - 3, 0))
                    time.sleep(server_side_sleep)
                else:
                    raise ChaliceViewError({
                        'msg': f'Received {dss_response.status_code} from DSS. Could not get file'
                    })
            else:
                raise GatewayTimeoutError({
                    'msg': f"DSS timed out getting file: '{file_uuid}', version: '{version}'."
                })

    @deprecated('DOS support will be removed')
    def file_to_drs(self, catalog: CatalogName, file):
        """
        Converts an aggregate file document to a DRS data object response.
        """
        urls = [
            self.file_url_func(catalog=catalog,
                               file_uuid=file['uuid'],
                               version=file['version'],
                               fetch=False,
                               wait='1',
                               fileName=file['name']),
            self._dos_gs_url(file['uuid'], file['version'])
        ]

        return {
            'id': file['uuid'],
            'urls': [
                {
                    'url': url
                }
                for url in urls
            ],
            'size': str(file['size']),
            'checksums': [
                {
                    'checksum': file['sha256'],
                    'type': 'sha256'
                }
            ],
            'aliases': [file['name']],
            'version': file['version'],
            'name': file['name']
        }


class GatewayTimeoutError(ChaliceViewError):
    STATUS_CODE = 504


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
                'self_uri': dss_drs_object_uri(self.uuid, version),
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

    >>> bad_access_id = 'KHsnbm90IGEnOiAnc3RyaW5nJ30sICdhd3MnKQ'
    >>> base64.urlsafe_b64decode(bad_access_id + '==')
    b"({'not a': 'string'}, 'aws')"

    >>> decode_access_id(bad_access_id)
    Traceback (most recent call last):
        ...
    ValueError: Malformed access ID
    """
    access_id = repr((token_str, replica)).encode()
    access_id = base64.urlsafe_b64encode(access_id)
    return access_id.rstrip(b'=').decode()


def decode_access_id(access_id: str) -> Tuple[str, str]:
    token = access_id.encode('ascii')  # Base64 is a subset of ASCII
    padding = b'=' * (-len(token) % 4)
    token = base64.urlsafe_b64decode(token + padding)
    token, replica = literal_eval(token.decode())
    if not isinstance(token, str) or not isinstance(replica, str):
        raise ValueError('Malformed access ID')
    return token, replica


def dss_drs_object_uri(file_uuid: str,
                       file_version: Optional[str] = None,
                       base_url: Optional[str] = None) -> str:
    """
    The drs:// URL for a given DSS file UUID and version. The return value will
    point at the bare-bones DRS data object endpoint in the web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the DSS file version of the file

    :param base_url: an optional service endpoint, e.g for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.
    """
    _, netloc = _endpoint(base_url)
    return drs_object_uri(netloc, file_uuid, **_url_query(file_version))


def dss_dos_object_url(catalog: CatalogName,
                       file_uuid: str,
                       file_version: Optional[str] = None,
                       base_url: Optional[str] = None) -> str:
    """
    The http:// or https:// URL for a given DSS file UUID and version. The
    return value will point at the bare-bones DOS data object endpoint in the
    web service.

    :param catalog: the name of the catalog to retrieve the file from

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the DSS file version of the file

    :param base_url: an optional service endpoint, e.g for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.
    """
    scheme, netloc = _endpoint(base_url)
    return str(furl(scheme=scheme,
                    netloc=netloc,
                    path=dos_object_url_path(file_uuid),
                    query_params=dict(_url_query(file_version), catalog=catalog)))


def dss_drs_object_url(file_uuid: str,
                       file_version: Optional[str] = None,
                       base_url: Optional[str] = None,
                       access_id: Optional[str] = None) -> str:
    """
    The http:// or https:// URL for a given DSS file UUID and version. The
    return value will point at the bare-bones DRS data object endpoint in the
    web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the optional DSS file version of the file

    :param base_url: an optional service endpoint, e.g for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.

    :param access_id: access id will be included in the URL if this parameter is
                      supplied
    """
    scheme, netloc = _endpoint(base_url)
    return str(furl(scheme=scheme,
                    netloc=netloc,
                    path=drs_object_url_path(file_uuid, access_id=access_id),
                    args=_url_query(file_version)))


def _endpoint(base_url: Optional[str]) -> Tuple[str, str]:
    if base_url is None:
        base_url = config.drs_endpoint()
    base_url = furl(base_url)
    return base_url.scheme, base_url.netloc


def _url_query(file_version: Optional[str]) -> Mapping[str, str]:
    return {'version': file_version} if file_version else {}
