from ast import (
    literal_eval,
)
import base64
from collections.abc import (
    Mapping,
)
from dataclasses import (
    dataclass,
    field,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
)
import urllib.parse

from chalice.app import (
    ChaliceViewError,
    Response,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)
import requests

from azul import (
    config,
    dss,
)
from azul.drs import (
    AccessMethod,
    drs_object_uri,
    drs_object_url_path,
)
from azul.lib import (
    cached_property,
    mutable_furl,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
    MutableJSON,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.service.controller import (
    ServiceController,
    validate_params,
)
from azul.service.index_service import (
    IndexService,
)


class DRSController(ServiceController):

    @cached_property
    def _service(self) -> IndexService:
        return IndexService()

    _drs_spec_description = fd('''
        This is a partial implementation of the [DRS 1.0.0 spec][1]. Not all
        features are implemented. This endpoint acts as a DRS-compliant proxy for
        accessing files in the underlying repository.

        [1]: https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.0.0/docs/

        Any errors encountered from the underlying repository are forwarded on as
        errors from this endpoint.
    ''')

    @classmethod
    def _get_object_response_schema(cls):
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

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            drs_object_url_path(object_id='{file_uuid}'),
            methods=['GET'],
            enabled=config.is_dss_enabled(),
            cors=True,
            spec={
                'summary': 'Get file DRS object',
                'tags': ['DRS'],
                'description': fd('''
                    This endpoint returns object metadata, and a list of access methods
                    that can be used to fetch object bytes.
                ''') + self._drs_spec_description,
                'parameters': self._file_fqid_parameters_spec,
                'responses': {
                    '200': {
                        'description': fd(
                            '''
                            A DRS object is returned. Two [`AccessMethod`s][1] are
                            included:

                            [1]: {link}

                            {access_methods}

                            If the object is not immediately ready, an `access_id` will
                            be returned instead of an `access_url`.
                            ''',
                            access_methods='\n'.join(f'- {am!s}' for am in AccessMethod),
                            link='https://ga4gh.github.io/data-repository-service-schemas'
                                 '/preview/release/drs-1.1.0/docs/#_accessmethod'),
                        **self._get_object_response_schema()
                    }
                }
            }
        )
        def get_object(file_uuid):
            """
            Return a DRS data object dictionary for a given DSS file UUID and version.

            If the file is already checked out, we can return a drs_object with a URL
            immediately. Otherwise, we need to send the request through the /access
            endpoint.
            """
            query_params = self._query_params(self.current_request)
            validate_params(query_params, version=str)
            return self.get_object(file_uuid, query_params)

        @self.app.route(
            drs_object_url_path(object_id='{file_uuid}', access_id='{access_id}'),
            methods=['GET'],
            enabled=config.is_dss_enabled(),
            cors=True,
            spec={
                'summary': 'Get a file with an access ID',
                'description': fd('''
                    This endpoint returns a URL that can be used to fetch the bytes of a
                    DRS object.

                    This method only needs to be called when using an `AccessMethod`
                    that contains an `access_id`.

                    An `access_id` is returned when the underlying file is not ready.
                    When the underlying repository is the DSS, the 202 response allowed
                    time for the DSS to do a checkout.
                ''') + self._drs_spec_description,
                'parameters': [
                    *self._file_fqid_parameters_spec,
                    params.path('access_id', str, description='Access ID returned from a previous request')
                ],
                'responses': {
                    '202': {
                        'description': fd('''
                            This response is issued if the object is not yet ready.
                            Respect the `Retry-After` header, then try again.
                        '''),
                        'headers': {
                            'Retry-After': responses.header(str, description=fd('''
                                Recommended number of seconds to wait before requesting
                                the URL specified in the Location header.
                            '''))
                        }
                    },
                    '200': {
                        'description': fd('''
                            The object is ready. The URL is in the response object.
                        '''),
                        **responses.json_content(schema.object(url=str))
                    }
                },
                'tags': ['DRS']
            }
        )
        def get_object_access(file_uuid, access_id):
            query_params = self._query_params(self.current_request)
            validate_params(query_params, version=str)
            return self.get_object_access(access_id, file_uuid, query_params)

        return locals()

    def _access_url(self, url):
        return {'url': url}

    def get_object(self, file_uuid, query_params):
        drs_object = DRSObject(file_uuid, version=query_params.get('version'))
        for access_method in AccessMethod:
            # We only want direct URLs for Google
            extra_params = dict(query_params, directurl=access_method.replica == 'gcp')
            response = self._dss_get_file(file_uuid, access_method.replica, **extra_params)
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
            response = self._dss_get_file(file_uuid, replica, **{
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

    def _dss_get_file(self, file_uuid, replica, **kwargs):
        dss_params = {
            'replica': replica,
            **kwargs
        }
        url = self.dss_file_url(file_uuid)
        return requests.api.get(str(url), params=dss_params, allow_redirects=False)

    @classmethod
    def dss_file_url(cls, file_uuid: str) -> mutable_furl:
        return mutable_furl(config.dss_endpoint).add(path=('files', file_uuid))


class GatewayTimeoutError(ChaliceViewError):
    STATUS_CODE = 504


@dataclass
class DRSObject:
    """"
    Used to build up a https://ga4gh.github.io/data-repository-service-schemas/docs/#_drsobject
    """
    uuid: str
    version: str | None = None
    access_methods: list[MutableJSON] = field(default_factory=list)

    def add_access_method(self,
                          access_method: AccessMethod, *,
                          url: str | None = None,
                          access_id: str | None = None):
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
        args = _url_query(replica='aws', version=self.version)
        url = DRSController.dss_file_url(self.uuid).add(args=args)
        headers = requests.api.head(str(url)).headers
        version = headers['x-dss-version']
        if self.version is not None:
            assert version == self.version
        uri = dss_drs_object_uri(file_uuid=self.uuid, file_version=version)
        return {
            **{
                'checksums': [
                    {'sha1': headers['x-dss-sha1']},
                    {'sha-256': headers['x-dss-sha256']}
                ],
                'created_time': timestamp(version),
                'id': self.uuid,
                'self_uri': str(uri),
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


def decode_access_id(access_id: str) -> tuple[str, str]:
    token = access_id.encode('ascii')  # Base64 is a subset of ASCII
    padding = b'=' * (-len(token) % 4)
    token = base64.urlsafe_b64decode(token + padding)
    token, replica = literal_eval(token.decode())
    if not isinstance(token, str) or not isinstance(replica, str):
        raise ValueError('Malformed access ID')
    return token, replica


def dss_drs_object_uri(*,
                       file_uuid: str,
                       file_version: str | None = None,
                       base_url: furl | None = None
                       ) -> mutable_furl:
    """
    The drs:// URL for a given DSS file UUID and version. The return value will
    point at the bare-bones DRS data object endpoint in the web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the DSS file version of the file

    :param base_url: an optional service endpoint, e.g. for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.
    """
    return drs_object_uri(base_url=_base_url(base_url),
                          path=(file_uuid,),
                          params=_url_query(version=file_version))


def dss_drs_object_url(*,
                       file_uuid: str,
                       file_version: str | None = None,
                       base_url: furl | None = None,
                       access_id: str | None = None
                       ) -> mutable_furl:
    """
    The http:// or https:// URL for a given DSS file UUID and version. The
    return value will point at the bare-bones DRS data object endpoint in the
    web service.

    :param file_uuid: the DSS file UUID of the file

    :param file_version: the optional DSS file version of the file

    :param base_url: an optional service endpoint, e.g. for local test servers.
                     If absent, the service endpoint for the current deployment
                     will be used.

    :param access_id: access id will be included in the URL if this parameter is
                      supplied
    """
    return mutable_furl(url=_base_url(base_url),
                        path=drs_object_url_path(object_id=file_uuid, access_id=access_id),
                        args=_url_query(version=file_version))


def _base_url(base_url: furl | None) -> furl:
    return config.drs_endpoint if base_url is None else base_url


def _url_query(*, version: str | None, **kwargs: str) -> Mapping[str, str]:
    """
    >>> _url_query(version=None, catalog='foo')
    {'catalog': 'foo'}

    >>> _url_query(version='', catalog='foo')
    {'catalog': 'foo', 'version': ''}

    Static type analysis would flag this, though:

    >>> _url_query(version=None, catalog=None)
    {'catalog': None}
    """
    return kwargs if version is None else kwargs | {'version': version}
