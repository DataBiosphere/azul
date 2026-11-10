import base64
import re
from typing import (
    Any,
    Callable,
)

from chalice.app import (
    BadRequestError,
    HeadersType,
    NotFoundError,
    Response,
)
from more_itertools import (
    one,
)

from azul.chalice import (
    Controller,
)
from azul.lib import (
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.service.contract_service import (
    ContractService,
    NoSuchContractError,
)


class ContractController(Controller):
    """
    Controller for listing and retrieving the contracts users can accept.
    Contracts have a name, are versioned with an integer and hashed with SHA256.
    """

    sha256_hexdigest_schema = schema.pattern('^[0-9a-f]{64}$')

    _current_version_name = 'current'

    _no_such_contract = 'There is no contract with the specified name'
    _no_such_contract_version = _no_such_contract + ' and version'

    @cached_property
    def _service(self) -> ContractService:
        return ContractService(package_root=self.app.package_root)

    _version_re = re.compile(rf'^({_current_version_name}|[1-9][0-9]*)$')

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/contracts',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'List the contracts a user can accept',
                'tags': ['Contracts'],
                'responses': {
                    '200': {
                        'description': fd('''
                            Maps the name of each contract to the URL of the
                            listing of its versions
                        '''),
                        **responses.json_content(
                            schema.object(
                                contracts=schema.object(
                                    additionalProperties=schema.make(str)
                                )
                            )
                        )
                    }
                }
            }
        )
        def list_contracts():
            return self.list_contracts(versions_view=list_contract_versions)

        @self.app.route(
            '/contracts/{name}',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'List the versions of a contract',
                'tags': ['Contracts'],
                'parameters': [
                    params.path('name', str, example='anvil_registered_access')
                ],
                'responses': {
                    '200': {
                        'description': fd('''
                            All versions of the contract, each with the SHA-256
                            digest of its text and the URL from which to
                            retrieve it, and which one of them is current
                        '''),
                        **responses.json_content(
                            schema.object(
                                contracts=schema.object(
                                    additionalProperties=schema.object(
                                        versions=schema.array(
                                            schema.object(
                                                version=int,
                                                sha256=self.sha256_hexdigest_schema,
                                                url=str
                                            )
                                        ),
                                        current=int
                                    )
                                )
                            )
                        )
                    },
                    '404': {
                        'description': self._no_such_contract
                    }
                }
            }
        )
        def list_contract_versions(name: str):
            return self.list_contract_versions(name, version_view=get_contract)

        @self.app.route(
            '/contracts/{name}/{version}',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Retrieve a version of a contract',
                'tags': ['Contracts'],
                'parameters': [
                    params.path('name', str, example='anvil_registered_access'),
                    params.path('version',
                                schema.pattern(self._version_re.pattern),
                                example=self._current_version_name,
                                description=fd(f'''
                                    The number of the version to retrieve,
                                    or `{self._current_version_name}` for the
                                    current version
                                '''))
                ],
                'responses': {
                    '200': {
                        'description': fd('''
                            The text of the contract, in Markdown
                        '''),
                        'headers': {
                            'Content-Digest': responses.header(
                                schema.pattern('^sha-256=:[A-Za-z0-9+/]{43}=:$'),
                                description=fd('''
                                    The SHA-256 digest of the text of the
                                    contract, as specified by [RFC 9530][1],
                                    with `sha-256` as the algorithm key. As
                                    required by that RFC, the digest is
                                    encoded in Base64, not in hexadecimal like
                                    the digests in the response from the
                                    `GET /contracts/{name}` endpoint.

                                    [1]: https://www.rfc-editor.org/rfc/rfc9530
                                ''')
                            ),
                            'ETag': responses.header(
                                schema.pattern('^"[1-9][0-9]*-[0-9a-f]{64}"$'),
                                description=fd(f'''
                                    The number of the version of the contract
                                    and the hexadecimal SHA-256 digest of its
                                    text, as listed by the
                                    `GET /contracts/{{name}}` endpoint, joined
                                    by a hyphen and enclosed in double quotes
                                    as required by [RFC 9110][1]. When this
                                    endpoint is reached by following the
                                    redirect for the
                                    `{self._current_version_name}` version,
                                    this header identifies the current version
                                    without the need to parse the URL the
                                    redirect pointed to.

                                    [1]: https://www.rfc-editor.org/rfc/rfc9110#field.etag
                                ''')
                            )
                        },
                        'content': {
                            'text/markdown': {}
                        }
                    },
                    '302': {
                        'description': fd(f'''
                            The `{self._current_version_name}` version was
                            requested. Retrieve the version the `Location`
                            header points to.
                        '''),
                        'headers': {
                            'Location': responses.header(str, description=fd('''
                                The URL of the current version of the contract
                            '''))
                        }
                    },
                    '400': {
                        'description': 'Invalid version'
                    },
                    '404': {
                        'description': self._no_such_contract_version
                    }
                }
            }
        )
        def get_contract(name: str, version: str):
            return self.get_contract(name, version, version_view=get_contract)

        return locals()

    def list_contracts(self, *, versions_view: Callable) -> JSON:
        return {
            'contracts': {
                name: self._self_url(versions_view, name=name)
                for name in self._service.names()
            }
        }

    def list_contract_versions(self, name: str, *, version_view: Callable) -> JSON:
        try:
            contract = self._service.contract(name)
        except NoSuchContractError:
            raise NotFoundError(self._no_such_contract, name)
        else:
            return {
                'contracts': {
                    name: {
                        'versions': [
                            {
                                'version': version,
                                'sha256': sha256,
                                'url': self._self_url(version_view,
                                                      name=name,
                                                      version=version)
                            }
                            for version, sha256 in contract.versions.items()
                        ],
                        'current': contract.current
                    }
                }
            }

    def get_contract(self,
                     name: str,
                     version: str,
                     *,
                     version_view: Callable
                     ) -> Response:
        if self._version_re.fullmatch(version) is None:
            raise BadRequestError('Invalid version', version)
        else:
            try:
                contract = self._service.contract(name)
                if version == self._current_version_name:
                    url = self._self_url(version_view, name=name, version=contract.current)
                    return Response(status_code=302, body='', headers={'Location': url})
                else:
                    version = int(version)
                    text = contract.text(version)
                    hex_digest = contract.versions[version]
                    b64_digest = base64.b64encode(bytes.fromhex(hex_digest)).decode()
                    headers: HeadersType = {
                        'Content-Type': 'text/markdown; charset=utf-8',
                        'Content-Digest': f'sha-256=:{b64_digest}:',
                        'ETag': f'"{version}-{hex_digest}"'
                    }
                    return Response(status_code=200, body=text, headers=headers)
            except NoSuchContractError:
                raise NotFoundError(self._no_such_contract_version, name, version)

    def _self_url(self, view: Callable, **params: str | int) -> str:
        """
        The URL of the route of the given view function, with the given values
        for the route's path parameters
        """
        path = one(getattr(view, 'path'))
        return str(self.app.base_url.add(path=path.format(**params)))
