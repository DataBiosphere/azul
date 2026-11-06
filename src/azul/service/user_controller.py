from collections.abc import (
    Iterator,
)
from contextlib import (
    contextmanager,
)
from copy import (
    copy,
)
import json
import logging
from typing import (
    Any,
    NotRequired,
)
import urllib.parse

from chalice.app import (
    BadRequestError,
    ForbiddenError,
    Response,
    UnauthorizedError,
)
import chevron

from azul.auth import (
    AccessTokenAuthentication,
    PersonalAccessTokenAuthentication,
)
from azul.chalice import (
    Controller,
)
from azul.csp import (
    CSP,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.strings import (
    back_quote,
    format_and_dedent as fd,
    is_redactable,
    join_grammatically,
)
from azul.lib.types import (
    JSON,
    JSONTypedDict,
    is_of_type,
    json_mapping,
    json_untyped_dict,
)
from azul.oauth2 import (
    Authorization,
    InvalidAccessTokenError,
)
from azul.openapi.responses import (
    json_content,
)
from azul.openapi.schema import (
    describe,
    nullable,
    object,
    optional,
)
from azul.service.contract_controller import (
    ContractController,
)
from azul.service.contract_service import (
    ContractService,
    NoSuchContractError,
)
from azul.service.user_service import (
    ContractAcceptance,
    ForeignTokenException,
    UnknownUserException,
    User,
    UserService,
    format_authorized_until,
)

log = logging.getLogger(__name__)


class UserUpdate(JSONTypedDict):
    accepted_contracts: NotRequired[dict[str, ContractAcceptance | None] | None]


class UserController(Controller):
    accepts_personal_access_token = True
    _json_content_type = 'application/json'
    _form_content_type = 'application/x-www-form-urlencoded'

    @cached_property
    def _service(self) -> UserService:
        return UserService()

    @cached_property
    def _contract_service(self) -> ContractService:
        return ContractService(package_root=self.app.package_root)

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/user/authorize',
            methods=['POST'],
            interactive=False,
            cors=True,
            content_types=[
                self._json_content_type,
                self._form_content_type
            ],
            spec={
                'summary': 'Obtain an OAuth 2.0 access token '
                           'in exchange for an authorization code',
                'description': fd('''
                    Invoke this endpoint as part of the authorization code flow
                    from a single-page app.

                    Note that while this endpoint is part of the authorization
                    code flow, which typically yields a refresh token along with
                    the access token, this endpoint returns only the access
                    token. Doing so is an aspect of the commonly adopted
                    security best practice known as [Backend For Frontend](
                    https://datatracker.ietf.org/doc/html/draft-ietf-oauth-browser-based-apps#section-6.1).

                    **When initiating the authorization code flow, be
                    sure to request the {required_scopes} scopes.**
                ''', required_scopes=self._required_scopes),
                'requestBody': {
                    'description': fd('''
                        JSON conforming to the Google Sign-In [CodeResponse](
                        https://developers.google.com/identity/oauth2/web/reference/js-reference#CodeResponse),
                        except for the error-related parts. Those should be
                        handled client-side.
                    '''),
                    'required': True,
                    **json_content(
                        object(
                            code=describe(str, fd('''
                                The authorization code of a successful token
                                response.
                            ''')),
                            scope=describe(str, fd('''
                                A space-delimited list of scopes that are
                                approved by the user. Must contain
                                {required_scopes}.
                            ''', required_scopes=self._required_scopes)),
                            state=optional(describe(str, fd('''
                                The string value that your application uses to
                                maintain state between your authorization
                                request and the response.
                            '''))),
                            additionalProperties=True
                        )
                    )
                },
                'tags': ['User'],
                'responses': {
                    '200': {
                        'description': fd('''
                            The authorization code flow was completed successfully
                        '''),
                        **json_content(
                            object(
                                access_token=describe(str, fd('''
                                    A fresh access token for the user
                                ''')),
                                expires_in=describe(int, fd('''
                                    Expiration of the access token, in seconds
                                ''')),
                                scope=describe(str, fd('''
                                    The scopes granted by the user
                                ''')),
                                token_type=describe(str, fd('''
                                    Always 'Bearer'
                                ''')),
                                id_token=describe(str, fd('''
                                    The OIDC ID token identifying the user
                                '''))
                            )
                        )
                    }
                }
            }
        )
        def authorize():
            return self._authorize()

        @self.app.route(
            '/user/token',
            interactive=True,
            cors=True,
            spec={
                'summary': 'Obtain a personal access token',
                'description': fd('''
                    Obtain a long-lived, application-specific personal access
                    token (APAT) in exchange for a valid OAuth 2.0 access token.
                    The access token must be passed in the `Authorization`
                    header as a Bearer token. The user must have previously
                    completed the authorization code flow. In the Swagger UI,
                    this can be done by clicking the Authorize button above.
                '''),
                'tags': ['User'],
                'responses': {
                    '200': {
                        'description': fd('''
                            A personal access token was successfully minted
                        '''),
                        **json_content(
                            object(
                                token=describe(str, fd('''
                                    The personal access token
                                '''))
                            )
                        )
                    },
                    '401': {
                        'description': fd('''
                            No valid OAuth 2.0 access token was provided
                        ''')
                    },
                    '403': {
                        'description': fd('''
                            A personal access token was provided, which cannot
                            be exchanged for another one
                        ''')
                    }
                }
            }
        )
        def token():
            return self._token()

        @self.app.route(
            '/user/revoke',
            methods=['POST'],
            interactive=True,
            cors=True,
            spec={
                'summary': 'Revoke all personal access tokens',
                'description': fd('''
                    Revoke all personal access tokens (APATs) for the
                    authenticated user. The user must provide a valid OAuth 2.0
                    access token in the `Authorization` header as a Bearer
                    token.
                '''),
                'tags': ['User'],
                'responses': {
                    '204': {
                        'description': fd('''
                            All personal access tokens were revoked
                        ''')
                    },
                    '401': {
                        'description': fd('''
                            No valid OAuth 2.0 access token was provided
                        ''')
                    },
                    '403': {
                        'description': fd('''
                            A personal access token was provided, which cannot
                            be used to revoke
                        ''')
                    }
                }
            }
        )
        def revoke():
            return self._revoke()

        @self.app.route(
            '/user',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Retrieve information about the user, '
                           'such as which contracts they accepted',
                'description': fd('''
                    Retrieve information about the authenticated user,
                    including the contracts they accepted. The user must
                    provide a valid OAuth 2.0 access token in the
                    `Authorization` header as a Bearer token, and must have
                    previously completed the authorization code flow.
                '''),
                'tags': ['User'],
                'responses': {
                    '200': {
                        'description': 'Information about the user',
                        **json_content(self._user_schema)
                    },
                    **self._user_error_responses
                }
            }
        )
        def get_user():
            return self._get_user()

        @self.app.route(
            '/user',
            methods=['PATCH'],
            cors=True,
            spec={
                'summary': 'Update information about the user, '
                           'such as the contracts they accepted',
                'description': fd('''
                    Accept contracts on behalf of the authenticated user, or
                    retract their acceptance. The user must provide a valid
                    OAuth 2.0 access token in the `Authorization` header as a
                    Bearer token, and must have previously completed the
                    authorization code flow.
                '''),
                'tags': ['User'],
                'requestBody': {
                    'description': fd('''
                        The attributes of the user to update. Currently, only
                        the `accepted_contracts` attribute can be updated.
                    '''),
                    'required': True,
                    **json_content(
                        object(
                            accepted_contracts=optional(describe(
                                nullable(object(
                                    additionalProperties=nullable(object(
                                        version=int,
                                        sha256=ContractController.sha256_hexdigest_schema
                                    ))
                                )),
                                fd('''
                                    Maps the name of each contract to the
                                    version of it the user accepts, or to
                                    `null` to retract the acceptance of that
                                    contract. The version must be accompanied
                                    by the SHA-256 digest of its text, as listed
                                    by the `GET /contracts/{name}` endpoint.
                                    Contracts not mentioned are left alone.
                                    `null` instead of a mapping retracts the
                                    acceptance of every contract. The time of
                                    acceptance is recorded by the server.
                                ''')
                            ))
                        )
                    )
                },
                'responses': {
                    '200': {
                        'description': fd('''
                            The user was updated. The response body is the same
                            as that of a subsequent `GET` request.
                        '''),
                        **json_content(self._user_schema)
                    },
                    '400': {
                        'description': fd('''
                            The request body is invalid, it names a contract
                            or version of a contract that does not exist, or
                            the SHA-256 digest does not match the version
                        ''')
                    },
                    **self._user_error_responses
                }
            }
        )
        def update_user():
            return self._update_user()

        @self.app.route(
            '/swagger/oauth2-redirect.html',
            interactive=False,
            cors=True,
            spec={
                'summary': 'Used for logging into the the Swagger UI',
                'tags': ['User'],
                'responses': {
                    '200': {
                        'description': 'The response body is an HTML page'
                    }
                }
            }
        )
        def oauth2_redirect():
            return self._oauth2_redirect()

        return locals()

    @cached_property
    def _required_scopes(self) -> str:
        scopes = sorted(self._service.required_scopes)
        scopes = list(map(back_quote, scopes))
        return join_grammatically(scopes)

    def _oauth2_redirect(self) -> Response:
        params = self._query_params(self.current_request)
        try:
            code = params['code']
        except KeyError:
            raise BadRequestError('Missing authorization code')
        nonce = CSP.new_nonce()
        template = self.app.load_static_resource(
            'swagger', 'oauth2-redirect.html.template.mustache'
        )
        body = chevron.render(template, {
            'NONCE': nonce,
            'CODE': json.dumps(code),
            'STATE': json.dumps(params.get('state', '')),
        })
        csp = CSP.for_azul(nonce=nonce)
        return Response(status_code=200,
                        body=body,
                        headers={
                            'Content-Type': 'text/html',
                            'Content-Security-Policy': str(csp),
                        })

    def _authorize(self) -> JSON:
        try:
            content_type, charset = self._request_content_type()
            if content_type == self._form_content_type:
                body = self.current_request.raw_body
                if isinstance(body, bytes):
                    body = body.decode(charset)
                params = urllib.parse.parse_qs(body)
                authorization = Authorization(
                    code=params['code'][0],
                    scope=' '.join(sorted(self._service.required_scopes))
                )
                redirect_uri = params['redirect_uri'][0]
            elif content_type == self._json_content_type:
                request: JSON = json_mapping(self.current_request.json_body)
                # FIXME: Use PEP 728 extra TypedDict items instead of removing them
                #        https://github.com/DataBiosphere/azul/issues/7625
                request = {
                    k: v
                    for k, v in request.items()
                    if k in Authorization.__annotations__.keys()
                }
                assert is_of_type(request, Authorization), R('Invalid request')
                authorization = request
                redirect_uri = None
            else:
                raise BadRequestError('Unsupported content type')
            if not is_redactable(authorization['code']):
                raise BadRequestError('Unexpected authorization code syntax')
            else:
                response = copy(self._service.authorize(authorization,
                                                        redirect_uri=redirect_uri))
                # Withhold refresh token from client for security reasons. The
                # property is required so we need to override the type checker
                # on that. This is safe because we made copy above.
                response.pop('refresh_token')  # type: ignore[misc]
                return json_untyped_dict(response)
        except AssertionError as e:
            if R.caused(e):
                raise BadRequestError(e.args)
            else:
                raise

    def _token(self) -> JSON:
        auth = self._access_token('Cannot exchange a personal access token for another')
        with self._rejecting_unusable_token(auth):
            apat_auth = self._service.mint_personal_access_token(auth)
        return {'token': apat_auth.token}

    def _revoke(self) -> Response:
        auth = self._access_token('Cannot revoke using a personal access token')
        with self._rejecting_unusable_token(auth):
            self._service.revoke_personal_access_tokens(auth)
        return Response(status_code=204, body='')

    @property
    def _user_schema(self) -> JSON:
        return object(
            email=describe(str, 'The email address of the user'),
            authorized_until=describe(str, fd('''
                When, in UTC, the user's authorization of this application
                lapses. The user's acceptance of every contract in
                `accepted_contracts` lapses at the same time. Authorizing this
                application again extends both.
            ''')),
            accepted_contracts=describe(
                object(
                    additionalProperties=object(
                        version=describe(int, fd('''
                            The version of the contract the user accepted
                        ''')),
                        sha256=describe(ContractController.sha256_hexdigest_schema, fd('''
                            The SHA-256 digest of the text of that version
                        ''')),
                        time=describe(str, fd('''
                            When the user accepted the contract, in UTC
                        '''))
                    )
                ),
                fd('''
                    The contracts the user accepted, by name. Contracts the
                    user did not accept are absent.
                ''')
            )
        )

    @property
    def _user_error_responses(self) -> JSON:
        return {
            '401': {
                'description': fd('''
                    No valid OAuth 2.0 access token was provided, or the user
                    has not completed the authorization code flow
                ''')
            },
            '403': {
                'description': fd('''
                    A personal access token was provided, which cannot be used
                    for this endpoint
                ''')
            }
        }

    def _get_user(self) -> JSON:
        auth = self._access_token('Cannot retrieve a user with a personal access token')
        with self._rejecting_unusable_token(auth):
            user = self._service.get_user(auth)
        return self._user_json(user)

    def _update_user(self) -> JSON:
        auth = self._access_token('Cannot update a user with a personal access token')
        request = self.current_request.json_body
        if not is_of_type(request, UserUpdate):
            raise BadRequestError('Invalid request body')
        else:
            accepted_contracts = request.get('accepted_contracts', {})
            if accepted_contracts is not None:
                for name, acceptance in accepted_contracts.items():
                    self._validate_acceptance(name, acceptance)
            with self._rejecting_unusable_token(auth):
                user = self._service.update_accepted_contracts(auth, accepted_contracts)
            return self._user_json(user)

    def _validate_acceptance(self,
                             name: str,
                             acceptance: ContractAcceptance | None
                             ) -> None:
        try:
            contract = self._contract_service.contract(name)
        except NoSuchContractError:
            raise BadRequestError('No such contract', name)
        else:
            if acceptance is not None:
                version, sha256 = acceptance['version'], acceptance['sha256']
                if version not in contract.versions:
                    raise BadRequestError('No such version of the contract', name, version)
                elif sha256 != contract.versions[version]:
                    raise BadRequestError('The SHA-256 digest does not match the version of the contract',
                                          name, version, sha256)

    def _user_json(self, user: User) -> JSON:
        return {
            'email': user['email'],
            'authorized_until': format_authorized_until(user),
            'accepted_contracts': {
                name: json_untyped_dict(contract)
                for name, contract in user['accepted_contracts'].items()
            }
        }

    def _access_token(self, message: str) -> AccessTokenAuthentication:
        """
        Return the access token the current request is authenticated with.

        :param message: The message of the error to raise if the request is
                          authenticated with a personal access token
        """
        auth = self._authentication(self.current_request)
        if isinstance(auth, PersonalAccessTokenAuthentication):
            raise ForbiddenError(message)
        elif isinstance(auth, AccessTokenAuthentication):
            return auth
        else:
            raise UnauthorizedError('Valid access token required')

    @contextmanager
    def _rejecting_unusable_token(self,
                                  auth: AccessTokenAuthentication
                                  ) -> Iterator[None]:
        """
        Translate the ways in which an access token can turn out to be unusable
        into the 401 response the endpoints document for that case. An
        unexpected response from the authorization server is not one of them;
        it raises, and the resulting 5xx trips an alarm, as it should.
        """
        try:
            yield
        except (InvalidAccessTokenError,
                ForeignTokenException,
                UnknownUserException) as e:
            log.warning('Rejecting %s', auth, exc_info=e)
            raise UnauthorizedError('Valid access token required')
