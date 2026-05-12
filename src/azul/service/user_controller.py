from copy import (
    copy,
)
import json
import logging
from typing import (
    Any,
)
import urllib.parse

from chalice.app import (
    BadRequestError,
    Response,
    UnauthorizedError,
)
import chevron

from azul.auth import (
    AccessTokenAuthentication,
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
    join_grammatically,
)
from azul.lib.types import (
    JSON,
    is_of_type,
    json_mapping,
    json_untyped_dict,
)
from azul.oauth2 import (
    Authorization,
)
from azul.openapi.responses import (
    json_content,
)
from azul.openapi.schema import (
    describe,
    object,
    optional,
)
from azul.service.user_service import (
    UserService,
)

log = logging.getLogger(__name__)


class UserController(Controller):
    _json_content_type = 'application/json'
    _form_content_type = 'application/x-www-form-urlencoded'

    @cached_property
    def _service(self) -> UserService:
        return UserService()

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/swagger/oauth2-redirect.html',
            interactive=False,
            cors=True,
            spec={
                'summary': 'Used internally by the Swagger UI',
                'tags': ['Auxiliary'],
                'responses': {
                    '200': {
                        'description': 'The response body is an HTML page'
                    }
                }
            }
        )
        def oauth2_redirect():
            return self._oauth2_redirect()

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
                'summary': 'Obtain an OAuth 2.0 access token in exchange for an authorization code',
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
                    }
                }
            }
        )
        def token():
            return self._token()

        return locals()

    @cached_property
    def _required_scopes(self) -> str:
        scopes = sorted(self._service.required_scopes)
        scopes = list(map(back_quote, scopes))
        return join_grammatically(scopes)

    def _oauth2_redirect(self) -> Response:
        params = self._query_params(self.current_request)
        nonce = CSP.new_nonce()
        template = self.app.load_static_resource(
            'swagger', 'oauth2-redirect.html.template.mustache'
        )
        body = chevron.render(template, {
            'NONCE': nonce,
            'CODE': json.dumps(params['code']),
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
            response = copy(self._service.authorize(authorization,
                                                    redirect_uri=redirect_uri))
            # Withhold refresh token from client for security reasons. The property
            # is required so we need to override the type checker on that. This is
            # safe because we made copy above.
            response.pop('refresh_token')  # type: ignore[misc]
            return json_untyped_dict(response)
        except AssertionError as e:
            if R.caused(e):
                raise BadRequestError(e.args)
            else:
                raise

    def _token(self) -> JSON:
        auth = self._authentication(self.current_request)
        if not isinstance(auth, AccessTokenAuthentication):
            raise UnauthorizedError('Valid access token required')
        else:
            try:
                self._service.narrow_token(auth)
            except TypeError:
                apat_auth = self._service.mint_personal_access_token(auth)
                return {'token': apat_auth.access_token}
            else:
                raise BadRequestError('Cannot exchange a personal access token for another')
