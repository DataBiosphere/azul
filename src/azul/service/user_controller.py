from copy import (
    copy,
)
import logging
from typing import (
    Any,
)

from chalice.app import (
    BadRequestError,
)

from azul.chalice import (
    Controller,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.strings import (
    format_and_dedent as fd,
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

    @cached_property
    def _service(self) -> UserService:
        return UserService()

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/user/authorize',
            methods=['POST'],
            interactive=False,
            cors=True,
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
                    sure to request the `openid` scope.**
                '''),
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
                                approved by the user. Must contain `openid`.
                            ''')),
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

        return locals()

    def _authorize(self) -> JSON:
        try:
            request: JSON = json_mapping(self.current_request.json_body)
            # FIXME: Use PEP 728 extra TypedDict items instead of removing them
            #        https://github.com/DataBiosphere/azul/issues/7625
            request = {
                k: v
                for k, v in request.items()
                if k in Authorization.__annotations__.keys()
            }
            assert is_of_type(request, Authorization), R('Invalid request')
            response = copy(self._service.authorize(request))
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
