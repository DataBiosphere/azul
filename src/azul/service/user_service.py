from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.types import (
    not_none,
)
from azul.oauth2 import (
    Authorization,
    OAuth2Client,
    TokenForCodeResponse,
)


class UserService:

    @cached_property
    def _oauth_client(self) -> OAuth2Client:
        return OAuth2Client()

    @property
    def _client_secret(self) -> str:
        path = config.oauth2_client_secret_path()
        response = aws.secretsmanager.get_secret_value(SecretId=path)
        return response['SecretString']

    @property
    def _client_id(self) -> str:
        return not_none(config.google_oauth2_client_id)

    def authorize(self, authorization: Authorization) -> TokenForCodeResponse:
        assert 'openid' in authorization['scope'].split(), R(
            'The authorization server did not return an OpenID Connect ID '
            'token in the response. Be sure to include the "openid" scope '
            'when requesting the authorization code.')
        response = self._oauth_client.token_for_code(
            authorization_code=authorization['code'],
            client_id=self._client_id,
            client_secret=self._client_secret
        )
        assert 'id_token' in response, response
        return response
