import logging
import os

from google.auth.transport.requests import (
    Request,
)
from google.oauth2 import (
    service_account,
)

from azul import (
    config,
)
from azul.dss import (
    shared_credentials,
)
from azul.logging import (
    configure_script_logging,
)
from azul.tdr import (
    AzulTDRClient,
)

log = logging.getLogger(__name__)


class SamRegistration:

    def __init__(self):
        with shared_credentials():
            credentials = service_account.Credentials.from_service_account_file(
                os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            )
        self.credentials: service_account.Credentials = credentials

    def register(self) -> None:
        token = self.get_access_token()
        response = AzulTDRClient.oauthed_http().request('POST',
                                                        f'{config.sam_service_url}/register/user/v1',
                                                        body='',
                                                        headers={'Authorization': f'Bearer {token}'})
        if response.status == 201:
            log.info('Google service account successfully registered with SAM.')
        elif response.status == 409:
            log.info('Google service account previously registered with SAM.')
        else:
            raise RuntimeError(f'Unexpected response during SAM registration: {response.data}')

    def get_access_token(self) -> str:
        credentials = self.credentials.with_scopes(['email', 'openid'])
        credentials.refresh(Request())
        return credentials.token


def main():
    configure_script_logging(log)
    sam = SamRegistration()
    sam.register()
    AzulTDRClient.verify_authorization()


if __name__ == '__main__':
    main()
