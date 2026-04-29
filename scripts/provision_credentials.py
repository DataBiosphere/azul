import argparse
import base64
import getpass
import json
import logging
import os
import time
from typing import (
    TYPE_CHECKING,
)
import uuid

from furl import (
    furl,
)
import google.auth
import googleapiclient.discovery
from googleapiclient.errors import (
    HttpError,
)

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
from azul.lib.strings import (
    format_and_dedent,
)
from azul.logging import (
    configure_script_logging,
)

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import (
        SecretsManagerClient,
    )

log = logging.getLogger(__name__)


class CredentialsProvisioner:

    @cached_property
    def _google_iam(self):
        credentials, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        return googleapiclient.discovery.build('iam', 'v1', credentials=credentials)

    @property
    def _secrets_manager(self) -> SecretsManagerClient:
        return aws.secretsmanager

    def provision_sa(self, args: argparse.Namespace) -> None:
        self._provision_sa(args.create, args.email, args.secret_name)

    def provision_hmac(self, args: argparse.Namespace) -> None:
        self._provision_hmac(args.create)

    def provision_oauth2_client_secret(self, args: argparse.Namespace) -> None:
        self._provision_oauth2_client_secret(args.create)

    def _provision_sa(self, create: bool, email: str, secret_name: str) -> None:
        secret_path = config.secret_path(secret_name)
        if create:
            self._create_secret(secret_path)
            if not self._secret_is_stored(secret_path):
                google_key = self._create_sa_credentials(email)
                self._write_secret_value(secret_path, google_key)
        else:
            self._destroy_sa_credentials(email, secret_path)
            self._destroy_secret(secret_path)

    def _create_sa_credentials(self, email: str) -> str:
        iam = self._google_iam
        key_name = 'projects/-/serviceAccounts/' + email
        keys = iam.projects().serviceAccounts().keys()
        key = keys.create(name=key_name, body={}).execute()
        log.info('Successfully created service account key for user %r', email)
        return base64.decodebytes(bytes(key['privateKeyData'], 'ascii')).decode()

    def _destroy_sa_credentials(self, email: str, secret_name: str) -> None:
        try:
            creds = self._secrets_manager.get_secret_value(SecretId=secret_name)
        except self._secrets_manager.exceptions.ResourceNotFoundException:
            log.info('Secret already deleted, cannot get key_id for %s', email)
            return
        else:
            key_id = json.loads(creds['SecretString'])['private_key_id']
            iam = self._google_iam
            try:
                key_name = f'projects/-/serviceAccounts/{email}/keys/{key_id}'
                iam.projects().serviceAccounts().keys().delete(name=key_name).execute()
            except HttpError as e:
                if e.resp.reason != 'Not Found':
                    raise
            log.info('Successfully deleted service account key with id %r for user %r',
                     key_id, email)

    def _provision_hmac(self, create: bool) -> None:
        secret_path = config.hmac_secret_path()
        if create:
            self._create_secret(secret_path)
            if not self._secret_is_stored(secret_path):
                self._write_secret_value(secret_path, self._random_hmac_key())
        else:
            self._destroy_secret(secret_path)

    def _random_hmac_key(self) -> str:
        # Even though an HMAC key can be any sequence of bytes, we restrict to
        # base64 in order to encode as string
        key = base64.encodebytes(os.urandom(48)).decode().replace('=', '').replace('\n', '')
        assert len(key) == 64
        return json.dumps({'key': key, 'key_id': str(uuid.uuid4())})

    def _provision_oauth2_client_secret(self, create: bool) -> None:
        secret_path = config.oauth2_client_secret_path()
        if create:
            self._create_secret(secret_path)
            client_id = config.google_oauth2_client_id
            assert client_id is not None, R('AZUL_GOOGLE_OAUTH2_CLIENT_ID is not set')
            google_project = config.google_project()
            assert google_project is not None, R('GOOGLE_PROJECT is not set')
            url = furl(scheme='https',
                       host='console.cloud.google.com',
                       path=['auth', 'clients', client_id],
                       args={'project': google_project})
            print(format_and_dedent('''
                Visit {url} and …

                1) Delete any disabled secrets, leaving only the current secret

                2) Add a new secret

                3) Copy the secret value

                4) Paste the secret value into the prompt below
            ''', url=url))
            secret_value = getpass.getpass('OAuth2 client secret (input will not be echoed back): ')
            assert secret_value, R('No secret value provided')
            self._write_secret_value(secret_path, secret_value)
            print(format_and_dedent('''
                The secret was successfully stored. Now it's time to …

                1) Deploy this Azul instance, unless you already did so before
                   invoking this script

                2) Test logging into the Swagger UI and any Data Browser
                   instance backed by this Azul deployment

                3) Wait 1 hour and 10 minutes for all access tokens to expire

                4) Repeat the tests from step 2, then disable (do not delete)
                   the old secret
            '''))
        else:
            self._destroy_secret(secret_path)

    def _secret_is_stored(self, path: str) -> bool:
        try:
            response = self._secrets_manager.get_secret_value(SecretId=path)
        except self._secrets_manager.exceptions.ResourceNotFoundException:
            return False
        try:
            return response['SecretString'] != ''
        except KeyError:
            return False

    def _create_secret(self, path: str) -> None:
        try:
            self._secrets_manager.create_secret(Name=path)
        except self._secrets_manager.exceptions.ResourceExistsException:
            log.info('AWS secret %s already exists.', path)
        else:
            log.info('AWS secret %s created.', path)

    def _write_secret_value(self, path: str, value: str) -> None:
        self._secrets_manager.put_secret_value(SecretId=path, SecretString=value)
        log.info('Successfully wrote value to AWS secret %r.', path)

    def _destroy_secret(self, path: str) -> None:
        try:
            response = self._secrets_manager.delete_secret(
                SecretId=path,
                ForceDeleteWithoutRecovery=True
            )
        except self._secrets_manager.exceptions.ResourceNotFoundException:
            log.info('AWS secret %s does not exist. No changes will be made.', path)
        else:
            assert response['Name'] == path
            # AWS docs recommend waiting for ResourceNotFoundException: "The
            # deletion is an asynchronous process. There might be a short delay"
            #
            # https://aws.amazon.com/premiumsupport/knowledge-center/delete-secrets-manager-secret/
            #
            deadline = time.time() + 60
            while True:
                try:
                    self._secrets_manager.describe_secret(SecretId=path)
                except self._secrets_manager.exceptions.ResourceNotFoundException:
                    log.info('Successfully deleted AWS secret %r.', path)
                    break
                else:
                    now = time.time()
                    if now >= deadline:
                        raise RuntimeError('Secret could not be destroyed', path)
                    else:
                        log.info('Secret %r not yet deleted. Will keep checking for %.3fs.',
                                 path, deadline - now)
                        time.sleep(5)


if __name__ == '__main__':
    # Suppress noisy warning from Google library. See
    #
    # https://github.com/googleapis/google-api-python-client/issues/299#issuecomment-255793971
    #
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    configure_script_logging(log)
    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_group = mode_parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--create', '-c', action='store_true', dest='create',
        help='Idempotently create credentials instead of destroying them.'
    )
    mode_group.add_argument(
        '--destroy', '-d', action='store_false', dest='create',
        help='Idempotently destroy credentials instead of creating them.'
    )

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='Specify action', dest='resource')
    subparsers.required = True

    sa_parser = subparsers.add_parser(
        'service_account',
        parents=[mode_parser],
        help='Create credentials for a Google service account and them in an '
             'AWS Secrets Manager secret.'
    )
    sa_parser.set_defaults(func=CredentialsProvisioner.provision_sa)
    sa_parser.add_argument(
        'email', type=str,
        help='The email address of the service account'
    )
    sa_parser.add_argument(
        'secret_name', type=str,
        help='The name of secret to store the service account credentials in'
    )

    hmac_parser = subparsers.add_parser(
        'hmac', parents=[mode_parser],
        help='Generate a random HMAC signing key and store it in an AWS '
             'Secrets Manager secret.'
    )
    hmac_parser.set_defaults(func=CredentialsProvisioner.provision_hmac)

    oauth_parser = subparsers.add_parser(
        'oauth2_client_secret',
        parents=[mode_parser],
        help='Store or rotate the OAuth2 client secret in an AWS '
             'Secrets Manager secret.'
    )
    oauth_parser.set_defaults(func=CredentialsProvisioner.provision_oauth2_client_secret)

    args = parser.parse_args()
    credentials_provisioner = CredentialsProvisioner()
    args.func(credentials_provisioner, args)
