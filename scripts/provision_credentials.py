import argparse
import base64
import json
import logging
import os
import time
import uuid

import google.auth
import googleapiclient.discovery
from googleapiclient.errors import (
    HttpError,
)

from azul import (
    cached_property,
    config,
)
from azul.deployment import (
    aws,
)
from azul.logging import (
    configure_script_logging,
)

logger = logging.getLogger(__name__)


def parse_google_key(response):
    return base64.decodebytes(bytes(response['privateKeyData'], 'ascii')).decode()


class CredentialsProvisioner:

    @cached_property
    def google_iam(self):
        credentials, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        return googleapiclient.discovery.build('iam', 'v1', credentials=credentials)

    @property
    def secrets_manager(self):
        return aws.secretsmanager

    def provision_google_from_args(self, args):
        self.provision_google(args.build, args.email, args.secret_name)

    def provision_hmac_from_args(self, args):
        self.provision_hmac(args.build)

    def provision_hmac(self, build):
        secret_name = config.secrets_manager_secret_name('indexer', 'hmac')
        if build:
            self._create_secret(secret_name)
            if not self._secret_is_stored(secret_name):
                self._write_secret_value(secret_name, self._random_hmac_key())
        else:
            self._destroy_aws_secrets_manager_secret(secret_name)

    def provision_google(self, build, email, secret_name):
        secret_name = config.secrets_manager_secret_name(secret_name)
        if build:
            self._create_secret(secret_name)
            if not self._secret_is_stored(secret_name):
                google_key = self._create_service_account_creds(email)
                self._write_secret_value(secret_name, google_key)
        else:
            self._destroy_service_account_creds(email, secret_name)
            self._destroy_aws_secrets_manager_secret(secret_name)

    @classmethod
    def _random_hmac_key(cls):
        # Even though an HMAC key can be any sequence of bytes, we restrict to
        # base64 in order to encode as string
        key = base64.encodebytes(os.urandom(48)).decode().replace('=', '').replace('\n', '')
        assert len(key) == 64
        return json.dumps({'key': key, 'key_id': str(uuid.uuid4())})

    def _write_secret_value(self, name, value):
        self.secrets_manager.put_secret_value(
            SecretId=name,
            SecretString=value
        )
        logger.info("Successfully wrote value to AWS secret '%s'.", name)

    def _create_secret(self, name):
        try:
            self.secrets_manager.create_secret(Name=name)
        except self.secrets_manager.exceptions.ResourceExistsException:
            logger.info('AWS secret %s already exists.', name)
        else:
            logger.info('AWS secret %s created.', name)

    def _secret_is_stored(self, name):
        try:
            response = self.secrets_manager.get_secret_value(SecretId=name)
        except self.secrets_manager.exceptions.ResourceNotFoundException:
            return False
        try:
            return response['SecretString'] != ''
        except KeyError:
            return False

    def _create_service_account_creds(self, service_account_email):
        iam = self.google_iam
        key = iam.projects().serviceAccounts().keys().create(
            name='projects/-/serviceAccounts/' + service_account_email, body={}
        ).execute()
        logger.info("Successfully created service account key for user '%s'", service_account_email)
        return parse_google_key(key)

    def _destroy_aws_secrets_manager_secret(self, secret_name):
        try:
            response = self.secrets_manager.delete_secret(
                SecretId=secret_name,
                ForceDeleteWithoutRecovery=True
            )
        except self.secrets_manager.exceptions.ResourceNotFoundException:
            logger.info('AWS secret %s does not exist. No changes will be made.', secret_name)
        else:
            assert response['Name'] == secret_name
            # AWS docs recommend waiting for ResourceNotFoundException:
            # "The deletion is an asynchronous process. There might be a short
            # delay". See https://aws.amazon.com/premiumsupport/knowledge-center/delete-secrets-manager-secret/
            deadline = time.time() + 60
            while True:
                try:
                    self.secrets_manager.describe_secret(SecretId=secret_name)
                except self.secrets_manager.exceptions.ResourceNotFoundException:
                    logger.info('Successfully deleted AWS secret %r.', secret_name)
                    break
                else:
                    now = time.time()
                    if now >= deadline:
                        raise RuntimeError('Secret could not be destroyed', secret_name)
                    else:
                        logger.info('Secret %r not yet deleted. Will keep checking for %.3fs.',
                                    secret_name, deadline - now)
                        time.sleep(5)

    def _destroy_service_account_creds(self, service_account_email, secret_name):
        try:
            creds = self.secrets_manager.get_secret_value(
                SecretId=config.secrets_manager_secret_name(secret_name)
            )
        except self.secrets_manager.exceptions.ResourceNotFoundException:
            logger.info('Secret already deleted, cannot get key_id for %s', service_account_email)
            return
        else:
            key_id = json.loads(creds['SecretString'])['private_key_id']
            iam = self.google_iam
            try:
                iam.projects().serviceAccounts().keys().delete(
                    name='projects/-/serviceAccounts/' + service_account_email + '/keys/' + key_id).execute()
            except HttpError as e:
                if e.resp.reason != 'Not Found':
                    raise
            logger.info("Successfully deleted service account key with id '%s' for user '%s'",
                        key_id, service_account_email)


if __name__ == '__main__':
    # Suppress noisy warning from Google library. See
    # https://github.com/googleapis/google-api-python-client/issues/299#issuecomment-255793971
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    configure_script_logging(logger)
    provision_parser = argparse.ArgumentParser(add_help=False)
    group = provision_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--build', '-b', action='store_true', dest='build',
                       help='Create credentials instead of destroying them. This action is idempotent.')
    group.add_argument('--destroy', '-d', action='store_false', dest='build',
                       help='Destroy credentials instead of building them. This action is idempotent.')
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='Specify action', dest='action')
    subparsers.required = True
    google_parser = subparsers.add_parser('google-key', parents=[provision_parser],
                                          help='Create Google service account key and store in an AWS secret.')
    google_parser.set_defaults(func=CredentialsProvisioner.provision_google_from_args)
    google_parser.add_argument('email', type=str,
                               help='Email address for the Google service account '
                                    'for which to provision credentials')
    google_parser.add_argument('secret_name', type=str,
                               help='Name of the AWS secret to store the Google service account credentials')
    hmac_parser = subparsers.add_parser('hmac-key', parents=[provision_parser],
                                        help='Create a random HMAC key and store in an AWS secret.')
    hmac_parser.set_defaults(func=CredentialsProvisioner.provision_hmac_from_args)
    args = parser.parse_args()
    credentials_provisioner = CredentialsProvisioner()
    args.func(credentials_provisioner, args)
