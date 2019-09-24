from contextlib import contextmanager
import json
import logging
import os
import tempfile
import types
from unittest.mock import MagicMock, patch

import boto3

from azul import config

logger = logging.getLogger(__name__)


def patch_client_for_direct_access(client):
    old_get_file = client.get_file
    old_get_bundle = client.get_bundle
    s3 = boto3.client('s3')
    dss_bucket = config.dss_main_bucket

    def new_get_file(self, uuid, replica, version=None):
        assert client is self
        if replica == 'aws' and version is not None:
            file_key = None
            try:
                file_key = f'files/{uuid}.{version}'
                logger.debug('Loading file %s from bucket %s', file_key, dss_bucket)
                file = json.load(s3.get_object(Bucket=dss_bucket, Key=file_key)['Body'])
            except Exception:
                logger.warning('Error accessing file %s in DSS bucket %s directly. '
                               'Falling back to official method.',
                               file_key, dss_bucket, exc_info=True)
            else:
                blob_key = None
                try:
                    content_type, _, rest = file['content-type'].partition(';')
                    assert content_type == 'application/json', content_type
                    blob_key = 'blobs/' + '.'.join(file[k] for k in ('sha256', 'sha1', 's3-etag', 'crc32c'))
                    logger.debug('Loading blob %s from bucket %s', blob_key, dss_bucket)
                    blob = json.load(s3.get_object(Bucket=dss_bucket, Key=blob_key)['Body'])
                except Exception:
                    logger.warning('Error accessing blob %s in DSS bucket %s directly. '
                                   'Falling back to official method.',
                                   blob_key, dss_bucket, exc_info=True)
                else:
                    return blob
        else:
            logger.warning('Conditions to access DSS bucket directly are not met. '
                           'Falling back to official method.')
        return old_get_file(uuid=uuid, version=version, replica=replica)

    class new_get_bundle:

        def _request(self, kwargs, **other_kwargs):
            uuid, version, replica = kwargs['uuid'], kwargs['version'], kwargs['replica']
            if replica == 'aws' and version is not None:
                try:
                    bundle_key = f'bundles/{uuid}.{version}'
                    logger.debug('Loading bundle %s from bucket %s', bundle_key, dss_bucket)
                    bundle = json.load(s3.get_object(Bucket=dss_bucket, Key=bundle_key)['Body'])
                except Exception:
                    logger.warning('Error accessing object %s in DSS bucket %s directly. '
                                   'Falling back to official method.',
                                   bundle_key, dss_bucket, exc_info=True)
                else:
                    # Massage manifest format to match results from dss
                    for f in bundle['files']:
                        f['s3_etag'] = f.pop('s3-etag')
                    mock_response = MagicMock()
                    mock_response.json = lambda: {'bundle': bundle, 'version': version, 'uuid': uuid}
                    mock_response.links.__getitem__.side_effect = KeyError()
                    return mock_response
            else:
                logger.warning('Conditions to access DSS bucket directly are not met. '
                               'Falling back to official method.')
            return old_get_bundle._request(kwargs, **other_kwargs)

    client.get_file = types.MethodType(new_get_file, client)
    client.get_bundle = new_get_bundle()


@contextmanager
def shared_dss_credentials():
    """
    A context manager that patches the process environment so that the DSS client is coaxed into using credentials
    for the Google service account that represents the Azul indexer lambda. This can be handy if a) other Google
    credentials with write access to DSS aren't available or b) you want to act on behalf of the Azul indexer,
    or rather *as* the indexer.
    """
    sm = boto3.client('secretsmanager')
    creds = sm.get_secret_value(SecretId=config.secrets_manager_secret_name('indexer', 'google_service_account'))
    with tempfile.NamedTemporaryFile(mode='w+') as f:
        f.write(creds['SecretString'])
        f.flush()
        with patch.dict(os.environ, GOOGLE_APPLICATION_CREDENTIALS=f.name):
            yield
