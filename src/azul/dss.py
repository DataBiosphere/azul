import json
import logging
import types
from unittest.mock import MagicMock

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
            try:
                file_key = f'files/{uuid}.{version}'
                logger.debug('Loading file %s from bucket %s', file_key, dss_bucket)
                file = json.load(s3.get_object(Bucket=dss_bucket, Key=file_key)['Body'])
                content_type, _, rest = file['content-type'].partition(';')
                assert content_type == 'application/json', content_type
                blob_key = 'blobs/' + '.'.join(file[k] for k in ('sha256', 'sha1', 's3-etag', 'crc32c'))
                logger.debug('Loading blob %s from bucket %s', blob_key, dss_bucket)
                blob = json.load(s3.get_object(Bucket=dss_bucket, Key=blob_key)['Body'])
            except Exception:
                logger.warning('Error accessing DSS bucket directly. '
                               'Falling back to official method.', exc_info=True)
            else:
                return blob
        else:
            logger.warning('Conditions to access DSS bucket directly are not met. '
                           'Falling back to official method.')
        return old_get_file(uuid=uuid, version=version, replica=replica)

    class new_get_bundle:
        def _request(self, kwargs, **other_kwargs):
            version = kwargs['version']
            uuid = kwargs['uuid']
            try:
                bundle_key = f'bundles/{uuid}.{version}'
                logger.debug('Loading bundle %s from bucket %s', bundle_key, dss_bucket)
                bundle = json.load(s3.get_object(Bucket=dss_bucket, Key=bundle_key)['Body'])
            except Exception:
                logger.warning('Error accessing DSS bucket directly. '
                               'Falling back to official method.', exc_info=True)
            else:
                # Massage manifest format to match results from dss
                for f in bundle['files']:
                    f['s3_etag'] = f.pop('s3-etag')
                mock_response = MagicMock()
                mock_response.json = lambda: {'bundle': bundle, 'version': version, 'uuid': uuid}
                mock_response.links.__getitem__.side_effect = KeyError()
                return mock_response
            return old_get_bundle._request(kwargs, **other_kwargs)

    client.get_file = types.MethodType(new_get_file, client)
    client.get_bundle = new_get_bundle()
