import json
import logging
import types

import boto3

from azul import config

logger = logging.getLogger(__name__)


def patch_client_for_direct_file_access(client):
    old_get_file = client.get_file
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
            except BaseException:
                logger.warning('Error accessing DSS bucket directly. '
                               'Falling back to official method.', exc_info=True)
            else:
                return blob
        else:
            logger.warning('Conditions to access DSS bucket directly are not met. '
                           'Falling back to official method.')
        return old_get_file(uuid=uuid, version=version, replica=replica)

    client.get_file = types.MethodType(new_get_file, client)
