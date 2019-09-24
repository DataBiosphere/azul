from contextlib import contextmanager
import json
import logging
import os
import tempfile
import types
from typing import Mapping, Optional, Any, Union
from unittest.mock import MagicMock, patch

import boto3
from botocore.response import StreamingBody
from hca.dss import DSSClient
from requests import Session

from azul import config
from azul.types import JSON

logger = logging.getLogger(__name__)


class MiniDSS:

    def __init__(self, dss_endpoint=None, **s3_client_kwargs) -> None:
        super().__init__()
        self.s3 = boto3.client('s3', **s3_client_kwargs)
        self.bucket = config.dss_main_bucket(dss_endpoint)

    def get_bundle(self, uuid: str, version: str, replica: str) -> JSON:
        assert replica == 'aws' and version is not None
        logger.debug('Loading bundle %s, version %s from bucket %s', uuid, version)
        bundle_key = f'bundles/{uuid}.{version}'
        try:
            bundle = json.load(self._get_object(bundle_key))
        except Exception:
            logger.warning('Error accessing bundle %s in DSS bucket %s directly.',
                           bundle_key, self.bucket, exc_info=True)
            raise
        # Massage manifest format to match results from DSS
        for f in bundle['files']:
            f['s3_etag'] = f.pop('s3-etag')
        return bundle

    def get_file(self, uuid: str, version: str, replica: str) -> Union[StreamingBody, JSON]:
        assert replica == 'aws' and version is not None
        logger.debug('Loading file %s, version %s', uuid, version)
        file_object = self._get_file_object(uuid, version)
        blob_key = self._get_blob_key(file_object)
        blob = self._get_blob(blob_key, file_object)
        return blob

    def get_native_file_url(self, uuid: str, version: str, replica: str) -> str:
        assert replica == 'aws' and version is not None
        file_object = self._get_file_object(uuid, version)
        blob_key = self._get_blob_key(file_object)
        return f's3://{self.bucket}/{blob_key}'

    def _get_file_object(self, uuid: str, version: str) -> JSON:
        file_key = f'files/{uuid}.{version}'
        try:
            return json.load(self._get_object(file_key))
        except Exception:
            logger.warning('Error accessing file %s in DSS bucket %s directly.',
                           file_key, self.bucket, exc_info=True)
            raise

    def _get_blob_key(self, file_object: JSON) -> str:
        try:
            return 'blobs/' + '.'.join(file_object[k] for k in ('sha256', 'sha1', 's3-etag', 'crc32c'))
        except Exception:
            logger.warning('Error determining blob key from file %r in DSS bucket %s directly.',
                           file_object, self.bucket, exc_info=True)
            raise

    def _get_blob(self, blob_key: str, file_object: JSON) -> Union[JSON, StreamingBody]:
        try:
            blob = self._get_object(blob_key)
            content_type, _, rest = file_object['content-type'].partition(';')
            if content_type == 'application/json':
                blob = json.load(blob)
            return blob
        except Exception:
            logger.warning('Error accessing blob %s in DSS bucket %s directly.',
                           blob_key, self.bucket, exc_info=True)
            raise

    def _get_object(self, key: str) -> StreamingBody:
        logger.debug('Loading object %s from bucket %s', key, self.bucket)
        return self.s3.get_object(Bucket=self.bucket, Key=key)['Body']


def patch_client_for_direct_access(client: DSSClient):
    old_get_file = client.get_file
    old_get_bundle = client.get_bundle
    mini_dss = MiniDSS()

    def new_get_file(self, uuid, replica, version=None):
        assert client is self
        try:
            blob = mini_dss.get_file(uuid, version, replica)
        except Exception:
            logger.warning('Failed getting file %s, version %s directly. '
                           'Falling back to official method')
            return old_get_file(uuid=uuid, version=version, replica=replica)
        else:
            return blob

    class NewGetBundle:
        def _request(self, kwargs, **other_kwargs):
            uuid, version, replica = kwargs['uuid'], kwargs['version'], kwargs['replica']
            try:
                bundle = mini_dss.get_bundle(uuid, version, replica)
                response = MagicMock()
                response.json = lambda: {'bundle': bundle, 'version': version, 'uuid': uuid}
                response.links.__getitem__.side_effect = KeyError()
            except Exception:
                logger.warning('Failed getting bundle file %s, version %s directly. '
                               'Falling back to official method', uuid, version)
                return old_get_bundle._request(kwargs, **other_kwargs)
            else:
                return response

    new_get_bundle = NewGetBundle()
    client.get_file = types.MethodType(new_get_file, client)
    client.get_bundle = new_get_bundle


class AzulDSSClient(DSSClient):
    """
    An DSSClient with Azul-specific extensions and fixes.
    """

    def __init__(self, *args, adapter_args: Optional[Mapping[str, Any]] = None, **kwargs):
        """
        Pass adapter_args=dict(pool_maxsize=self.num_workers) in order to avoid the resource warnings

        :param args: positional arguments to pass to DSSClient constructor
        :param adapter_args: optional keyword arguments to request's HTTPAdapter class
        :param kwargs: keyword arguments to pass to DSSClient constructor
        """
        self._adapter_args = adapter_args  # yes, this must come first
        super().__init__(*args, **kwargs)

    def _set_retry_policy(self, session: Session):
        if self._adapter_args is None:
            super()._set_retry_policy(session)
        else:
            from requests.sessions import HTTPAdapter

            class MyHTTPAdapter(HTTPAdapter):

                # noinspection PyMethodParameters
                def __init__(self_, *args, **kwargs):
                    kwargs.update(self._adapter_args)
                    super().__init__(*args, **kwargs)

            with patch('hca.util.HTTPAdapter', new=MyHTTPAdapter):
                super()._set_retry_policy(session)


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
