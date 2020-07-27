from contextlib import (
    contextmanager,
)
from datetime import (
    datetime,
)
import json
import logging
import os
import tempfile
import types
from typing import (
    Mapping,
    NamedTuple,
    Optional,
    Union,
)
from unittest.mock import (
    patch,
)

import boto3
from botocore.response import (
    StreamingBody,
)
from hca.dss import (
    DSSClient,
)
# noinspection PyProtectedMember
from humancellatlas.data.metadata.helpers.dss import (
    _DSSClient,
)
from urllib3 import (
    Timeout,
)

from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


def client(dss_endpoint: Optional[str] = None, num_workers=None) -> DSSClient:
    swagger_url = (dss_endpoint or config.dss_endpoint) + '/swagger.json'
    client = AzulDSSClient(swagger_url=swagger_url, num_workers=num_workers)
    client.timeout_policy = Timeout(connect=10, read=40)
    return client


def direct_access_client(dss_endpoint: Optional[str] = None, num_workers=None) -> DSSClient:
    dss_client = client(dss_endpoint=dss_endpoint, num_workers=num_workers)
    if config.dss_direct_access:
        _patch_client_for_direct_access(dss_client)
    return dss_client


class MiniDSS:

    def __init__(self, dss_endpoint: str, **s3_client_kwargs) -> None:
        super().__init__()
        self.bucket = aws.dss_main_bucket(dss_endpoint)
        with aws.direct_access_credentials(dss_endpoint, lambda_name='indexer'):
            # FIXME: make region configurable (https://github.com/DataBiosphere/azul/issues/1560)
            self.s3 = aws.client('s3', region_name='us-east-1', **s3_client_kwargs)

    def get_bundle(self, uuid: str, version: str, replica: str) -> JSON:
        assert replica == 'aws' and version is not None
        logger.debug('Loading bundle %s, version %s.', uuid, version)
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
        logger.debug('Loading file %s, version %s.', uuid, version)
        file_object = self._get_file_object(uuid, version)
        blob_key = self._get_blob_key(file_object)
        blob = self._get_blob(blob_key, file_object)
        return blob

    def get_native_file_url(self, uuid: str, version: str, replica: str) -> str:
        assert replica == 'aws' and version is not None
        file_object = self._get_file_object(uuid, version)
        blob_key = self._get_blob_key(file_object)
        return f's3://{self.bucket}/{blob_key}'

    class Checksums(NamedTuple):
        # must be in the order they occur in a blob key
        sha256: str
        sha1: str
        s3_etag: str
        crc32c: str

        @classmethod
        def from_blob_key(cls, blob_key: str):
            return cls(*blob_key.split('/')[-1].split('.'))

        def to_tags(self):
            return {f'hca-dss-{k}': v for k, v in self._asdict().items()}

    def retag_blob(self, uuid: str, version: str, replica: str):
        assert replica == 'aws' and version is not None
        logger.debug('Updating checksum tags on blob for file %s, version %s.', uuid, version)
        file_object = self._get_file_object(uuid, version)
        blob_key = self._get_blob_key(file_object)
        checksums = self.Checksums.from_blob_key(blob_key)
        tags = self._get_object_tags(blob_key)
        new_tags = {**tags, **checksums.to_tags()}
        if tags == new_tags:
            logger.debug('Checksum tags on blob for file %s, version %s are already up-to-date.', uuid, version)
        else:
            self._put_object_tags(blob_key, new_tags)

    def _get_object_tags(self, blob_key: str):
        logger.debug('Getting tags for blob %s in bucket %s.', blob_key, self.bucket)
        tagging = self.s3.get_object_tagging(Bucket=self.bucket, Key=blob_key)
        tag_set = tagging['TagSet']
        tags = {tag['Key']: tag['Value'] for tag in tag_set}
        return tags

    def _put_object_tags(self, key: str, tags: Mapping[str, str]):
        logger.debug('Putting tags for object %s in bucket %s.', key, self.bucket)
        new_tag_set = [{'Key': k, 'Value': v} for k, v in tags.items()]
        self.s3.put_object_tagging(Bucket=self.bucket,
                                   Key=key,
                                   Tagging={'TagSet': new_tag_set})

    def _get_file_object(self, uuid: str, version: str) -> JSON:
        file_key = f'files/{uuid}.{version}'
        try:
            return json.load(self._get_object(file_key))
        except Exception as e:
            logger.warning('Error accessing file %s in DSS bucket %s directly.',
                           file_key, self.bucket, exc_info=e)
            raise

    def _get_blob_key(self, file_object: JSON) -> str:
        try:
            return 'blobs/' + '.'.join(file_object[k] for k in ('sha256', 'sha1', 's3-etag', 'crc32c'))
        except Exception as e:
            logger.warning('Error determining blob key from file %r in DSS bucket %s directly.',
                           file_object, self.bucket, exc_info=e)
            raise

    def _get_blob(self, blob_key: str, file_object: JSON) -> Union[JSON, StreamingBody]:
        try:
            blob = self._get_object(blob_key)
            content_type, _, rest = file_object['content-type'].partition(';')
            if content_type == 'application/json':
                blob = json.load(blob)
            return blob
        except Exception as e:
            logger.warning('Error accessing blob %s in DSS bucket %s directly.',
                           blob_key, self.bucket, exc_info=e)
            raise

    def _get_object(self, key: str) -> StreamingBody:
        logger.debug('Loading object %s from bucket %s', key, self.bucket)
        return self.s3.get_object(Bucket=self.bucket, Key=key)['Body']


def _patch_client_for_direct_access(client: DSSClient):
    old_get_file = client.get_file
    old_get_bundle = client.get_bundle
    mini_dss = MiniDSS(config.dss_endpoint)

    def new_get_file(self, uuid, replica, version=None):
        assert client is self
        try:
            blob = mini_dss.get_file(uuid, version, replica)
        except Exception:
            logger.warning('Failed getting file %s, version %s directly. '
                           'Falling back to official method', uuid, version)
            return old_get_file(uuid=uuid, version=version, replica=replica)
        else:
            return blob

    class NewGetBundle:

        def paginate(self, *args, **kwargs):
            uuid, version, replica = kwargs['uuid'], kwargs['version'], kwargs['replica']
            try:
                bundle = mini_dss.get_bundle(uuid, version, replica)
            except Exception:
                logger.warning('Failed getting bundle file %s, version %s directly. '
                               'Falling back to official method', uuid, version)
                return old_get_bundle.paginate(*args, **kwargs)
            else:
                page = {'bundle': bundle, 'version': version, 'uuid': uuid}
                return [page]

    new_get_bundle = NewGetBundle()
    client.get_file = types.MethodType(new_get_file, client)
    client.get_bundle = new_get_bundle


class AzulDSSClient(_DSSClient):
    """
    An DSSClient with Azul-specific extensions and fixes.
    """

    def __init__(self, *args, num_workers: int = None, **kwargs):
        super().__init__(*args,
                         adapter_args=None if num_workers is None else dict(pool_maxsize=num_workers),
                         **kwargs)


@contextmanager
def shared_credentials():
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


version_format = '%Y-%m-%dT%H%M%S.%fZ'


def new_version():
    return datetime.utcnow().strftime(version_format)


def validate_version(version: str):
    """
    >>> validate_version('2018-10-18T150431.370880Z')
    '2018-10-18T150431.370880Z'

    >>> validate_version('2018-10-18T150431.0Z')
    Traceback (most recent call last):
    ...
    ValueError: ('2018-10-18T150431.0Z', '2018-10-18T150431.000000Z')

    >>> validate_version(' 2018-10-18T150431.370880Z')
    Traceback (most recent call last):
    ...
    ValueError: time data ' 2018-10-18T150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'

    >>> validate_version('2018-10-18T150431.370880')
    Traceback (most recent call last):
    ...
    ValueError: time data '2018-10-18T150431.370880' does not match format '%Y-%m-%dT%H%M%S.%fZ'

    >>> validate_version('2018-10-187150431.370880Z')
    Traceback (most recent call last):
    ...
    ValueError: time data '2018-10-187150431.370880Z' does not match format '%Y-%m-%dT%H%M%S.%fZ'
    """
    reparsed_version = datetime.strptime(version, version_format).strftime(version_format)
    if version != reparsed_version:
        raise ValueError(version, reparsed_version)
    return version
