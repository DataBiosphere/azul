from abc import (
    ABCMeta,
)
import hashlib
import io
import json
import time
from typing import (
    Union,
)
from unittest.mock import (
    MagicMock,
    Mock,
    patch,
)

import attr
import certifi
from chalice.config import (
    Config as ChaliceConfig,
)
from furl import (
    furl,
)
from google.auth.transport.urllib3 import (
    AuthorizedHttp,
)
import urllib3

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.deployment import (
    aws,
)
from azul.drs import (
    Access,
    AccessMethod,
    DRSObject,
)
from azul.http import (
    http_client,
    raise_on_status,
)
from azul.indexer.mirror_service import (
    MirrorService,
    MirrorWorkerService,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.metadata.hca import (
    HCAFile,
)
from azul.plugins.repository.dss import (
    DSSFileDownload,
)
from azul.service.index_service import (
    IndexService,
)
from azul.terra import (
    TerraClient,
)
from azul_test_case import (
    DCP1TestCase,
    DCP2TestCase,
)
from service import (
    MirrorTestCase,
    S3TestCase,
)
from urllib3_mock import (
    Urllib3Mock,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class RepositoryFilesTestCase(LocalAppTestCase, metaclass=ABCMeta):

    @classmethod
    def app_name(cls) -> str:
        return 'service'

    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    def assertUrlEqual(self, a: Union[str, furl], b: Union[str, furl]):
        if isinstance(a, str):
            a = furl(a)
        if isinstance(b, str):
            b = furl(b)
        self.assertEqual(a.scheme, b.scheme)
        self.assertEqual(a.username, b.username)
        self.assertEqual(a.password, b.password)
        self.assertEqual(a.host, b.host)
        self.assertEqual(a.port, b.port)
        self.assertEqual(a.path, b.path)
        self.assertEqual(sorted(a.args.allitems()), sorted(b.args.allitems()))


class TestRepositoryFilesWithTDR(DCP2TestCase, RepositoryFilesTestCase):

    @patch.object(MirrorService, '_info_exists', new=Mock(return_value=False))
    @patch.object(TerraClient,
                  '_http_client',
                  AuthorizedHttp(MagicMock(),
                                 urllib3.PoolManager(ca_certs=certifi.where())))
    def test(self):
        client = http_client(log)

        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T12:11:54.054628Z'
        organic_file_name = 'foo.txt'
        drs_path_id = 'v1_c99baa6f-24ce-4837-8c4a-47ca4ec9d292_b967ecc9-98b2-43c6-8bac-28c0a4fa7812'
        drs_uri = f'drs://{self._drs_domain_name}/{drs_path_id}'
        file = HCAFile(uuid=file_uuid,
                       name=organic_file_name,
                       version=file_version,
                       drs_uri=drs_uri,
                       size=1,
                       source=self.source.ref,
                       content_type='text/plain',
                       sha256='123',
                       crc32c='abc')
        for fetch in True, False:
            with self.subTest(fetch=fetch):
                with patch.object(IndexService,
                                  'get_data_file',
                                  return_value=file):
                    azul_url = self.base_url.set(path=['repository', 'files', file_uuid],
                                                 args=dict(catalog=self.catalog, version=file_version))
                    if fetch:
                        azul_url.path.segments.insert(0, 'fetch')

                    file_name = 'foo.gz'
                    gs_bucket_name = 'gringotts-wizarding-bank'
                    gs_drs_id = 'some_dataset_id/some_object_id'
                    gs_file_url = f'gs://{gs_bucket_name}/{gs_drs_id}/{file_name}'

                    pre_signed_gs = furl(url=gs_file_url,
                                         args={
                                             'X-Goog-Algorithm': 'SOMEALGORITHM',
                                             'X-Goog-Credential': 'SOMECREDENTIAL',
                                             'X-Goog-Date': 'CURRENTDATE',
                                             'X-Goog-Expires': '900',
                                             'X-Goog-SignedHeaders': 'host',
                                             'X-Goog-Signature': 'SOMESIGNATURE',
                                         })
                    access = Access(method=AccessMethod.https, url=str(pre_signed_gs))
                    with patch.object(DRSObject, 'get', return_value=access):
                        response = client.request('GET', str(azul_url), redirect=False)
                        self.assertEqual(200 if fetch else 302, response.status)
                        if fetch:
                            response = json.loads(response.data)
                            self.assertUrlEqual(pre_signed_gs, response['Location'])
                            self.assertEqual(302, response['Status'])
                        else:
                            response = dict(response.headers)
                            self.assertUrlEqual(pre_signed_gs, response['Location'])

        file = attr.evolve(file, drs_uri=None)
        with self.subTest('phantom'):
            with patch.object(IndexService,
                              'get_data_file',
                              return_value=file):
                response = client.request('GET', str(azul_url), redirect=False)
            self.assertEqual(response.status, 404)


class TestRepositoryFilesWithDSS(DCP1TestCase,
                                 RepositoryFilesTestCase,
                                 S3TestCase):

    @patch.object(MirrorService, '_info_exists', new=Mock(return_value=False))
    @patch.object(type(config), 'dss_direct_access_role', new=Mock(return_value=None))
    def test(self):
        self.maxDiff = None
        key = ('blobs/6929799f227ae5f0b3e0167a6cf2bd683db097848af6ccde6329185212598779'
               '.f2237ad0a776fd7057eb3d3498114c85e2f521d7'
               '.7e892bf8f6aa489ccb08a995c7f017e1.'
               '847325b6')
        bucket_name = 'org-humancellatlas-dss-checkout-staging'
        self._create_test_bucket(bucket_name)
        self._s3.upload_fileobj(Bucket=bucket_name,
                                Fileobj=io.BytesIO(b'foo'),
                                Key=key)
        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T12:11:54.054628Z'
        organic_file_name = 'foo.txt'
        file = HCAFile(uuid=file_uuid,
                       name=organic_file_name,
                       version=file_version,
                       drs_uri=f'drs://{self._drs_domain_name}/{file_uuid}?version={file_version}',
                       size=3,
                       source=self.source.ref,
                       content_type='text/plain',
                       sha256='123',
                       crc32c='abc')
        with patch.object(IndexService, 'get_data_file', return_value=file):
            args = {
                'replica': 'aws',
                'version': file_version
            }
            dss_url = furl(url=config.dss_endpoint,
                           path=('v1', 'files', file_uuid),
                           args=args)
            dss_token = 'some_token'
            dss_url_with_token = dss_url.copy().add(args={'token': dss_token})
            for fetch in True, False:
                for wait in None, 0, 1:
                    for file_name, signature in [(None, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM='),
                                                 (organic_file_name, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM=',),
                                                 ('foo bar.txt', 'grbM6udwp0n/QE/L/RYfjtQCS/U='),
                                                 ('foo&bar.txt', 'r4C8YxpJ4nXTZh+agBsfhZ2e7fI=')]:
                        with self.subTest(fetch=fetch, file_name=file_name, wait=wait):
                            with Urllib3Mock(DSSFileDownload) as helper:
                                fixed_time = 1547691253.07010
                                expires = str(round(fixed_time + 3600))
                                s3_url = furl(url=f'https://{bucket_name}.s3.amazonaws.com',
                                              path=key,
                                              args={
                                                  'AWSAccessKeyId': 'SOMEACCESSKEY',
                                                  'Signature': 'SOMESIGNATURE=',
                                                  'x-amz-security-token': 'SOMETOKEN',
                                                  'Expires': expires
                                              })
                                helper.add(method='GET',
                                           url=str(dss_url),
                                           status=301,
                                           headers={
                                               'Location': str(dss_url_with_token),
                                               'Retry-After': '10'
                                           })
                                azul_url = self.base_url.set(path=['repository', 'files', file_uuid],
                                                             args=dict(catalog=self.catalog, version=file_version))
                                if fetch:
                                    azul_url.path.segments.insert(0, 'fetch')
                                if wait is not None:
                                    azul_url.args['wait'] = str(wait)
                                if file_name is not None:
                                    azul_url.args['fileName'] = file_name

                                def request_azul(url, expect_status):
                                    retry_after = 1
                                    expect_retry_after = None if wait or expect_status == 302 else retry_after
                                    before = time.monotonic()
                                    with patch.object(type(aws), 'dss_checkout_bucket', return_value=bucket_name):
                                        with patch('time.time', new=lambda: 1547691253.07010):
                                            response = self._http_client.request('GET', url, redirect=False)
                                    if wait and expect_status == 301:
                                        self.assertLess(retry_after, time.monotonic() - before)
                                    if fetch:
                                        self.assertEqual(200, response.status)
                                        response = response.json()
                                        self.assertEqual(expect_status, response['Status'])
                                    else:
                                        if response.status != expect_status:
                                            raise_on_status(response)
                                        response = dict(response.headers)
                                    if expect_retry_after is None:
                                        self.assertNotIn('Retry-After', response)
                                    else:
                                        actual_retry_after = response['Retry-After']
                                        if fetch:
                                            self.assertEqual(expect_retry_after, actual_retry_after)
                                        else:
                                            self.assertEqual(str(expect_retry_after), actual_retry_after)
                                    return response['Location']

                                location = request_azul(url=str(azul_url), expect_status=301)

                                if file_name is None:
                                    file_name = organic_file_name

                                azul_url.args['token'] = dss_token
                                azul_url.args['requestIndex'] = '1'
                                azul_url.args['fileName'] = file_name
                                azul_url.args['replica'] = 'aws'
                                azul_url.args['sha256'] = file.sha256
                                self.assertUrlEqual(azul_url, location)

                                helper.add(method='GET',
                                           url=str(dss_url_with_token),
                                           status=302,
                                           headers={'Location': str(s3_url)})

                                location = request_azul(url=location, expect_status=302)

                                args = {
                                    'response-content-disposition': f'attachment;filename={file_name}',
                                    'AWSAccessKeyId': self.mock_boto_credentials.access_key,
                                    'Signature': signature,
                                    'Expires': expires,
                                    'x-amz-security-token': self.mock_boto_credentials.token
                                }
                                re_pre_signed_s3_url = furl(url=f'https://{bucket_name}.s3.amazonaws.com',
                                                            path=key,
                                                            args=args)
                                self.assertUrlEqual(re_pre_signed_s3_url, location)


class TestRepositoryFilesWithMirroring(DCP2TestCase,
                                       RepositoryFilesTestCase,
                                       MirrorTestCase):

    def test(self):
        file_content = b'Contents of foo'
        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T12:11:54.054628Z'
        organic_file_name = 'foo.txt'
        file = HCAFile(uuid=file_uuid,
                       name=organic_file_name,
                       version=file_version,
                       drs_uri=None,
                       size=len(file_content),
                       content_type='text/plain',
                       source=self.source.ref,
                       sha256=hashlib.sha256(file_content).hexdigest(),
                       crc32c=None)

        mirror_service = MirrorWorkerService(catalog=self.catalog,
                                             schema_url_func=MagicMock())
        with patch.object(MirrorWorkerService, '_download', return_value=file_content):
            mirror_service._mirror_file(file)
        self.assertTrue(mirror_service._info_exists(file))

        client = http_client(log)
        args = dict(catalog=self.catalog, version=file_version)
        azul_url = self.base_url.set(path=['repository', 'files', file_uuid], args=args)
        with patch.object(IndexService, 'get_data_file', return_value=file):
            response = client.request('GET', str(azul_url), redirect=False)
        self.assertEqual(302, response.status)

        signed_url = furl(response.headers['Location'])
        self.assertEqual('https', signed_url.scheme)
        self.assertEqual(f'{self.mirror_bucket}.s3.{config.region}.amazonaws.com',
                         signed_url.netloc)
        self.assertEqual('/' + mirror_service._file_object_key(file),
                         str(signed_url.path))
        self.assertEqual(f'attachment;filename="{file.name}"',
                         signed_url.args.get('response-content-disposition'))
