import io
import json
import logging
import os
import time
from unittest import (
    mock,
)
from unittest.mock import (
    MagicMock,
)

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
from moto import (
    mock_s3,
)
import requests
import responses
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
    DRSClient,
)
from azul.http import (
    http_client,
)
from azul.logging import (
    configure_test_logging,
)
from azul.service.repository_service import (
    RepositoryService,
)
from azul.service.source_service import (
    NotFound,
    SourceService,
)
from azul.terra import (
    TDRSourceSpec,
    TerraClient,
)
from azul.types import (
    JSON,
)
from service import (
    DSSUnitTestCase,
    patch_source_cache,
)

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(logger)


class RepositoryPluginTestCase(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    def chalice_config(self):
        return ChaliceConfig.create(lambda_timeout=15)

    def assertUrlEqual(self, a: furl, b: furl):
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


@mock.patch.object(SourceService, '_put', new=MagicMock())
@mock.patch.object(SourceService, '_get')
class TestTDRRepositoryProxy(RepositoryPluginTestCase):
    catalog = 'testtdr'
    catalog_config = {
        catalog: config.Catalog(name=catalog,
                                atlas='hca',
                                internal=False,
                                plugins=dict(metadata=config.Catalog.Plugin(name='hca'),
                                             repository=config.Catalog.Plugin(name='tdr')))
    }

    mock_service_url = f'https://serpentine.datarepo-dev.broadinstitute.net.test.{config.domain_name}'
    mock_source_names = ['mock_snapshot_1', 'mock_snapshot_2']
    make_mock_source_spec = 'tdr:mock:snapshot/{}:'.format
    mock_source_specs = ','.join(map(make_mock_source_spec, mock_source_names))

    @mock.patch.dict(os.environ,
                     AZUL_TDR_SERVICE_URL=mock_service_url,
                     AZUL_TDR_SOURCES=mock_source_specs)
    @mock.patch.object(TerraClient,
                       '_http_client',
                       AuthorizedHttp(MagicMock(),
                                      urllib3.PoolManager(ca_certs=certifi.where())))
    def test_repository_files_proxy(self, mock_get_cached_sources):
        mock_get_cached_sources.return_value = []
        client = http_client()

        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T121154.054628Z'
        organic_file_name = 'foo.txt'
        drs_path_id = 'v1_c99baa6f-24ce-4837-8c4a-47ca4ec9d292_b967ecc9-98b2-43c6-8bac-28c0a4fa7812'
        file_doc = {
            'name': organic_file_name,
            'version': file_version,
            'drs_path': drs_path_id,
            'size': 1,
        }
        for fetch in True, False:
            with self.subTest(fetch=fetch):
                with mock.patch.object(RepositoryService,
                                       'get_data_file',
                                       return_value=file_doc):
                    azul_url = self.base_url.set(path=['repository', 'files', file_uuid],
                                                 args=dict(catalog=self.catalog, version=file_version))
                    if fetch:
                        azul_url.path.segments.insert(0, 'fetch')

                    file_name = 'foo.gz'
                    gs_bucket_name = 'gringotts-wizarding-bank'
                    gs_drs_id = 'some_dataset_id/some_object_id'
                    gs_file_url = f'gs://{gs_bucket_name}/{gs_drs_id}/{file_name}'

                    pre_signed_gs = furl(
                        url=gs_file_url,
                        args={
                            'X-Goog-Algorithm': 'SOMEALGORITHM',
                            'X-Goog-Credential': 'SOMECREDENTIAL',
                            'X-Goog-Date': 'CURRENTDATE',
                            'X-Goog-Expires': '900',
                            'X-Goog-SignedHeaders': 'host',
                            'X-Goog-Signature': 'SOMESIGNATURE',
                        })
                    with mock.patch.object(DRSClient,
                                           'get_object',
                                           return_value=Access(method=AccessMethod.https,
                                                               url=str(pre_signed_gs))):
                        response = client.request('GET', str(azul_url), redirect=False)
                        self.assertEqual(200 if fetch else 302, response.status)
                        if fetch:
                            response = json.loads(response.data)
                            self.assertUrlEqual(pre_signed_gs, response['Location'])
                            self.assertEqual(302, response["Status"])
                        else:
                            response = dict(response.headers)
                            self.assertUrlEqual(pre_signed_gs, response['Location'])

    @mock.patch.dict(os.environ,
                     {f'AZUL_TDR_{catalog.upper()}_SOURCES': mock_source_specs})
    def test_list_sources(self,
                          mock_get_cached_sources,
                          ):
        # Includes extra sources to check that the endpoint only returns results
        # for the current catalog
        extra_sources = ['foo', 'bar']
        mock_source_names_by_id = {
            str(i): source_name
            for i, source_name in enumerate(self.mock_source_names + extra_sources)
        }
        mock_source_jsons = [
            {
                'id': id,
                'spec': str(TDRSourceSpec.parse(self.make_mock_source_spec(name)).effective)
            }
            for id, name in mock_source_names_by_id.items()
            if name not in extra_sources
        ]
        client = http_client()
        azul_url = furl(self.base_url,
                        path='/repository/sources',
                        query_params=dict(catalog=self.catalog))

        def _list_sources(headers) -> JSON:
            response = client.request('GET',
                                      azul_url.url,
                                      headers=headers)
            self.assertEqual(response.status, 200)
            return json.loads(response.data)

        def _test(*, authenticate: bool, cache: bool):
            with self.subTest(authenticate=authenticate, cache=cache):
                response = _list_sources({'Authorization': 'Bearer foo_token'}
                                         if authenticate else {})
                self.assertEqual(response, {
                    'sources': [
                        {
                            'sourceId': source['id'],
                            'sourceSpec': source['spec']
                        }
                        for source in mock_source_jsons
                    ]
                })

        mock_get_cached_sources.return_value = mock_source_jsons
        _test(authenticate=True, cache=True)
        _test(authenticate=False, cache=True)
        mock_get_cached_sources.return_value = None
        mock_get_cached_sources.side_effect = NotFound('foo_token')
        with mock.patch('azul.terra.TDRClient.snapshot_names_by_id',
                        return_value=mock_source_names_by_id):
            _test(authenticate=True, cache=False)
            _test(authenticate=False, cache=False)


class TestDSSRepositoryProxy(RepositoryPluginTestCase, DSSUnitTestCase):
    # These are the credentials defined in
    #
    # moto.instance_metadata.responses.InstanceMetadataResponse
    #
    # which, for reasons yet to be determined, is used on Travis but not when I
    # run this locally. Maybe it's the absence of ~/.aws/credentials. The
    # credentials that @mock_sts provides look more realistic but boto3's STS
    # credential provider would be skipped on CI because the lack of
    # ~/.aws/credentials there implies that AssumeRole credentials aren't
    # configured, causing boto3 to default to use credentials from mock instance
    # metadata.
    #
    mock_access_key_id = 'test-key'  # @mock_sts uses AKIAIOSFODNN7EXAMPLE
    mock_secret_access_key = 'test-secret-key'  # @mock_sts uses wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY
    mock_session_token = 'test-session-token'  # @mock_sts token starts with AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk

    @mock.patch.dict(os.environ,
                     AWS_ACCESS_KEY_ID=mock_access_key_id,
                     AWS_SECRET_ACCESS_KEY=mock_secret_access_key,
                     AWS_SESSION_TOKEN=mock_session_token)
    @mock.patch.object(type(config), 'dss_direct_access_role')
    @mock_s3
    @patch_source_cache
    def test_repository_files_proxy(self, dss_direct_access_role):
        dss_direct_access_role.return_value = None
        self.maxDiff = None
        key = ("blobs/6929799f227ae5f0b3e0167a6cf2bd683db097848af6ccde6329185212598779"
               ".f2237ad0a776fd7057eb3d3498114c85e2f521d7"
               ".7e892bf8f6aa489ccb08a995c7f017e1."
               "847325b6")
        bucket_name = 'org-humancellatlas-dss-checkout-staging'
        s3 = aws.client('s3')
        s3.create_bucket(Bucket=bucket_name,
                         CreateBucketConfiguration={
                             'LocationConstraint': config.region
                         })
        s3.upload_fileobj(Bucket=bucket_name, Fileobj=io.BytesIO(b'foo'), Key=key)
        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T121154.054628Z'
        organic_file_name = 'foo.txt'
        file_doc = {
            'name': organic_file_name,
            'version': file_version,
            'drs_path': None,
            'size': 3,
        }
        with mock.patch.object(RepositoryService, 'get_data_file', return_value=file_doc):
            dss_url = furl(
                url=config.dss_endpoint,
                path='/v1/files',
                args={
                    'replica': 'aws',
                    'version': file_version
                }).add(path=file_uuid)
            dss_token = 'some_token'
            dss_url_with_token = dss_url.copy().add(args={'token': dss_token})
            for fetch in True, False:
                for wait in None, 0, 1:
                    for file_name, signature in [(None, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM='),
                                                 (organic_file_name, 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM=',),
                                                 ('foo bar.txt', 'grbM6udwp0n/QE/L/RYfjtQCS/U='),
                                                 ('foo&bar.txt', 'r4C8YxpJ4nXTZh+agBsfhZ2e7fI=')]:
                        with self.subTest(fetch=fetch, file_name=file_name, wait=wait):
                            with responses.RequestsMock() as helper:
                                helper.add_passthru(str(self.base_url))
                                fixed_time = 1547691253.07010
                                expires = str(round(fixed_time + 3600))
                                s3_url = furl(
                                    url=f'https://{bucket_name}.s3.amazonaws.com',
                                    path=key,
                                    args={
                                        'AWSAccessKeyId': 'SOMEACCESSKEY',
                                        'Signature': 'SOMESIGNATURE=',
                                        'x-amz-security-token': 'SOMETOKEN',
                                        'Expires': expires
                                    })
                                helper.add(responses.Response(method='GET',
                                                              url=str(dss_url),
                                                              status=301,
                                                              headers={'Location': str(dss_url_with_token),
                                                                       'Retry-After': '10'}))
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
                                    with mock.patch.object(type(aws), 'dss_checkout_bucket', return_value=bucket_name):
                                        with mock.patch('time.time', new=lambda: 1547691253.07010):
                                            response = requests.get(url, allow_redirects=False)
                                    if wait and expect_status == 301:
                                        self.assertLessEqual(retry_after, time.monotonic() - before)
                                    if fetch:
                                        self.assertEqual(200, response.status_code)
                                        response = response.json()
                                        self.assertEqual(expect_status, response["Status"])
                                    else:
                                        if response.status_code != expect_status:
                                            response.raise_for_status()
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
                                self.assertUrlEqual(azul_url, location)

                                helper.add(responses.Response(method='GET',
                                                              url=str(dss_url_with_token),
                                                              status=302,
                                                              headers={'Location': str(s3_url)}))

                                location = request_azul(url=location, expect_status=302)

                                re_pre_signed_s3_url = furl(
                                    url=f'https://{bucket_name}.s3.amazonaws.com',
                                    path=key,
                                    args={
                                        'response-content-disposition': f'attachment;filename={file_name}',
                                        'AWSAccessKeyId': self.mock_access_key_id,
                                        'Signature': signature,
                                        'Expires': expires,
                                        'x-amz-security-token': self.mock_session_token
                                    })
                                self.assertUrlEqual(re_pre_signed_s3_url, location)
