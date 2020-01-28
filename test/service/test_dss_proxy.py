import io
import logging
import os
import time
from unittest import mock

import boto3
from chalice.config import Config as ChaliceConfig
from furl import furl
from moto import mock_s3
import requests
import responses

from app_test_case import LocalAppTestCase
from azul import config
from azul.logging import configure_test_logging
from retorts import ResponsesHelper

logger = logging.getLogger(__name__)


def setUpModule():
    configure_test_logging(logger)


# These are the credentials defined in moto.instance_metadata.responses.InstanceMetadataResponse which, for reasons
# yet to be determined, is used on Travis but not when I run this locally. Maybe it's the absence of
# ~/.aws/credentials. The credentials that @mock_sts provides look more realistic but boto3's STS credential provider
# would be skipped on CI because the lack of ~/.aws/credentials there implies that AssumeRole credentials aren't
# configured, causing boto3 to default to use credentials from mock instance metadata.
#
mock_access_key_id = 'test-key'  # @mock_sts uses AKIAIOSFODNN7EXAMPLE
mock_secret_access_key = 'test-secret-key'  # @mock_sts uses wJalrXUtnFEMI/K7MDENG/bPxRfiCYzEXAMPLEKEY
mock_session_token = 'test-session-token'  # @mock_sts token starts with  AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk …


class TestDssProxy(LocalAppTestCase):

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

    @mock.patch.dict(os.environ,
                     AWS_ACCESS_KEY_ID=mock_access_key_id,
                     AWS_SECRET_ACCESS_KEY=mock_secret_access_key,
                     AWS_SESSION_TOKEN=mock_session_token)
    @mock_s3
    def test_dss_files_proxy(self):
        self.maxDiff = None
        key = ("blobs/6929799f227ae5f0b3e0167a6cf2bd683db097848af6ccde6329185212598779"
               ".f2237ad0a776fd7057eb3d3498114c85e2f521d7"
               ".7e892bf8f6aa489ccb08a995c7f017e1."
               "847325b6")
        bucket_name = 'org-humancellatlas-dss-checkout-staging'
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket=bucket_name)
        s3.upload_fileobj(Bucket=bucket_name, Fileobj=io.BytesIO(b'foo'), Key=key)
        file_uuid = '701c9a63-23da-4978-946b-7576b6ad088a'
        file_version = '2018-09-12T121154.054628Z'
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
                for file_name, signature in [(None, 'gZoVAj4HD+6Nb/dgik2M+ihvkzM='),
                                             ('foo.txt', 'Wg8AqCTzZAuHpCN8AKPKWcsFHAM=',),
                                             ('foo bar.txt', 'grbM6udwp0n/QE/L/RYfjtQCS/U='),
                                             ('foo&bar.txt', 'r4C8YxpJ4nXTZh+agBsfhZ2e7fI=')]:
                    with self.subTest(fetch=fetch, file_name=file_name, wait=wait):
                        with ResponsesHelper() as helper:
                            helper.add_passthru(self.base_url)
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
                                                          url=dss_url.url,
                                                          status=301,
                                                          headers={'Location': dss_url_with_token.url,
                                                                   'Retry-After': '10'}))
                            azul_url = furl(
                                url=self.base_url,
                                path='/fetch/dss/files' if fetch else '/dss/files',
                                args={
                                    'replica': 'aws',
                                    'version': file_version
                                }).add(path=file_uuid)
                            if wait is not None:
                                azul_url.args['wait'] = str(wait)
                            if file_name is not None:
                                azul_url.args['fileName'] = file_name

                            def request_azul(url, expect_status):
                                retry_after = 1
                                expect_retry_after = None if wait or expect_status == 302 else retry_after
                                before = time.monotonic()
                                with mock.patch.object(type(config), 'dss_checkout_bucket', return_value=bucket_name):
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

                            location = request_azul(url=azul_url.url, expect_status=301)

                            azul_url.args['token'] = dss_token
                            azul_url.args['requestIndex'] = '1'
                            self.assertUrlEqual(azul_url, location)

                            helper.add(responses.Response(method='GET',
                                                          url=dss_url_with_token.url,
                                                          status=302,
                                                          headers={'Location': s3_url.url}))

                            location = request_azul(url=location, expect_status=302)

                            if file_name is None:
                                file_name = file_uuid
                            re_pre_signed_s3_url = furl(
                                url=f'https://{bucket_name}.s3.amazonaws.com',
                                path=key,
                                args={
                                    'response-content-disposition': f'attachment;filename={file_name}',
                                    'AWSAccessKeyId': mock_access_key_id,
                                    'Signature': signature,
                                    'Expires': expires,
                                    'x-amz-security-token': mock_session_token
                                })
                            self.assertUrlEqual(re_pre_signed_s3_url, location)
