from unittest import mock

from chalice import (
    BadRequestError,
    ChaliceViewError,
)
from moto import (
    mock_s3,
    mock_sts,
)

from azul import config
from azul.logging import configure_test_logging
from azul.service.responseobjects.storage_service import StorageService
from azul_test_case import AzulTestCase
from lambdas.service import app


def setUpModule():
    configure_test_logging()


class TestQueryShortener(AzulTestCase):

    @mock.patch('azul.service.responseobjects.storage_service.StorageService.put')
    @mock.patch('azul.service.responseobjects.storage_service.StorageService.get')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_valid_url(self, current_request, storage_service_get, storage_service_put):
        """
        Passing in a valid url should create an object in s3 and return a link
        that redirects to the given url
        """
        current_request.json_body = {
            'url': 'https://dev.data.humancellatlas.org/explore/specimens'
                   '?filter=%5B%7B%22facetName%22%3A%22organ%22%2C%22terms%22%3A%5B%22bone%22%5D%7D%5D'
        }
        storage_service_get.side_effect = StorageService().client.exceptions.NoSuchKey({}, "")
        response = app.shorten_query_url()
        self.assertEqual({'url': f'http://{config.url_redirect_full_domain_name}/FFq'}, response)
        storage_service_put.assert_called_once()

    @mock_sts
    @mock_s3
    @mock.patch('lambdas.service.app.app.current_request')
    def test_whitelisting(self, current_request):
        """
        URL shortener should accept any humancellatlas domain
        """
        urls = [
            'https://humancellatlas.org',
            'http://humancellatlas.org',
            'https://humancellatlas.org/',
            'https://humancellatlas.org/abc',
            'https://subdomain.humancellatlas.org/',
            'https://sub.subdomain.humancellatlas.org/abc/def'
        ]
        StorageService().create_bucket(config.url_redirect_full_domain_name)
        for i in range(len(urls)):
            with self.subTest(i=i):
                current_request.json_body = {'url': urls[i]}
                app.shorten_query_url()

    @mock_sts
    @mock_s3
    @mock.patch('lambdas.service.app.app.current_request')
    def test_invalid_url(self, current_request):
        """
        URL shortener should reject any non-URL argument and any non-HCA URL
        """
        urls = [
            'https://hmanclatls.org',
            'https://humancellatlas.orgo',
            'http://humancellatlas.xyz.org',
            'humancellatlas.org'
        ]
        StorageService().create_bucket(config.url_redirect_full_domain_name)
        for i in range(len(urls)):
            with self.subTest(i=i):
                current_request.json_body = {'url': urls[i]}
                self.assertRaises(BadRequestError, app.shorten_query_url)

    @mock_sts
    @mock_s3
    @mock.patch('lambdas.service.app.app.current_request')
    def test_shortened_url_matching(self, current_request):
        """
        URL shortener should return the same response URL for identical input URLs
        """
        current_request.json_body = {'url': 'https://humancellatlas.org'}

        StorageService().create_bucket(config.url_redirect_full_domain_name)
        shortened_url1 = app.shorten_query_url()
        shortened_url2 = app.shorten_query_url()
        self.assertEqual(shortened_url1, shortened_url2)

    @mock_sts
    @mock_s3
    @mock.patch('lambdas.service.app.hash_url')
    @mock.patch('lambdas.service.app.app.current_request')
    def test_shortened_url_collision(self, current_request, hash_url):
        """
        URL shortener should increase the key length by one for each time there is a key collision on
        non-matching URLs, raising an exception if an entire key matches another
        """
        hash_url.return_value = 'abcde'
        StorageService().create_bucket(config.url_redirect_full_domain_name)

        current_request.json_body = {'url': 'https://humancellatlas.org'}
        self.assertEqual(app.shorten_query_url()['url'], f'http://{config.url_redirect_full_domain_name}/abc')

        current_request.json_body = {'url': 'https://humancellatlas.org/2'}
        self.assertEqual(app.shorten_query_url()['url'], f'http://{config.url_redirect_full_domain_name}/abcd')

        current_request.json_body = {'url': 'https://humancellatlas.org/3'}
        self.assertEqual(app.shorten_query_url()['url'], f'http://{config.url_redirect_full_domain_name}/abcde')

        current_request.json_body = {'url': 'https://humancellatlas.org/4'}
        self.assertRaises(ChaliceViewError, app.shorten_query_url)
