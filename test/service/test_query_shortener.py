from unittest import (
    mock,
)

from moto import (
    mock_s3,
    mock_sts,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    config,
)
from azul.logging import (
    configure_test_logging,
)
from azul.service.storage_service import (
    StorageService,
)
from retorts import (
    ResponsesHelper,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestQueryShortener(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def _shorten_query_url(self, url, expect_status=None):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            response = requests.post(self.base_url + '/url', json={'url': url})
            if expect_status is None:
                response.raise_for_status()
            else:
                self.assertEqual(response.status_code, expect_status)
            return response.json()

    @mock_sts
    @mock_s3
    @mock.patch('azul.service.storage_service.StorageService.put')
    @mock.patch('azul.service.storage_service.StorageService.get')
    def test_valid_url(self, storage_service_get, storage_service_put):
        """
        Passing in a valid url should create an object in s3 and return a link
        that redirects to the given url
        """
        storage_service_get.side_effect = StorageService().client.exceptions.NoSuchKey({}, "")
        response = self._shorten_query_url(
            'https://dev.singlecell.gi.ucsc.edu/explore/specimens'
            '?filter=%5B%7B%22facetName%22%3A%22organ%22%2C%22terms%22%3A%5B%22bone%22%5D%7D%5D'
        )
        self.assertEqual({'url': f'http://{config.url_redirect_full_domain_name}/pv9'}, response)
        storage_service_put.assert_called_once()

    @mock_sts
    @mock_s3
    def test_whitelisting(self):
        """
        URL shortener should accept any humancellatlas domain
        """
        urls = [
            'https://singlecell.gi.ucsc.edu',
            'http://singlecell.gi.ucsc.edu',
            'https://singlecell.gi.ucsc.edu/',
            'https://singlecell.gi.ucsc.edu/abc',
            'https://subdomain.singlecell.gi.ucsc.edu/',
            'https://sub.subdomain.singlecell.gi.ucsc.edu/abc/def'
        ]
        StorageService().create_bucket(config.url_redirect_full_domain_name)
        for url in urls:
            with self.subTest(url=url):
                self._shorten_query_url(url)

    @mock_sts
    @mock_s3
    def test_invalid_url(self):
        """
        URL shortener should reject any non-URL argument and any non-HCA URL
        """
        urls = [
            'https://asinglecell.gi.ucsc.edu',
            'https://singlecll.gi.ucsc.edu',
            'https://singlecell.gi.ucsc.edut',
            'http://singlecell.gi.xyz.edu',
            'singlecell.gi.ucsc.edu'
        ]
        StorageService().create_bucket(config.url_redirect_full_domain_name)
        for url in urls:
            with self.subTest(url=url):
                self._shorten_query_url(url, expect_status=400)

    @mock_sts
    @mock_s3
    def test_shortened_url_matching(self):
        """
        URL shortener should return the same response URL for identical input URLs
        """
        url = 'https://singlecell.gi.ucsc.edu'
        StorageService().create_bucket(config.url_redirect_full_domain_name)
        shortened_url1 = self._shorten_query_url(url)
        shortened_url2 = self._shorten_query_url(url)
        self.assertEqual(shortened_url1, shortened_url2)

    @mock_sts
    @mock_s3
    def test_shortened_url_collision(self):
        """
        URL shortener should increase the key length by one for each time there is a key collision on
        non-matching URLs, raising an exception if an entire key matches another
        """
        with mock.patch.object(self.app_module, 'hash_url') as hash_url:
            hash_url.return_value = 'abcde'
            StorageService().create_bucket(config.url_redirect_full_domain_name)

            self.assertEqual(self._shorten_query_url('https://singlecell.gi.ucsc.edu')['url'],
                             f'http://{config.url_redirect_full_domain_name}/abc')

            self.assertEqual(self._shorten_query_url('https://singlecell.gi.ucsc.edu/2')['url'],
                             f'http://{config.url_redirect_full_domain_name}/abcd')

            self.assertEqual(self._shorten_query_url('https://singlecell.gi.ucsc.edu/3')['url'],
                             f'http://{config.url_redirect_full_domain_name}/abcde')

            self._shorten_query_url('https://singlecell.gi.ucsc.edu/4', expect_status=500)
