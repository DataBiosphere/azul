import json
import unittest
from urllib import parse

import requests

from azul.logging import configure_test_logging
from service import WebServiceTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class PaginationTestCase(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()
        cls._fill_index()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def get_base_url(self):
        return self.base_url + '/index/files'

    def assert_page1_correct(self, json_response):
        """
        Helper function that asserts the given response is correct for the first page.
        :param json_response: A JSON response dictionary
        :return:
        """
        if 'search_before' in json_response['pagination']:
            self.assertIsNone(json_response['pagination']['search_before'],
                              "search_before != null on first page of results")
            self.assertIsNone(json_response['pagination']['search_before_uid'],
                              "search_before != null on first page of results")
        else:
            self.fail("search_before not set to null on first page of results")

        num_hits = len(json_response['hits'])
        if 'search_after' in json_response['pagination']:
            self.assertEqual(json_response['pagination']['search_after'],
                             json.dumps(json_response['hits'][num_hits - 1]['entryId']),
                             "search_after not set to last returned document on first page")
            self.assertIsNotNone(json_response['pagination']['search_after_uid'])
        else:
            self.fail("search_after not set on first page of results")

    def assert_page2_correct(self, json_response, json_response_second, sort_order):
        """
        Helper function that asserts the given response is correct for the second page.
        :param json_response: A JSON response dictionary for the first page
        :param json_response_second: A JSON response dictionary for the second page
        :param sort_order: A string that will be appended to any error messages to indicate the sort order.
        :return:
        """
        num_hits_first = len(json_response['hits'])
        num_hits_second = len(json_response_second['hits'])
        if 'search_before' in json_response_second['pagination']:
            self.assertEqual(json_response_second['pagination']['search_before'],
                             json.dumps(json_response_second['hits'][0]['entryId']),
                             "search_before on second page not set to first returned document on second page, "
                             + "order=" + sort_order)
            self.assertIsNotNone(json_response_second['pagination']['search_before_uid'],
                                 "No search_before_uid returned on second page")
        else:
            self.fail("search_before not set on second page of results, sortOrder=" + sort_order)

        if 'search_after' in json_response['pagination']:
            self.assertEqual(json_response_second['pagination']['search_after'],
                             json.dumps(json_response_second['hits'][num_hits_second - 1]['entryId']),
                             "search_after on second page not set to last returned document on second page, "
                             + "order=" + sort_order)
            self.assertIsNotNone(json_response['pagination']['search_after_uid'],
                                 "No search_after_uid returned on second page")
        else:
            self.fail("search_after not set on second page of results, sortOrder=" + sort_order)

        self.assertNotEqual(json_response['hits'][0], json_response_second['hits'][0],
                            "first hit of first page is the same as first hit of second page")
        self.assertNotEqual(json_response['hits'][num_hits_first - 1], json_response['hits'][0],
                            "last hit of first page is the same as first hit of second page")

    def assert_pagination_navigation(self, request_params, pagination):
        """
        Helper function that asserts all the parameters used in the original
        request are included in the 'next' and 'previous' pagination urls
        """
        urls_checked = 0
        for page in ('next', 'previous'):
            page_url = pagination.get(page)
            if page_url:
                urls_checked += 1
                page_params = self._parse_url_qs(page_url)
                for param in request_params:
                    self.assertIn(param, page_params)
                    self.assertEqual(request_params[param], page_params[param])
        self.assertGreater(urls_checked, 0)

    @classmethod
    def _parse_url_qs(cls, url):
        url_parts = parse.urlparse(url)
        query_dict = dict(parse.parse_qsl(url_parts.query, keep_blank_values=True))
        return query_dict

    def test_search_after_page1(self):
        """
        Tests that search_after pagination works for the first returned page.
        :return:
        """
        content = requests.get(self.get_base_url() + '?sort=entryId&order=desc').content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

    def test_search_after_page1_explicit_from(self):
        """
        Tests that response contains information to enable search_after pagination, even if the user explicitly
        passes from and size variables.
        :return:
        """
        content = requests.get(self.get_base_url() + '?sort=entryId&size=10&order=desc').content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

    def test_search_after_page2(self):
        """
        Tests that the second page returned in search_after pagination mode is correct.
        :return:
        """
        # Fetch and check first page.
        content = requests.get(self.get_base_url() + '?sort=entryId&order=desc').content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

        # Fetch the second page using search_after
        search_after = json_response['pagination']['search_after']
        search_after_uid = json_response['pagination']['search_after_uid']
        content = requests.get(self.get_base_url() +
                               f'?sort=entryId'
                               f'&order=desc'
                               f'&search_after={search_after}'
                               f'&search_after_uid={search_after_uid}').content
        json_response_second = json.loads(content)
        self.assert_page2_correct(json_response, json_response_second, "desc")

    def test_search_after_last_page(self):
        """
        Tests that the last page returned in search_after pagination mode is correct.
        :return:
        """
        content = requests.get(self.get_base_url() + '?sort=entryId&order=asc').content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)
        # Store the search_after for the last result of the first page.
        search_after_lrfp = json_response['pagination']['search_after']
        search_after_lrfp_uid = json_response['pagination']['search_after_uid']
        content = requests.get(self.get_base_url() +
                               f'?sort=entryId'
                               f'&order=asc'
                               f'&search_after={search_after_lrfp}'
                               f'&search_after_uid={search_after_lrfp_uid}').content
        json_response_second = json.loads(content)
        self.assert_page2_correct(json_response, json_response_second, "asc")

        search_after = json_response_second['pagination']['search_before']
        search_after_uid = json_response_second['pagination']['search_before_uid']

        content = requests.get(self.get_base_url() +
                               f'?sort=entryId'
                               f'&order=desc'
                               f'&search_after={search_after}'
                               f'&search_after_uid={search_after_uid}'
                               f'&order=desc').content
        json_response = json.loads(content)
        if 'search_before' in json_response['pagination']:
            self.assertEqual(json_response['pagination']['search_before'], search_after_lrfp,
                             "search_before on last page is not set correctly")
            self.assertIsNotNone(json_response['pagination']['search_before_uid'],
                                 "search_before_uid on last page is not set")
        else:
            self.fail("search_before not set on last page of results")

        if 'search_after' in json_response['pagination']:
            self.assertIsNone(json_response['pagination']['search_after'],
                              "search_after is not null on last page")
            self.assertIsNone(json_response['pagination']['search_after_uid'],
                              "search_after_uid is not null on last page")
        else:
            self.fail("search_after not set to [] on first page of results")

    def test_next_and_previous_page_links(self):
        """
        Test that the next and previous links in pages are correct.
        :return:
        """
        genus_species = None
        # On first pass verify pagination with no filters, then pull a value out
        # of termFacets to use in the second pass with a filter parameter.
        for use_filters in (False, True):
            with self.subTest(use_filters=use_filters):
                params = {
                    'sort': 'entryId',
                    'order': 'desc',
                }
                if use_filters:
                    filters = {
                        'genusSpecies': {
                            'is': [genus_species]
                        }
                    }
                    params.update({'filters': json.dumps(filters)})

                # Fetch and check first page.
                content = requests.get(self.get_base_url(), params=params).content
                json_response = json.loads(content)
                self.assert_page1_correct(json_response)
                self.assert_pagination_navigation(params, json_response['pagination'])

                # Fetch the second page using next
                url = json_response['pagination']['next']

                content = requests.get(url).content
                json_response_second = json.loads(content)
                self.assert_page2_correct(json_response, json_response_second, "desc")
                self.assert_pagination_navigation(params, json_response_second['pagination'])

                # Fetch the first page using previous in first page
                url = json_response_second['pagination']['previous']
                content = requests.get(url).content
                json_response_first = json.loads(content)
                self.assert_page1_correct(json_response_first)
                self.assert_pagination_navigation(params, json_response_first['pagination'])

                genus_species = json_response_first['termFacets']['genusSpecies']['terms'][0]['term']


if __name__ == '__main__':
    unittest.main()
