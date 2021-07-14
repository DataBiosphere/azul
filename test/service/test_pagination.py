import json
from typing import (
    Dict,
    cast,
)
import unittest
from urllib import (
    parse,
)

from furl import (
    furl,
)
import requests

from azul.logging import (
    configure_test_logging,
)
from service import (
    WebServiceTestCase,
    patch_dss_endpoint,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


def parse_url_qs(url) -> Dict[str, str]:
    url_parts = parse.urlparse(url)
    query_dict = dict(parse.parse_qsl(url_parts.query, keep_blank_values=True))
    # some PyCharm stub gets in the way, making the cast necessary
    return cast(Dict[str, str], query_dict)


@patch_dss_endpoint
@patch_source_cache
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

    def assert_page1_correct(self, json_response):
        """
        Helper function that asserts the given response is correct for the first page.
        :param json_response: A JSON response dictionary
        :return:
        """
        pagination = json_response['pagination']
        next_ = parse_url_qs(pagination['next'])
        self.assertIsNone(pagination['previous'])

        num_hits = len(json_response['hits'])
        self.assertEqual(next_['search_after'],
                         json.dumps(json_response['hits'][num_hits - 1]['entryId']),
                         "search_after not set to last returned document on first page")
        self.assertIsNotNone(next_['search_after_uid'])

    def assert_page2_correct(self, json_response, json_response_second, sort_order):
        """
        Helper function that asserts the given response is correct for the second page.
        :param json_response: A JSON response dictionary for the first page
        :param json_response_second: A JSON response dictionary for the second page
        :param sort_order: A string used to indicate the sort order within subtests.
        :return:
        """
        num_hits_first = len(json_response['hits'])
        num_hits_second = len(json_response_second['hits'])
        second_page_next = parse_url_qs(json_response_second['pagination']['next'])
        second_page_previous = parse_url_qs(json_response_second['pagination']['previous'])

        with self.subTest(sort_order=sort_order):
            self.assertIsNotNone(json_response_second['pagination']['next'])
            self.assertIsNotNone(json_response_second['pagination']['previous'])

            self.assertIsNotNone(second_page_previous['search_before_uid'],
                                 "No search_before_uid returned on second page")
            self.assertIsNotNone(second_page_next['search_after_uid'],
                                 "No search_after_uid returned on second page")

            self.assertEqual(second_page_previous['search_before'],
                             json.dumps(json_response_second['hits'][0]['entryId']),
                             "search_before on second page not set to first returned document on second page")
            self.assertEqual(second_page_next['search_after'],
                             json.dumps(json_response_second['hits'][num_hits_second - 1]['entryId']),
                             "search_after on second page not set to last returned document on second page")

            self.assertNotEqual(json_response['hits'][0], json_response_second['hits'][0],
                                "first hit of first page is the same as first hit of second page")
            self.assertNotEqual(json_response['hits'][num_hits_first - 1], json_response['hits'][0],
                                "last hit of first page is the same as first hit of second page")

    def assert_filters(self, request_filters: str, page_filters: str):
        """
        Due to implicit source filtering, a `sourceId` entry may be present in
        the pagination filters that was not included in the request, and the
        list of `sourceId`'s may be a subset of those specified in the request.
        """
        request_filters = json.loads(request_filters)
        page_filters = json.loads(page_filters)
        try:
            request_source_ids = request_filters['sourceId']
        except KeyError:
            page_filters.pop('sourceId', None)
            self.assertEqual(request_filters, page_filters)
        else:
            page_source_ids = page_filters['sourceId']
            self.assertTrue(set(request_source_ids) <= set(page_source_ids))

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
                page_params = parse_url_qs(page_url)
                for param in request_params:
                    self.assertIn(param, page_params)
                    if param == 'filters':
                        self.assert_filters(request_params[param], page_params[param])
                    else:
                        self.assertEqual(request_params[param], page_params[param])
        self.assertGreater(urls_checked, 0)

    def test_search_after_page1(self):
        """
        Tests that search_after pagination works for the first returned page.
        :return:
        """
        url = self.base_url.set(path='/index/files',
                                args=dict(catalog=self.catalog,
                                          sort='entryId',
                                          order='desc'))
        response = requests.get(str(url))
        response.raise_for_status()
        json_response = response.json()
        self.assert_page1_correct(json_response)

    def test_search_after_page1_explicit_from(self):
        """
        Tests that response contains information to enable search_after pagination, even if the user explicitly
        passes from and size variables.
        :return:
        """
        url = self.base_url.set(path='/index/files',
                                args=dict(catalog=self.catalog,
                                          sort='entryId',
                                          size=10,
                                          order='desc'))
        response = requests.get(str(url))
        response.raise_for_status()
        json_response = response.json()
        self.assert_page1_correct(json_response)

    def test_search_after_page2(self):
        """
        Tests that the second page returned in search_after pagination mode is correct.
        :return:
        """
        # Fetch and check first page.
        url = self.base_url.set(path='/index/files',
                                args=dict(catalog=self.catalog,
                                          sort='entryId',
                                          order='desc'))
        response = requests.get(str(url))
        response.raise_for_status()
        json_response = response.json()
        self.assert_page1_correct(json_response)

        # Fetch the second page using search_after
        response = requests.get(json_response['pagination']['next'])
        response.raise_for_status()
        json_response_second = response.json()
        self.assert_page2_correct(json_response, json_response_second, "desc")

    def test_search_after_last_page(self):
        """
        Tests that the last page returned in search_after pagination mode is correct.
        :return:
        """
        url = self.base_url.set(path='/index/files',
                                args=dict(catalog=self.catalog,
                                          sort='entryId',
                                          order='asc'))
        response = requests.get(str(url))
        response.raise_for_status()
        json_response = response.json()
        self.assert_page1_correct(json_response)
        # Store the search_after for the last result of the first page.
        first_page_next = parse_url_qs(json_response['pagination']['next'])
        search_after_lrfp = first_page_next['search_after']

        response = requests.get(json_response['pagination']['next'])
        response.raise_for_status()
        json_response_second = response.json()
        self.assert_page2_correct(json_response, json_response_second, "asc")

        second_page_previous = parse_url_qs(json_response_second['pagination']['previous'])
        third_request_params = dict(catalog=self.catalog,
                                    sort='entryId',
                                    order='desc',
                                    # FIXME: issue: these are deprecated and should not be used in tests
                                    search_after=second_page_previous['search_before'],
                                    search_after_uid=second_page_previous['search_before_uid'])

        url = self.base_url.set(path='/index/files', args=third_request_params)
        response = requests.get(str(url))
        response.raise_for_status()
        json_response_third = response.json()
        third_page_previous = parse_url_qs(json_response_third['pagination']['previous'])

        self.assertEqual(third_page_previous['search_before'], search_after_lrfp,
                         "search_before on last page is not set correctly")
        self.assertIsNotNone(third_page_previous['search_before_uid'],
                             "search_before_uid on last page is not set")
        self.assertIsNone(json_response_third['pagination']['next'])

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
                    'catalog': self.catalog,
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
                url = self.base_url.set(path='/index/files', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                json_response = response.json()
                self.assert_page1_correct(json_response)
                self.assert_pagination_navigation(params, json_response['pagination'])

                # Fetch the second page using next
                url = furl(json_response['pagination']['next'])

                response = requests.get(str(url))
                response.raise_for_status()
                json_response_second = response.json()
                self.assert_page2_correct(json_response, json_response_second, "desc")
                self.assert_pagination_navigation(params, json_response_second['pagination'])

                # Fetch the first page using previous in first page
                url = furl(json_response_second['pagination']['previous'])
                response = requests.get(str(url))
                response.raise_for_status()
                json_response_first = response.json()
                self.assert_page1_correct(json_response_first)
                self.assert_pagination_navigation(params, json_response_first['pagination'])

                genus_species = json_response_first['termFacets']['genusSpecies']['terms'][0]['term']


if __name__ == '__main__':
    unittest.main()
