#!/usr/bin/python

import requests
import json
import unittest
import os


# Assumes a database initialized with >SEARCH_AFTER_THRESHOLD records, where
# SEARCH_AFTER_THRESHOLD is defined in responseobjects/elastic_request_builder.py
class PaginationTestCase(unittest.TestCase):
    @staticmethod
    def get_base_url():
        """
        :return: The base URL to test.
        """
        return "{}/repository/files".format(os.getenv("BASE_URL", "http://localhost:80"))

    def assert_page1_correct(self, json_response):
        """
        Helper function that asserts the given response is correct for the first page.
        :param json_response: A JSON response dictionary
        :return:
        """
        if 'search_before' in json_response['pagination']:
            self.assertEqual(json_response['pagination']['search_before'], [],
                             "search_before != [] on first page of results")
        else:
            self.fail("search_before not set to [] on first page of results")

        num_hits = len(json_response['hits'])
        if 'search_after' in json_response['pagination']:
            self.assertEqual(len(json_response['pagination']['search_after']), 2,
                             "Wrong number of arguments for search_after on first page of results")
            self.assertEqual(json_response['pagination']['search_after'][0],
                             json_response['hits'][num_hits - 1]['entity_id'],
                             "search_after not set to last returned document on first page")
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
            self.assertEqual(json_response_second['pagination']['search_before'][0],
                             json_response_second['hits'][0]['entity_id'],
                             "search_before on second page not set to first returned document on second page, "
                             + "order=" + sort_order)
        else:
            self.fail("search_before not set on second page of results, sortOrder=" + sort_order)

        if 'search_after' in json_response['pagination']:
            self.assertEqual(json_response_second['pagination']['search_after'][0],
                             json_response_second['hits'][num_hits_second - 1]['entity_id'],
                             "search_after on second page not set to last returned document on second page, "
                             + "order=" + sort_order)
        else:
            self.fail("search_after not set on second page of results, sortOrder=" + sort_order)

        self.assertNotEqual(json_response['hits'][0], json_response_second['hits'][0],
                            "first hit of first page is the same as first hit of second page")
        self.assertNotEqual(json_response['hits'][num_hits_first - 1], json_response['hits'][0],
                            "last hit of first page is the same as first hit of second page")

    def test_search_after_page1(self):
        """
        Tests that search_after pagination works for the first returned page.
        :return:
        """
        content = requests.get("{}?sort=entity_id&order=desc".format(self.get_base_url())).content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

    def test_search_after_page1_explicit_from(self):
        """
        Tests that response contains information to enable search_after pagination, even if the user explicitly
        passes from and size variables.
        :return:
        """
        content = requests.get("{}?sort=entity_id&from=1&size=10&order=desc".format(self.get_base_url())).content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

    def test_search_after_page2(self):
        """
        Tests that the second page returned in search_after pagination mode is correct.
        :return:
        """
        # Fetch and check first page.
        content = requests.get("{}?sort=entity_id&order=desc".format(self.get_base_url())).content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)

        # Fetch the second page using search_after
        search_after = json_response['pagination']['search_after']
        url = "{}?sort=entity_id&search_after={}&search_after={}".format(self.get_base_url(), search_after[0],
                                                                         search_after[1])
        content = requests.get(url).content
        json_response_second = json.loads(content)
        self.assert_page2_correct(json_response, json_response_second, "desc")

    def test_search_after_last_page(self):
        """
        Tests that the last page returned in search_after pagination mode is correct.
        :return:
        """
        content = requests.get("{}?sort=entity_id&order=asc".format(self.get_base_url())).content
        json_response = json.loads(content)
        self.assert_page1_correct(json_response)
        # Store the search_after for the last result of the first page.
        search_after_lrfp = json_response['pagination']['search_after']

        content = requests.get("{}?sort=entity_id&order=asc&search_after={}&search_after=[]".format(self.get_base_url(),
                                                                                                    search_after_lrfp[
                                                                                                        0],
                                                                                                    search_after_lrfp[
                                                                                                        1])).content
        json_response_second = json.loads(content)
        self.assert_page2_correct(json_response, json_response_second, "asc")

        search_after = json_response_second['pagination']['search_before']

        content = requests.get(
            "{}?sort=entity_id&search_after={}&search_after={}&order=desc".format(self.get_base_url(), search_after[0],
                                                                                  search_after[1])).content
        json_response = json.loads(content)
        if 'search_before' in json_response['pagination']:
            self.assertEqual(len(json_response['pagination']['search_before']), 2)
            self.assertEqual(json_response['pagination']['search_before'], search_after_lrfp)
        else:
            self.fail("search_before not set on last page of results")

        if 'search_after' in json_response['pagination']:
            self.assertEqual(json_response['pagination']['search_after'], [])
        else:
            self.fail("search_after not set to [] on first page of results")


if __name__ == '__main__':
    unittest.main()
