#!/usr/bin/python

import json
import unittest
from responseobjects.api_response import KeywordSearchResponse


class MyTestCase(unittest.TestCase):
    def test_file_search_response(self):
        """
        This method tests the FileSearchResponse object. It will make sure the functionality works as
        appropriate by asserting the apiResponse attribute is the same as expected.
        :return:
        """
        with open('test_mapping_config.json') as json_mapping:
            test_mapping = json.load(json_mapping)
            json_mapping.close()

        with open('test1index.json') as json_test:
            test_json = json.load(json_test)
            json_test.close()

        with open('keyword_test1.json') as test1:
            keyword_test = json.load(test1)
            test1.close()
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(test_mapping, test_json)
        print(json.dumps(keyword_response.return_response().to_json()))
        # Use cmp() method for comparing two dictionaries
        print type(keyword_test)
        cmp(keyword_test, keyword_response.return_response().to_json())
        self.assertEqual(keyword_test, keyword_response.return_response().to_json())
        self.assertEqual('test', 'test')

if __name__ == '__main__':
    unittest.main()
