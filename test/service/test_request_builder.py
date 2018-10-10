#!/usr/bin/python

import json
import difflib
import logging.config
import os
import unittest
from service import WebServiceTestCase
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump as EsTd

logger = logging.getLogger(__name__)


class TestRequestBuilder(WebServiceTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.request_config_filepath = os.path.join(cls.data_directory, 'request_builder_test_config.json')

    def _load_json(self, name):
        return EsTd.open_and_return_json(os.path.join(os.path.dirname(__file__), name))

    @staticmethod
    def compare_dicts(actual_output, expected_output):
        """"Print the two outputs along with a diff of the two"""
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(
            actual_output[:20],
            expected_output[:20]))
        for i, s in enumerate(
                difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))

    def test_create_request(self):
        """
        Tests creation of a simple request
        :return: True or False depending on the assertion
        """
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(os.path.join(self.data_directory, 'request_builder_test_input1.json'))
        # Create a simple filter to test on
        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_empty(self):
        """
        Tests creation of an empty request. That is, no filter
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(os.path.join(self.data_directory, 'request_builder_test_input2.json'))
        # Create empty filter
        # TODO: Need some form of handler for the query language
        sample_filter = {}
        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_complex(self):
        """
        Tests creation of a complex request.
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(os.path.join(self.data_directory, 'request_builder_test_input3.json'))
        # Create sample filter
        sample_filter = {
            "entity_id":
                {
                    "is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]
                },
            "entity_version":
                {
                    "is": ["1993-07-19T23:50:09"]
                }
        }

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_missing_values(self):
        """
        Tests creation of a request for facets that do not have a value
        """
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(
            os.path.join(self.data_directory, 'request_builder_test_missing_values1.json'))
        # Create a filter for missing values
        sample_filter = {"entity_id": {"is": None}}

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_terms_and_missing_values(self):
        """
        Tests creation of a request for a combination of facets that do and do not have a value
        """
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(
            os.path.join(self.data_directory, 'request_builder_test_missing_values2.json'))
        # Create a filter for missing values
        sample_filter = {
            "term1": {"is": None},
            "term2": {"is": ["test"]},
            "term3": {"is": None},
        }

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_aggregate(self):
        """
        Tests creation of an ES aggregate
        """
        # Load files required for this test
        expected_output = self._load_json(
            os.path.join(self.data_directory, 'request_builder_test_aggregate.json'))

        sample_filter = {}

        # Create a request object
        agg_field = 'facet1'
        aggregation = EsTd.create_aggregate(
            sample_filter,
            facet_config={agg_field: f'{agg_field}.translation'},
            agg=agg_field
        )
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            aggregation.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_projects(self):
        """
        Test creation of a projects index request
        Request should have _project aggregations containing project_id buckets at the top level
        and sub-aggregations within each project bucket
        """
        # Load files required for this test
        request_config = self._load_json(self.request_config_filepath)
        expected_output = self._load_json(
            os.path.join(self.data_directory, 'request_builder_test_input_projects.json'))

        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True,
            entity_type='projects')
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        self.assertEqual(actual_output, expected_output)

    def test_project_summaries(self):
        """
        Test creation of project summary
        Summary should be added to dict of corresponding project id in hits.
        """
        hits = [{'entryId': 'a'}, {'entryId': 'b'}]
        es_response = self._load_json(
            os.path.join(self.data_directory, 'request_builder_project_summaries_input.json'))
        EsTd().add_project_summaries(hits, es_response)

        expected_output = self._load_json(
            os.path.join(self.data_directory, 'request_builder_project_summaries_output.json'))

        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(hits, sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        self.assertEqual(actual_output, expected_output)


if __name__ == '__main__':
    unittest.main()
