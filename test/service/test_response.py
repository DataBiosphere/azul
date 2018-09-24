#!/usr/bin/python

import json
import unittest
import os

import requests

from azul.service.responseobjects.hca_response_v5 import (FileSearchResponse,
                                                          KeywordSearchResponse,
                                                          ProjectSummaryResponse)
from service import WebServiceTestCase


class TestResponse(WebServiceTestCase):

    def test_key_search_files_response(self):
        """
        This method tests the KeywordSearchResponse object for the files entity type.
        It will make sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            hits=self._load("response_test_input.json"),
            entity_type='files'
        ).return_response().to_json()

        self.assertEqual(json.dumps(keyword_response, sort_keys=True),
                         json.dumps(self._load("response_keysearch_files_output.json"), sort_keys=True))

    def test_key_search_specimens_response(self):
        """
        KeywordSearchResponse for the specimens endpoint should return file type summaries instead of files
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            hits=self._load("response_test_input.json"),
            entity_type='specimens'
        ).return_response().to_json()

        self.assertEqual(json.dumps(keyword_response, sort_keys=True),
                         json.dumps(self._load("response_keysearch_specimens_output.json"), sort_keys=True))

    def test_file_search_response(self):
        """
        n=1: Test the FileSearchResponse object, making sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.

        n=2: Tests the FileSearchResponse object, using 'search_after' pagination.
        """
        for n in 1, 2:
            with self.subTest(n=n):
                filesearch_response = FileSearchResponse(
                    hits=self._load("response_test_input.json"),
                    pagination=self._load(f"response_pagination_input{n}.json"),
                    facets=self._load("response_facets_empty.json"),
                    entity_type="files"
                ).return_response().to_json()

                self.assertEqual(json.dumps(filesearch_response, sort_keys=True),
                                 json.dumps(self._load(f"response_filesearch_output{n}.json"), sort_keys=True))

    def test_file_search_response_file_summaries(self):
        """
        Test non-'files' entity type passed to FileSearchResponse will give file summaries
        """
        filesearch_response = FileSearchResponse(
            hits=self._load("response_test_input.json"),
            pagination=self._load(f"response_pagination_input1.json"),
            facets=self._load("response_facets_empty.json"),
            entity_type="specimens"
        ).return_response().to_json()

        for hit in filesearch_response['hits']:
            self.assertTrue('fileTypeSummaries' in hit)
            self.assertFalse('files' in hit)

    def test_file_search_response_add_facets(self):
        """
        Test adding facets to FileSearchResponse with missing values in one facet
        and no missing values in the other

        null term should not appear if there are no missing values
        """
        facets = FileSearchResponse.add_facets(self._load('response_facets_populated.json'))
        self.assertEqual(json.dumps(facets, sort_keys=True),
                         json.dumps(self._load('response_filesearch_add_facets_output.json'), sort_keys=True))

    def test_summary_endpoint(self):
        for entity_type in 'specimens', 'files':
            with self.subTest(entity_type=entity_type):
                url = self.base_url + "repository/summary/" + entity_type
                response = requests.get(url)
                response.raise_for_status()
                summary_object = response.json()
                self.assertGreater(summary_object['fileCount'], 0)
                self.assertGreater(summary_object['organCount'], 0)
                self.assertIsNotNone(summary_object['organSummaries'])

    def test_default_sorting_parameter(self):
        base_url = self.base_url
        url = base_url + "repository/files"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['pagination']["sort"], "specimenId")

    def test_project_summary_cell_count(self):
        """
        Test per organ and total cell counter in ProjectSummaryResponse
        Should return a correct total cell count and per organ cell count
        Should not double count cell count from specimens with an already counted id
            (i.e. each unique specimen counted exactly once)
        """
        es_hit = self._load('response_project_cell_count_input.json')
        expected_output = self._load('response_project_cell_count_output.json')

        total_cell_count, organ_cell_count = ProjectSummaryResponse.get_cell_count(es_hit)

        self.assertEqual(total_cell_count,
                         sum([cell_count['value'] for cell_count in expected_output]))
        self.assertEqual(json.dumps(organ_cell_count, sort_keys=True),
                         json.dumps(expected_output, sort_keys=True))

    def test_project_get_bucket_terms(self):
        """
        Test getting all unique terms of a given facet of a given project
        Should only return values of the given project
        Should return an empty list if project has no values in the term or if project does not exist
        """
        project_buckets = self._load('response_project_get_bucket_input.json')

        bucket_terms_1 = ProjectSummaryResponse.get_bucket_terms('project1', project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_1, ['term1', 'term2', 'term3'])

        bucket_terms_2 = ProjectSummaryResponse.get_bucket_terms('project2', project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_2, [])

        bucket_terms_3 = ProjectSummaryResponse.get_bucket_terms('project3', project_buckets, 'term_bucket')
        self.assertEqual(bucket_terms_3, [])

    def test_project_get_bucket_values(self):
        """
        Test getting value of a given aggregation of a given project
        Should only value of the given project
        Should return -1 if project is not found
        """
        project_buckets = self._load('response_project_get_bucket_input.json')

        bucket_terms_1 = ProjectSummaryResponse.get_bucket_value('project1', project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_1, 2)

        bucket_terms_2 = ProjectSummaryResponse.get_bucket_value('project2', project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_2, 4)

        bucket_terms_3 = ProjectSummaryResponse.get_bucket_value('project3', project_buckets, 'value_bucket')
        self.assertEqual(bucket_terms_3, -1)

    def test_projects_key_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type repsponses
        """
        keyword_response = KeywordSearchResponse(
            hits=self._load("response_test_input.json"),
            entity_type='projects'
        ).return_response().to_json()

        self.assertEqual(json.dumps(keyword_response, sort_keys=True),
                         json.dumps(self._load("response_projects_keysearch_output.json"), sort_keys=True))

    def test_projects_file_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type repsponses
        """
        keyword_response = FileSearchResponse(
            hits=self._load("response_test_input.json"),
            pagination=self._load(f"response_pagination_input1.json"),
            facets=self._load("response_facets_populated.json"),
            entity_type='projects'
        ).return_response().to_json()

        self.assertEqual(json.dumps(keyword_response, sort_keys=True),
                         json.dumps(self._load("response_projects_filesearch_output.json"), sort_keys=True))

    def _load(self, filename):
        data_folder_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        with open(os.path.join(data_folder_filepath, filename)) as fp:
            return json.load(fp)


if __name__ == '__main__':
    unittest.main()
