import logging

import requests

from service import WebServiceTestCase


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class RepositorySpecimenEndpointTest(WebServiceTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_basic_response(self):
        url = self.base_url + "/repository/samples"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('fileType', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        self.assertIn('hits', response_json)
        self.assertGreater(len(response_json['hits']), 0)
        for hit in response_json['hits']:
            self.assertIn('protocols', hit)
            self.assertIn('entryId', hit)
            assert_file_type_summaries(hit)
            self.assertIn('projects', hit)
            self.assertIn('samples', hit)
            self.assertIn('specimens', hit)
            self.assertIn('cellLines', hit)
            self.assertIn('donorOrganisms', hit)
            self.assertIn('organoids', hit)
            self.assertIn('cellSuspensions', hit)
            self.assertNotIn('projectSummary', hit)
            self.assertNotIn('files', hit)
        self.assertIn('pagination', response_json)
        self.assertIn('termFacets', response_json)
