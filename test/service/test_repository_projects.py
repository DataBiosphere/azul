import requests

from azul.logging import configure_test_logging
from service import WebServiceTestCase


def setUpModule():
    configure_test_logging()


class RepositoryProjectsEndpointTest(WebServiceTestCase):
    # Set a seed so that we can test the detail response with a stable project ID
    seed = 123

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @staticmethod
    def get_project_detail_properties():
        """Get a list of properties that are only returned in the /repository/projects/{id} response"""
        return ['contributors', 'projectDescription', 'publications']

    def test_list_response(self):
        """
        Make call to endpoint that returns multiple projects
        A list of hits should be returned
        Certain fields should not be in the project object
        """
        url = self.base_url + '/repository/projects'
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
            self.assertIn('specimens', hit)
            self.assertNotIn('projectSummary', hit)
            self.assertNotIn('files', hit)
            for project in hit['projects']:
                for prop in RepositoryProjectsEndpointTest.get_project_detail_properties():
                    self.assertNotIn(prop, project)
            self._test_detail_response(hit['entryId'])
        self.assertIn('pagination', response_json)
        self.assertIn('termFacets', response_json)

    def _test_detail_response(self, uuid):
        """
        Make call to endpoint that returns a single project
        A single hit should be returned
        Certain fields should be in the project object
        """
        url = self.base_url + '/repository/projects/' + uuid
        response = requests.get(url)
        response.raise_for_status()
        hit = response.json()

        self.assertEqual(len(hit['fileTypeSummaries']), 1)
        self.assertIn('fileType', hit['fileTypeSummaries'][0])
        self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
        self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        self.assertIn('protocols', hit)
        self.assertIn('entryId', hit)
        self.assertIn('projects', hit)
        self.assertIn('specimens', hit)
        self.assertNotIn('projectSummary', hit)
        self.assertNotIn('files', hit)

        for project in hit['projects']:
            for prop in RepositoryProjectsEndpointTest.get_project_detail_properties():
                self.assertIn(prop, project)
