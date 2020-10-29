from more_itertools import (
    one,
)
import requests

from azul.logging import (
    configure_test_logging,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
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

    def test_projects_response(self):
        """
        Verify some basic properties of the /index/projects response and
        that each hit in the response is equal to the single hit response of a
        a request for one project (eg. /index/projects/{uuid})
        """

        def get_response_json(uuid=None):
            url = f'{self.base_url}/index/projects/{uuid if uuid else ""}'
            response = requests.get(url, params=dict(catalog=self.catalog))
            response.raise_for_status()
            return response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('fileType', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        hit_properties = {
            'protocols',
            'entryId',
            'projects',
            'samples',
            'specimens',
            'cellLines',
            'donorOrganisms',
            'organoids',
            'cellSuspensions',
            'fileTypeSummaries'
        }
        projects_properties = {
            'projectTitle',
            'projectShortname',
            'laboratory',
            'projectDescription',
            'contributors',
            'publications',
            'arrayExpressAccessions',
            'geoSeriesAccessions',
            'insdcProjectAccessions',
            'insdcStudyAccessions',
            'supplementaryLinks',
            'contributorMatrices'
        }
        response_json = get_response_json()
        self.assertIn('hits', response_json)
        self.assertGreater(len(response_json['hits']), 0)
        for hit in response_json['hits']:
            self.assertEqual(hit_properties, set(hit.keys()))
            self.assertEqual(projects_properties, set(one(hit['projects']).keys()))
            assert_file_type_summaries(hit)
            self.assertNotIn('projectSummary', hit)
            self.assertNotIn('files', hit)
            single_hit = get_response_json(hit['entryId'])
            self.assertEqual(hit, single_hit)
        self.assertIn('pagination', response_json)
        self.assertIn('termFacets', response_json)
