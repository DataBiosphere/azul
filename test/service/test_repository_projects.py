from more_itertools import (
    one,
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


@patch_dss_endpoint
@patch_source_cache
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
            url = self.base_url.set(path=('index', 'projects', uuid or ''),
                                    args=dict(catalog=self.catalog))
            response = requests.get(str(url))
            response.raise_for_status()
            return response.json()

        def assert_file_type_summaries(hit):
            self.assertEqual(len(hit['fileTypeSummaries']), 1)
            self.assertIn('source', hit['fileTypeSummaries'][0])
            self.assertIn('format', hit['fileTypeSummaries'][0])
            self.assertGreater(hit['fileTypeSummaries'][0]['count'], 0)
            self.assertGreater(hit['fileTypeSummaries'][0]['totalSize'], 0)

        hit_properties = {
            'protocols',
            'entryId',
            'projects',
            'sources',
            'samples',
            'specimens',
            'cellLines',
            'donorOrganisms',
            'organoids',
            'cellSuspensions',
            'fileTypeSummaries'
        }
        projects_properties = {
            'accessible',
            'aggregateSubmissionDate',
            'aggregateUpdateDate',
            'projectId',
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
            'matrices',
            # FIXME: Remove deprecated field `hits[].projects[].contributorMatrices`
            #        https://github.com/DataBiosphere/azul/issues/3526
            'contributorMatrices',
            'contributedAnalyses',
            'submissionDate',
            'updateDate',
            'accessions',
            'estimatedCellCount'
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
