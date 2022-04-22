from collections import (
    Counter,
)
from itertools import (
    product,
)
import json
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    cast,
)
import unittest
from unittest import (
    mock,
)
from urllib.parse import (
    parse_qs,
    parse_qsl,
    urlparse,
)

import attr
from more_itertools import (
    one,
)
import requests

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    cached_property,
    config,
)
from azul.collections import (
    none_safe_key,
)
from azul.indexer import (
    BundleFQID,
)
from azul.indexer.document import (
    null_str,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins.metadata.hca.stages.response import (
    Pagination,
    SearchResponseFactory,
)
from azul.types import (
    JSON,
)
from service import (
    DSSUnitTestCase,
    WebServiceTestCase,
    patch_dss_source,
    patch_source_cache,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


def parse_url_qs(url) -> Dict[str, str]:
    url_parts = urlparse(url)
    query_dict = dict(parse_qsl(url_parts.query, keep_blank_values=True))
    # some PyCharm stub gets in the way, making the cast necessary
    return cast(Dict[str, str], query_dict)


@patch_dss_source
@patch_source_cache
class TestResponse(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            cls.bundle_fqid(uuid='fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a',
                            version='2019-02-14T19:24:38.034764Z'),
            cls.bundle_fqid(uuid='d0e17014-9a58-4763-9e66-59894efbdaa8',
                            version='2018-10-03T14:41:37.044509Z'),
            cls.bundle_fqid(uuid='e0ae8cfa-2b51-4419-9cde-34df44c6458a',
                            version='2018-12-05T23:09:17.591044Z'),
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T20:46:55.866661Z'),
            cls.bundle_fqid(uuid='412cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T20:46:55.866661Z'),
            cls.bundle_fqid(uuid='ffac201f-4b1c-4455-bd58-19c1a9e863b4',
                            version='2019-10-09T17:07:35.528600Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def get_hits(self, entity_type: str, entity_id: str):
        """
        Fetches hits from ES instance searching for a particular entity ID
        """
        body = {
            "query": {
                "term": {
                    "entity_id.keyword": entity_id
                }
            }
        }
        # Tests are assumed to only ever run with the azul dev index
        results = self.es_client.search(index=config.es_index_name(catalog=self.catalog,
                                                                   entity_type=entity_type,
                                                                   aggregate=True),
                                        body=body)
        return self._index_service.translate_fields(catalog=self.catalog,
                                                    doc=[results['hits']['hits'][0]['_source']],
                                                    forward=False)

    @cached_property
    def _index_service(self):
        return IndexService()

    @property
    def paginations(self):
        return [
            Pagination(count=2,
                       order='desc',
                       pages=1,
                       size=5,
                       sort='entryId',
                       total=2,
                       previous=None,
                       next=None),
            Pagination(count=2,
                       order='desc',
                       pages=1,
                       size=5,
                       sort='entryId',
                       total=2,
                       previous=None,
                       next=str(self.base_url.set(path='/index/files',
                                                  args=dict(size=5,
                                                            search_after='cbb998ce-ddaf-34fa-e163-d14b399c6b34',
                                                            search_after_uid='meta%2332'))))
        ]

    def test_file_search_response(self):
        """
        n=0: Test the SearchResponse object, making sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.

        n=1: Tests the SearchResponse object, using 'next' pagination.
        """
        hits = [
            {
                "bundles": [
                    {
                        "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                        "bundleVersion": "2018-11-02T11:33:44.698028Z"
                    }
                ],
                "cellLines": [

                ],
                "cellSuspensions": [
                    {
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "selectedCellType": [None],
                        "totalCells": 1,
                    }
                ],
                "donorOrganisms": [
                    {
                        "biologicalSex": ["female"],
                        "disease": ['normal'],
                        "developmentStage": [None],
                        "genusSpecies": ["Australopithecus"],
                        "id": ["DID_scRSq06"],
                        "donorCount": 1,
                        "organismAge": [{"value": "38", "unit": "year"}],
                        "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                    }
                ],
                "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                "files": [
                    {
                        "contentDescription": [None],
                        "format": "fastq.gz",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "name": "SRR3562915_1.fastq.gz",
                        "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                        "size": 195142097,
                        "fileSource": None,
                        "url": None,
                        "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                        "version": "2018-11-02T113344.698028Z"
                    }
                ],
                "organoids": [

                ],
                "projects": [
                    {
                        "laboratory": ["John Dear"],
                        "projectId": ["e8642221-4c2c-4fd7-b926-a68bce363c88"],
                        "projectShortname": ["Single of human pancreas"],
                        "projectTitle": ["Single cell transcriptome patterns."],
                        "estimatedCellCount": None,
                    }
                ],
                "protocols": [
                    {
                        "libraryConstructionApproach": ["Smart-seq2"],
                        "nucleicAcidSource": ["single cell"],
                    },
                    {
                        "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                        "pairedEnd": [True],
                    }
                ],
                "samples": [
                    {
                        "sampleEntityType": ["specimens"],
                        "effectiveOrgan": ['pancreas'],
                        "disease": ["normal"],
                        "id": ["DID_scRSq06_pancreas"],
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism",
                        ],
                    }
                ],
                "sources": [{
                    "sourceId": "6aaf72a6-0a45-5886-80cf-48f8d670dc26",
                    "sourceSpec": "https://test:/2"
                }],
                "specimens": [
                    {
                        "disease": ["normal"],
                        "id": ["DID_scRSq06_pancreas"],
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism",
                        ],
                    }
                ],
                "dates": [
                    {
                        "aggregateLastModifiedDate": None,
                        "aggregateSubmissionDate": None,
                        "aggregateUpdateDate": None,
                        "lastModifiedDate": "2018-11-02T10:35:07.705000Z",
                        "submissionDate": "2018-11-02T10:03:39.600000Z",
                        "updateDate": "2018-11-02T10:35:07.705000Z",
                    }
                ]
            }
        ]
        responses = [
            {
                "hits": hits,
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "next": None,
                    "previous": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {}
            },
            {
                "hits": hits,
                "pagination": {
                    "count": 2,
                    "order": "desc",
                    "pages": 1,
                    "next": str(self.base_url.set(path='/index/files',
                                                  args=dict(size=5,
                                                            search_after='cbb998ce-ddaf-34fa-e163-d14b399c6b34',
                                                            search_after_uid='meta%2332'))),
                    "previous": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {}
            }
        ]
        for n in 0, 1:
            with self.subTest(n=n):
                # FIXME: Use response from `/index/files` to validate
                #        https://github.com/DataBiosphere/azul/issues/2970
                hits = self.get_hits('files', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb')
                factory = SearchResponseFactory(hits=hits,
                                                pagination=self.paginations[n],
                                                aggs={},
                                                entity_type='files',
                                                catalog=self.catalog)
                response = factory.make_response()
                self.assertElasticEqual(responses[n], response)

    def test_file_search_response_file_summaries(self):
        """
        Test non-'files' entity type passed to SearchResponse will give file summaries
        """
        # FIXME: Use response from `/index/samples` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('samples', 'a21dc760-a500-4236-bcff-da34a0e873d2')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs={},
                                        entity_type='samples',
                                        catalog=self.catalog)
        response = factory.make_response()

        for hit in response['hits']:
            self.assertTrue('fileTypeSummaries' in hit)
            self.assertFalse('files' in hit)

    canned_aggs = {
        "organ": {
            "doc_count": 21,
            "untagged": {
                "doc_count": 0
            },
            "myTerms": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "silver",
                        "doc_count": 11
                    },
                    {
                        "key": "teal",
                        "doc_count": 10
                    }
                ]
            }
        },
        "disease": {
            "doc_count": 21,
            "untagged": {
                "doc_count": 12
            },
            "myTerms": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "silver",
                        "doc_count": 9
                    }
                ]
            }
        }
    }

    def test_file_search_response_add_facets(self):
        """
        Test adding facets to SearchResponse with missing values in one facet
        and no missing values in the other

        null term should not appear if there are no missing values
        """
        # FIXME: Use response from `/index/files` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        factory = SearchResponseFactory(hits=[],
                                        pagination=self.paginations[0],
                                        aggs=self.canned_aggs,
                                        entity_type='files',
                                        catalog=self.catalog)
        facets = factory.make_facets()
        expected_output = {
            "organ": {
                "terms": [
                    {
                        "term": "silver",
                        "count": 11
                    },
                    {
                        "term": "teal",
                        "count": 10
                    }
                ],
                "total": 21,
                "type": "terms"
            },
            "disease": {
                "terms": [
                    {
                        "term": "silver",
                        "count": 9
                    },
                    {
                        "term": None,
                        "count": 12
                    }
                ],
                "total": 21,
                "type": "terms"
            }
        }
        self.assertElasticEqual(facets, expected_output)

    def test_sorting_details(self):
        for entity_type in 'files', 'samples', 'projects', 'bundles':
            with self.subTest(entity_type=entity_type):
                response = requests.get(str(self.base_url.set(path=('index', entity_type),
                                                              args=self._params())))
                response.raise_for_status()
                response_json = response.json()
                # Verify default sort field is set correctly
                self.assertEqual(response_json['pagination']["sort"], self.app_module.sort_defaults[entity_type][0])
                # Verify all fields in the response that are lists of primitives are sorted
                for hit in response_json['hits']:
                    self._verify_sorted_lists(hit)

    def test_transform_request_with_file_url(self):
        for entity_type in ('files', 'bundles'):
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type), args=self._params())
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                for hit in response_json['hits']:
                    if entity_type == 'files':
                        self.assertEqual(len(hit['files']), 1)
                    else:
                        self.assertGreater(len(hit['files']), 0)
                    for file in hit['files']:
                        self.assertIn('url', file.keys())
                        actual_url = urlparse(file['url'])
                        actual_query_vars = {k: one(v) for k, v in parse_qs(actual_url.query).items()}
                        self.assertEqual(url.netloc, actual_url.netloc)
                        self.assertEqual(url.scheme, actual_url.scheme)
                        self.assertIsNotNone(actual_url.path)
                        self.assertEqual(self.catalog, actual_query_vars['catalog'])
                        self.assertIsNotNone(actual_query_vars['version'])

    def test_projects_file_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        # FIXME: Use response from `/index/projects` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('projects', 'e8642221-4c2c-4fd7-b926-a68bce363c88')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs=self.canned_aggs,
                                        entity_type='projects',
                                        catalog=self.catalog)
        response = factory.make_response()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [None],
                            "totalCells": 1,
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "developmentStage": [None],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": [{"value": "38", "unit": "year"}],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}],
                        }
                    ],
                    "entryId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                    "fileTypeSummaries": [
                        {
                            "contentDescription": [None],
                            "count": 2,
                            "format": "fastq.gz",
                            "matrixCellCount": None,
                            "isIntermediate": None,
                            "fileSource": [None],
                            "totalSize": 385472253.0
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "contributors": [
                                {
                                    "contactName": "Matthew,,Green",
                                    "correspondingContributor": False,
                                    "email": "hewgreen@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "Human Cell Atlas wrangler"
                                },
                                {
                                    "contactName": "Martin, Enge",
                                    "correspondingContributor": None,
                                    "email": "martin.enge@gmail.com",
                                    "institution": "University",
                                    "laboratory": None,
                                    "projectRole": None
                                },
                                {
                                    "contactName": "Laura,,Huerta",
                                    "correspondingContributor": False,
                                    "email": "lauhuema@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "external curator"
                                }
                            ],
                            "laboratory": ["John Dear"],
                            "projectDescription": "As organisms age, cells accumulate genetic and epigenetic changes "
                                                  "that eventually lead to impaired organ function or catastrophic "
                                                  "failure such as cancer. Here we describe a single-cell "
                                                  "transcriptome analysis of 2544 human pancreas cells from donors, "
                                                  "spanning six decades of life. We find that islet cells from older "
                                                  "donors have increased levels of disorder as measured both by noise "
                                                  "in the transcriptome and by the number of cells which display "
                                                  "inappropriate hormone expression, revealing a transcriptional "
                                                  "instability associated with aging. By analyzing the spectrum of "
                                                  "somatic mutations in single cells from previously-healthy donors, "
                                                  "we find a specific age-dependent mutational signature "
                                                  "characterized by C to A and C to G transversions, indicators of "
                                                  "oxidative stress, which is absent in single cells from human brain "
                                                  "tissue or in a tumor cell line. Cells carrying a high load of such "
                                                  "mutations also express higher levels of stress and senescence "
                                                  "markers, including FOS, JUN, and the cytoplasmic superoxide "
                                                  "dismutase SOD1, markers previously linked to pancreatic diseases "
                                                  "with substantial age-dependent risk, such as type 2 diabetes "
                                                  "mellitus and adenocarcinoma. Thus, our single-cell approach "
                                                  "unveils gene expression changes and somatic mutations acquired in "
                                                  "aging human tissue, and identifies molecular pathways induced by "
                                                  "these genetic changes that could influence human disease. Also, "
                                                  "our results demonstrate the feasibility of using single-cell "
                                                  "RNA-seq data from primary cells to derive meaningful insights into "
                                                  "the genetic processes that operate on aging human tissue and to "
                                                  "determine which molecular mechanisms are coordinated with these "
                                                  "processes. Examination of single cells from primary human pancreas "
                                                  "tissue",
                            "projectId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "doi": "10.1016/j.cell.2017.09.004",
                                    "officialHcaPublication": None,
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals "
                                                        "Transcriptional Signatures of Aging and Somatic Mutation "
                                                        "Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ],
                            "supplementaryLinks": [
                                'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results'
                            ],
                            "estimatedCellCount": None,
                            "matrices": {},
                            "contributedAnalyses": {},
                            "accessions": [],
                        }
                    ],
                    "protocols": [
                        {
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "nucleicAcidSource": ["single cell"],
                        },
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "pairedEnd": [True],
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": ["specimens"],
                            "effectiveOrgan": ["pancreas"],
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                        }
                    ],
                    "sources": [{
                        "sourceId": "6aaf72a6-0a45-5886-80cf-48f8d670dc26",
                        "sourceSpec": "https://test:/2"
                    }],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ],
                        }
                    ],
                    "dates": [
                        {
                            "aggregateLastModifiedDate": "2018-11-02T10:35:07.705000Z",
                            "aggregateSubmissionDate": "2018-11-02T10:02:12.133000Z",
                            "aggregateUpdateDate": "2018-11-02T10:35:07.705000Z",
                            "lastModifiedDate": "2018-11-02T10:07:39.499000Z",
                            "submissionDate": "2018-11-02T10:02:12.133000Z",
                            "updateDate": "2018-11-02T10:07:39.499000Z",
                        }
                    ]
                }
            ],
            "pagination": {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "next": None,
                "previous": None,
                "size": 5,
                "sort": "entryId",
                "total": 2
            },
            "termFacets": {
                "disease": {
                    "terms": [
                        {
                            "count": 9,
                            "term": "silver"
                        },
                        {
                            "count": 12,
                            "term": None
                        }
                    ],
                    "total": 21,
                    "type": "terms"
                },
                "organ": {
                    "terms": [
                        {
                            "count": 11,
                            "term": "silver"
                        },
                        {
                            "count": 10,
                            "term": "teal"
                        }
                    ],
                    "total": 21,
                    "type": "terms"
                }
            }
        }

        self.assertElasticEqual(expected_response, response)

    def test_project_accessions_response(self):
        """
        This method tests the SearchResponse object for the projects entity type,
        specifically making sure the accessions fields are present in the response.
        """
        # FIXME: Use response from `/index/projects` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('projects', '627cb0ba-b8a1-405a-b58f-0add82c3d635')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs={},
                                        entity_type='projects',
                                        catalog=self.catalog)
        response = factory.make_response()
        expected_hits = [
            {
                "cellLines": [

                ],
                "cellSuspensions": [
                    {
                        "organ": ["brain"],
                        "organPart": ["amygdala"],
                        "selectedCellType": [None],
                        "totalCells": 10000,
                    }
                ],
                "donorOrganisms": [
                    {
                        "biologicalSex": ["male"],
                        "disease": ['H syndrome'],
                        "developmentStage": ["human adult stage"],
                        "genusSpecies": ["Homo sapiens"],
                        "id": ["donor_ID_1"],
                        "donorCount": 1,
                        "organismAge": [{"value": "20", "unit": "year"}],
                        "organismAgeRange": [{"gte": 630720000.0, "lte": 630720000.0}],
                    }
                ],
                "entryId": "627cb0ba-b8a1-405a-b58f-0add82c3d635",
                "fileTypeSummaries": [
                    {
                        "contentDescription": [None],
                        "count": 1,
                        "format": "bai",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 2395616.0
                    },
                    {
                        "contentDescription": [None],
                        "count": 1,
                        "format": "bam",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 55840108
                    },
                    {
                        "contentDescription": [None],
                        "count": 1,
                        "format": "csv",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 665
                    },
                    {
                        "contentDescription": [None],
                        "count": 1,
                        "format": "unknown",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 2645006
                    },
                    {
                        "contentDescription": [None],
                        "count": 2,
                        "format": "mtx",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 6561141
                    },
                    {
                        "contentDescription": [None],
                        "count": 3,
                        "format": "fastq.gz",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": [None],
                        "totalSize": 44668092
                    },
                    {
                        "contentDescription": [None],
                        "count": 3,
                        "format": "h5",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 5573714
                    },
                    {
                        "contentDescription": [None],
                        "count": 4,
                        "format": "tsv",
                        "matrixCellCount": None,
                        "isIntermediate": None,
                        "fileSource": ['DCP/2 Analysis'],
                        "totalSize": 15872628
                    }
                ],
                "organoids": [

                ],
                "projects": [
                    {
                        "contributors": [
                            {
                                "contactName": "John,D,Doe. ",
                                "correspondingContributor": False,
                                "email": "dummy@email.com",
                                "institution": "EMBL-EBI",
                                "laboratory": "Department of Biology",
                                "projectRole": "principal investigator"
                            }
                        ],
                        "laboratory": ["Department of Biology"],
                        "projectDescription": "Contains a small file set from the dataset: 4k PBMCs from a "
                                              "Healthy Donor, a Single Cell Gene Expression Dataset by Cell "
                                              "Ranger 2.1.0. Peripheral blood mononuclear cells (PBMCs) were "
                                              "taken from a healthy donor (same donor as pbmc8k). PBMCs are "
                                              "primary cells with relatively small amounts of RNA (~1pg "
                                              "RNA/cell). Data/Analysis can be found here "
                                              "https://support.10xgenomics.com/single-cell-gene-expression/datasets"
                                              "/2.1.0/pbmc4k and all data is licensed under the creative commons "
                                              "attribution license (https://creativecommons.org/licenses/by/4.0/). "
                                              "This test also contains extensive metadata for browser testing. "
                                              "Metadata is fabricated.",
                        "projectId": "627cb0ba-b8a1-405a-b58f-0add82c3d635",
                        "projectShortname": "staging/10x/2019-02-14T18:29:38Z",
                        "projectTitle": "10x 1 Run Integration Test",
                        "publications": [
                            {
                                "doi": "10.1016/j.cell.2016.07.054",
                                "officialHcaPublication": None,
                                "publicationTitle": "A title of a publication goes here.",
                                "publicationUrl": "https://europepmc.org"
                            }
                        ],
                        "supplementaryLinks": [None],
                        "estimatedCellCount": None,
                        "matrices": {},
                        "contributedAnalyses": {},
                        "accessions": [
                            {"namespace": "array_express", "accession": "E-AAAA-00"},
                            {"namespace": "geo_series", "accession": "GSE00000"},
                            {"namespace": "insdc_project", "accession": "SRP000000"},
                            {"namespace": "insdc_project", "accession": "SRP000001"},
                            {"namespace": "insdc_study", "accession": "PRJNA000000"},
                        ],
                    }
                ],
                "protocols": [
                    {
                        "workflow": ['cellranger_v1.0.2'],
                    },
                    {
                        "libraryConstructionApproach": ["10X v2 sequencing"],
                        "nucleicAcidSource": [None],
                    },
                    {
                        "instrumentManufacturerModel": ["Illumina HiSeq 2500"],
                        "pairedEnd": [False],
                    }
                ],
                "samples": [
                    {
                        "sampleEntityType": ["specimens"],
                        "effectiveOrgan": ["brain"],
                        "disease": ["H syndrome"],
                        "id": ["specimen_ID_1"],
                        "organ": ["brain"],
                        "organPart": ["amygdala"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism"
                        ],
                    }
                ],
                "sources": [{
                    "sourceId": "6aaf72a6-0a45-5886-80cf-48f8d670dc26",
                    "sourceSpec": "https://test:/2"
                }],
                "specimens": [
                    {
                        "disease": ["H syndrome"],
                        "id": ["specimen_ID_1"],
                        "organ": ["brain"],
                        "organPart": ["amygdala"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism"
                        ],
                    }
                ],
                "dates": [
                    {
                        "aggregateLastModifiedDate": "2019-02-14T19:19:57.464000Z",
                        "aggregateSubmissionDate": "2019-02-14T18:29:42.531000Z",
                        "aggregateUpdateDate": "2019-02-14T19:19:57.464000Z",
                        "lastModifiedDate": "2019-02-14T18:29:48.555000Z",
                        "submissionDate": "2019-02-14T18:29:42.531000Z",
                        "updateDate": "2019-02-14T18:29:48.555000Z",
                    }
                ]
            }
        ]
        self.assertElasticEqual(expected_hits, response['hits'])

    def test_cell_suspension_response(self):
        # FIXME: Use response from `/index/projects` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('projects', '250aef61-a15b-4d97-b8b4-54bb997c1d7d')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs={},
                                        entity_type='projects',
                                        catalog=self.catalog)
        response = factory.make_response()
        cell_suspension = one(response['hits'][0]['cellSuspensions'])
        self.assertEqual(["Plasma cells"], cell_suspension['selectedCellType'])

    def test_cell_line_response(self):
        """
        Test SearchResponse contains the correct cell_line and sample field values
        """
        # FIXME: Use response from `/index/projects` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('projects', 'c765e3f9-7cfc-4501-8832-79e5f7abd321')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs={},
                                        entity_type='projects',
                                        catalog=self.catalog)
        response = factory.make_response()
        expected_cell_lines = {
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2', 'cell_line_GM18517'],
            'cellLineType': ['primary', 'stem cell-derived'],
            'modelOrgan': ['blood (parent_cell_line)', 'blood (child_cell_line)'],
        }
        hits = response['hits']
        cell_lines = one(one(hits)['cellLines'])
        self.assertElasticEqual(expected_cell_lines, cell_lines)
        expected_samples = {
            'sampleEntityType': ['cellLines'],
            'effectiveOrgan': ['blood (child_cell_line)'],
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2'],
            'cellLineType': ['stem cell-derived'],
            'modelOrgan': ['blood (child_cell_line)'],
        }
        samples = one(one(hits)['samples'])
        self.assertElasticEqual(samples, expected_samples)

    def test_file_response(self):
        """
        Test SearchResponse contains the correct file field values
        """
        # FIXME: Use response from `/index/projects` to validate
        #        https://github.com/DataBiosphere/azul/issues/2970
        hits = self.get_hits('files', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a')
        factory = SearchResponseFactory(hits=hits,
                                        pagination=self.paginations[0],
                                        aggs={},
                                        entity_type='files',
                                        catalog=self.catalog)
        response = factory.make_response()
        expected_file = {
            'contentDescription': ['RNA sequence'],
            'format': 'fastq.gz',
            'matrixCellCount': None,
            'isIntermediate': None,
            'name': 'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.1.fastq.gz',
            'sha256': '709fede4736213f0f71ae4d76719fd51fa402a9112582a4c52983973cb7d7e47',
            'size': 22819025,
            'fileSource': None,
            'url': None,
            'uuid': 'a8b8479d-cfa9-4f74-909f-49552439e698',
            'version': '2019-10-09T172251.560099Z'
        }
        file = one(one(response['hits'])['files'])
        self.assertElasticEqual(file, expected_file)

    def test_filter_with_none(self):
        """
        Test response when using a filter with a None value
        """
        test_data_values = [["year"], [None], ["year", None]]
        for test_data in test_data_values:
            with self.subTest(test_data=test_data):
                params = self._params(size=10, filters={'organismAgeUnit': {'is': test_data}})
                url = self.base_url.set(path='/index/samples', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                organism_age_units = {
                    None if oa is None else oa['unit']
                    for hit in response_json['hits']
                    for donor in hit['donorOrganisms']
                    for oa in donor['organismAge']
                }
                # Assert that the organismAge unit values found in the response only match what was filtered for
                self.assertEqual(organism_age_units, set(test_data))

    def test_filter_by_projectId(self):
        """
        Test response when using a projectId filter
        """
        test_data_sets = [
            {
                'id': '627cb0ba-b8a1-405a-b58f-0add82c3d635',
                'title': '10x 1 Run Integration Test'
            },
            {
                'id': '250aef61-a15b-4d97-b8b4-54bb997c1d7d',
                'title': 'Bone marrow plasma cells from hip replacement surgeries'
            }
        ]
        for test_data in test_data_sets:
            for entity_type in 'files', 'samples', 'projects', 'bundles':
                with self.subTest(entity_type=entity_type):
                    params = self._params(size=2, filters={'projectId': {'is': [test_data['id']]}})
                    url = self.base_url.set(path=('index', entity_type), args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response_json = response.json()
                    for hit in response_json['hits']:
                        for project in hit['projects']:
                            if entity_type == 'projects':
                                self.assertEqual(test_data['title'], project['projectTitle'])
                                self.assertEqual(test_data['id'], project['projectId'])
                            else:
                                self.assertIn(test_data['title'], project['projectTitle'])
                                self.assertIn(test_data['id'], project['projectId'])
                    for term in response_json['termFacets']['project']['terms']:
                        self.assertEqual(term['projectId'], [test_data['id']])

    def test_filter_by_contentDescription(self):
        url = self.base_url.set(path='/index/files')
        params = self._params(size=3,
                              filters={'contentDescription': {'is': ['RNA sequence']}},
                              sort='fileName',
                              order='asc')
        response = requests.get(str(url), params=params)
        response.raise_for_status()
        response_json = response.json()
        expected = [
            'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.1.fastq.gz',
            'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.2.fastq.gz'
        ]
        actual = [file['name']
                  for hit in response_json['hits']
                  for file in hit['files']]
        self.assertEqual(actual, expected)

    def test_translated_facets(self):
        """
        Test that response facets values are correctly translated back to the
        correct data types and that the translated None value is not present.
        """
        url = self.base_url.set(path='/index/samples',
                                args=(self._params(size=10)))
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']

        paired_end_terms = {term['term'] for term in facets['pairedEnd']['terms']}
        self.assertEqual(paired_end_terms, {'true', 'false'})

        preservation_method_terms = {term['term'] for term in facets['preservationMethod']['terms']}
        self.assertEqual(preservation_method_terms, {None})

        model_organ_part_terms = {term['term'] for term in facets['modelOrganPart']['terms']}
        self.assertEqual(model_organ_part_terms, {None})

        for facet in facets.values():
            for term in facet['terms']:
                self.assertNotEqual(term['term'], null_str.to_index(None))

    def test_sample(self):
        """
        Test that sample(s) in the response contain values matching values in the source cellLine/organoid/specimen
        """
        for entity_type in 'projects', 'samples', 'files', 'bundles':
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type), args=self._params())
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                if entity_type == 'samples':
                    for hit in response_json['hits']:
                        for sample in hit['samples']:
                            sample_entity_type = sample['sampleEntityType']
                            for key, val in sample.items():
                                if key not in [
                                    'sampleEntityType',
                                    'effectiveOrgan',
                                    'accessible',
                                ]:
                                    if isinstance(val, list):
                                        for one_val in val:
                                            self.assertIn(one_val, hit[sample_entity_type][0][key])
                                    else:
                                        self.assertIn(val, hit[sample_entity_type][0][key])

    def test_bundles_outer_entity(self):
        entity_type = 'bundles'
        url = self.base_url.set(path=('index', entity_type), args=self._params())
        response = requests.get(str(url))
        response.raise_for_status()
        response = response.json()
        indexed_bundles = set(self.bundles())
        self.assertEqual(len(self.bundles()), len(indexed_bundles))
        actual_bundles = {
            self.bundle_fqid(uuid=bundle['bundleUuid'],
                             version=bundle['bundleVersion'])
            for hit in response['hits']
            for bundle in hit['bundles']
        }
        self.assertEqual(len(response['hits']), len(actual_bundles))
        self.assertSetEqual(indexed_bundles, actual_bundles)

    def test_ranged_values(self):
        test_hits = [
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
                    "developmentStage": [None],
                    "disease": ['normal'],
                    "genusSpecies": [
                        "Homo sapiens"
                    ],
                    "id": [
                        "HPSI0314i-hoik",
                        "HPSI0214i-wibj",
                        "HPSI0314i-sojd",
                        "HPSI0214i-kucg"
                    ],
                    "donorCount": 4,
                    "organismAge": [
                        {"value": "45-49", "unit": "year"},
                        {"value": "65-69", "unit": "year"}
                    ],
                    "organismAgeRange": [
                        {
                            "gte": 2049840000.0,
                            "lte": 2175984000.0
                        },
                        {
                            "gte": 1419120000.0,
                            "lte": 1545264000.0
                        }
                    ],
                }
            ],
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
                    "developmentStage": [None],
                    "disease": ['normal'],
                    "genusSpecies": [
                        "Homo sapiens"
                    ],
                    "id": [
                        "HPSI0314i-hoik",
                        "HPSI0214i-wibj",
                        "HPSI0314i-sojd",
                        "HPSI0214i-kucg"
                    ],
                    "donorCount": 4,
                    "organismAge": [
                        {"value": "40-44", "unit": "year"},
                        {"value": "55-59", "unit": "year"}
                    ],
                    "organismAgeRange": [
                        {
                            "gte": 1734480000.0,
                            "lte": 1860624000.0
                        },
                        {
                            "gte": 1261440000.0,
                            "lte": 1387584000.0
                        }
                    ],
                }
            ]
        ]

        for relation, range_value, expected_hits in [('contains', (1419130000, 1545263000), test_hits[:1]),
                                                     ('within', (1261430000, 1545265000), test_hits),
                                                     ('intersects', (1860623000, 1900000000), test_hits[1:]),
                                                     ('contains', (1860624000, 2049641000), []),
                                                     ('within', (1734490000, 1860623000), []),
                                                     ('intersects', (1860624100, 2049641000), [])]:
            with self.subTest(relation=relation, value=range_value):
                params = self._params(filters={'organismAgeRange': {relation: [range_value]}},
                                      order='desc',
                                      sort='entryId')
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                actual_value = [hit['donorOrganisms'] for hit in response.json()['hits']]
                self.assertElasticEqual(expected_hits, actual_value)

    def test_ordering(self):
        sort_fields = [
            ('cellCount', lambda hit: hit['cellSuspensions'][0]['totalCells']),
            ('donorCount', lambda hit: hit['donorOrganisms'][0]['donorCount'])
        ]

        for sort_field, accessor in sort_fields:
            responses = {
                order: requests.get(str(self.base_url.set(path='/index/projects',
                                                          args=self._params(order=order, sort=sort_field))))
                for order in ['asc', 'desc']
            }
            hit_sort_values = {}
            for order, response in responses.items():
                response.raise_for_status()
                hit_sort_values[order] = [accessor(hit) for hit in response.json()['hits']]

            self.assertEqual(hit_sort_values['asc'],
                             sorted(hit_sort_values['asc'], key=none_safe_key()))
            self.assertEqual(hit_sort_values['desc'],
                             sorted(hit_sort_values['desc'], key=none_safe_key(), reverse=True))

    def test_missing_field_sorting(self):
        """
        Test that sorting by a field that doesn't exist in all hits produces
        results with the hits missing the field placed at the end of a
        ascending sort and the beginning of a descending sort.
        """
        ascending_values = [
            ['induced pluripotent'],
            ['induced pluripotent'],
            ['primary', 'stem cell-derived'],
            None,  # The last 4 hits don't have any 'cellLines' inner entities
            None,  # so for purposes of this test we use None to represent
            None,  # that there is a hit however it has no 'cellLineType'.
            None
        ]

        def extract_cell_line_types(response_json):
            # For each hit yield the 'cellLineType' value or None if not present
            for hit in response_json['hits']:
                if hit['cellLines']:
                    yield one(hit['cellLines'])['cellLineType']
                else:
                    yield None

        for ascending in (True, False):
            with self.subTest(ascending=ascending):
                params = self._params(size=15,
                                      sort='cellLineType',
                                      order='asc' if ascending else 'desc')
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                actual_values = list(extract_cell_line_types(response_json))
                expected = ascending_values if ascending else list(reversed(ascending_values))
                self.assertEqual(actual_values, expected)

    def test_multivalued_field_sorting(self):
        """
        Test that sorting by a multi-valued field responds with hits that are
        correctly sorted based on the first value from each multi-valued field, and
        that each multi-valued field itself is sorted low to high regardless of the search sort
        """
        for order, reverse in (('asc', False), ('desc', True)):
            with self.subTest(order=order, reverse=reverse):
                params = self._params(size=15, sort='laboratory', order=order)
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                laboratories = []
                for hit in response_json['hits']:
                    laboratory = one(hit['projects'])['laboratory']
                    self.assertEqual(laboratory, sorted(laboratory))
                    laboratories.append(laboratory[0])
                self.assertGreater(len(laboratories), 1)
                self.assertEqual(laboratories, sorted(laboratories, reverse=reverse))

    def test_aggregate_date_sort(self):
        """
        Verify the search results can be sorted by the entity and aggregate dates.
        """
        test_cases = {
            'bundles': {
                'submissionDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T11:33:44.698028Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2018-12-05T23:09:17.591044Z', 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                    ('2019-02-14T19:24:38.034764Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T17:07:35.528600Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                ],
                'updateDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T11:33:44.698028Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2018-12-05T23:09:17.591044Z', 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                    ('2019-02-14T19:24:38.034764Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T17:07:35.528600Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                ],
                'lastModifiedDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T11:33:44.698028Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2018-12-05T23:09:17.591044Z', 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                    ('2019-02-14T19:24:38.034764Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T17:07:35.528600Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                ],
                'aggregateSubmissionDate': [
                    ('2018-10-01T14:22:24.370000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-11T21:18:01.605000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-11T21:18:01.605000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T10:02:12.133000Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2018-12-04T16:22:45.367000Z', 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                    ('2019-02-14T18:29:42.531000Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T15:31:09.188000Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                ],
                'aggregateUpdateDate': [
                    ('2018-10-01T20:13:06.669000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:45:01.366000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:45:01.366000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T10:35:07.705000Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2019-02-14T19:19:57.464000Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T15:52:49.512000Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                    (None, 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                ],
                'aggregateLastModifiedDate': [
                    ('2018-10-01T20:13:06.669000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:45:01.366000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:45:01.366000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-11-02T10:35:07.705000Z', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                    ('2018-12-04T16:22:46.893000Z', 'e0ae8cfa-2b51-4419-9cde-34df44c6458a'),
                    ('2019-02-14T19:19:57.464000Z', 'fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a'),
                    ('2019-10-09T15:52:49.512000Z', 'ffac201f-4b1c-4455-bd58-19c1a9e863b4'),
                ],
            },
            'projects': {
                'submissionDate': [
                    ('2018-10-01T14:22:24.370000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:01.605000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:01.605000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:02:12.133000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2018-12-04T16:22:45.367000Z', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                    ('2019-02-14T18:29:42.531000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:31:09.188000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                ],
                'updateDate': [
                    ('2018-10-01T14:34:10.121000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:06.651000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:06.651000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:07:39.499000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2019-02-14T18:29:48.555000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:32:48.934000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                    (None, 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                ],
                'lastModifiedDate': [
                    ('2018-10-01T14:34:10.121000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:06.651000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:06.651000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:07:39.499000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2018-12-04T16:22:45.367000Z', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                    ('2019-02-14T18:29:48.555000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:32:48.934000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                ],
                'aggregateSubmissionDate': [
                    ('2018-10-01T14:22:24.370000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:01.605000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:01.605000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:02:12.133000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2018-12-04T16:22:45.367000Z', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                    ('2019-02-14T18:29:42.531000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:31:09.188000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                ],
                'aggregateUpdateDate': [
                    ('2018-10-01T20:13:06.669000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-18T20:45:01.366000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-18T20:45:01.366000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:35:07.705000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2019-02-14T19:19:57.464000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:52:49.512000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                    (None, 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                ],
                'aggregateLastModifiedDate': [
                    ('2018-10-01T20:13:06.669000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-18T20:45:01.366000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-18T20:45:01.366000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-11-02T10:35:07.705000Z', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
                    ('2018-12-04T16:22:46.893000Z', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
                    ('2019-02-14T19:19:57.464000Z', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
                    ('2019-10-09T15:52:49.512000Z', '88ec040b-8705-4f77-8f41-f81e57632f7d'),
                ],
            },
            'samples': {
                'submissionDate': [
                    ('2018-10-01T14:22:25.143000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:02.654000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                    ('2018-10-11T21:18:02.696000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:02.732000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:02.785000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                    ('2018-11-02T10:02:12.298000Z', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
                    ('2018-12-04T16:22:45.625000Z', '195b2621-ec05-4618-9063-c56048de97d1'),
                    ('2019-02-14T18:29:42.550000Z', '58c60e15-e07c-4875-ac34-f026d6912f1c'),
                    ('2019-10-09T15:31:09.237000Z', 'caadf4b5-f5e4-4416-9f04-9c1f902cc601'),
                ],
                'updateDate': [
                    ('2018-10-01T14:57:17.976000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:06.725000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                    ('2018-10-11T21:18:06.730000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:12.763000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:12.864000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                    ('2018-11-02T10:09:26.517000Z', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
                    ('2019-02-14T18:29:49.006000Z', '58c60e15-e07c-4875-ac34-f026d6912f1c'),
                    ('2019-10-09T15:32:51.765000Z', 'caadf4b5-f5e4-4416-9f04-9c1f902cc601'),
                    (None, '195b2621-ec05-4618-9063-c56048de97d1'),
                ],
                'lastModifiedDate': [
                    ('2018-10-01T14:57:17.976000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:06.725000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                    ('2018-10-11T21:18:06.730000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:12.763000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:12.864000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                    ('2018-11-02T10:09:26.517000Z', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
                    ('2018-12-04T16:22:45.625000Z', '195b2621-ec05-4618-9063-c56048de97d1'),
                    ('2019-02-14T18:29:49.006000Z', '58c60e15-e07c-4875-ac34-f026d6912f1c'),
                    ('2019-10-09T15:32:51.765000Z', 'caadf4b5-f5e4-4416-9f04-9c1f902cc601'),
                ],
                # samples have no 'aggregate…Date' values
            },
            'files': {
                'submissionDate': [
                    ('2018-10-01T14:22:24.380000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-01T14:22:24.389000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T14:22:24.511000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T14:22:24.755000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-11T21:18:01.623000Z', '281c2d08-9e43-47f9-b937-e733e3ba3322'),
                    ('2018-10-11T21:18:01.642000Z', 'ae1d6fa7-964f-465a-8c78-565206827434'),
                    ('2018-10-11T21:18:01.654000Z', 'f518a8cc-e1d9-4fc9-bc32-491dd8543902'),
                    ('2018-10-11T21:18:01.964000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-11T21:18:01.979000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-11T21:18:01.990000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:32:25.801000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:32:25.877000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:32:25.951000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:32:26.026000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:32:26.097000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8'),
                    ('2018-10-18T20:32:26.174000Z', '3c41b5b6-f480-4d47-8c5e-155e7c1adf54'),
                    ('2018-10-18T20:32:26.243000Z', '022a217c-384d-4d9d-8631-6397b6838e3a'),
                    ('2018-10-18T20:32:26.313000Z', '9b778e46-0c51-4260-8e3f-000ecc145f0a'),
                    ('2018-10-18T20:32:26.383000Z', 'af025a74-53f1-4972-b50d-53095b5ffac2'),
                    ('2018-10-18T20:32:26.453000Z', 'e8395271-7c8e-4ec4-9598-495df43fe5fd'),
                    ('2018-10-18T20:32:26.528000Z', '211a8fbf-b190-4576-ac2f-2b1a91743abb'),
                    ('2018-10-18T20:32:26.603000Z', '17222e3a-5757-45e9-9dfe-c4b6aa10f28a'),
                    ('2018-10-18T20:32:26.681000Z', '2fb8a975-b50c-4528-b850-838a19e19a1e'),
                    ('2018-11-02T10:03:39.593000Z', '70d1af4a-82c8-478a-8960-e9028b3616ca'),
                    ('2018-11-02T10:03:39.600000Z', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
                    ('2018-12-04T16:22:46.380000Z', '12b25cbd-8cfa-4f0e-818f-d6ba3e823af4'),
                    ('2018-12-04T16:22:46.388000Z', '65d3d936-ae9d-4a18-a8c7-73ce6132355e'),
                    ('2019-02-14T18:29:42.574000Z', '7df5d656-43cb-49f9-b81d-86cca3c44a65'),
                    ('2019-02-14T18:29:42.587000Z', 'acd7d986-73ab-4d0b-9ead-377f3a2d646d'),
                    ('2019-02-14T18:29:42.597000Z', 'f9a78d6a-7c80-4c45-bedf-4bc152dc172d'),
                    ('2019-02-14T19:15:11.524000Z', 'bd1307b9-70b5-49e4-8e02-9d4ca0d64747'),
                    ('2019-02-14T19:15:11.667000Z', 'cf3453a3-68fb-4156-bc3e-0f08f7e6512c'),
                    ('2019-02-14T19:15:11.818000Z', '234b0359-3853-4df4-898f-5182f698d48b'),
                    ('2019-02-14T19:15:11.972000Z', 'd95392c5-1958-4825-9076-2a9c130c53f3'),
                    ('2019-02-14T19:15:12.117000Z', 'b9609367-7006-4055-8815-1bad881a1502'),
                    ('2019-02-14T19:15:12.259000Z', 'ebb2ec91-2cd0-4ec4-ba2b-5a6d6630bc5a'),
                    ('2019-02-14T19:15:12.404000Z', '1ab612ca-2a5a-4443-8004-bb5f0f784c67'),
                    ('2019-02-14T19:15:12.551000Z', '34c64244-d3ed-4841-84b7-aa4cbb9d794b'),
                    ('2019-02-14T19:15:12.703000Z', '71710439-3864-4fc6-bc48-ca2ac90f7ccf'),
                    ('2019-02-14T19:15:12.844000Z', '2ab5242e-f118-48e3-afe5-c2287fa2e2b1'),
                    ('2019-02-14T19:15:12.989000Z', '6da39577-256d-43fd-97c4-a3bedaa54273'),
                    ('2019-02-14T19:15:13.138000Z', '86a93e19-eb89-4c27-8b64-006f96bb2c83'),
                    ('2019-02-14T19:15:13.280000Z', '0f858ddb-6d93-404e-95fd-0c200921dd40'),
                    ('2019-10-09T15:31:58.607000Z', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a'),
                    ('2019-10-09T15:31:58.617000Z', 'fa17159e-52ec-4a88-80cf-a3be5e2e9988'),
                ],
                'updateDate': [
                    ('2018-10-01T15:40:51.754000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-01T15:42:33.208000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T16:09:56.972000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T16:09:57.110000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-18T20:32:16.894000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-18T20:32:18.864000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-18T20:32:20.845000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:37:28.333000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8'),
                    ('2018-10-18T20:39:10.339000Z', '9b778e46-0c51-4260-8e3f-000ecc145f0a'),
                    ('2018-10-18T20:39:13.335000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:39:16.337000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:39:22.340000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:39:25.337000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:39:40.335000Z', 'af025a74-53f1-4972-b50d-53095b5ffac2'),
                    ('2018-10-18T20:39:55.336000Z', 'e8395271-7c8e-4ec4-9598-495df43fe5fd'),
                    ('2018-10-18T20:39:58.363000Z', '17222e3a-5757-45e9-9dfe-c4b6aa10f28a'),
                    ('2018-10-18T20:39:58.363000Z', '211a8fbf-b190-4576-ac2f-2b1a91743abb'),
                    ('2018-10-18T20:40:01.344000Z', '3c41b5b6-f480-4d47-8c5e-155e7c1adf54'),
                    ('2018-10-18T20:40:13.334000Z', '2fb8a975-b50c-4528-b850-838a19e19a1e'),
                    ('2018-10-18T20:40:54.699000Z', '281c2d08-9e43-47f9-b937-e733e3ba3322'),
                    ('2018-10-18T20:40:55.940000Z', 'ae1d6fa7-964f-465a-8c78-565206827434'),
                    ('2018-10-18T20:40:57.146000Z', 'f518a8cc-e1d9-4fc9-bc32-491dd8543902'),
                    ('2018-10-18T20:45:01.366000Z', '022a217c-384d-4d9d-8631-6397b6838e3a'),
                    ('2018-11-02T10:35:03.810000Z', '70d1af4a-82c8-478a-8960-e9028b3616ca'),
                    ('2018-11-02T10:35:07.705000Z', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
                    ('2019-02-14T18:31:45.892000Z', '7df5d656-43cb-49f9-b81d-86cca3c44a65'),
                    ('2019-02-14T18:31:46.472000Z', 'f9a78d6a-7c80-4c45-bedf-4bc152dc172d'),
                    ('2019-02-14T18:32:02.053000Z', 'acd7d986-73ab-4d0b-9ead-377f3a2d646d'),
                    ('2019-02-14T19:19:33.461000Z', 'b9609367-7006-4055-8815-1bad881a1502'),
                    ('2019-02-14T19:19:36.460000Z', '1ab612ca-2a5a-4443-8004-bb5f0f784c67'),
                    ('2019-02-14T19:19:39.469000Z', 'bd1307b9-70b5-49e4-8e02-9d4ca0d64747'),
                    ('2019-02-14T19:19:39.470000Z', '34c64244-d3ed-4841-84b7-aa4cbb9d794b'),
                    ('2019-02-14T19:19:42.465000Z', '234b0359-3853-4df4-898f-5182f698d48b'),
                    ('2019-02-14T19:19:42.465000Z', 'cf3453a3-68fb-4156-bc3e-0f08f7e6512c'),
                    ('2019-02-14T19:19:45.468000Z', '71710439-3864-4fc6-bc48-ca2ac90f7ccf'),
                    ('2019-02-14T19:19:45.468000Z', 'd95392c5-1958-4825-9076-2a9c130c53f3'),
                    ('2019-02-14T19:19:48.464000Z', 'ebb2ec91-2cd0-4ec4-ba2b-5a6d6630bc5a'),
                    ('2019-02-14T19:19:51.465000Z', '2ab5242e-f118-48e3-afe5-c2287fa2e2b1'),
                    ('2019-02-14T19:19:54.466000Z', '6da39577-256d-43fd-97c4-a3bedaa54273'),
                    ('2019-02-14T19:19:54.466000Z', '86a93e19-eb89-4c27-8b64-006f96bb2c83'),
                    ('2019-02-14T19:19:57.464000Z', '0f858ddb-6d93-404e-95fd-0c200921dd40'),
                    ('2019-10-09T15:52:46.609000Z', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a'),
                    ('2019-10-09T15:52:49.512000Z', 'fa17159e-52ec-4a88-80cf-a3be5e2e9988'),
                    (None, '12b25cbd-8cfa-4f0e-818f-d6ba3e823af4'),
                    (None, '65d3d936-ae9d-4a18-a8c7-73ce6132355e'),
                ],
                'lastModifiedDate': [
                    ('2018-10-01T15:40:51.754000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-01T15:42:33.208000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T16:09:56.972000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T16:09:57.110000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-18T20:32:16.894000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-18T20:32:18.864000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-18T20:32:20.845000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:37:28.333000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8'),
                    ('2018-10-18T20:39:10.339000Z', '9b778e46-0c51-4260-8e3f-000ecc145f0a'),
                    ('2018-10-18T20:39:13.335000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:39:16.337000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:39:22.340000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:39:25.337000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:39:40.335000Z', 'af025a74-53f1-4972-b50d-53095b5ffac2'),
                    ('2018-10-18T20:39:55.336000Z', 'e8395271-7c8e-4ec4-9598-495df43fe5fd'),
                    ('2018-10-18T20:39:58.363000Z', '17222e3a-5757-45e9-9dfe-c4b6aa10f28a'),
                    ('2018-10-18T20:39:58.363000Z', '211a8fbf-b190-4576-ac2f-2b1a91743abb'),
                    ('2018-10-18T20:40:01.344000Z', '3c41b5b6-f480-4d47-8c5e-155e7c1adf54'),
                    ('2018-10-18T20:40:13.334000Z', '2fb8a975-b50c-4528-b850-838a19e19a1e'),
                    ('2018-10-18T20:40:54.699000Z', '281c2d08-9e43-47f9-b937-e733e3ba3322'),
                    ('2018-10-18T20:40:55.940000Z', 'ae1d6fa7-964f-465a-8c78-565206827434'),
                    ('2018-10-18T20:40:57.146000Z', 'f518a8cc-e1d9-4fc9-bc32-491dd8543902'),
                    ('2018-10-18T20:45:01.366000Z', '022a217c-384d-4d9d-8631-6397b6838e3a'),
                    ('2018-11-02T10:35:03.810000Z', '70d1af4a-82c8-478a-8960-e9028b3616ca'),
                    ('2018-11-02T10:35:07.705000Z', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
                    ('2018-12-04T16:22:46.380000Z', '12b25cbd-8cfa-4f0e-818f-d6ba3e823af4'),
                    ('2018-12-04T16:22:46.388000Z', '65d3d936-ae9d-4a18-a8c7-73ce6132355e'),
                    ('2019-02-14T18:31:45.892000Z', '7df5d656-43cb-49f9-b81d-86cca3c44a65'),
                    ('2019-02-14T18:31:46.472000Z', 'f9a78d6a-7c80-4c45-bedf-4bc152dc172d'),
                    ('2019-02-14T18:32:02.053000Z', 'acd7d986-73ab-4d0b-9ead-377f3a2d646d'),
                    ('2019-02-14T19:19:33.461000Z', 'b9609367-7006-4055-8815-1bad881a1502'),
                    ('2019-02-14T19:19:36.460000Z', '1ab612ca-2a5a-4443-8004-bb5f0f784c67'),
                    ('2019-02-14T19:19:39.469000Z', 'bd1307b9-70b5-49e4-8e02-9d4ca0d64747'),
                    ('2019-02-14T19:19:39.470000Z', '34c64244-d3ed-4841-84b7-aa4cbb9d794b'),
                    ('2019-02-14T19:19:42.465000Z', '234b0359-3853-4df4-898f-5182f698d48b'),
                    ('2019-02-14T19:19:42.465000Z', 'cf3453a3-68fb-4156-bc3e-0f08f7e6512c'),
                    ('2019-02-14T19:19:45.468000Z', '71710439-3864-4fc6-bc48-ca2ac90f7ccf'),
                    ('2019-02-14T19:19:45.468000Z', 'd95392c5-1958-4825-9076-2a9c130c53f3'),
                    ('2019-02-14T19:19:48.464000Z', 'ebb2ec91-2cd0-4ec4-ba2b-5a6d6630bc5a'),
                    ('2019-02-14T19:19:51.465000Z', '2ab5242e-f118-48e3-afe5-c2287fa2e2b1'),
                    ('2019-02-14T19:19:54.466000Z', '6da39577-256d-43fd-97c4-a3bedaa54273'),
                    ('2019-02-14T19:19:54.466000Z', '86a93e19-eb89-4c27-8b64-006f96bb2c83'),
                    ('2019-02-14T19:19:57.464000Z', '0f858ddb-6d93-404e-95fd-0c200921dd40'),
                    ('2019-10-09T15:52:46.609000Z', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a'),
                    ('2019-10-09T15:52:49.512000Z', 'fa17159e-52ec-4a88-80cf-a3be5e2e9988'),
                ],
                # files have no 'aggregate…Date' values
            },
        }
        for entity_type, fields in test_cases.items():
            for field, direction in product(fields, ['asc', 'desc']):
                with self.subTest(entity_type=entity_type, field=field, direction=direction):
                    expected = fields[field]
                    if direction == 'asc':
                        self.assertEqual(expected,
                                         sorted(expected, key=lambda x: (x[0] is None, x[0])))
                    params = self._params(size=50, sort=field, order=direction)
                    url = self.base_url.set(path=('index', entity_type), args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response_json = response.json()
                    actual = [
                        (dates[field], hit['entryId'])
                        for hit in response_json['hits']
                        for dates in hit['dates']
                    ]
                    expected = fields[field] if direction == 'asc' else fields[field][::-1]
                    self.assertEqual(expected, actual)

    def test_aggregate_date_filter(self):
        """
        Verify the search results can be filtered by the entity and aggregate dates.
        """
        test_cases = {
            'bundles': {
                'submissionDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ],
                'updateDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ],
                'lastModifiedDate': [
                    ('2018-10-03T14:41:37.044509Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:46:55.866661Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:46:55.866661Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ],
                'aggregateSubmissionDate': [
                    ('2018-10-01T14:22:24.370000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-11T21:18:01.605000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-11T21:18:01.605000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ],
                'aggregateUpdateDate': [
                    ('2018-10-01T20:13:06.669000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:45:01.366000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:45:01.366000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ],
                'aggregateLastModifiedDate': [
                    ('2018-10-01T20:13:06.669000Z', 'd0e17014-9a58-4763-9e66-59894efbdaa8'),
                    ('2018-10-18T20:45:01.366000Z', '411cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                    ('2018-10-18T20:45:01.366000Z', '412cd8d5-5990-43cd-84cc-6c7796b8a76d'),
                ]
            },
            'projects': {
                'submissionDate': [
                    ('2018-10-01T14:22:24.370000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:01.605000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:01.605000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ],
                'updateDate': [
                    ('2018-10-01T14:34:10.121000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:06.651000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:06.651000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ],
                'lastModifiedDate': [
                    ('2018-10-01T14:34:10.121000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:06.651000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:06.651000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ],
                'aggregateSubmissionDate': [
                    ('2018-10-01T14:22:24.370000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-11T21:18:01.605000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-11T21:18:01.605000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ],
                'aggregateUpdateDate': [
                    ('2018-10-01T20:13:06.669000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-18T20:45:01.366000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-18T20:45:01.366000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ],
                'aggregateLastModifiedDate': [
                    ('2018-10-01T20:13:06.669000Z', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
                    ('2018-10-18T20:45:01.366000Z', '2c4724a4-7252-409e-b008-ff5c127c7e89'),
                    ('2018-10-18T20:45:01.366000Z', '2c5724a4-7252-409e-b008-ff5c127c7e89'),
                ]
            },
            'samples': {
                'submissionDate': [
                    ('2018-10-01T14:22:25.143000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:02.654000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                    ('2018-10-11T21:18:02.696000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:02.732000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:02.785000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                ],
                'updateDate': [
                    ('2018-10-01T14:57:17.976000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:06.725000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                    ('2018-10-11T21:18:06.730000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:12.763000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:12.864000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                ],
                'lastModifiedDate': [
                    ('2018-10-01T14:57:17.976000Z', '79682426-b813-4f69-8c9c-2764ffac5dc1'),
                    ('2018-10-11T21:18:06.725000Z', '73f10dad-afc5-4d1d-a71c-4a8b6fff9172'),
                    ('2018-10-11T21:18:06.730000Z', '308eea51-d14b-4036-8cd1-cfd81d7532c3'),
                    ('2018-10-11T21:18:12.763000Z', 'b7214641-1ac5-4f60-b795-cb33a7c25434'),
                    ('2018-10-11T21:18:12.864000Z', '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'),
                ],
                # samples have no 'aggregate…Date' values
            },
            'files': {
                'submissionDate': [
                    ('2018-10-01T14:22:24.380000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-01T14:22:24.389000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T14:22:24.511000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T14:22:24.755000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-11T21:18:01.623000Z', '281c2d08-9e43-47f9-b937-e733e3ba3322'),
                    ('2018-10-11T21:18:01.642000Z', 'ae1d6fa7-964f-465a-8c78-565206827434'),
                    ('2018-10-11T21:18:01.654000Z', 'f518a8cc-e1d9-4fc9-bc32-491dd8543902'),
                    ('2018-10-11T21:18:01.964000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-11T21:18:01.979000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-11T21:18:01.990000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:32:25.801000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:32:25.877000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:32:25.951000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:32:26.026000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:32:26.097000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8')
                ],
                'updateDate': [
                    ('2018-10-01T15:40:51.754000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-01T15:42:33.208000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T16:09:56.972000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T16:09:57.110000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-18T20:32:16.894000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-18T20:32:18.864000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-18T20:32:20.845000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:37:28.333000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8'),
                    ('2018-10-18T20:39:10.339000Z', '9b778e46-0c51-4260-8e3f-000ecc145f0a'),
                    ('2018-10-18T20:39:13.335000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:39:16.337000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:39:22.340000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:39:25.337000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:39:40.335000Z', 'af025a74-53f1-4972-b50d-53095b5ffac2'),
                    ('2018-10-18T20:39:55.336000Z', 'e8395271-7c8e-4ec4-9598-495df43fe5fd')
                ],
                'lastModifiedDate': [
                    ('2018-10-01T15:40:51.754000Z', '80036f72-7fde-46e9-821b-17dbbe0509bb'),
                    ('2018-10-01T15:42:33.208000Z', '042dce4a-003b-492b-9371-e1897f52d8d9'),
                    ('2018-10-01T16:09:56.972000Z', '300ee490-edca-46b1-b23d-c9458ebb9c6e'),
                    ('2018-10-01T16:09:57.110000Z', '665b4341-9950-4e59-a401-e4a097256f1e'),
                    ('2018-10-18T20:32:16.894000Z', '213381ea-6161-4159-853e-cfcae4968001'),
                    ('2018-10-18T20:32:18.864000Z', '9ee3da9e-83ca-4c02-84d6-ac09702b12ba'),
                    ('2018-10-18T20:32:20.845000Z', '330a08ca-ae8e-4f1f-aa03-970abcd27f39'),
                    ('2018-10-18T20:37:28.333000Z', 'fd16b62e-e540-4f03-8ba0-07d0c204e3c8'),
                    ('2018-10-18T20:39:10.339000Z', '9b778e46-0c51-4260-8e3f-000ecc145f0a'),
                    ('2018-10-18T20:39:13.335000Z', 'cf93f747-1392-4670-8eb3-3ac60a96855e'),
                    ('2018-10-18T20:39:16.337000Z', '477c0b3e-4a06-4214-8f27-58199ba63528'),
                    ('2018-10-18T20:39:22.340000Z', '50be9b67-fae5-4472-9719-478dd1303d6e'),
                    ('2018-10-18T20:39:25.337000Z', 'ad6d5170-d74b-408c-af6b-25a14315c9da'),
                    ('2018-10-18T20:39:40.335000Z', 'af025a74-53f1-4972-b50d-53095b5ffac2'),
                    ('2018-10-18T20:39:55.336000Z', 'e8395271-7c8e-4ec4-9598-495df43fe5fd')
                ],
                # files have no 'aggregate…Date' values
            },
        }
        for entity_type, fields in test_cases.items():
            for field, expected in fields.items():
                with self.subTest(entity_type=entity_type, field=field):
                    filters = {
                        field: {
                            'within': [
                                [
                                    '2018-10-01T00:00:00.000000Z',
                                    '2018-11-01T00:00:00.000000Z'
                                ]
                            ]
                        }
                    }
                    params = self._params(filters=filters, size=15, sort=field, order='asc')
                    url = self.base_url.set(path=('index', entity_type), args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response_json = response.json()
                    actual = [
                        (dates[field], hit['entryId'])
                        for hit in response_json['hits']
                        for dates in hit['dates']
                    ]
                    self.assertEqual(expected, actual)

    def test_disease_facet(self):
        """
        Verify the values of the different types of disease facets
        """
        test_data = {
            # disease specified in donor, specimen, and sample (the specimen)
            '627cb0ba-b8a1-405a-b58f-0add82c3d635': {
                'sampleDisease': [{'term': 'H syndrome', 'count': 1}],
                'donorDisease': [{'term': 'H syndrome', 'count': 1}],
                'specimenDisease': [{'term': 'H syndrome', 'count': 1}],
            },
            # disease specified in donor only
            '250aef61-a15b-4d97-b8b4-54bb997c1d7d': {
                'sampleDisease': [{'term': None, 'count': 1}],
                'donorDisease': [{'term': 'isolated hip osteoarthritis', 'count': 1}],
                'specimenDisease': [{'term': None, 'count': 1}],
            },
            # disease specified in donor and specimen, not in sample (the cell line)
            'c765e3f9-7cfc-4501-8832-79e5f7abd321': {
                'sampleDisease': [{'term': None, 'count': 1}],
                'donorDisease': [{'term': 'normal', 'count': 1}],
                'specimenDisease': [{'term': 'normal', 'count': 1}]
            }
        }
        self._assert_term_facets(test_data,
                                 str(self.base_url.set(path='/index/projects')))

    def test_contentDescription_facet(self):
        url = self.base_url.set(path='/index/projects')
        test_data = {
            '88ec040b-8705-4f77-8f41-f81e57632f7d': {
                'contentDescription': [{'term': 'RNA sequence', 'count': 1}]
            }
        }
        self._assert_term_facets(test_data, str(url))

    def _assert_term_facets(self, project_term_facets: JSON, url: str) -> None:
        for project_id, term_facets in project_term_facets.items():
            with self.subTest(project_id=project_id):
                params = self._params(filters={'projectId': {'is': [project_id]}})
                response = requests.get(url, params=params)
                response.raise_for_status()
                response_json = response.json()
                actual_term_facets = response_json['termFacets']
                for facet, terms in term_facets.items():
                    self.assertEqual(actual_term_facets[facet]['terms'], terms)

    def test_organism_age_facet(self):
        """
        Verify the terms of the organism age facet
        """
        test_data = {
            # This project has one donor organism
            '627cb0ba-b8a1-405a-b58f-0add82c3d635': {
                'organismAge': [
                    {
                        'term': {
                            'value': '20',
                            'unit': 'year'
                        },
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': 'year',
                        'count': 1
                    }
                ]
            },
            # This project has multiple donor organisms
            '2c4724a4-7252-409e-b008-ff5c127c7e89': {
                'organismAge': [
                    {
                        'term': {
                            'value': '40-44',
                            'unit': 'year'
                        },
                        'count': 1
                    },
                    {
                        'term': {
                            'value': '55-59',
                            'unit': 'year'
                        },
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': 'year',
                        'count': 1
                    }
                ]
            },
            # This project has one donor but donor has no age
            'c765e3f9-7cfc-4501-8832-79e5f7abd321': {
                'organismAge': [
                    {
                        'term': None,
                        'count': 1
                    }
                ],
                'organismAgeUnit': [
                    {
                        'term': None,
                        'count': 1
                    }
                ]
            }
        }
        self._assert_term_facets(test_data,
                                 str(self.base_url.set(path='/index/projects')))

    def test_organism_age_facet_search(self):
        """
        Verify filtering by organism age
        """
        test_cases = [
            (
                '627cb0ba-b8a1-405a-b58f-0add82c3d635',
                {
                    'is': [
                        {
                            'value': '20',
                            'unit': 'year'
                        }
                    ]
                }
            ),
            (
                'c765e3f9-7cfc-4501-8832-79e5f7abd321',
                {
                    'is': [
                        None
                    ]
                }
            ),
            (
                None,
                {
                    'is': [
                        {}
                    ]
                }
            ),
            (
                None,
                {
                    'is': [
                        {
                            'value': None,
                            'unit': 'weeks'
                        }
                    ]
                }
            )
        ]
        for project_id, filters in test_cases:
            with self.subTest(filters=filters):
                url = self.base_url.set(path='/index/projects',
                                        args=dict(catalog=self.catalog,
                                                  filters=json.dumps({'organismAge': filters})))
                response = requests.get(str(url))
                if project_id is None:
                    self.assertTrue(response.status_code, 400)
                else:
                    response.raise_for_status()
                    response = response.json()
                    hit = one(response['hits'])
                    self.assertEqual(hit['entryId'], project_id)
                    donor_organism = one(hit['donorOrganisms'])
                    age = one(one(filters.values()))
                    self.assertEqual(donor_organism['organismAge'],
                                     [None if age is None else age])

    def test_pagination_search_after_search_before(self):
        """
        Test search_after and search_before values when using sorting on a field containing None values
        """
        params = self._params(size=3, sort='workflow', order='asc')
        url = self.base_url.set(path='/index/samples', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        first_page_next = parse_url_qs(response_json['pagination']['next'])

        expected_entry_ids = [
            '58c60e15-e07c-4875-ac34-f026d6912f1c',
            '195b2621-ec05-4618-9063-c56048de97d1',
            '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d',
        ]
        self.assertEqual(expected_entry_ids, [h['entryId'] for h in response_json['hits']])

        # NOTE: The sort field `workflow` is an `analysis_protocol` field and
        # does not exist in all bundles. This is why the `search_after` field
        # has the value `null` (JSON representation of `None`) because the last
        # row in this page of results does not have an `analysis_protocol` or
        # `workflow` field. If the last row did have a `workflow` field with a
        # value `None`, `search_after` would be a translated `None` (`"~null"`)
        self.assertIsNotNone(response_json['pagination']['next'])
        self.assertIsNone(response_json['pagination']['previous'])
        self.assertEqual([None, '2d8282f0-6cbb-4d5a-822c-4b01718b4d0d'],
                         json.loads(first_page_next['search_after']))

        response = requests.get(response_json['pagination']['next'])
        response.raise_for_status()
        response_json = response.json()
        second_page_next = parse_url_qs(response_json['pagination']['next'])
        second_page_previous = parse_url_qs(response_json['pagination']['previous'])

        expected_entry_ids = [
            '308eea51-d14b-4036-8cd1-cfd81d7532c3',
            '73f10dad-afc5-4d1d-a71c-4a8b6fff9172',
            '79682426-b813-4f69-8c9c-2764ffac5dc1',
        ]
        self.assertEqual(expected_entry_ids, [h['entryId'] for h in response_json['hits']])

        self.assertEqual([None, '79682426-b813-4f69-8c9c-2764ffac5dc1'],
                         json.loads(second_page_next['search_after']))
        self.assertEqual([None, '308eea51-d14b-4036-8cd1-cfd81d7532c3'],
                         json.loads(second_page_previous['search_before']))

    def test_filter_by_publication_title(self):
        cases = [
            (
                'Systematic comparative analysis of single cell RNA-sequencing methods',
                {
                    'a8b8479d-cfa9-4f74-909f-49552439e698',
                    '7338932d-edc9-49a9-8dbf-e459a465800f'
                }

            ),
            (
                'Palantir characterizes cell fate continuities in human hematopoiesis',
                set()
            )
        ]
        for title, expected_files in cases:
            with self.subTest(title=title):
                filters = {
                    'publicationTitle': {
                        'is': [title]
                    }
                }
                expected_terms = {
                    'terms': [
                        {
                            'term': None,
                            'count': 25
                        },
                        {
                            'term': 'A title of a publication goes here.',
                            'count': 16
                        },
                        {
                            'term': 'Single-Cell Analysis of Human Pancreas Reveals Transcriptional '
                                    'Signatures of Aging and Somatic Mutation Patterns.',
                            'count': 2
                        },
                        {
                            'term': 'Systematic comparative analysis of single cell RNA-sequencing methods',
                            'count': 2
                        }
                    ],
                    'total': 45,
                    'type': 'terms'
                }
                url = self.base_url.set(path='/index/files',
                                        args=dict(filters=json.dumps(filters)))
                response = requests.get(str(url))
                self.assertEqual(200, response.status_code)
                self.assertEqual(expected_terms,
                                 response.json()['termFacets']['publicationTitle'])
                files = {
                    one(hit['files'])['uuid']
                    for hit in response.json()['hits']
                }
                self.assertEqual(expected_files, files)

    def test_access(self):
        filtered_entity_types = {
            'bundles': True,
            'files': True,
            'samples': True,
            'projects': False
        }

        def _test(entity_type: str, expect_empty: bool, expect_accessible: bool):
            with self.subTest(entity_type=entity_type, access=expect_accessible):
                url = str(self.base_url.set(path=('index', entity_type)))
                response = requests.get(url)
                self.assertEqual(200, response.status_code)
                hits = response.json()['hits']
                if expect_empty:
                    self.assertEqual([], hits)
                else:
                    self.assertGreater(len(hits), 0)
                    for hit in hits:
                        entity = one(hit[entity_type])
                        self.assertEqual(expect_accessible, entity['accessible'])

        for entity_type in filtered_entity_types:
            _test(entity_type, expect_empty=False, expect_accessible=True)

        with mock.patch('azul.plugins.repository.dss.Plugin.sources', return_value=[]):
            for entity_type, is_filtered in filtered_entity_types.items():
                _test(entity_type, expect_empty=is_filtered, expect_accessible=False)

    def test_filter_by_accession(self):
        def request_accessions(nested_properties):
            params = self._params(filters={
                'accessions': {
                    'is': [nested_properties]
                }
            })
            url = self.base_url.set(path='/index/projects')
            response = requests.get(str(url), params=params)
            self.assertEqual(200, response.status_code)
            return response.json()

        for nested_properties, expected_projects in [
            (
                dict(namespace='array_express', accession='E-AAAA-00'),
                {'627cb0ba-b8a1-405a-b58f-0add82c3d635'}
            ),
            (
                dict(namespace='geo_series', accession='GSE132044'),
                {'88ec040b-8705-4f77-8f41-f81e57632f7d'}
            ),
            (
                dict(accession='GSE132044'),
                {'88ec040b-8705-4f77-8f41-f81e57632f7d'}
            ),
            (
                dict(namespace='geo_series'),
                {
                    '627cb0ba-b8a1-405a-b58f-0add82c3d635',
                    '88ec040b-8705-4f77-8f41-f81e57632f7d'
                }
            )
        ]:
            with self.subTest(nested_properties=nested_properties):
                response = request_accessions(nested_properties)
                actual_projects = {
                    one(hit['projects'])['projectId']
                    for hit in response['hits']
                }
                self.assertEqual(expected_projects, actual_projects)
                for hits in response['hits']:
                    accession_properties = [
                        {key: value}
                        for accession in one(hits['projects'])['accessions']
                        for key, value in accession.items()
                    ]
                    for key, value in nested_properties.items():
                        self.assertIn({key: value}, accession_properties)


@patch_dss_source
@patch_source_cache
class TestFileTypeSummaries(WebServiceTestCase):

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            cls.bundle_fqid(uuid='fce68057-b0f0-5d11-b9a7-30e8fa3259a8',
                            version='2021-02-09T01:30:00.000000Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_grouping(self):
        url = self.base_url.set(path='/index/projects')
        filters = {'projectId': {'is': ['f2fe82f0-4454-4d84-b416-a885f3121e59']}}
        params = {
            'catalog': self.catalog,
            'filters': json.dumps(filters)
        }
        response = requests.get(str(url), params=params)
        response.raise_for_status()
        response_json = response.json()
        file_type_summaries = one(response_json['hits'])['fileTypeSummaries']
        expected = [
            {
                'format': 'fastq.gz',
                'count': 117,
                'totalSize': 1670420872710.0,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': ['DNA sequence'],
                'fileSource': [None],
            },
            {
                'format': 'fastq.gz',
                'count': 3,
                'totalSize': 128307505318.0,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': ['Cellular Genetics'],
                'fileSource': [None],
            },
            {
                'format': 'loom',
                'count': 40,
                'totalSize': 59207580244.0,
                'matrixCellCount': None,
                'isIntermediate': True,
                'contentDescription': ['Count Matrix'],
                'fileSource': ['DCP/2 Analysis'],
            },
            {
                'format': 'loom',
                'count': 1,
                'totalSize': 5389602923.0,
                'matrixCellCount': None,
                'isIntermediate': False,
                'contentDescription': ['Count Matrix'],
                'fileSource': ['DCP/2 Analysis'],
            },
            {
                'format': 'bam',
                'count': 40,
                'totalSize': 1659270110045.0,
                'matrixCellCount': None,
                'isIntermediate': None,
                'contentDescription': [None],
                'fileSource': ['DCP/2 Analysis'],
            },
        ]
        self.assertElasticEqual(file_type_summaries, expected)


@patch_dss_source
@patch_source_cache
class TestResponseInnerEntitySamples(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            # A bundle with 1 specimen and 1 cell line sample entities
            cls.bundle_fqid(uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                            version='2019-10-03T10:55:24.911627Z'),
            # A bundle with 4 organoid sample entities
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T20:46:55.866661Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_inner_entity_samples(self):
        """
        Verify aggregated 'samples' inner entities are grouped by sample entity
        type and that results can be filtered by sampleEntityType.
        """
        # Note that this test is against the /index/projects endpoint, so when
        # a filter such as {'sampleEntityType':{'is':['cell_lines']}} is used
        # and finds a project with both cell line and specimen samples, the
        # response hit 'samples' inner entity (an aggregate grouped by entity
        # type) will contain both samples.
        expected_filter_hits = {
            'cell_lines': [
                # One hit for a project with 2 different type samples
                [
                    {
                        'sampleEntityType': ['cellLines'],
                        'effectiveOrgan': ['immune system'],
                        'id': ['Cell_line_2'],
                        'cellLineType': ['primary'],
                        'modelOrgan': ['immune system'],
                    },
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['embryo'],
                        'id': ['Specimen1'],
                        'organ': ['embryo'],
                        'organPart': ['skin epidermis'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                    },
                ]
            ],
            'organoids': [
                # One hit for a project with 4 samples of the same type
                [
                    {
                        'sampleEntityType': ['organoids'],
                        'effectiveOrgan': ['Brain'],
                        'id': [
                            'Org_HPSI0214i-kucg_2_2',
                            'Org_HPSI0214i-wibj_2_2',
                            'Org_HPSI0314i-hoik_1_2',
                            'Org_HPSI0314i-sojd_3_2',
                        ],
                        'modelOrgan': ['Brain'],
                        'modelOrganPart': [None],
                    }
                ]
            ],
            'specimens': [
                # Two hits, one for a project with 2 different type samples and
                # one for a project with 1 sample
                [
                    {
                        'sampleEntityType': ['cellLines'],
                        'effectiveOrgan': ['immune system'],
                        'id': ['Cell_line_2'],
                        'cellLineType': ['primary'],
                        'modelOrgan': ['immune system'],
                    },
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['embryo'],
                        'id': ['Specimen1'],
                        'organ': ['embryo'],
                        'organPart': ['skin epidermis'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                    },
                ],
                [
                    {
                        'sampleEntityType': ['specimens'],
                        'effectiveOrgan': ['pancreas'],
                        'id': ['DID_scRSq06_pancreas'],
                        'organ': ['pancreas'],
                        'organPart': ['islet of Langerhans'],
                        'disease': ['normal'],
                        'preservationMethod': [None],
                        'source': ['specimen_from_organism'],
                    }
                ],
            ],
        }
        for entity_type, expected_hits in expected_filter_hits.items():
            with self.subTest(entity_type=entity_type):
                params = {
                    'filters': json.dumps({'sampleEntityType': {'is': [entity_type]}}),
                    'catalog': self.catalog,
                    'size': 5,
                    'sort': 'projectTitle',
                    'order': 'asc',
                }
                url = self.base_url.set(path='/index/projects', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                hits = response_json['hits']
                self.assertEqual(expected_hits, [hit['samples'] for hit in hits])


@patch_dss_source
@patch_source_cache
class TestSchemaTestDataCannedBundle(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return [
            # Bundles for project 90bf705c
            # https://github.com/HumanCellAtlas/schema-test-data/tree/2a62a7f4
            cls.bundle_fqid(uuid='1f6afb64-fa14-5c6f-a474-a742540108a3',
                            version='2021-01-01T00:00:00.000000Z'),
            cls.bundle_fqid(uuid='3ac62c33-93e1-56b4-b857-59497f5d942d',
                            version='2021-01-01T00:00:00.000000Z'),
            cls.bundle_fqid(uuid='4da04038-adab-59a9-b6c4-3a61242cc972',
                            version='2021-01-01T00:00:00.000000Z'),
            cls.bundle_fqid(uuid='8c1773c3-1885-545f-9381-0dab1edf6074',
                            version='2021-01-01T00:00:00.000000Z'),
            cls.bundle_fqid(uuid='d7b8cbff-aee9-5a05-a4a1-d8f4e720aee7',
                            version='2021-01-01T00:00:00.000000Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_project_cell_count(self):
        """
        Verify the project 'estimatedCellCount' value across the various endpoints
        """
        expected_cell_counts = {
            'files': [10000] * 7,
            'samples': [10000] * 3,
            'projects': [10000],
            'bundles': [10000] * 5,
        }
        params = {'catalog': self.catalog}
        for entity_type in expected_cell_counts.keys():
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type), args=params)
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()
                actual_cell_counts = []
                for hit in response_json['hits']:
                    project = one(hit['projects'])
                    actual_cell_counts.append(project['estimatedCellCount'])
                self.assertEqual(expected_cell_counts[entity_type],
                                 actual_cell_counts)

    def test_summary_cell_counts(self):
        url = self.base_url.set(path='/index/summary',
                                args=dict(catalog=self.catalog))
        response = requests.get(str(url))
        response.raise_for_status()
        summary = response.json()
        self.assertEqual(1, summary['projectCount'])
        self.assertEqual(7, summary['fileCount'])
        expected_summary_cell_counts = [
            {
                'organType': ['blood'],
                'countOfDocsWithOrganType': 2,
                'totalCellCountByOrgan': 20000.0 + 20000.0
            }
        ]
        self.assertEqual(expected_summary_cell_counts, summary['cellCountSummaries'])

    def test_protocols(self):
        """
        Verify the protocol fields
        """
        params = {'catalog': self.catalog}
        url = self.base_url.set(path='/index/projects', args=params)
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        hit = one(response_json['hits'])
        expected_protocols = [
            # analysis protocol
            {
                'workflow': [
                    'Combined_AnalysisProt',
                    'Visiumanalysis'
                ]
            },
            # imaging protocol
            {
                'assayType': []
            },
            # library preparation protocol
            {
                'libraryConstructionApproach': [
                    "10x 3' v3",
                    'Visium Spatial Gene Expression'
                ],
                'nucleicAcidSource': [
                    'single cell',
                    'single nucleus'
                ]
            },
            # sequencing protocol
            {
                'instrumentManufacturerModel': [
                    'EFO_0008637'
                ],
                'pairedEnd': [
                    True
                ]
            }
        ]
        self.assertEqual(expected_protocols, hit['protocols'])


@attr.s(auto_attribs=True, frozen=True)
class CellCounts:
    estimated_cell_count: Optional[int]
    total_cells: Dict[str, Optional[int]]

    @classmethod
    def from_response(cls, hit: JSON) -> 'CellCounts':
        return cls(estimated_cell_count=one(hit['projects'])['estimatedCellCount'],
                   total_cells={
                       one(cell_suspension['organ']): cell_suspension['totalCells']
                       for cell_suspension in hit['cellSuspensions']
                   })


@patch_dss_source
@patch_source_cache
class TestSortAndFilterByCellCount(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return [
            # Two bundles for the same project with 7738 total cell suspension cells
            # project=4e6f083b, cs-cells=3869, p-cells=None
            cls.bundle_fqid(uuid='97f0cc83-f0ac-417a-8a29-221c77debde8',
                            version='2019-10-14T19:54:15.397406Z'),
            # project=4e6f083b, cs-cells=3869, p-cells=None
            cls.bundle_fqid(uuid='8c90d4fe-9a5d-4e3d-ada2-0414b666b880',
                            version='2019-10-14T19:54:15.397546Z'),
            # A bundle with cell suspension cell counts
            # project=627cb0ba, cs-cells=10000, p-cells=None
            cls.bundle_fqid(uuid='fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a',
                            version='2019-02-14T19:24:38.034764Z'),
            # A bundle with cell suspension cell counts
            # project=2c4724a4, cs-cells=6210, p-cells=None
            cls.bundle_fqid(uuid='411cd8d5-5990-43cd-84cc-6c7796b8a76d',
                            version='2018-10-18T20:46:55.866661Z'),
            # A bundle with project cell counts
            # project=50151324, cs-cells=None, p-cells=88000
            cls.bundle_fqid(uuid='2c7d06b8-658e-4c51-9de4-a768322f84c5',
                            version='2021-09-21T17:27:23.898000Z'),
            # A bundle with project & cell suspension cell counts
            # project=2d846095, cs-cells=1, p-cells=3589
            cls.bundle_fqid(uuid='80baee6e-00a5-4fdc-bfe3-d339ff8a7178',
                            version='2021-03-12T22:43:32.330000Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_sorting_by_cell_count(self):
        """
        Verify sorting projects by the total cell suspension cell count, the
        project cell count, and the effective cell count.
        """
        test_cases = {
            'cellCount': [
                CellCounts(88_000, {'mouth mucosa': None}),
                CellCounts(3589, {'brain': 1}),
                CellCounts(None, {'Brain': 6210}),
                CellCounts(None, {'presumptive gut': 3869, 'endoderm': 3869}),
                CellCounts(None, {'brain': 10_000}),
            ],
            'projectEstimatedCellCount': [
                CellCounts(3589, {'brain': 1}),
                CellCounts(88_000, {'mouth mucosa': None}),
                CellCounts(None, {'Brain': 6210}),
                CellCounts(None, {'presumptive gut': 3869, 'endoderm': 3869}),
                CellCounts(None, {'brain': 10_000}),
            ],
            'effectiveCellCount': [
                CellCounts(3589, {'brain': 1}),
                CellCounts(None, {'Brain': 6210}),
                CellCounts(None, {'presumptive gut': 3869, 'endoderm': 3869}),
                CellCounts(None, {'brain': 10_000}),
                CellCounts(88_000, {'mouth mucosa': None}),
            ]
        }
        for ascending in False, True:
            for field, expected in test_cases.items():
                with self.subTest(facet=field, ascending=ascending):
                    params = {
                        'catalog': self.catalog,
                        'sort': field,
                        'order': 'asc' if ascending else 'desc'
                    }
                    url = self.base_url.set(path='/index/projects', args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response = response.json()
                    actual = list(map(CellCounts.from_response, response['hits']))
                    if not ascending:
                        expected = list(reversed(expected))
                    self.assertEqual(expected, actual)

    def test_filter_by_cell_count(self):
        """
        Verify filtering projects by the total cell suspension cell count, the
        project cell count, and the effective cell count.
        """
        test_cases = {
            'cellCount': {
                None: [],
                6210: [
                    CellCounts(None, {'Brain': 6210}),
                ],
                (3000, 8000): [
                    CellCounts(None, {'Brain': 6210}),
                    CellCounts(None, {'presumptive gut': 3869, 'endoderm': 3869}),
                ],
            },
            'projectEstimatedCellCount': {
                None: [
                    CellCounts(None, {'Brain': 6210}),
                    CellCounts(None, {'presumptive gut': 3869, 'endoderm': 3869}),
                    CellCounts(None, {'brain': 10_000})
                ],
                3589: [
                    CellCounts(3589, {'brain': 1}),
                ],
                (6000, 100_000): [
                    CellCounts(88_000, {'mouth mucosa': None}),
                ],
            },
            'effectiveCellCount': {
                None: [],
                10_000: [
                    CellCounts(None, {'brain': 10_000})
                ],
                (3000, 7000): [
                    CellCounts(3589, {'brain': 1}),
                    CellCounts(None, {'Brain': 6210}),
                ],
            },
        }
        for field, test_case in test_cases.items():
            for filter, expected in test_case.items():
                with self.subTest(facet=field, value=filter):
                    filters = {
                        field:
                            {'within': [filter]}
                            if isinstance(filter, tuple) else
                            {'is': [filter]}
                    }
                    params = {
                        'catalog': self.catalog,
                        'sort': field,
                        'order': 'asc',
                        'filters': json.dumps(filters)
                    }
                    url = self.base_url.set(path='/index/projects', args=params)
                    response = requests.get(str(url))
                    response.raise_for_status()
                    response = response.json()
                    actual = list(map(CellCounts.from_response, response['hits']))
                    self.assertEqual(actual, expected)


@patch_dss_source
@patch_source_cache
class TestProjectMatrices(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return super().bundles() + [
            # A hacky CGM subgraph (project 8185730f)
            # 8 supplementary file CGMs each with a 'submitter_id'
            cls.bundle_fqid(uuid='4b03c1ce-9df1-5cd5-a8e4-48a2fe095081',
                            version='2021-02-10T16:56:40.419579Z'),
            # A hacky DCP/1 matrix service subgraph (project 8185730f)
            # 3 supplementary file matrices each with a 'submitter_id'
            cls.bundle_fqid(uuid='8338b891-f3fa-5e7b-885f-e4ee5689ee15',
                            version='2020-12-03T10:39:17.144517Z'),
            # An intermediate DCP/2 analysis subgraph (project 8185730f)
            # 1 intermediate analysis file matrix
            cls.bundle_fqid(uuid='7eb74d9f-8346-5420-b7e4-b486f99451a8',
                            version='2020-02-03T10:30:00.000000Z'),
            # A top-level DCP/2 analysis subgraph (project 8185730f)
            # 1 analysis file matrix with a 'submitter_id'
            cls.bundle_fqid(uuid='00f48893-5e9d-52cd-b32d-af88edccabfa',
                            version='2020-02-03T10:30:00.000000Z'),
            # An organic CGM subgraph (project bd400331)
            # 2 analysis file CGMs each with a 'file_source'
            cls.bundle_fqid(uuid='04836733-0449-4e57-be2e-6f3b8fbdfb12',
                            version='2021-05-10T23:25:12.412000Z')
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def params(self,
               project_id: str,
               facet: Optional[str] = None,
               value: Optional[str] = None) -> JSON:
        return {
            'filters': json.dumps(
                {
                    'projectId': {'is': [project_id]},
                    **({facet: {'is': [value]}} if facet else {})
                }
            ),
            'catalog': self.catalog,
            'size': 500
        }

    def test_file_source_facet(self):
        """
        Verify the 'fileSource' facet is populated with the human-readable
        versions of the name used to generate the 'submitter_id' UUID, and that
        the facet values match the hits[].files[].fileSource values.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']
        expected_counts = {
            'DCP/2 Analysis': 3,
            'DCP/1 Matrix Service': 3,
            'HCA Release': 1,
            'ArrayExpress': 7
        }
        expected_facets = [
            {'term': key, 'count': val}
            for key, val in expected_counts.items()
        ]
        self.assertElasticEqual(expected_facets, facets['fileSource']['terms'])
        actual_counts = Counter()
        for hit in response_json['hits']:
            file = one(hit['files'])
            actual_counts[file['fileSource']] += 1
        self.assertEqual(expected_counts, actual_counts)

    def test_is_intermediate_facet(self):
        """
        Verify the 'isIntermediate' facet.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        facets = response_json['termFacets']
        expected = [
            {'term': None, 'count': 1},
            {'term': 'false', 'count': 12},
            {'term': 'true', 'count': 1}
        ]
        self.assertElasticEqual(expected, facets['isIntermediate']['terms'])

    def test_contributor_matrix_files(self):
        """
        Verify the files endpoint returns all the files from both the analysis
        and CGM bundles, and that supplementary file matrices can be found by
        their stratification values.
        """
        expected = {
            (None, None): [
                # 8 supplementary files from bundle 4b03c1ce
                'E-MTAB-7316.processed.1.zip',
                'E-MTAB-7316.processed.2.zip',
                'E-MTAB-7316.processed.3.zip',
                'E-MTAB-7316.processed.4.zip',
                'E-MTAB-7316.processed.5.zip',
                'E-MTAB-7316.processed.6.zip',
                'E-MTAB-7316.processed.7.zip',
                'WongRetinaCelltype.csv',
                # 3 supplementary files from bundle 8338b891
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip',
                # 2 analysis files from bundle 7eb74d9f
                '1116b396-448e-4dd1-b9c9-78357c511e15.bam',
                '1116b396-448e-4dd1-b9c9-78357c511e15.loom',
                # 1 analysis file from bundle 00f48893
                'wong-retina-human-eye-10XV2.loom'
            ],
            ('developmentStage', 'adult'): [
                'E-MTAB-7316.processed.1.zip',
                'E-MTAB-7316.processed.2.zip',
                'E-MTAB-7316.processed.3.zip',
                'E-MTAB-7316.processed.4.zip',
                'E-MTAB-7316.processed.5.zip',
                'E-MTAB-7316.processed.6.zip',
                'E-MTAB-7316.processed.7.zip',
                'WongRetinaCelltype.csv',
            ],
            ('developmentStage', 'human adult stage'): [
                '1116b396-448e-4dd1-b9c9-78357c511e15.bam',
                '1116b396-448e-4dd1-b9c9-78357c511e15.loom',
                'wong-retina-human-eye-10XV2.loom'
            ],
            ('developmentStage', None): [
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip'
            ],
        }
        for (facet, value), expected_files in expected.items():
            with self.subTest(facet=facet, value=value):
                params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b',
                                     facet=facet,
                                     value=value)
                url = self.base_url.set(path='/index/files', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                actual_files = [one(hit['files'])['name'] for hit in response_json['hits']]
                self.assertEqual(sorted(expected_files), sorted(actual_files))

    def test_matrices_tree(self):
        """
        Verify the projects endpoint includes a valid 'matrices' and
        'contributedAnalyses' tree inside the projects inner-entity.
        """
        params = self.params(project_id='8185730f-4113-40d3-9cc3-929271784c2b')
        url = self.base_url.set(path='/index/projects', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        hit = one(response_json['hits'])
        self.assertEqual('8185730f-4113-40d3-9cc3-929271784c2b', hit['entryId'])
        matrices = {
            'genusSpecies': {
                'Homo sapiens': {
                    'developmentStage': {
                        'human adult stage': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Analysis file, source from submitter_id
                                                'name': 'wong-retina-human-eye-10XV2.loom',
                                                'size': 255471211,
                                                'fileSource': 'DCP/2 Analysis',
                                                'matrixCellCount': None,
                                                'uuid': 'bd98f428-881e-501a-ac16-24f27a68ce2f',
                                                'version': '2021-02-11T23:11:45.000000Z',
                                                'contentDescription': ['Count Matrix'],
                                                'format': 'loom',
                                                'isIntermediate': False,
                                                'sha256': '6a6483c2e78da77017e912a4d350f141'
                                                          'bda1ec7b269f20ca718b55145ee5c83c',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/bd98f428-881e-501a-ac16-24f27a68ce2f',
                                                    args=dict(catalog='test', version='2021-02-11T23:11:45.000000Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        'Unspecified': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                                                'size': 76742835,
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrixCellCount': None,
                                                'uuid': '538faa28-3235-5e4b-a998-5672e2d964e8',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': 'edb8e0139fece9702d89ae5fe7f761c4'
                                                          '1c291ef6a71129c6420857e025228a24',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/538faa28-3235-5e4b-a998-5672e2d964e8',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip',
                                                'size': 124022765,
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrixCellCount': None,
                                                'uuid': '6c142250-567c-5b63-bd4f-0d78499863f8',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': 'cb1467f4d23a2429b4928943b51652b3'
                                                          '2edb949099250d28cf400d13074f5440',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/6c142250-567c-5b63-bd4f-0d78499863f8',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                                                'size': 154980798,
                                                'fileSource': 'DCP/1 Matrix Service',
                                                'matrixCellCount': None,
                                                'uuid': '8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                                'version': '2020-12-03T10:39:17.144517Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'loom',
                                                'isIntermediate': False,
                                                'sha256': '724b2c0ddf33c662b362179bc6ca90cd'
                                                          '866b99b340d061463c35d27cfd5a23c5',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                                    args=dict(catalog='test', version='2020-12-03T10:39:17.144517Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertElasticEqual(matrices, one(hit['projects'])['matrices'])
        contributed_analyses = {
            'genusSpecies': {
                'Homo sapiens': {
                    'developmentStage': {
                        'adult': {
                            'organ': {
                                'eye': {
                                    'libraryConstructionApproach': {
                                        '10X v2 sequencing': [
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.1.zip',
                                                'size': 69813802,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': '87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': '331bd925c08539194eb06e197a1238e1'
                                                          '306c3b7876b6fe13548d03824cc4b68b',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.2.zip',
                                                'size': 118250749,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': '733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': 'cb7beb6f4e8c684e41d25aa4dc1294dc'
                                                          'b1e070e87f9ed852463bf651d511a36b',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.3.zip',
                                                'size': 187835236,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': 'c59e2de5-01fe-56eb-be56-679ed14161bf',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': '6372732e9fe9b8d58c8be8df88ea439d'
                                                          '5c68ee9bb02e3d472c94633fadf782a1',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/c59e2de5-01fe-56eb-be56-679ed14161bf',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.4.zip',
                                                'size': 38722784,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': '68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': 'f1458913c223553d09966ff94f0ed3d8'
                                                          '7e7cdfce21904f32943d70f691d8f7a0',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.5.zip',
                                                'size': 15535233,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': '0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': '053074e25a96a463c081e38bcd02662b'
                                                          'a1536dd0cb71411bd111b8a2086a03e1',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.6.zip',
                                                'size': 17985905,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': 'cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': '1c57cba1ade259fc9ec56b914b507507'
                                                          'd75ccbf6ddeebf03ba00c922c30e0c6e',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'E-MTAB-7316.processed.7.zip',
                                                'size': 7570475,
                                                'fileSource': 'ArrayExpress',
                                                'matrixCellCount': None,
                                                'uuid': '5b465aad-0981-5152-b468-e615e20f5884',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'zip',
                                                'isIntermediate': False,
                                                'sha256': 'af3ea779ca01a2ba65f9415720a44648'
                                                          'ef28a6ed73c9ec30e54ed4ba9895f590',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/5b465aad-0981-5152-b468-e615e20f5884',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            },
                                            {
                                                # Supplementary file, source from submitter_id
                                                'name': 'WongRetinaCelltype.csv',
                                                'size': 2300969,
                                                'fileSource': 'HCA Release',
                                                'matrixCellCount': None,
                                                'uuid': 'b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                                'version': '2021-02-10T16:56:40.419579Z',
                                                'contentDescription': ['Matrix'],
                                                'format': 'csv',
                                                'isIntermediate': False,
                                                'sha256': '4f515b8fbbec8bfbc72c8c0d656897ee'
                                                          '37bfa30bab6eb50fdc641924227be674',
                                                'url': str(self.base_url.set(
                                                    path='/repository/files/b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                                    args=dict(catalog='test', version='2021-02-10T16:56:40.419579Z')
                                                ))
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.assertElasticEqual(contributed_analyses,
                                one(hit['projects'])['contributedAnalyses'])

    def test_matrix_cell_count(self):
        """
        Verify analysis file matrixCellCount values are correctly reported
        on all endpoints
        """
        params = self.params(project_id='bd400331-54b9-4fcc-bff6-6bb8b079ee1f')

        # Verify matrix cell counts in each hit from the non-files endpoints
        expected_counts = {
            'Rds': 54140,
            'fastq.gz': None
        }
        for endpoint in ('projects', 'samples'):
            with self.subTest(endpoint=endpoint):
                url = self.base_url.set(path=('index', endpoint), args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                response_json = response.json()
                for hit in response_json['hits']:
                    actual_counts = {
                        s['format']: s['matrixCellCount']
                        for s in hit['fileTypeSummaries']
                    }
                    self.assertEqual(expected_counts, actual_counts)

        # Verify matrix cell counts across all hits in the 'files' endpoint
        expected_counts = {
            'Rds': 54140 * 3,  # 3 analysis files
        }
        actual_counts = Counter()
        url = self.base_url.set(path='/index/files', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        for hit in response_json['hits']:
            file = one(hit['files'])
            file_format = file['format']
            count = file['matrixCellCount']
            if count is not None:
                actual_counts[file_format] += count
        self.assertEqual(expected_counts, actual_counts)


@patch_dss_source
@patch_source_cache
class TestResponseFields(WebServiceTestCase):
    maxDiff = None

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return [
            # An imaging bundle with no cell suspension
            # files=227, donors=1, cs-cells=0, p-cells=0, organ=brain, labs=None
            cls.bundle_fqid(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c',
                            version='2019-04-03T10:34:26.471000Z'),
            # A bundle with project cell counts
            # files=1, donors=1, cs-cells=0, p-cells=88000, organ=mouth mucosa, labs=2
            cls.bundle_fqid(uuid='2c7d06b8-658e-4c51-9de4-a768322f84c5',
                            version='2021-09-21T17:27:23.898000Z'),
            # An analysis bundle with cell suspension cell counts
            # files=1, donor=3, cs-cells=44000, p-cells=0, organ=eye, labs=11
            cls.bundle_fqid(uuid='00f48893-5e9d-52cd-b32d-af88edccabfa',
                            version='2020-02-03T10:30:00.000000Z'),
            # A bundle with project & cell suspension cell counts
            # files=2, donor=1, cs-cells=1, p-cells=3589, organ=brain, labs=3
            cls.bundle_fqid(uuid='80baee6e-00a5-4fdc-bfe3-d339ff8a7178',
                            version='2021-03-12T22:43:32.330000Z'),
        ]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    def test_summary_response(self):
        url = self.base_url.set(path='/index/summary',
                                args=dict(catalog=self.catalog))
        response = requests.get(str(url))
        response.raise_for_status()
        summary = response.json()
        self.assertEqual(1 + 1 + 1 + 1, summary['projectCount'])
        self.assertEqual(1 + 1 + 3 + 1, summary['specimenCount'])
        self.assertEqual(2, summary['speciesCount'])
        self.assertEqual(227 + 1 + 1 + 2, summary['fileCount'])
        self.assertEqual(838022993.0, summary['totalFileSize'])
        self.assertEqual(1 + 1 + 3 + 1, summary['donorCount'])
        self.assertEqual(15, summary['labCount'])
        self.assertEqual({'brain', 'eye', 'mouth mucosa'}, set(summary['organTypes']))
        expected_file_counts = {
            'tiff': 221,
            'json': 6,
            'fastq.gz': 2,
            'loom': 1,
            'mtx.gz': 1
        }
        actual_file_counts = {
            s['format']: s['count']
            for s in summary['fileTypeSummaries']
        }
        self.assertEqual(expected_file_counts, actual_file_counts)
        expected_cell_count_summaries = [
            {
                'organType': ['eye'],
                'countOfDocsWithOrganType': 5,
                'totalCellCountByOrgan': 44000.0
            },
            {
                # Note that 'brain' from the imaging bundle is not represented here
                # since these values are tallied from the cell suspensions and the
                # imaging bundle does not have any cell suspensions.
                'organType': ['brain'],
                'countOfDocsWithOrganType': 1,
                'totalCellCountByOrgan': 1.0
            },
            {
                'organType': ['mouth mucosa'],
                'countOfDocsWithOrganType': 1,
                'totalCellCountByOrgan': 0.0
            }
        ]
        self.assertEqual(expected_cell_count_summaries, summary['cellCountSummaries'])
        expected_projects = [
            {
                'projects': {'estimatedCellCount': 3589.0},
                'cellSuspensions': {'totalCells': 1.0}
            },
            {
                'projects': {'estimatedCellCount': 88000.0},
                'cellSuspensions': {'totalCells': None}
            },
            {
                'projects': {'estimatedCellCount': None},
                'cellSuspensions': {'totalCells': 44000.0}
            }
        ]
        self.assertElasticEqual(expected_projects, summary['projects'])

    def test_filtered_summary_cell_counts(self):
        # Bundle 00f48893 has 5 cell suspensions from 3 donors:
        # Donor 427c0a62 (female)    Donor 66b7152c (female)   Donor b8049daa (male)
        # -------------------------  ------------------------  -------------------------
        # CS 1d3e48d7 (10000 cells)  CS 0aabed05 (4000 cells)  CS eb32bfc6 (10000 cells)
        # CS b1b6ea44 (10000 cells)                            CS 932000d6 (10000 cells)
        filters = {
            'bundleUuid': {
                'is': [
                    '00f48893-5e9d-52cd-b32d-af88edccabfa'
                ]
            }
        }
        expected_projects = [
            {
                'projects': {'estimatedCellCount': 0.0},
                'cellSuspensions': {'totalCells': None}
            },
            {
                'projects': {'estimatedCellCount': None},
                'cellSuspensions': {'totalCells': 44000.0}
            },
            {
                'projects': {'estimatedCellCount': 0.0},
                'cellSuspensions': {'totalCells': 0.0}
            }
        ]
        for values in (['male', 'female'], ['male'], ['female']):
            with self.subTest(values=values):
                filters['biologicalSex'] = {'is': values}
                url = self.base_url.set(path='/index/summary',
                                        args=dict(catalog=self.catalog,
                                                  filters=json.dumps(filters)))
                response = requests.get(str(url))
                response.raise_for_status()
                summary = response.json()
                self.assertElasticEqual(expected_projects, summary['projects'])

    def test_summary_filter_none(self):
        for use_filter, labCount in [(False, 15), (True, 1)]:
            with self.subTest(use_filter=use_filter, labCount=labCount):
                params = dict(catalog=self.catalog)
                if use_filter:
                    params['filters'] = json.dumps({"organPart": {"is": [None]}})
                url = self.base_url.set(path='/index/summary', args=params)
                response = requests.get(str(url))
                response.raise_for_status()
                summary_object = response.json()
                self.assertEqual(summary_object['labCount'], labCount)

    def test_projects_response(self):
        """
        Verify a project's contributors, laboratory, and publications.
        """
        params = {
            'catalog': self.catalog,
            'filters': json.dumps({
                'projectId': {
                    'is': ['50151324-f3ed-4358-98af-ec352a940a61']
                }
            })
        }
        url = self.base_url.set(path='/index/projects', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        response_json = response.json()
        project = one(one(response_json['hits'])['projects'])
        expected_contributors = [
            {
                'institution': 'National Institutes of Health',
                'contactName': 'Drake,W,Williams',
                'projectRole': 'experimental scientist',
                'laboratory': 'National Institute of Dental and Craniofacial Research,',
                'correspondingContributor': False,
                'email': None
            },
            {
                'institution': 'National Institutes of Health',
                'contactName': 'Niki,,Moutsopoulos',
                'projectRole': 'principal investigator',
                'laboratory': 'National Institute of Dental and Craniofacial Research,',
                'correspondingContributor': True,
                'email': 'nmoutsopoulos@dir.nidr.nih.gov'
            },
            {
                'institution': 'University of California, Santa Cruz',
                'contactName': 'Tiana,,Pereira',
                'projectRole': 'data curator',
                'laboratory': 'Human Cell Atlas Data Coordination Platform',
                'correspondingContributor': False,
                'email': 'tmpereir@ucsc.edu'
            }
        ]
        self.assertElasticEqual(expected_contributors, project['contributors'])
        expected_laboratory = [
            'Human Cell Atlas Data Coordination Platform',
            'National Institute of Dental and Craniofacial Research,'
        ]
        self.assertElasticEqual(expected_laboratory, project['laboratory'])
        expected_publications = [
            {
                'publicationTitle': 'Human oral mucosa cell atlas reveals a '
                                    'stromal-neutrophil axis regulating tissue immunity',
                'officialHcaPublication': False,
                'publicationUrl': 'https://pubmed.ncbi.nlm.nih.gov/34129837/',
                'doi': '10.1016/j.cell.2021.05.013'
            }
        ]
        self.assertEqual(expected_publications, project['publications'])


@patch_dss_source
@patch_source_cache
class TestUnpopulatedIndexResponse(WebServiceTestCase):

    @classmethod
    def bundles(cls) -> List[BundleFQID]:
        return []

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._setup_indices()
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        cls._teardown_indices()
        super().tearDownClass()

    @property
    def facets(self) -> Sequence[str]:
        return self.app_module.app.metadata_plugin.facets

    def fields(self) -> Mapping[str, str]:
        return self.app_module.app.metadata_plugin.field_mapping

    def entity_types(self) -> List[str]:
        return [
            entity_type
            for entity_type in self.index_service.entity_types(self.catalog)
            if entity_type != 'cell_suspensions'
        ]

    def test_empty_response(self):
        for entity_type in self.entity_types():
            with self.subTest(entity_type=entity_type):
                url = self.base_url.set(path=('index', entity_type),
                                        args=dict(order='asc'))
                response = requests.get(str(url))
                response.raise_for_status()
                sort_field, _ = self.app_module.sort_defaults[entity_type]
                expected_response = {
                    'hits': [],
                    'pagination': {
                        'count': 0,
                        'total': 0,
                        'size': 10,
                        'next': None,
                        'previous': None,
                        'pages': 0,
                        'sort': sort_field,
                        'order': 'asc'
                    },
                    'termFacets': {
                        facet: {'terms': [], 'total': 0, 'type': 'terms'}
                        for facet in self.facets
                    }
                }
                self.assertEqual(expected_response, response.json())

    def test_sorted_responses(self):
        # FIXME: Can't sort on fields of nested type
        #        https://github.com/DataBiosphere/azul/issues/2621
        sortable_fields = {
            field
            for field in self.fields()
            if field not in {'assayType', 'organismAgeRange', 'accessions'}
        }

        for entity_type, field in product(self.entity_types(), sortable_fields):
            with self.subTest(entity=entity_type, field=field):
                url = self.base_url.set(path=('index', entity_type),
                                        args=dict(sort=field))
                response = requests.get(str(url))
                self.assertEqual(200, response.status_code)


class TestPortalIntegrationResponse(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    maxDiff = None

    # Mocked DB content for the tests
    _portal_integrations_db = [
        {
            "portal_id": "9852dece-443d-42e8-869c-17b9a86d447e",
            "integrations": [
                {
                    "integration_id": "b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35",
                    "integration_type": "get_manifest",
                    "entity_type": "file",
                    "manifest_type": "full",
                },
                {
                    "integration_id": "977854a0-2eea-4fec-9459-d4807fe79f0c",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                }
            ]
        },
        {
            "portal_id": "f58bdc5e-98cd-4df4-80a4-7372dc035e87",
            "integrations": [
                {
                    "integration_id": "e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                },
                {
                    "integration_id": "dbfe9394-a326-4574-9632-fbadb51a7b1a",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["90bd6933-40c0-48d4-8d76-778c103bf545"]
                },
                {
                    "integration_id": "f13ddf2d-d913-492b-9ea8-2de4b1881c26",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": ["cddab57b-6868-4be4-806f-395ed9dd635a"]
                },
                {
                    "integration_id": "224b1d42-b939-4d10-8a8f-2b2ac304b813",
                    "integration_type": "get",
                    "entity_type": "project",
                    # NO entity_ids field
                }
            ]
        }
    ]

    def _mock_portal_crud(self, operation):
        operation(self._portal_integrations_db)

    def _get_integrations(self, params: dict) -> dict:
        url = self.base_url.set(path='/integrations', args=params)
        response = requests.get(str(url))
        response.raise_for_status()
        return response.json()

    @classmethod
    def _extract_integration_ids(cls, response_json):
        return [
            integration['integration_id']
            for portal in response_json
            for integration in portal['integrations']
        ]

    @mock.patch('azul.portal_service.PortalService._crud')
    def test_integrations(self, portal_crud):
        """
        Verify requests specifying `integration_type` and `entity_type` only return integrations matching those types
        """
        test_cases = [
            ('get_manifest', 'file', ['b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35']),
            ('get', 'bundle', []),
            (
                'get',
                'project',
                [
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            )
        ]
        portal_crud.side_effect = self._mock_portal_crud
        with mock.patch.object(type(config), 'dss_deployment_stage', 'prod'):
            for integration_type, entity_type, expected_integration_ids in test_cases:
                params = dict(integration_type=integration_type, entity_type=entity_type)
                with self.subTest(**params):
                    response_json = self._get_integrations(params)
                    found_integration_ids = self._extract_integration_ids(response_json)
                    self.assertEqual(len(expected_integration_ids), len(found_integration_ids))
                    self.assertEqual(set(expected_integration_ids), set(found_integration_ids))
                    self.assertTrue(all(isinstance(integration.get('entity_ids', []), list)
                                        for portal in response_json
                                        for integration in portal['integrations']))

    @mock.patch('azul.portal_service.PortalService._crud')
    def test_integrations_by_entity_ids(self, portal_crud):
        """
        Verify requests specifying `entity_ids` only return integrations matching those entity_ids
        """

        # 224b1d42-b939-4d10-8a8f-2b2ac304b813 must appear in every test since it lacks the entity_ids field
        test_cases = [
            # One project entity id specified by one integration
            (
                'cddab57b-6868-4be4-806f-395ed9dd635a',
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # Two project entity ids specified by two different integrations
            (
                'cddab57b-6868-4be4-806f-395ed9dd635a, 90bd6933-40c0-48d4-8d76-778c103bf545',
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # One project entity id specified by two different integrations
            (
                'c4077b3c-5c98-4d26-a614-246d12c2e5d7',
                [
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # Blank entity id, to match integrations lacking the entity_id field
            (
                '',
                [
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            ),
            # No entity id, accepting all integrations
            (
                None,
                [
                    'f13ddf2d-d913-492b-9ea8-2de4b1881c26',
                    'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                    '977854a0-2eea-4fec-9459-d4807fe79f0c',
                    'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                    '224b1d42-b939-4d10-8a8f-2b2ac304b813'
                ]
            )
        ]

        portal_crud.side_effect = self._mock_portal_crud
        with mock.patch.object(type(config), 'dss_deployment_stage', 'prod'):
            for entity_ids, integration_ids in test_cases:
                params = dict(integration_type='get', entity_type='project')
                if entity_ids is not None:
                    params['entity_ids'] = entity_ids
                with self.subTest(**params):
                    response_json = self._get_integrations(params)
                    found_integration_ids = self._extract_integration_ids(response_json)
                    self.assertEqual(set(integration_ids), set(found_integration_ids))


class TestListCatalogsResponse(LocalAppTestCase, DSSUnitTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return 'service'

    def test(self):
        response = requests.get(str(self.base_url.set(path='/index/catalogs')))
        self.assertEqual(200, response.status_code)
        self.assertEqual({
            'default_catalog': 'test',
            'catalogs': {
                'test': {
                    'internal': False,
                    'atlas': 'hca',
                    'plugins': {
                        'metadata': {
                            'name': 'hca',
                        },
                        'repository': {
                            'name': 'dss',
                            'sources': [
                                'https://dss.data.humancellatlas.org/v1:2/2'
                            ],
                        }
                    }
                }
            }
        }, response.json())


if __name__ == '__main__':
    unittest.main()
