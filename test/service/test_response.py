import json
import unittest
from unittest import mock
import urllib.parse

from more_itertools import one
import requests

from app_test_case import LocalAppTestCase
from azul import config
from azul.logging import configure_test_logging
from azul.plugin import Plugin
from azul.service.responseobjects.hca_response_v5 import (
    FileSearchResponse,
    KeywordSearchResponse,
)
from service import WebServiceTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestResponse(WebServiceTestCase):
    maxDiff = None
    bundles = WebServiceTestCase.bundles + [
        ('fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a', '2019-02-14T192438.034764Z'),
        ('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'),
        ('e0ae8cfa-2b51-4419-9cde-34df44c6458a', '2018-12-05T230917.591044Z'),
        ('411cd8d5-5990-43cd-84cc-6c7796b8a76d', '2018-10-18T204655.866661Z'),
        ('412cd8d5-5990-43cd-84cc-6c7796b8a76d', '2018-10-18T204655.866661Z'),
        ('ffac201f-4b1c-4455-bd58-19c1a9e863b4', '2019-10-09T170735.528600Z'),
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
        results = self.es_client.search(index=config.es_index_name(entity_type, aggregate=True), body=body)
        return Plugin.load().translate_fields([results['hits']['hits'][0]['_source']], forward=False)

    def test_key_search_files_response(self):
        """
        This method tests the KeywordSearchResponse object for the files entity type.
        It will make sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.
        """
        # Still need a way to test the response.
        keyword_response = KeywordSearchResponse(
            # the entity_id is hardcoded, but corresponds to the bundle above
            hits=self.get_hits('files', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
            entity_type='files'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "bundles": [
                        {
                            "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                            "bundleVersion": "2018-11-02T113344.698028Z"
                        }
                    ],
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}]
                        }
                    ],
                    "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                    "files": [
                        {
                            "content_description": [],
                            "format": "fastq.gz",
                            "name": "SRR3562915_1.fastq.gz",
                            "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                            "size": 195142097,
                            "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                            "version": "2018-11-02T113344.698028Z"
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "laboratory": ["John Dear"],
                            "projectShortname": ["Single of human pancreas"],
                            "projectTitle": ["Single cell transcriptome patterns."]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True],
                            "workflow": [],
                            "assayType": [],
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
                                "specimen_from_organism"
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ]
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_key_search_samples_response(self):
        """
        KeywordSearchResponse for the specimens endpoint should return file type summaries instead of files
        """
        keyword_response = KeywordSearchResponse(
            # the entity_id is hardcoded, but corresponds to the bundle above
            hits=self.get_hits('samples', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            entity_type='samples'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}]
                        }
                    ],
                    "entryId": "a21dc760-a500-4236-bcff-da34a0e873d2",
                    "fileTypeSummaries": [
                        {
                            "count": 2,
                            "fileType": "fastq.gz",
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "laboratory": ["John Dear"],
                            "projectShortname": ["Single of human pancreas"],
                            "projectTitle": ["Single cell transcriptome patterns."]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True],
                            "workflow": [],
                            "assayType": [],
                        }
                    ],
                    "samples": [
                        {
                            "sampleEntityType": "specimens",
                            "effectiveOrgan": "pancreas",
                            "id": "DID_scRSq06_pancreas",
                            "disease": ["normal"],
                            "organ": "pancreas",
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": None,
                            "source": "specimen_from_organism",
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism",
                            ]
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    paginations = [
        {
            "count": 2,
            "order": "desc",
            "pages": 1,
            "size": 5,
            "sort": "entryId",
            "total": 2
        },
        {
            "count": 2,
            "order": "desc",
            "pages": 1,
            "search_after": "cbb998ce-ddaf-34fa-e163-d14b399c6b34",
            "search_after_uid": "meta#32",
            "size": 5,
            "sort": "entryId",
            "total": 2
        }
    ]

    def test_file_search_response(self):
        """
        n=0: Test the FileSearchResponse object, making sure the functionality works as appropriate by asserting the
        apiResponse attribute is the same as expected.

        n=1: Tests the FileSearchResponse object, using 'search_after' pagination.
        """
        hits = [
            {
                "bundles": [
                    {
                        "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                        "bundleVersion": "2018-11-02T113344.698028Z"
                    }
                ],
                "cellLines": [

                ],
                "cellSuspensions": [
                    {
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "selectedCellType": [],
                        "totalCells": 1
                    }
                ],
                "donorOrganisms": [
                    {
                        "biologicalSex": ["female"],
                        "disease": ['normal'],
                        "genusSpecies": ["Australopithecus"],
                        "id": ["DID_scRSq06"],
                        "donorCount": 1,
                        "organismAge": ["38"],
                        "organismAgeUnit": ["year"],
                        "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}]
                    }
                ],
                "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                "files": [
                    {
                        "content_description": [],
                        "format": "fastq.gz",
                        "name": "SRR3562915_1.fastq.gz",
                        "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                        "size": 195142097,
                        "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                        "version": "2018-11-02T113344.698028Z"
                    }
                ],
                "organoids": [

                ],
                "projects": [
                    {
                        "laboratory": ["John Dear"],
                        "projectShortname": ["Single of human pancreas"],
                        "projectTitle": ["Single cell transcriptome patterns."]
                    }
                ],
                "protocols": [
                    {
                        "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                        "libraryConstructionApproach": ["Smart-seq2"],
                        "pairedEnd": [True],
                        "workflow": [],
                        "assayType": [],
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
                        ]
                    }
                ],
                "specimens": [
                    {
                        "disease": ["normal"],
                        "id": ["DID_scRSq06_pancreas"],
                        "organ": ["pancreas"],
                        "organPart": ["islet of Langerhans"],
                        "preservationMethod": [None],
                        "source": [
                            "specimen_from_organism",
                        ]
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
                    "search_after": None,
                    "search_after_uid": None,
                    "search_before": None,
                    "search_before_uid": None,
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
                    "search_after": "cbb998ce-ddaf-34fa-e163-d14b399c6b34",
                    "search_after_uid": "meta#32",
                    "search_before": None,
                    "search_before_uid": None,
                    "size": 5,
                    "sort": "entryId",
                    "total": 2
                },
                "termFacets": {}
            }
        ]
        for n in 0, 1:
            with self.subTest(n=n):
                filesearch_response = FileSearchResponse(
                    hits=self.get_hits('files', '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb'),
                    pagination=self.paginations[n],
                    facets={},
                    entity_type="files"
                ).return_response().to_json()
                self.assertElasticsearchResultsEqual(filesearch_response, responses[n])

    def test_file_search_response_file_summaries(self):
        """
        Test non-'files' entity type passed to FileSearchResponse will give file summaries
        """
        filesearch_response = FileSearchResponse(
            hits=self.get_hits('samples', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            pagination=self.paginations[0],
            facets={},
            entity_type="samples"
        ).return_response().to_json()

        for hit in filesearch_response['hits']:
            self.assertTrue('fileTypeSummaries' in hit)
            self.assertFalse('files' in hit)

    facets_populated = {
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
        Test adding facets to FileSearchResponse with missing values in one facet
        and no missing values in the other

        null term should not appear if there are no missing values
        """
        facets = FileSearchResponse.add_facets(self.facets_populated)
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
        self.assertElasticsearchResultsEqual(facets, expected_output)

    def test_default_sorting_parameter(self):
        for entity_type in 'files', 'samples', 'projects', 'bundles':
            with self.subTest(entity_type=entity_type):
                base_url = self.base_url
                url = base_url + "/repository/" + entity_type
                response = requests.get(url)
                response.raise_for_status()
                summary_object = response.json()
                self.assertEqual(summary_object['pagination']["sort"], self.app_module.sort_defaults[entity_type][0])

    def test_transform_request_with_file_url(self):
        base_url = self.base_url
        for entity_type in ('files', 'bundles'):
            with self.subTest(entity_type=entity_type):
                url = base_url + f"/repository/{entity_type}"
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()
                for hit in response_json['hits']:
                    if entity_type == 'files':
                        self.assertEqual(len(hit['files']), 1)
                    else:
                        self.assertGreater(len(hit['files']), 0)
                    for file in hit['files']:
                        self.assertIn('url', file.keys())
                        actual_url = urllib.parse.urlparse(file['url'])
                        actual_query_vars = {k: one(v) for k, v in urllib.parse.parse_qs(actual_url.query).items()}
                        expected_base_url = urllib.parse.urlparse(base_url)
                        self.assertEqual(expected_base_url.netloc, actual_url.netloc)
                        self.assertEqual(expected_base_url.scheme, actual_url.scheme)
                        self.assertIsNotNone(actual_url.path)
                        self.assertEqual('aws', actual_query_vars['replica'])
                        self.assertIsNotNone(actual_query_vars['version'])

    def test_projects_key_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
            entity_type='projects'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}]
                        }
                    ],
                    "entryId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                    "fileTypeSummaries": [
                        {
                            "count": 2,
                            "fileType": "fastq.gz",
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "arrayExpressAccessions": [],
                            "geoSeriesAccessions": [],
                            "insdcProjectAccessions": [],
                            "insdcStudyAccessions": [],
                            "contributors": [
                                {
                                    "contactName": "Martin, Enge",
                                    "correspondingContributor": None,
                                    "email": "martin.enge@gmail.com",
                                    "institution": "University",
                                    "laboratory": None,
                                    "projectRole": None
                                },
                                {
                                    "contactName": "Matthew,,Green",
                                    "correspondingContributor": False,
                                    "email": "hewgreen@ebi.ac.uk",
                                    "institution": "Farmers Trucks",
                                    "laboratory": "John Dear",
                                    "projectRole": "Human Cell Atlas wrangler"
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
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals "
                                                        "Transcriptional Signatures of Aging and Somatic Mutation "
                                                        "Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ],
                            "supplementaryLinks": [
                                "https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True],
                            "workflow": [],
                            "assayType": [],
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
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ]
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_projects_file_search_response(self):
        """
        Test building response for projects
        Response should include project detail fields that do not appear for other entity type responses
        """
        keyword_response = FileSearchResponse(
            hits=self.get_hits('projects', 'e8642221-4c2c-4fd7-b926-a68bce363c88'),
            pagination=self.paginations[0],
            facets=self.facets_populated,
            entity_type='projects'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ['normal'],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06"],
                            "donorCount": 1,
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "organismAgeRange": [{"gte": 1198368000.0, "lte": 1198368000.0}]
                        }
                    ],
                    "entryId": "e8642221-4c2c-4fd7-b926-a68bce363c88",
                    "fileTypeSummaries": [
                        {
                            "count": 2,
                            "fileType": "fastq.gz",
                            "totalSize": 385472253
                        }
                    ],
                    "organoids": [
                    ],
                    "projects": [
                        {
                            "arrayExpressAccessions": [],
                            "geoSeriesAccessions": [],
                            "insdcProjectAccessions": [],
                            "insdcStudyAccessions": [],
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
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals "
                                                        "Transcriptional Signatures of Aging and Somatic Mutation "
                                                        "Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ],
                            "supplementaryLinks": [
                                'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results'
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True],
                            "workflow": [],
                            "assayType": [],
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
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["normal"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ]
                        }
                    ]
                }
            ],
            "pagination": {
                "count": 2,
                "order": "desc",
                "pages": 1,
                "search_after": None,
                "search_after_uid": None,
                "search_before": None,
                "search_before_uid": None,
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

        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_project_accessions_response(self):
        """
        This method tests the KeywordSearchResponse object for the projects entity type,
        specifically making sure the accessions fields are present in the response.
        """
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', '627cb0ba-b8a1-405a-b58f-0add82c3d635'),
            entity_type='projects'
        ).return_response().to_json()
        expected_response = {
            "hits": [
                {
                    "cellLines": [

                    ],
                    "cellSuspensions": [
                        {
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "selectedCellType": [],
                            "totalCells": 10000
                        }
                    ],
                    "donorOrganisms": [
                        {
                            "biologicalSex": ["male"],
                            "disease": ['H syndrome'],
                            "genusSpecies": ["Homo sapiens"],
                            "id": ["donor_ID_1"],
                            "donorCount": 1,
                            "organismAge": ["20"],
                            "organismAgeUnit": ["year"],
                            "organismAgeRange": [{"gte": 630720000.0, "lte": 630720000.0}]
                        }
                    ],
                    "entryId": "627cb0ba-b8a1-405a-b58f-0add82c3d635",
                    "fileTypeSummaries": [
                        {
                            "count": 1,
                            "fileType": "bai",
                            "totalSize": 2395616
                        },
                        {
                            "count": 1,
                            "fileType": "bam",
                            "totalSize": 55840108
                        },
                        {
                            "count": 1,
                            "fileType": "csv",
                            "totalSize": 665
                        },
                        {
                            "count": 1,
                            "fileType": "unknown",
                            "totalSize": 2645006
                        },
                        {
                            "count": 2,
                            "fileType": "mtx",
                            "totalSize": 6561141
                        },
                        {
                            "count": 3,
                            "fileType": "fastq.gz",
                            "totalSize": 44668092
                        },
                        {
                            "count": 3,
                            "fileType": "h5",
                            "totalSize": 5573714
                        },
                        {
                            "count": 4,
                            "fileType": "tsv",
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
                            "arrayExpressAccessions": ["E-AAAA-00"],
                            "geoSeriesAccessions": ["GSE00000"],
                            "insdcProjectAccessions": ["SRP000000"],
                            "insdcStudyAccessions": ["PRJNA000000"],
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
                            "projectShortname": "staging/10x/2019-02-14T18:29:38Z",
                            "projectTitle": "10x 1 Run Integration Test",
                            "publications": [
                                {
                                    "publicationTitle": "A title of a publication goes here.",
                                    "publicationUrl": "https://europepmc.org"
                                }
                            ],
                            "supplementaryLinks": [],
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina HiSeq 2500"],
                            "libraryConstructionApproach": ["10X v2 sequencing"],
                            "pairedEnd": [False],
                            "workflow": ['cellranger_v1.0.2'],
                            "assayType": [],
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
                            ]
                        }
                    ],
                    "specimens": [
                        {
                            "disease": ["H syndrome"],
                            "id": ["specimen_ID_1"],
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "preservationMethod": [None],
                            "source": [
                                "specimen_from_organism"
                            ]
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_cell_suspension_response(self):
        """
        Test KeywordSearchResponse contains the correct selectedCellType value
        """
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', '250aef61-a15b-4d97-b8b4-54bb997c1d7d'),
            entity_type='projects'
        ).return_response().to_json()

        cell_suspension = one(keyword_response['hits'][0]['cellSuspensions'])
        self.assertEqual(["Plasma cells"], cell_suspension['selectedCellType'])

    def test_cell_line_response(self):
        """
        Test KeywordSearchResponse contains the correct cell_line and sample field values
        """
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('projects', 'c765e3f9-7cfc-4501-8832-79e5f7abd321'),
            entity_type='projects'
        ).return_response().to_json()
        expected_cell_lines = {
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2', 'cell_line_GM18517'],
            'cellLineType': ['primary', 'stem cell-derived'],
            'modelOrgan': ['blood (parent_cell_line)', 'blood (child_cell_line)'],
        }
        cell_lines = one(one(keyword_response['hits'])['cellLines'])
        self.assertElasticsearchResultsEqual(cell_lines, expected_cell_lines)
        expected_samples = {
            'sampleEntityType': ['cellLines'],
            'effectiveOrgan': ['blood (child_cell_line)'],
            'id': ['cell_line_Day7_hiPSC-CM_BioRep2'],
            'cellLineType': ['stem cell-derived'],
            'modelOrgan': ['blood (child_cell_line)'],
        }
        samples = one(one(keyword_response['hits'])['samples'])
        self.assertElasticsearchResultsEqual(samples, expected_samples)

    def test_file_response(self):
        """
        Test KeywordSearchResponse contains the correct file field values
        """
        keyword_response = KeywordSearchResponse(
            hits=self.get_hits('files', '4015da8b-18d8-4f3c-b2b0-54f0b77ae80a'),
            entity_type='files'
        ).return_response().to_json()
        expected_file = {
            'content_description': ['RNA sequence'],
            'format': 'fastq.gz',
            'name': 'Cortex2.CCJ15ANXX.SM2_052318p4_D8.unmapped.1.fastq.gz',
            'sha256': '709fede4736213f0f71ae4d76719fd51fa402a9112582a4c52983973cb7d7e47',
            'size': 22819025,
            'uuid': 'a8b8479d-cfa9-4f74-909f-49552439e698',
            'version': '2019-10-09T172251.560099Z'
        }
        file = one(one(keyword_response['hits'])['files'])
        self.assertElasticsearchResultsEqual(file, expected_file)

    def test_filter_with_none(self):
        """
        Test response when using a filter with a None value
        """
        test_data_values = [["year"], [None], ["year", None]]
        for test_data in test_data_values:
            with self.subTest(test_data=test_data):
                url = self.base_url + "/repository/samples"
                params = {
                    'size': 10,
                    'filters': json.dumps({'organismAgeUnit': {'is': test_data}})
                }
                response = requests.get(url, params=params)
                response.raise_for_status()
                response_json = response.json()
                organism_age_units = {
                    oau
                    for hit in response_json['hits']
                    for donor in hit['donorOrganisms']
                    for oau in donor['organismAgeUnit']
                }
                # Assert that the organismAgeUnits values found in the response only match what was filtered for
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
                    url = self.base_url + "/repository/" + entity_type
                    params = {
                        'size': 2,
                        'filters': json.dumps({'projectId': {'is': [test_data['id']]}})
                    }
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    response_json = response.json()
                    for hit in response_json['hits']:
                        for project in hit['projects']:
                            if entity_type == 'projects':
                                self.assertEqual(test_data['title'], project['projectTitle'])
                            else:
                                self.assertIn(test_data['title'], project['projectTitle'])
                    for term in response_json['termFacets']['project']['terms']:
                        self.assertEqual(term['projectId'], [test_data['id']])

    def test_translated_facets(self):
        """
        Test that response facets values are correctly translated back to correct data types
        and that the translated None value '__null__' is not present
        """
        url = self.base_url + "/repository/samples"
        params = {'size': 10, 'filters': json.dumps({})}
        response = requests.get(url, params=params)
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
                self.assertNotEqual(term['term'], config.null_keyword)

    def test_sample(self):
        """
        Test that sample(s) in the response contain values matching values in the source cellLine/organoid/specimen
        """
        for entity_type in 'projects', 'samples', 'files', 'bundles':
            with self.subTest(entity_type=entity_type):
                url = self.base_url + "/repository/" + entity_type
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()
                if entity_type == 'samples':
                    for hit in response_json['hits']:
                        for sample in hit['samples']:
                            sample_entity_type = sample['sampleEntityType']
                            for key, val in sample.items():
                                if key not in ['sampleEntityType', 'effectiveOrgan']:
                                    if isinstance(val, list):
                                        for one_val in val:
                                            self.assertIn(one_val, hit[sample_entity_type][0][key])
                                    else:
                                        self.assertIn(val, hit[sample_entity_type][0][key])

    def test_bundles_outer_entity(self):
        entity_type = 'bundles'
        url = self.base_url + "/repository/" + entity_type
        response = requests.get(url)
        response.raise_for_status()
        response = response.json()
        indexed_uuids = set(self.bundles)
        self.assertEqual(len(self.bundles), len(indexed_uuids))
        hits_uuids = {
            (one(hit['bundles'])['bundleUuid'], one(hit['bundles'])['bundleVersion'])
            for hit in response['hits']
        }
        self.assertEqual(len(response['hits']), len(hits_uuids))
        self.assertSetEqual(indexed_uuids, hits_uuids)

    def test_ranged_values(self):
        test_hits = [
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
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
                        "45-49",
                        "65-69"
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
                    "organismAgeUnit": [
                        "year"
                    ]
                }
            ],
            [
                {
                    "biologicalSex": [
                        "male",
                        "female"
                    ],
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
                        "40-44",
                        "55-59"
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
                    "organismAgeUnit": [
                        "year"
                    ]
                }
            ]
        ]

        url = self.base_url + '/repository/projects'
        for relation, range_value, expected_hits in [('contains', (1419130000, 1545263000), test_hits[:1]),
                                                     ('within', (1261430000, 1545265000), test_hits),
                                                     ('intersects', (1860623000, 1900000000), test_hits[1:]),
                                                     ('contains', (1860624000, 2049641000), []),
                                                     ('within', (1734490000, 1860623000), []),
                                                     ('intersects', (1860624100, 2049641000), [])]:
            with self.subTest(relation=relation, value=range_value):
                params = {
                    'filters': json.dumps({'organismAgeRange': {relation: [range_value]}}),
                    'order': 'desc',
                    'sort': 'entryId'
                }
                response = requests.get(url, params=params)
                actual_value = [hit['donorOrganisms'] for hit in response.json()['hits']]
                self.assertElasticsearchResultsEqual(expected_hits, actual_value)

    def test_ordering(self):
        sort_fields = [
            ('cellCount', lambda hit: hit['cellSuspensions'][0]['totalCells']),
            ('donorCount', lambda hit: hit['donorOrganisms'][0]['donorCount'])
        ]

        url = self.base_url + '/repository/projects'
        for sort_field, accessor in sort_fields:
            responses = {
                order: requests.get(url, params={
                    'filters': '{}',
                    'order': order,
                    'sort': sort_field
                })
                for order in ['asc', 'desc']
            }
            hit_sort_values = {}
            for order, response in responses.items():
                response.raise_for_status()
                hit_sort_values[order] = [accessor(hit) for hit in response.json()['hits']]

            self.assertEqual(hit_sort_values['asc'], sorted(hit_sort_values['asc']))
            self.assertEqual(hit_sort_values['desc'], sorted(hit_sort_values['desc'], reverse=True))


class TestResponseSummary(WebServiceTestCase):
    maxDiff = None
    bundles = WebServiceTestCase.bundles + [
        ('dcccb551-4766-4210-966c-f9ee25d19190', '2018-10-18T204655.866661Z'),
        ('94f2ba52-30c8-4de0-a78e-f95a3f8deb9c', '2019-04-03T103426.471000Z')  # an imaging bundle
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
        """
        Verify the /repository/summary response with two sequencing bundles and
        one imaging bundle that has no cell suspension.

        - bundle=aaa96233…, fileCount=2, donorCount=1, totalCellCount=1.0, organType=pancreas
        - bundle=dcccb551…, fileCount=19, donorCount=4, totalCellCount=6210.0, organType=Brain
        - bundle=94f2ba52…, fileCount=227, donorCount=1, totalCellCount=0, organType=brain
        """
        url = self.base_url + "/repository/summary"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['fileCount'], 2 + 19 + 227)
        self.assertEqual(summary_object['donorCount'], 1 + 4 + 1)
        self.assertEqual(summary_object['totalCellCount'], 1.0 + 6210.0 + 0)
        file_counts_expected = {
            'tiff': 221,
            'json': 6,
            'fastq.gz': 5,
            'tsv': 4,
            'h5': 3,
            'pdf': 3,
            'mtx': 2,
            'bai': 1,
            'bam': 1,
            'csv': 1,
            'unknown': 1
        }
        file_counts_actual = {summary['fileType']: summary['count'] for summary in summary_object['fileTypeSummaries']}
        self.assertEqual(file_counts_actual, file_counts_expected)
        self.assertEqual(set(summary_object['organTypes']), {'Brain', 'brain', 'pancreas'})
        self.assertEqual(summary_object['cellCountSummaries'], [
            # 'brain' from the imaging bundle is not represented in cellCountSummaries as these values are tallied
            # from the cell suspensions and the imaging bundle does not have any cell suspensions
            {'organType': ['Brain'], 'countOfDocsWithOrganType': 1, 'totalCellCountByOrgan': 6210.0},
            {'organType': ['pancreas'], 'countOfDocsWithOrganType': 1, 'totalCellCountByOrgan': 1.0},
        ])

    def test_summary_filter_none(self):
        # This request should match the 94f2ba52 bundle which has no 'paired_end' field and not match the
        # dcccb551 or aaa96233 bundles which have 'pared_end' set to True
        url = self.base_url + '/repository/summary?filters={"pairedEnd": {"is": [null]}}'
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['donorCount'], 1)
        self.assertEqual(summary_object['specimenCount'], 1)
        self.assertEqual(summary_object['projectCount'], 1)
        self.assertEqual(summary_object['totalCellCount'], 0.0)
        # Mus musculus only
        self.assertEqual(summary_object['speciesCount'], 1)

        # This request should match bundles 94f2ba52 and dcccb551 but not aaa96233 which has a 'organ_part' value.
        # Bundle 94f2ba52 has a Specimen with 'organ' but no 'organ_part'
        # Bundle dcccb551 has Specimens with 'organ_part' however the Organoid with no 'model_organ_part' is the Sample
        url = self.base_url + '/repository/summary?filters={"organPart": {"is": [null]}}'
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertEqual(summary_object['donorCount'], 5)
        self.assertEqual(summary_object['specimenCount'], 5)
        self.assertEqual(summary_object['projectCount'], 2)
        self.assertEqual(summary_object['totalCellCount'], 6210.0)
        # Muc muscuclus (94f2ba52) and Homo sapiens (dcccb551)
        self.assertEqual(summary_object['speciesCount'], 2)


class TestPortalIntegrationResponse(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    maxDiff = None

    # Mocked db content for the tests
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
                    "entity_ids": {
                        "prod": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                    }
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
                    "entity_ids": {
                        "prod": ["c4077b3c-5c98-4d26-a614-246d12c2e5d7"]
                    }
                },
                {
                    "integration_id": "dbfe9394-a326-4574-9632-fbadb51a7b1a",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": {
                        "prod": ["90bd6933-40c0-48d4-8d76-778c103bf545"]
                    }
                },
                {
                    "integration_id": "f13ddf2d-d913-492b-9ea8-2de4b1881c26",
                    "integration_type": "get",
                    "entity_type": "project",
                    "entity_ids": {
                        "prod": ["cddab57b-6868-4be4-806f-395ed9dd635a"]
                    }
                }
            ]
        }
    ]

    def _get_integrations(self, params: dict) -> dict:
        url = self.base_url + '/integrations'
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @classmethod
    def _extract_integration_ids(cls, response_json):
        return [integration['integration_id']
                for portal in response_json
                for integration in portal['integrations']]

    @mock.patch('azul.project.hca.Plugin.portal_integrations_db')
    def test_integrations(self, portal_integrations_db):
        """
        Verify requests specifying `integration_type` and `entity_type` only return integrations matching those types
        """
        test_cases = [
            ('get_manifest', 'file', ['b87b7f30-2e60-4ca5-9a6f-00ebfcd35f35']),
            ('get', 'bundle', []),
            ('get', 'project', ['977854a0-2eea-4fec-9459-d4807fe79f0c',
                                'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138',
                                'dbfe9394-a326-4574-9632-fbadb51a7b1a',
                                'f13ddf2d-d913-492b-9ea8-2de4b1881c26'])
        ]
        portal_integrations_db.return_value = self._portal_integrations_db
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

    @mock.patch('azul.project.hca.Plugin.portal_integrations_db')
    def test_integrations_by_entity_ids(self, portal_integrations_db):
        """
        Verify requests specifying `entity_ids` only return integrations matching those entity_ids
        """
        test_cases = [
            # One project entity id specified by one integration
            (['cddab57b-6868-4be4-806f-395ed9dd635a'],
             ['f13ddf2d-d913-492b-9ea8-2de4b1881c26']),
            # Two project entity ids specified by two different integrations
            (['cddab57b-6868-4be4-806f-395ed9dd635a', '90bd6933-40c0-48d4-8d76-778c103bf545'],
             ['f13ddf2d-d913-492b-9ea8-2de4b1881c26', 'dbfe9394-a326-4574-9632-fbadb51a7b1a']),
            # One project entity id specified by two different integrations
            (['c4077b3c-5c98-4d26-a614-246d12c2e5d7'],
             ['977854a0-2eea-4fec-9459-d4807fe79f0c', 'e8b3ca4f-bcf5-42eb-b58c-de6d7e0fe138'])
        ]
        portal_integrations_db.return_value = self._portal_integrations_db
        with mock.patch.object(type(config), 'dss_deployment_stage', 'prod'):
            for entity_ids, integration_ids in test_cases:
                params = dict(integration_type='get', entity_type='project', entity_ids=','.join(entity_ids))
                with self.subTest(**params):
                    response_json = self._get_integrations(params)
                    found_integration_ids = self._extract_integration_ids(response_json)
                    self.assertEqual(set(integration_ids), set(found_integration_ids))


if __name__ == '__main__':
    unittest.main()
