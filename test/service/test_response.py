#!/usr/bin/python

import json
import logging
import unittest
import urllib.parse

from more_itertools import one
import requests

from azul import config
from azul.service.responseobjects.hca_response_v5 import (FileSearchResponse,
                                                          KeywordSearchResponse,
                                                          ProjectSummaryResponse)
from service import WebServiceTestCase


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestResponse(WebServiceTestCase):

    maxDiff = None
    bundles = WebServiceTestCase.bundles + [
        ('fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a', '2019-02-14T192438.034764Z'),
        ('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z')
    ]

    def get_hits(self, entity_type: str, entity_id: str):
        """Fetches hits from es instance searching for a particular entity ID"""
        body = {
            "query": {
                "term": {
                    "entity_id.keyword": entity_id
                }
            }
        }
        # Tests are assumed to only ever run with the azul dev index
        results = self.es_client.search(index=config.es_index_name(entity_type, aggregate=True), body=body)
        return [results['hits']['hits'][0]['_source']]

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
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
                        }
                    ],
                    "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                    "files": [
                        {
                            "format": "fastq.gz",
                            "name": "SRR3562915_1.fastq.gz",
                            "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                            "size": 195142097,
                            "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                            "version": "2018-11-02T113344.698028Z"
                        }
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
                            "pairedEnd": [True]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ["normal"],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "preservationMethod": [None],
                            "source": [
                                "cell_suspension",
                                "donor_organism",
                                "specimen_from_organism"
                            ]
                        }
                    ]
                }
            ]
        }
        self.assertElasticsearchResultsEqual(keyword_response, expected_response)

    def test_key_search_specimens_response(self):
        """
        KeywordSearchResponse for the specimens endpoint should return file type summaries instead of files
        """
        keyword_response = KeywordSearchResponse(
            # the entity_id is hardcoded, but corresponds to the bundle above
            hits=self.get_hits('specimens', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            entity_type='specimens'
        ).return_response().to_json()

        expected_response = {
            "hits": [
                {
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
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
                            "pairedEnd": [True]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ["normal"],
                            "genusSpecies": ["Australopithecus"],
                            "id": "DID_scRSq06_pancreas",
                            "organ": "pancreas",
                            "organPart": ["islet of Langerhans"],
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "preservationMethod": None,
                            "source": [
                                "cell_suspension",
                                "specimen_from_organism",
                                "donor_organism"
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
        responses = [
            {
                "hits": [
                    {
                        "bundles": [
                            {
                                "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                                "bundleVersion": "2018-11-02T113344.698028Z"
                            }
                        ],
                        "cellSuspensions": [
                            {
                                "organ": ["pancreas"],
                                "organPart": ["islet of Langerhans"],
                                "selectedCellType": [],
                                "totalCells": 1
                            }
                        ],
                        "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                        "files": [
                            {
                                "format": "fastq.gz",
                                "name": "SRR3562915_1.fastq.gz",
                                "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                                "size": 195142097,
                                "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                                "version": "2018-11-02T113344.698028Z"
                            }
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
                                "pairedEnd": [True]
                            }
                        ],
                        "specimens": [
                            {
                                "biologicalSex": ["female"],
                                "disease": ["normal"],
                                "genusSpecies": ["Australopithecus"],
                                "id": ["DID_scRSq06_pancreas"],
                                "organ": ["pancreas"],
                                "organPart": ["islet of Langerhans"],
                                "organismAge": ["38"],
                                "organismAgeUnit": ["year"],
                                "preservationMethod": [None],
                                "source": [
                                    "cell_suspension",
                                    "specimen_from_organism",
                                    "donor_organism"
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
                "termFacets": {}
            },
            {
                "hits": [
                    {
                        "bundles": [
                            {
                                "bundleUuid": "aaa96233-bf27-44c7-82df-b4dc15ad4d9d",
                                "bundleVersion": "2018-11-02T113344.698028Z"
                            }
                        ],
                        "cellSuspensions": [
                            {
                                "organ": ["pancreas"],
                                "organPart": ["islet of Langerhans"],
                                "selectedCellType": [],
                                "totalCells": 1
                            }
                        ],
                        "entryId": "0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb",
                        "files": [
                            {
                                "format": "fastq.gz",
                                "name": "SRR3562915_1.fastq.gz",
                                "sha256": "77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a",
                                "size": 195142097,
                                "uuid": "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb",
                                "version": "2018-11-02T113344.698028Z"
                            }
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
                                "pairedEnd": [True]
                            }
                        ],
                        "specimens": [
                            {
                                "biologicalSex": ["female"],
                                "disease": ["normal"],
                                "genusSpecies": ["Australopithecus"],
                                "id": ["DID_scRSq06_pancreas"],
                                "organ": ["pancreas"],
                                "organPart": ["islet of Langerhans"],
                                "organismAge": ["38"],
                                "organismAgeUnit": ["year"],
                                "preservationMethod": [None],
                                "source": [
                                    "cell_suspension",
                                    "specimen_from_organism",
                                    "donor_organism"
                                ]
                            }
                        ]
                    }
                ],
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
            hits=self.get_hits('specimens', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
            pagination=self.paginations[0],
            facets={},
            entity_type="specimens"
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

    def test_summary_endpoint(self):
        url = self.base_url + "/repository/summary"
        response = requests.get(url)
        response.raise_for_status()
        summary_object = response.json()
        self.assertGreaterEqual(summary_object['fileCount'], 1)
        self.assertGreaterEqual(summary_object['organCount'], 1)
        self.assertGreaterEqual(len(summary_object['fileTypeSummaries']), 1)
        self.assertGreaterEqual(summary_object['fileTypeSummaries'][0]['totalSize'], 1)
        self.assertIsNotNone(summary_object['organSummaries'])

    def test_default_sorting_parameter(self):
        # FIXME: local import for now to delay side effects of the import like logging being configured
        # https://github.com/DataBiosphere/azul/issues/637
        from lambdas.service.app import sort_defaults
        for entity_type in 'files', 'specimens', 'projects':
            with self.subTest(entity_type=entity_type):
                base_url = self.base_url
                url = base_url + "/repository/" + entity_type
                response = requests.get(url)
                response.raise_for_status()
                summary_object = response.json()
                self.assertEqual(summary_object['pagination']["sort"], sort_defaults[entity_type][0])

    def test_transform_request_with_file_url(self):
        base_url = self.base_url
        url = base_url + "/repository/files"
        response = requests.get(url)
        response.raise_for_status()
        response_json = response.json()
        bundle_files = [file_data for hit in response_json['hits'] for file_data in hit['files']]
        for file_data in bundle_files:
            self.assertIn('url', file_data.keys())
            actual_url = urllib.parse.urlparse(file_data['url'])
            actual_query_vars = {k: one(v) for k, v in urllib.parse.parse_qs(actual_url.query).items()}
            expected_base_url = urllib.parse.urlparse(base_url)
            self.assertEqual(expected_base_url.netloc, actual_url.netloc)
            self.assertEqual(expected_base_url.scheme, actual_url.scheme)
            self.assertIsNotNone(actual_url.path)
            self.assertEqual('aws', actual_query_vars['replica'])
            self.assertIsNotNone(actual_query_vars['version'])

    def test_project_summary_cell_count(self):
        """
        Test per organ and total cell counter in ProjectSummaryResponse. Should return a correct total cell count and
        per organ cell count. Should not double count cell count from cell suspensions with an already counted id (
        i.e. each unique cell suspension counted exactly once).
        """
        es_hit = {
            "specimens": [
                {
                    "biomaterial_id": ["specimen1", "specimen3"],
                    "disease": ["disease1"],
                    "donor_biomaterial_id": ["donor1"],
                    "genus_species": ["species1"]
                },
                {
                    "biomaterial_id": ["specimen2"],
                    "disease": ["disease1"],
                    "donor_biomaterial_id": ["donor1"],
                    "genus_species": ["species1"]
                }
            ],
            "cell_suspensions": [
                {
                    "organ": ["organ1"],
                    "total_estimated_cells": 6,
                },
                {
                    "organ": ["organ2"],
                    "total_estimated_cells": 3,
                }

            ],
            "files": [],
            "protocols": [],
            "project": {
                "document_id": "a"
            }
        }

        expected_output = [
            {
                "key": "organ1",
                "value": 6
            },
            {
                "key": "organ2",
                "value": 3
            }
        ]

        total_cell_count, organ_cell_count = ProjectSummaryResponse.get_cell_count(es_hit)

        self.assertEqual(total_cell_count,
                         sum([cell_count['value'] for cell_count in expected_output]))
        self.assertElasticsearchResultsEqual(organ_cell_count, expected_output)

    project_buckets = {
        "buckets": [
            {
                "key": "project1",
                "term_bucket": {
                    "buckets": [
                        {
                            "key": "term1"
                        },
                        {
                            "key": "term2"
                        },
                        {
                            "key": "term3"
                        }
                    ]
                },
                "value_bucket": {
                    "value": 2
                }
            },
            {
                "key": "project2",
                "term_bucket": {
                    "buckets": []
                },
                "value_bucket": {
                    "value": 4
                }
            }
        ]
    }

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
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
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
                            "projectDescription": "As organisms age, cells accumulate genetic and epigenetic changes that eventually lead to impaired organ function or catastrophic failure such as cancer. Here we describe a single-cell transcriptome analysis of 2544 human pancreas cells from donors, spanning six decades of life. We find that islet cells from older donors have increased levels of disorder as measured both by noise in the transcriptome and by the number of cells which display inappropriate hormone expression, revealing a transcriptional instability associated with aging. By analyzing the spectrum of somatic mutations in single cells from previously-healthy donors, we find a specific age-dependent mutational signature characterized by C to A and C to G transversions, indicators of oxidative stress, which is absent in single cells from human brain tissue or in a tumor cell line. Cells carrying a high load of such mutations also express higher levels of stress and senescence markers, including FOS, JUN, and the cytoplasmic superoxide dismutase SOD1, markers previously linked to pancreatic diseases with substantial age-dependent risk, such as type 2 diabetes mellitus and adenocarcinoma. Thus, our single-cell approach unveils gene expression changes and somatic mutations acquired in aging human tissue, and identifies molecular pathways induced by these genetic changes that could influence human disease. Also, our results demonstrate the feasibility of using single-cell RNA-seq data from primary cells to derive meaningful insights into the genetic processes that operate on aging human tissue and to determine which molecular mechanisms are coordinated with these processes. Examination of single cells from primary human pancreas tissue",
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals Transcriptional Signatures of Aging and Somatic Mutation Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ["normal"],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "preservationMethod": [None],
                            "source": [
                                "cell_suspension",
                                "specimen_from_organism",
                                "donor_organism"
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
                    "cellSuspensions": [
                        {
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "selectedCellType": [],
                            "totalCells": 1
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
                            "projectDescription": "As organisms age, cells accumulate genetic and epigenetic changes that eventually lead to impaired organ function or catastrophic failure such as cancer. Here we describe a single-cell transcriptome analysis of 2544 human pancreas cells from donors, spanning six decades of life. We find that islet cells from older donors have increased levels of disorder as measured both by noise in the transcriptome and by the number of cells which display inappropriate hormone expression, revealing a transcriptional instability associated with aging. By analyzing the spectrum of somatic mutations in single cells from previously-healthy donors, we find a specific age-dependent mutational signature characterized by C to A and C to G transversions, indicators of oxidative stress, which is absent in single cells from human brain tissue or in a tumor cell line. Cells carrying a high load of such mutations also express higher levels of stress and senescence markers, including FOS, JUN, and the cytoplasmic superoxide dismutase SOD1, markers previously linked to pancreatic diseases with substantial age-dependent risk, such as type 2 diabetes mellitus and adenocarcinoma. Thus, our single-cell approach unveils gene expression changes and somatic mutations acquired in aging human tissue, and identifies molecular pathways induced by these genetic changes that could influence human disease. Also, our results demonstrate the feasibility of using single-cell RNA-seq data from primary cells to derive meaningful insights into the genetic processes that operate on aging human tissue and to determine which molecular mechanisms are coordinated with these processes. Examination of single cells from primary human pancreas tissue",
                            "projectShortname": "Single of human pancreas",
                            "projectTitle": "Single cell transcriptome patterns.",
                            "publications": [
                                {
                                    "publicationTitle": "Single-Cell Analysis of Human Pancreas Reveals Transcriptional Signatures of Aging and Somatic Mutation Patterns.",
                                    "publicationUrl": "https://www.ncbi.nlm.nih.gov/pubmed/28965763"
                                }
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina NextSeq 500"],
                            "libraryConstructionApproach": ["Smart-seq2"],
                            "pairedEnd": [True]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["female"],
                            "disease": ["normal"],
                            "genusSpecies": ["Australopithecus"],
                            "id": ["DID_scRSq06_pancreas"],
                            "organ": ["pancreas"],
                            "organPart": ["islet of Langerhans"],
                            "organismAge": ["38"],
                            "organismAgeUnit": ["year"],
                            "preservationMethod": [None],
                            "source": [
                                "cell_suspension",
                                "donor_organism",
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

    def test_project_summary_response(self):
        """
        Test that ProjectSummaryResponse will correctly do the per-project aggregations

        Should only return values associated with the given project id
        Should sum cell counts per-organ per-project and return an organ summary
        Should correctly get distinct values for diseases, species, library construction approaches for each project
        Should correctly count donor ids within a project
        """
        # Stripped down response from ES partially based on real data
        hits = [
            {
                "_id": "bae45747-546a-4aed-9377-08e9115a8fb8",
                "_source": {
                    "entity_id": "bae45747-546a-4aed-9377-08e9115a8fb8",
                    "contents": {
                        "entryId": "bae45747-546a-4aed-9377-08e9115a8fb8",
                        "specimens": [
                            {
                                "disease": ["glioblastoma"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["Q4_DEMO-donor_MGH30"],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["brain"],
                                "total_estimated_cells": 0
                            }
                        ],
                        "protocols": [
                            {
                                "library_construction_approach": ["Smart-seq2"],
                                "instrument_manufacturer_model": ["Illumina NextSeq 500"]
                            }
                        ]
                    }
                }
            },
            {
                "_id": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                "_source": {
                    "entity_id": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                    "contents": {
                        "entryId": "6ec8e247-2eb0-42d1-823f-75facd03988d",
                        "specimens": [
                            {
                                "disease": ["normal"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["284C-A1"],
                                "genus_species": ["Homo sapiens"]
                            },
                            {
                                "disease": ["normal"],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": ["284C-A1"],
                                "genus_species": ["Homo sapiens"]
                            },
                            {
                                "disease": ["not normal"],
                                "organ": ["brain"],
                                "_type": ["specimen"],
                                "total_estimated_cells": 10,
                                "donor_biomaterial_id": ["284C-A2"],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["spleen"],
                                "total_estimated_cells": 39300000
                            },
                            {
                                "organ": ["spleen"],
                                "total_estimated_cells": 1
                            },
                            {
                                "organ": ["brain"],
                                "total_estimated_cells": 10
                            }
                        ],
                        "protocols": [
                            {
                                "library_construction_approach": ["Illumina NextSeq 500"],
                                "instrument_manufacturer_model": ["Smart-seq2"]
                            }
                        ]
                    }
                }
            },
            {
                "_id": "6504d48c-1610-43aa-8cf8-214a960e110c",
                "_source": {
                    "entity_id": "6504d48c-1610-43aa-8cf8-214a960e110c",
                    "contents": {
                        "entryId": "6504d48c-1610-43aa-8cf8-214a960e110c",
                        "specimens": [
                            {
                                "disease": [],
                                "_type": ["specimen"],
                                "donor_biomaterial_id": [
                                    "CB8",
                                    "CB6",
                                    "CB2",
                                    "BM8",
                                    "BM6",
                                    "BM5",
                                    "BM4",
                                    "CB1",
                                    "CB5",
                                    "CB7",
                                    "BM2",
                                    "BM3",
                                    "BM7",
                                    "CB4",
                                    "CB3",
                                    "BM1"
                                ],
                                "genus_species": ["Homo sapiens"]
                            }
                        ],
                        "cell_suspensions": [
                            {
                                "organ": ["hematopoietic system"],
                                "total_estimated_cells": 528092
                            }
                        ],
                        "protocols": [
                            {
                                "library_construction_approach": ["Celera PicoPlus 3000"],
                                "instrument_manufacturer_model": ["Smart-seq2"]
                            }
                        ]
                    }
                }
            }
        ]

        project_summary1 = ProjectSummaryResponse(hits[0]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(1, project_summary1['donorCount'])
        self.assertEqual(0, project_summary1['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary1['genusSpecies']))
        self.assertEqual(['Smart-seq2'], sorted(project_summary1['libraryConstructionApproach']))
        self.assertEqual(['glioblastoma'], sorted(project_summary1['disease']))
        expected_organ_summary1 = [
            {
                "organType": "brain",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 0.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary1, sort_keys=True),
                         json.dumps(project_summary1['organSummaries'], sort_keys=True))

        project_summary2 = ProjectSummaryResponse(hits[1]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(2, project_summary2['donorCount'])
        self.assertEqual(39300011, project_summary2['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary2['genusSpecies']))
        self.assertEqual(['Illumina NextSeq 500'], sorted(project_summary2['libraryConstructionApproach']))
        self.assertEqual(['normal', 'not normal'], sorted(project_summary2['disease']))
        expected_organ_summary2 = [
            {
                "organType": "spleen",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 39300001.0
            },
            {
                "organType": "brain",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 10.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary2, sort_keys=True),
                         json.dumps(project_summary2['organSummaries'], sort_keys=True))

        project_summary3 = ProjectSummaryResponse(hits[2]['_source']['contents']).apiResponse.to_json()
        self.assertEqual(16, project_summary3['donorCount'])
        self.assertEqual(528092, project_summary3['totalCellCount'])
        self.assertEqual(['Homo sapiens'], sorted(project_summary3['genusSpecies']))
        self.assertEqual(['Celera PicoPlus 3000'], sorted(project_summary3['libraryConstructionApproach']))
        self.assertEqual([], sorted(project_summary3['disease']))
        expected_organ_summary3 = [
            {
                "organType": "hematopoietic system",
                "countOfDocsWithOrganType": 1,
                "totalCellCountByOrgan": 528092.0
            }
        ]
        self.assertEqual(json.dumps(expected_organ_summary3, sort_keys=True),
                         json.dumps(project_summary3['organSummaries'], sort_keys=True))

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
                    "cellSuspensions": [
                        {
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "selectedCellType": [],
                            "totalCells": 10000
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
                            "projectDescription": "Contains a small file set from the dataset: 4k PBMCs from a Healthy Donor, a Single Cell Gene Expression Dataset by Cell Ranger 2.1.0. Peripheral blood mononuclear cells (PBMCs) were taken from a healthy donor (same donor as pbmc8k). PBMCs are primary cells with relatively small amounts of RNA (~1pg RNA/cell). Data/Analysis can be found here https://support.10xgenomics.com/single-cell-gene-expression/datasets/2.1.0/pbmc4k and all data is licensed under the creative commons attribution license (https://creativecommons.org/licenses/by/4.0/). This test also contains extensive metadata for browser testing. Metadata is fabricated.",
                            "projectShortname": "staging/10x/2019-02-14T18:29:38Z",
                            "projectTitle": "10x 1 Run Integration Test",
                            "publications": [
                                {
                                    "publicationTitle": "A title of a publication goes here.",
                                    "publicationUrl": "https://europepmc.org"
                                }
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "instrumentManufacturerModel": ["Illumina HiSeq 2500"],
                            "libraryConstructionApproach": ["10X v2 sequencing"],
                            "pairedEnd": [False]
                        }
                    ],
                    "specimens": [
                        {
                            "biologicalSex": ["male"],
                            "disease": ["H syndrome"],
                            "genusSpecies": ["Homo sapiens"],
                            "id": ["specimen_ID_1"],
                            "organ": ["brain"],
                            "organPart": ["amygdala"],
                            "organismAge": ["20"],
                            "organismAgeUnit": ["year"],
                            "preservationMethod": [None],
                            "source": [
                                "cell_suspension",
                                "donor_organism",
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
            for entity_type in 'files', 'specimens', 'projects':
                with self.subTest(entity_type=entity_type):
                    url = self.base_url + "/repository/" + entity_type + "?size=2" \
                                          "&filters={'file':{'projectId':{'is':['" + test_data['id'] + "']}}}"
                    response = requests.get(url)
                    response.raise_for_status()
                    response_json = response.json()
                    for hit in response_json['hits']:
                        for project in hit['projects']:
                            if entity_type == 'projects':
                                self.assertEqual(test_data['title'], project['projectTitle'])
                            else:
                                self.assertIn(test_data['title'], project['projectTitle'])


if __name__ == '__main__':
    unittest.main()
