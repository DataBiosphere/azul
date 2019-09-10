import difflib
import json
import unittest

from azul.logging import configure_test_logging
from azul.plugin import ServiceConfig
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump
from service import WebServiceTestCase


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestRequestBuilder(WebServiceTestCase):
    service_config = ServiceConfig(
        translation={
            "entity_id": "entity_id",
            "entity_version": "entity_version",
            "projectId": "contents.projects.document_id",
            "institution": "contents.projects.institutions",
            "laboratory": "contents.projects.laboratory",
            "libraryConstructionApproach": "contents.protocols.library_construction_approach",
            "disease": "contents.specimens.disease",
            "donorId": "contents.specimens.donor_biomaterial_id",
            "genusSpecies": "contents.specimens.genus_species"
        },
        autocomplete_translation={
            "files": {
                "entity_id": "entity_id",
                "entity_version": "entity_version"
            },
            "donor": {
                "donor": "donor_uuid"
            }
        },
        manifest={},
        facets=[],
        autocomplete_mapping_config={},
        cart_item={},
        order_config=[]
    )

    @staticmethod
    def compare_dicts(actual_output, expected_output):
        """"
        Print the two outputs along with a diff of the two
        """
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))

    def test_create_request(self):
        """
        Tests creation of a simple request
        """
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_id.keyword": [
                                            "cbb998ce-ddaf-34fa-e163-d14b399c6b34"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }
        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_empty(self):
        """
        Tests creation of an empty request. That is, no filter
        """
        expected_output = {
            "query": {
                "bool": {}
            }
        }
        sample_filter = {}
        self._test_create_request(expected_output, sample_filter, post_filter=False)

    def test_create_request_complex(self):
        """
        Tests creation of a complex request.
        """
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_id.keyword": [
                                            "cbb998ce-ddaf-34fa-e163-d14b399c6b34"
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_version.keyword": [
                                            "1993-07-19T23:50:09"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }
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
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_missing_values(self):
        """
        Tests creation of a request for facets that do not have a value
        """
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "terms": {
                                                    "contents.protocols.library_construction_approach.keyword": [
                                                        "__null__"
                                                    ]
                                                }
                                            },
                                            {
                                                "bool": {
                                                    "must_not": [
                                                        {
                                                            "exists": {
                                                                "field": "contents."
                                                                         "protocols."
                                                                         "library_construction_approach"
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }
        # Create a filter for missing values
        sample_filter = {"libraryConstructionApproach": {"is": [None]}}
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_terms_and_missing_values(self):
        """
        Tests creation of a request for a combination of facets that do and do not have a value
        """
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "terms": {
                                                    "contents.projects.laboratory.keyword": ["__null__"]
                                                }
                                            },
                                            {
                                                "bool": {
                                                    "must_not": [
                                                        {
                                                            "exists": {
                                                                "field": "contents.projects.laboratory"
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "contents.projects.institutions.keyword": ["Hogwarts"]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "terms": {
                                                    "contents.specimens.disease.keyword": ["__null__", "Dragon Pox"]
                                                }
                                            },
                                            {
                                                "bool": {
                                                    "must_not": [
                                                        {
                                                            "exists": {
                                                                "field": "contents.specimens.disease"
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }
        # Create a filter for missing values
        sample_filter = {
            "laboratory": {"is": [None]},
            "institution": {"is": ["Hogwarts"]},
            "disease": {"is": [None, "Dragon Pox"]},
        }
        self._test_create_request(expected_output, sample_filter)

    def _test_create_request(self, expected_output, sample_filter, post_filter=True):
        es_td = ElasticTransformDump(self.service_config)
        es_search = es_td._create_request(sample_filter, post_filter=post_filter)
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)
        self.compare_dicts(actual_output, expected_output)
        self.assertEqual(actual_output, expected_output)

    def test_create_aggregate(self):
        """
        Tests creation of an ES aggregate
        """
        expected_output = {
            "filter": {
                "bool": {}
            },
            "aggs": {
                "myTerms": {
                    "terms": {
                        "field": "facet1.translation.keyword",
                        "size": 99999
                    }
                },
                "untagged": {
                    "missing": {
                        "field": "facet1.translation.keyword"
                    }
                }
            }
        }
        sample_filter = {}
        agg_field = 'facet1'
        es_td = ElasticTransformDump()
        aggregation = es_td._create_aggregate(sample_filter,
                                              facet_config={agg_field: f'{agg_field}.translation'},
                                              agg=agg_field)
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(aggregation.to_dict(), sort_keys=True)
        self.compare_dicts(actual_output, expected_output)
        self.assertEqual(actual_output, expected_output)


if __name__ == '__main__':
    unittest.main()
