from collections.abc import (
    Mapping,
    Sequence,
)
import json

import attr

from azul import (
    CatalogName,
)
from azul.logging import (
    configure_test_logging,
)
from azul.plugins import (
    FieldPath,
    ManifestConfig,
    MetadataPlugin,
    SpecialFields,
)
from azul.plugins.metadata.hca import (
    Plugin,
)
from azul.plugins.metadata.hca.service.aggregation import (
    HCAAggregationStage,
)
from azul.service import (
    Filters,
)
from azul.service.elasticsearch_service import (
    ElasticsearchService,
    ToDictStage,
)
from indexer import (
    DCP1CannedBundleTestCase,
)
from service import (
    WebServiceTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class TestRequestBuilder(DCP1CannedBundleTestCase, WebServiceTestCase):
    # Subclass the class under test so we can inject a mock plugin
    @attr.s(frozen=True, auto_attribs=True)
    class Service(ElasticsearchService):
        plugin: MetadataPlugin

        def metadata_plugin(self, catalog: CatalogName) -> MetadataPlugin:
            return self.plugin

    # The mock plugin
    class MockPlugin(Plugin):

        @property
        def special_fields(self) -> SpecialFields:
            return SpecialFields(source_id='sourceId')

        @property
        def _field_mapping(self) -> MetadataPlugin._FieldMapping:
            return {
                'default_factory': {},
                'entity_id': 'entity_id',
                'sources': {
                    'id': 'sourceId'
                },
                'contents': {
                    'projects': {
                        'document_id': 'projectId',
                        'institutions': 'institution',
                        'laboratory': 'laboratory'
                    },
                    'library_preparation_protocols': {
                        'library_construction_approach': 'libraryConstructionApproach'
                    },
                    'specimens': {
                        'disease': 'specimenDisease',
                        'donor_biomaterial_id': 'donorId',
                        'genus_species': 'genusSpecies'
                    }
                }
            }

        @property
        def manifest(self) -> ManifestConfig:
            return {}

        @property
        def facets(self) -> Sequence[str]:
            return []

    sources_filter = {
        'constant_score': {
            'filter': {
                'terms': {
                    'sources.id.keyword': []
                }
            }
        }
    }

    def test_create_request(self):
        """
        Tests creation of a simple request
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'terms': {
                                        'entity_id.keyword': [
                                            'cbb998ce-ddaf-34fa-e163-d14b399c6b34'
                                        ]
                                    }
                                }
                            }
                        },
                        self.sources_filter
                    ]
                }
            }
        }
        sample_filter = {'entity_id': {'is': ['cbb998ce-ddaf-34fa-e163-d14b399c6b34']}}
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
            'query': {
                'bool': {
                    'must': [
                        self.sources_filter
                    ]
                }
            }
        }
        sample_filter = {}
        self._test_create_request(expected_output, sample_filter, post_filter=False)

    def test_create_request_complex(self):
        """
        Tests creation of a complex request.
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'terms': {
                                        'entity_id.keyword': [
                                            'cbb998ce-ddaf-34fa-e163-d14b399c6b34'
                                        ]
                                    }
                                }
                            }
                        },
                        self.sources_filter
                    ]
                }
            }
        }
        sample_filter = {
            'entity_id':
                {
                    'is': ['cbb998ce-ddaf-34fa-e163-d14b399c6b34']
                }
        }
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_missing_values(self):
        """
        Tests creation of a request for facets that do not have a value
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.'
                                                    'library_preparation_protocols.'
                                                    'library_construction_approach.keyword': [
                                                        '~null'
                                                    ]
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.'
                                                                         'library_preparation_protocols.'
                                                                         'library_construction_approach'
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
                        self.sources_filter
                    ]
                }
            }
        }
        # Create a filter for missing values
        sample_filter = {'libraryConstructionApproach': {'is': [None]}}
        self._test_create_request(expected_output, sample_filter)

    def test_create_request_terms_and_missing_values(self):
        """
        Tests creation of a request for a combination of facets that do and do
        not have a value
        """
        expected_output = {
            'post_filter': {
                'bool': {
                    'must': [
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.projects.laboratory.keyword': ['~null']
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.projects.laboratory'
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
                            'constant_score': {
                                'filter': {
                                    'terms': {
                                        'contents.projects.institutions.keyword': ['Hogwarts']
                                    }
                                }
                            }
                        },
                        {
                            'constant_score': {
                                'filter': {
                                    'bool': {
                                        'should': [
                                            {
                                                'terms': {
                                                    'contents.specimens.disease.keyword': ['~null', 'Dragon Pox']
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'must_not': [
                                                        {
                                                            'exists': {
                                                                'field': 'contents.specimens.disease'
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
                        self.sources_filter
                    ]
                }
            }
        }
        # Create a filter for missing values
        sample_filter = {
            'laboratory': {'is': [None]},
            'institution': {'is': ['Hogwarts']},
            'specimenDisease': {'is': [None, 'Dragon Pox']},
        }
        self._test_create_request(expected_output, sample_filter)

    def _test_create_request(self,
                             expected_output,
                             sample_filter,
                             post_filter=True
                             ):
        service = self.Service(self.MockPlugin())
        filters = Filters(explicit=sample_filter, source_ids=set())
        request = self._prepare_request(filters, post_filter, service)
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(request.to_dict(), sort_keys=True)
        self.assertEqual(actual_output, expected_output)

    def _prepare_request(self, filters, post_filter, service):
        entity_type = 'files'
        pipeline = service.create_chain(catalog=self.catalog,
                                        entity_type=entity_type,
                                        filters=filters,
                                        post_filter=post_filter,
                                        document_slice=None)
        pipeline = ToDictStage(service=service,
                               catalog=self.catalog,
                               entity_type=entity_type).wrap(pipeline)
        pipeline = HCAAggregationStage.create_and_wrap(pipeline)
        request = pipeline.prepare_request(service.create_request(self.catalog, entity_type))
        return request

    def test_create_aggregate(self):
        """
        Tests creation of an ES aggregate
        """
        expected_output = {
            'filter': {
                'bool': {
                    'must': [
                        self.sources_filter
                    ]
                }
            },
            'aggs': {
                'myTerms': {
                    'terms': {
                        'field': 'path.to.foo.keyword',
                        'size': 99999
                    },
                    'meta': {
                        'path': ['path', 'to', 'foo']
                    }
                },
                'untagged': {
                    'missing': {
                        'field': 'path.to.foo.keyword'
                    }
                }
            }
        }

        class MockPlugin(self.MockPlugin):

            @property
            def field_mapping(self) -> Mapping[str, FieldPath]:
                return {
                    'sourceId': ('sources', 'id'),
                    'foo': ('path', 'to', 'foo')
                }

            @property
            def facets(self) -> Sequence[str]:
                return ['foo']

        service = self.Service(MockPlugin())

        filters = Filters(explicit={}, source_ids=set())
        post_filter = True
        request = self._prepare_request(filters, post_filter, service)
        aggregation = request.aggs['foo']
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(aggregation.to_dict(), sort_keys=True)
        self.assertEqual(actual_output, expected_output)
