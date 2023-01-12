from functools import (
    partial,
)
from typing import (
    Mapping,
    Sequence,
)

from more_itertools import (
    one,
)

from azul import (
    JSON,
)
from azul.collections import (
    dict_merge,
)
from azul.json import (
    copy_json,
)
from azul.service.elasticsearch_service import (
    ResponseTriple,
)
from azul.service.repository_service import (
    SearchResponseStage,
    SummaryResponseStage,
)
from azul.types import (
    MutableJSON,
)


class AnvilSummaryResponseStage(SummaryResponseStage):

    @property
    def aggs_by_authority(self) -> Mapping[str, Sequence[str]]:
        return {
            'activities': [
                'activities.activity_type'
            ],
            'biosamples': [
                'biosamples.anatomical_site'
            ],
            'datasets': [
                'datasets.title'
            ],
            'donors': [
                'donors.organism_type'
            ],
            'files': [
                'files.file_format'
            ]
        }

    def process_response(self, response: JSON) -> JSON:
        def count(field, key):
            return {key: response[field]['doc_count']}

        def bucket_count(field, key, buckets_key, bucket_key):
            return count(field, key) | {
                buckets_key: [
                    {
                        'count': bucket['doc_count'],
                        bucket_key: bucket['key']
                    }
                    for bucket in response[field]['myTerms']['buckets']
                ]
            }

        return dict_merge([
            bucket_count('files.file_format', 'fileCount', 'fileFormats', 'format'),
            bucket_count('activities.activity_type', 'activityCount', 'activityTypes', 'type'),
            bucket_count('donors.organism_type', 'donorCount', 'donorSpecies', 'species'),
            count('biosamples.anatomical_site', 'biosampleCount')
        ])


class AnvilSearchResponseStage(SearchResponseStage):

    def process_response(self, response: ResponseTriple) -> MutableJSON:
        hits, pagination, aggs = response
        return dict(
            hits=list(map(self._make_hit, hits)),
            pagination=pagination,
            termFacets=dict(zip(aggs.keys(), map(self._make_terms, aggs.values())))
        )

    def _make_terms(self, agg: JSON) -> JSON:
        # FIXME: much of this is duplicated from
        #        azul.plugins.metadata.hca.service.response.SearchResponseFactory
        #        https://github.com/DataBiosphere/azul/issues/4135
        def choose_entry(_term):
            if 'key_as_string' in _term:
                return _term['key_as_string']
            elif (term_key := _term['key']) is None:
                return None
            elif isinstance(term_key, bool):
                return str(term_key).lower()
            elif isinstance(term_key, dict):
                return term_key
            else:
                return str(term_key)

        terms = [
            {
                'term': choose_entry(bucket),
                'count': bucket['doc_count']
            }
            for bucket in agg['myTerms']['buckets']
        ]

        # Add the untagged_count to the existing termObj for a None value, or add a new one
        untagged_count = agg['untagged']['doc_count']
        if untagged_count > 0:
            for term in terms:
                if term['term'] is None:
                    term['count'] += untagged_count
                    break
            else:
                terms.append({'term': None, 'count': untagged_count})

        return {
            'terms': terms,
            'total': 0 if len(agg['myTerms']['buckets']) == 0 else agg['doc_count'],
            # FIXME: Remove type from termsFacets in /index responses
            #        https://github.com/DataBiosphere/azul/issues/2460
            'type': 'terms'
        }

    def _make_hit(self, es_hit: JSON) -> MutableJSON:
        return {
            'entryId': es_hit['entity_id'],
            'sources': list(map(self._make_source, es_hit['sources'])),
            'bundles': list(map(self._make_bundle, es_hit['bundles'])),
            **self._make_contents(es_hit['contents'])
        }

    def _make_source(self, es_source: JSON) -> MutableJSON:
        return {
            'sourceSpec': es_source['spec'],
            'sourceId': es_source['id']
        }

    def _make_bundle(self, es_bundle: JSON) -> MutableJSON:
        return {
            'bundleUuid': es_bundle['uuid'],
            'bundleVersion': es_bundle['version']
        }

    def _make_contents(self, es_contents: JSON) -> MutableJSON:
        return {
            inner_entity_type: (
                [self._pivotal_entity(inner_entity_type, one(inner_entities))]
                if inner_entity_type == self.entity_type else
                list(map(partial(self._non_pivotal_entity, inner_entity_type), inner_entities))
            )
            for inner_entity_type, inner_entities in es_contents.items()
        }

    def _pivotal_entity(self,
                        inner_entity_type: str,
                        inner_entity: JSON
                        ) -> MutableJSON:
        inner_entity = copy_json(inner_entity)
        if inner_entity_type == 'files':
            inner_entity['uuid'] = inner_entity['document_id']
        return inner_entity

    def _non_pivotal_entity(self,
                            inner_entity_type: str,
                            inner_entity: JSON
                            ) -> MutableJSON:
        fields = self._non_pivotal_fields_by_entity_type[inner_entity_type]
        return {
            k: v
            for k, v in inner_entity.items()
            if k in fields
        }

    _non_pivotal_fields_by_entity_type = {
        'activities': {
            'activity_type',
            'assay_type',
            'data_modality'
        },
        'biosamples': {
            'anatomical_site',
            'biosample_type',
            'disease',
            'donor_age_at_collection_unit',
            'donor_age_at_collection',
        },
        'datasets': {
            'dataset_id',
            'title'
        },
        'donors': {
            'organism_type',
            'phenotypic_sex',
            'reported_ethnicity',
            'genetic_ancestry'
        },
        'files': {
            'count',
            'data_modality',
            'file_format',
            'reference_assembly'
        }
    }
