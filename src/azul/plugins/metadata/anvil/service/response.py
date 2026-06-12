from functools import (
    partial,
)
from typing import (
    Mapping,
    Sequence,
    cast,
)

from more_itertools import (
    one,
)

from azul.lib import (
    cached_property,
)
from azul.lib.json import (
    copy_any_json,
    copy_json,
)
from azul.lib.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
    MutableJSONArray,
    MutableJSONs,
    json_element_mappings,
    json_float,
    json_int,
    json_item_sequences,
    json_mapping,
    json_sequence_of_mappings,
    json_str,
    json_untyped_dict,
    optional,
)
from azul.plugins import (
    SpecialFields,
)
from azul.service.index_service import (
    SearchResponseStage,
    SummaryResponseStage,
)
from azul.service.query_service import (
    ResponseTriple,
    untagged_agg_name,
    values_agg_name,
)
from azul.source import (
    SourceRef,
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
                'donors.organism_type',
                'diagnoses.disease',
                'diagnoses.phenotype'
            ],
            'files': [
                'files.file_format',
                'totalFileSize'
            ]
        }

    def process_response(self, response: JSON) -> MutableJSON:
        def doc_count(field: str) -> int:
            return json_int(json_mapping(response[field])['doc_count'])

        def bucket_count(field: str, bucket_key: str):
            aggs = json_mapping(response[field])
            agg = json_mapping(aggs[values_agg_name])
            buckets = json_element_mappings(agg['buckets'])
            return [
                {
                    'count': bucket['doc_count'],
                    bucket_key: bucket['key']
                }
                for bucket in buckets
            ]

        return {
            'activityCount': doc_count('activities.activity_type'),
            'activityTypes': bucket_count('activities.activity_type', 'type'),
            'biosampleCount': doc_count('biosamples.anatomical_site'),
            'datasetCount': doc_count('datasets.title'),
            'donorCount': doc_count('donors.organism_type'),
            'donorDiagnosisDiseases': bucket_count('diagnoses.disease', 'disease'),
            'donorDiagnosisPhenotypes': bucket_count('diagnoses.phenotype', 'phenotype'),
            'donorSpecies': bucket_count('donors.organism_type', 'species'),
            'fileCount': doc_count('files.file_format'),
            'fileFormats': bucket_count('files.file_format', 'format'),
            'totalFileSize': json_float(json_mapping(response['totalFileSize'])['value']),
        }


class AnvilSearchResponseStage(SearchResponseStage):

    def process_response(self, response: ResponseTriple) -> MutableJSON:
        hits, pagination, aggs = response
        return dict(
            hits=list(map(self._make_hit, hits)),
            pagination=json_untyped_dict(pagination),
            termFacets=dict(zip(
                aggs.keys(),
                map(self._make_terms, map(json_mapping, aggs.values())))
            )
        )

    def _make_terms(self, agg: JSON) -> MutableJSON:
        # FIXME: much of this is duplicated from
        #        azul.plugins.metadata.hca.service.response.SearchResponseFactory
        #        https://github.com/DataBiosphere/azul/issues/4135
        def choose_entry(_term) -> AnyMutableJSON:
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

        buckets = json_mapping(agg[values_agg_name])['buckets']

        terms: MutableJSONs = [
            {
                'term': choose_entry(bucket),
                'count': json_int(bucket['doc_count'])
            }
            for bucket in json_element_mappings(buckets)
        ]

        # Add the untagged_count to the existing termObj for a None value,
        # or add a new one
        untagged_count = json_int(json_mapping(agg[untagged_agg_name])['doc_count'])
        if untagged_count > 0:
            for term in terms:
                if term['term'] is None:
                    term['count'] = json_int(term['count']) + untagged_count
                    break
            else:
                terms.append({'term': None, 'count': untagged_count})

        return {
            # Mypy doesn't allow MutableJSONs to be used in place of
            # MutableJSONArray due to list invariance:
            # (see https://mypy.readthedocs.io/en/stable/common_issues.html#invariance-vs-covariance)
            # it is possible for the caller to modify the return value in a way
            # that violates the type annotation for `terms`, e.g.
            #
            # x = self._make_terms(...); x['terms'].append('not a dict')
            #
            # The cast is always safe because the local variable `terms` goes
            # out of scope immediately afterward, so no one ever holds onto a
            # reference to `terms` that uses the MutableJSONs type annotation.
            'terms': cast(MutableJSONArray, terms),
            'total': 0 if len(terms) == 0 else json_int(agg['doc_count']),
            # FIXME: Remove type from termsFacets in /index responses
            #        https://github.com/DataBiosphere/azul/issues/2460
            'type': 'terms'
        }

    def _make_hit(self, es_hit: JSON) -> MutableJSON:
        contents = json_mapping(es_hit['contents'])
        sources = json_sequence_of_mappings(es_hit['sources'])
        bundles = json_element_mappings(es_hit['bundles'])
        source: SourceRef = SourceRef.from_json(one(sources))
        return {
            'entryId': json_str(es_hit['entity_id']),
            # Note that there is a brittle coupling that must be maintained
            # between the `sources` and `bundles` field paths here and the
            # renamed fields in `Plugin.manifest_config`.
            'sources': list(map(self._make_source, sources)),
            'bundles': list(map(self._make_bundle, bundles)),
            **self._make_contents(source, contents)
        }

    def _make_source(self, es_source: JSON) -> MutableJSON:
        return {
            self._special_fields.source_prefix.name_in_hit: json_str(es_source['prefix']),
            self._special_fields.source_spec.name_in_hit: json_str(es_source['spec']),
            self._special_fields.source_id.name_in_hit: json_str(es_source['id'])
        }

    @cached_property
    def _special_fields(self) -> SpecialFields:
        return self.plugin.special_fields

    def _make_bundle(self, es_bundle: JSON) -> MutableJSON:
        return {
            self._special_fields.bundle_uuid.name_in_hit: json_str(es_bundle['uuid']),
            self._special_fields.bundle_version.name_in_hit: json_str(es_bundle['version'])
        }

    def _make_contents(self, source: SourceRef, es_contents: JSON) -> MutableJSON:
        return {
            inner_entity_type: (
                [
                    self._pivotal_entity(source,
                                         inner_entity_type,
                                         json_mapping(one(inner_entities)))
                ]
                if inner_entity_type == self.entity_type else
                list(map(partial(self._non_pivotal_entity, inner_entity_type), inner_entities))
            )
            for inner_entity_type, inner_entities in json_item_sequences(es_contents)
        }

    def _pivotal_entity(self,
                        source: SourceRef,
                        inner_entity_type: str,
                        inner_entity: JSON,
                        ) -> MutableJSON:
        inner_entity = copy_json(inner_entity)
        if inner_entity_type == 'files':
            inner_entity['azul_url'] = self._file_url(uuid=json_str(inner_entity['document_id']),
                                                      version=json_str(inner_entity['version']),
                                                      drs_uri=optional(json_str, inner_entity['drs_uri']))
            inner_entity['azul_mirror_uri'] = self._file_mirror_uri(source, inner_entity)
            inner_entity.pop('version', None)
        return inner_entity

    def _non_pivotal_entity(self,
                            inner_entity_type: str,
                            inner_entity: JSON
                            ) -> MutableJSON:
        fields = self._non_pivotal_fields_by_entity_type[inner_entity_type]
        return {
            k: copy_any_json(v)
            for k, v in inner_entity.items()
            if k in fields
        }

    @cached_property
    def _non_pivotal_fields_by_entity_type(self) -> dict[str, set[str]]:
        return {
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
                'consent_group',
                'data_modality',
                'dataset_id',
                'data_use_permission',
                'duos_id',
                'registered_identifier',
                'title'
            },
            'diagnoses': {
                'disease',
                'phenotype',
                'phenopacket',
                'onset_age_unit',
                'diagnosis_age_unit',
                *(
                    # These fields are of high cardinality, but the number of
                    # aggregated inner entities per donor should be low. Since
                    # diagnoses do not appear in the index as outer entities,
                    # this is our only opportunity to display these fields.
                    [
                        'diagnosis_age',
                        'onset_age'
                    ]
                    if self.entity_type == 'donors' else
                    []
                )
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
                'file_size',
                'is_supplementary',
                'reference_assembly'
            }
        }
