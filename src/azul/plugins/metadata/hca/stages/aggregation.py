from typing import (
    Mapping,
)

from elasticsearch_dsl import (
    Q,
    Search,
)
from elasticsearch_dsl.aggs import (
    Agg,
)

from azul import (
    cached_property,
    config,
)
from azul.service.elasticsearch_service import (
    AggregationStage,
)
from azul.types import (
    MutableJSON,
)


class HCAAggregationStage(AggregationStage):

    def _prepare_aggregation(self, *, facet: str, facet_path: str) -> Agg:
        agg = super()._prepare_aggregation(facet=facet, facet_path=facet_path)

        if facet == 'project':
            sub_path = self.plugin.field_mapping['projectId'] + '.keyword'
            agg.aggs['myTerms'].bucket(name='myProjectIds',
                                       agg_type='terms',
                                       field=sub_path,
                                       size=config.terms_aggregation_size)
        elif facet == 'fileFormat':
            # FIXME: Use of shadow field is brittle
            #        https://github.com/DataBiosphere/azul/issues/2289
            def set_summary_agg(field: str, bucket: str) -> None:
                path = self.plugin.field_mapping[field] + '_'
                agg.aggs['myTerms'].metric(bucket, 'sum', field=path)
                agg.aggs['untagged'].metric(bucket, 'sum', field=path)

            set_summary_agg(field='fileSize', bucket='size_by_type')
            set_summary_agg(field='matrixCellCount', bucket='matrix_cell_count_by_type')

        return agg


class HCASummaryAggregationStage(HCAAggregationStage):

    def prepare_request(self, request: Search) -> Search:
        request = super().prepare_request(request)
        entity_type = self.entity_type

        def add_filters_sum_agg(parent_field, parent_bucket, child_field, child_bucket):
            parent_field_type = self.service.field_type(self.catalog, tuple(parent_field.split('.')))
            null_value = parent_field_type.to_index(None)
            request.aggs.bucket(
                parent_bucket,
                'filters',
                filters={
                    'hasSome': Q('bool', must=[
                        Q('exists', field=parent_field),  # field exists...
                        Q('bool', must_not=[  # ...and is not zero or null
                            Q('terms', **{parent_field: [0, null_value]})
                        ])
                    ])
                },
                other_bucket_key='hasNone',
            ).metric(
                child_bucket,
                'sum',
                field=child_field
            )

        if entity_type == 'files':
            # Add a total file size aggregate
            request.aggs.metric('totalFileSize',
                                'sum',
                                field='contents.files.size_')
        elif entity_type == 'cell_suspensions':
            # Add a cell count aggregate per organ
            request.aggs.bucket(
                'cellCountSummaries',
                'terms',
                field='contents.cell_suspensions.organ.keyword',
                size=config.terms_aggregation_size
            ).bucket(
                'cellCount',
                'sum',
                field='contents.cell_suspensions.total_estimated_cells_'
            )
        elif entity_type == 'samples':
            # Add an organ aggregate to the Elasticsearch request
            request.aggs.bucket('organTypes',
                                'terms',
                                field='contents.samples.effective_organ.keyword',
                                size=config.terms_aggregation_size)
        elif entity_type == 'projects':
            # Add project cell count sum aggregates from the projects with and
            # without any cell suspension cell counts.
            add_filters_sum_agg(parent_field='contents.cell_suspensions.total_estimated_cells',
                                parent_bucket='cellSuspensionCellCount',
                                child_field='contents.projects.estimated_cell_count_',
                                child_bucket='projectCellCount')
            # Add cell suspensions cell count sum aggregates from projects
            # with and without a project level estimated cell count.
            add_filters_sum_agg(parent_field='contents.projects.estimated_cell_count',
                                parent_bucket='projectCellCount',
                                child_field='contents.cell_suspensions.total_estimated_cells_',
                                child_bucket='cellSuspensionCellCount')
        else:
            assert False, entity_type

        threshold = config.precision_threshold
        for agg_name, cardinality in self._cardinality_aggregations.items():
            request.aggs.metric(agg_name,
                                'cardinality',
                                field=cardinality + '.keyword',
                                precision_threshold=str(threshold))

        self._annotate_aggs_for_translation(request)
        request = request.extra(size=0)
        return request

    @cached_property
    def _cardinality_aggregations(self) -> Mapping[str, str]:
        return {
            'samples': {
                'specimenCount': 'contents.specimens.document_id',
                'speciesCount': 'contents.donors.genus_species',
                'donorCount': 'contents.donors.document_id',
            },
            'projects': {
                'labCount': 'contents.projects.laboratory',
            }
        }.get(self.entity_type, {})

    def process_response(self, response: MutableJSON) -> MutableJSON:
        response = super().process_response(response)
        result = response['aggregations']
        threshold = config.precision_threshold

        for agg_name in self._cardinality_aggregations:
            agg_value = result[agg_name]['value']
            assert agg_value <= threshold / 2, (agg_name, agg_value, threshold)

        return result
