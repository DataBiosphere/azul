from opensearchpy import (
    Search,
)
from opensearchpy.helpers.aggs import (
    Agg,
)

from azul.indexer.document import (
    FieldPath,
)
from azul.lib.types import (
    MutableJSON,
    json_dict,
)
from azul.service.query_service import (
    AggregationStage,
    SummaryAggregationStage,
)


class AnvilAggregationStage(AggregationStage):

    def _prepare_aggregation(self, *, facet: str, facet_path: FieldPath) -> Agg:
        agg = super()._prepare_aggregation(facet=facet, facet_path=facet_path)
        return agg


class AnvilSummaryAggregationStage(SummaryAggregationStage, AnvilAggregationStage):

    def prepare_request(self, request: Search) -> Search:
        request = super().prepare_request(request)
        request = request.extra(size=0)
        if self.entity_type == 'files':
            request.aggs.metric('totalFileSize',
                                'sum',
                                field='contents.files.file_size_')
        return request

    def process_response(self, response: MutableJSON) -> MutableJSON:
        response = super().process_response(response)
        return json_dict(response['aggregations'])
