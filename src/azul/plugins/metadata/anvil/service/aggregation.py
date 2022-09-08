from elasticsearch_dsl import (
    Search,
)
from elasticsearch_dsl.aggs import (
    Agg,
)

from azul.plugins import (
    FieldPath,
)
from azul.service.elasticsearch_service import (
    AggregationStage,
)
from azul.types import (
    MutableJSON,
)


class AnvilAggregationStage(AggregationStage):

    def _prepare_aggregation(self, *, facet: str, facet_path: FieldPath) -> Agg:
        agg = super()._prepare_aggregation(facet=facet, facet_path=facet_path)
        return agg


class AnvilSummaryAggregationStage(AnvilAggregationStage):

    def prepare_request(self, request: Search) -> Search:
        request = super().prepare_request(request)
        request = request.extra(size=0)
        return request

    def process_response(self, response: MutableJSON) -> MutableJSON:
        response = super().process_response(response)
        return response['aggregations']
