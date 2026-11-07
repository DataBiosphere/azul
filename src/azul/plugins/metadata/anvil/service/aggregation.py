from opensearchpy import (
    Search,
)

from azul.lib.types import (
    MutableJSON,
    json_dict,
)
from azul.service.query_service import (
    SummaryAggregationStage,
)


class AnvilSummaryAggregationStage(SummaryAggregationStage):

    def prepare_request(self, request: Search, /) -> Search:
        request = super().prepare_request(request)
        request = request.extra(size=0)
        if self.entity_type == 'files':
            request.aggs.metric('totalFileSize',
                                'sum',
                                field='contents.files.file_size_')
        return request

    def process_response(self, response: MutableJSON, /) -> MutableJSON:
        response = super().process_response(response)
        return json_dict(response['aggregations'])
