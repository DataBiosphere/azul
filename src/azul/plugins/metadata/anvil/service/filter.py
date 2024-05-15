from azul.service.elasticsearch_service import (
    FilterStage,
)


class AnvilFilterStage(FilterStage):

    def _limit_access(self) -> bool:
        return self.entity_type != 'datasets'
