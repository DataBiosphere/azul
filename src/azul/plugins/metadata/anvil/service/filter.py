from azul.service.elasticsearch_service import (
    FilterStage,
)


class AnvilFilterStage(FilterStage):

    def _limit_access(self) -> bool:
        return self.service.always_limit_access() or self.entity_type != 'datasets'
