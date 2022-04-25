from azul.service.elasticsearch_service import (
    FilterStage,
)


class HCAFilterStage(FilterStage):

    def _reify_filters(self):
        if self.entity_type == 'projects':
            filters = self.filters.explicit
        else:
            filters = super()._reify_filters()
        return filters
