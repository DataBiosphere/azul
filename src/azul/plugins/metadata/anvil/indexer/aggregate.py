from operator import (
    itemgetter,
)
from typing import (
    Any,
    Optional,
)

from azul.collections import (
    compose_keys,
    none_safe_tuple_key,
)
from azul.indexer.aggregate import (
    Accumulator,
    DistinctAccumulator,
    GroupingAggregator,
    SetOfDictAccumulator,
    SimpleAggregator,
    SumAccumulator,
)
from azul.types import (
    JSON,
)


class ActivityAggregator(SimpleAggregator):
    pass


class BiosampleAggregator(SimpleAggregator):

    def _get_accumulator(self, field: str) -> Optional[Accumulator]:
        if field == 'donor_age_at_collection':
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         itemgetter('lte', 'gte')))
        else:
            return super()._get_accumulator(field)


class DatasetAggregator(SimpleAggregator):
    pass


class DiagnosisAggregator(SimpleAggregator):

    def _get_accumulator(self, field: str) -> Optional[Accumulator]:
        if field in ('diagnosis_age', 'onset_age'):
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         itemgetter('lte', 'gte')))
        else:
            return super()._get_accumulator(field)


class DonorAggregator(SimpleAggregator):
    pass


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return super()._transform_entity(entity) | {'count': (entity['document_id'], 1)}

    def _group_keys(self, entity) -> tuple[Any, ...]:
        return entity['file_format'],

    def _get_accumulator(self, field: str) -> Optional[Accumulator]:
        if field == 'count':
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._get_accumulator(field)
