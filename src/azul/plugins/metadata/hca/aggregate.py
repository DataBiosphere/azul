from dataclasses import (
    dataclass,
)
from operator import (
    itemgetter,
)
from typing import (
    Any,
    Optional,
    Tuple,
)

from azul import (
    cached_property,
)
from azul.collections import (
    compose_keys,
    none_safe_itemgetter,
    none_safe_tuple_key,
)
from azul.indexer.aggregate import (
    Accumulator,
    DistinctAccumulator,
    FrequencySetAccumulator,
    GroupingAggregator,
    ListAccumulator,
    SetAccumulator,
    SetOfDictAccumulator,
    SimpleAggregator,
    SingleValueAccumulator,
    SumAccumulator,
    UniqueValueCountAccumulator,
)
from azul.indexer.document import (
    Aggregate,
    FieldTypes,
    pass_thru_int,
)
from azul.types import (
    JSON,
)


@dataclass
class HCAAggregate(Aggregate):

    @cached_property
    def total_estimated_cells(self) -> int:
        cs: JSON
        return sum(cs['total_estimated_cells']
                   for cs in self.contents['cell_suspensions']
                   if cs['total_estimated_cells'] is not None)

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return dict(super().field_types(field_types),
                    total_estimated_cells=pass_thru_int)

    def to_json(self) -> JSON:
        return dict(super().to_json(),
                    total_estimated_cells=self.total_estimated_cells)


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        fqid = entity['uuid'], entity['version']
        return dict(size=(fqid, entity['size']),
                    file_format=entity['file_format'],
                    file_source=entity['file_source'],
                    is_intermediate=entity['is_intermediate'],
                    count=(fqid, 1),
                    content_description=entity['content_description'],
                    matrix_cell_count=(fqid, entity.get('matrix_cell_count')),
                    submission_date=entity['submission_date'],
                    update_date=entity['update_date'])

    def _group_keys(self, entity) -> Tuple[Any, ...]:
        return (
            frozenset(entity['content_description']),
            entity['file_format'],
            entity['is_intermediate']
        )

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field in ('content_description', 'file_format', 'is_intermediate'):
            return SingleValueAccumulator()
        elif field == 'file_source':
            return SetAccumulator(max_size=100)
        elif field in ('size', 'count', 'matrix_cell_count'):
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._get_accumulator(field)

    def _get_default_accumulator(self) -> Optional[Accumulator]:
        return None


class SampleAggregator(SimpleAggregator):
    pass


class SpecimenAggregator(SimpleAggregator):
    pass


class CellSuspensionAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'total_estimated_cells': (entity['document_id'], entity['total_estimated_cells']),
        }

    def _group_keys(self, entity) -> Tuple[Any, ...]:
        return frozenset(entity['organ']),

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._get_accumulator(field)


class CellLineAggregator(SimpleAggregator):
    pass


class DonorOrganismAggregator(SimpleAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'donor_count': entity['biomaterial_id']
        }

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'organism_age_range':
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         itemgetter('lte', 'gte')))
        elif field == 'organism_age':
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         none_safe_itemgetter('value', 'unit')))
        elif field == 'donor_count':
            return UniqueValueCountAccumulator()
        else:
            return super()._get_accumulator(field)


class OrganoidAggregator(SimpleAggregator):
    pass


class ProjectAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return ListAccumulator(max_size=100)
        elif field in ('project_description',
                       'contact_names',
                       'contributors',
                       'publications'):
            return None
        else:
            return super()._get_accumulator(field)


class ProtocolAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
        else:
            return super()._get_accumulator(field)

    def _get_default_accumulator(self) -> Optional[Accumulator]:
        return SetAccumulator()


class SequencingInputAggregator(SimpleAggregator):
    pass


class SequencingProcessAggregator(SimpleAggregator):

    def _get_default_accumulator(self) -> Optional[Accumulator]:
        return SetAccumulator(max_size=10)


class MatricesAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field == 'file':
            return SetOfDictAccumulator(max_size=100, key=itemgetter('uuid'))
        else:
            return SetAccumulator()
