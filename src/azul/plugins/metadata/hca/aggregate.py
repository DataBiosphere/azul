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
                    source=entity['source'],
                    is_intermediate=entity['is_intermediate'],
                    count=(fqid, 1),
                    content_description=entity['content_description'],
                    matrix_cell_count=(fqid, entity['matrix_cell_count']))

    def _group_keys(self, entity) -> Tuple[Any]:
        return entity['file_format'], entity['is_intermediate']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field in ('file_format', 'is_intermediate'):
            return SingleValueAccumulator()
        elif field in ('source', 'content_description'):
            return SetAccumulator(max_size=100)
        elif field in ('size', 'count', 'matrix_cell_count'):
            return DistinctAccumulator(SumAccumulator())
        else:
            return None


class SampleAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class SpecimenAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class CellSuspensionAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'total_estimated_cells': (entity['document_id'], entity['total_estimated_cells']),
        }

    def _group_keys(self, entity) -> Tuple[Any]:
        return frozenset(entity['organ']),

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return DistinctAccumulator(SumAccumulator())
        else:
            return SetAccumulator(max_size=100)


class CellLineAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


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
            return SetAccumulator(max_size=100)


class OrganoidAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


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
            return SetAccumulator(max_size=100)


class ProtocolAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
        else:
            return SetAccumulator()


class SequencingProcessAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=10)


class MatricesAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'document_id':
            return None
        elif field == 'file':
            return SetOfDictAccumulator(max_size=100, key=itemgetter('uuid'))
        else:
            return SetAccumulator()
