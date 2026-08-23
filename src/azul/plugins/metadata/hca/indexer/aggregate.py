from operator import (
    itemgetter,
)
from typing import (
    Any,
)

from more_itertools import (
    one,
)

from azul.field_type import (
    FieldTypes,
    null_int,
)
from azul.indexer.aggregate import (
    Accumulator,
    DictAccumulator,
    DistinctAccumulator,
    FrequencySetAccumulator,
    GroupingAggregator,
    MaxAccumulator,
    MinAccumulator,
    SetAccumulator,
    SetOfDictAccumulator,
    SimpleAggregator,
    SingleValueAccumulator,
    SumAccumulator,
    UniqueValueCountAccumulator,
)
from azul.indexer.document import (
    Aggregate,
)
from azul.lib import (
    cached_property,
)
from azul.lib.collections import (
    compose_keys,
    none_safe_itemgetter,
    none_safe_key,
    none_safe_tuple_key,
)
from azul.lib.types import (
    JSON,
    json_element_mappings,
    json_int,
    optional,
)


class HCAAggregate(Aggregate):

    @cached_property
    def cell_count(self) -> int:
        assert self.contents is not None, self
        return sum(json_int(cs['total_estimated_cells'])
                   for cs in json_element_mappings(self.contents['cell_suspensions'])
                   if cs['total_estimated_cells'] is not None)

    @cached_property
    def effective_cell_count(self) -> int:
        assert self.contents is not None, self
        if self.entity.entity_type == 'projects':
            project = one(json_element_mappings(self.contents['projects']))
            project_cells = optional(json_int, project['estimated_cell_count'])
            if project_cells is None:
                return self.cell_count
            else:
                return project_cells
        else:
            return self.cell_count

    @classmethod
    def field_types(cls, field_types: FieldTypes) -> FieldTypes:
        return dict(super().field_types(field_types),
                    cell_count=null_int,
                    effective_cell_count=null_int)

    def to_json(self) -> JSON:
        return dict(super().to_json(),
                    cell_count=self.cell_count,
                    effective_cell_count=self.effective_cell_count)


class HCAEntityAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('biomaterial_id', 'document_id'):
            if self.outer_entity_type == 'files':
                return self._id_field_accumulator(field)
            else:
                return None
        elif field in ('biomaterial_name', 'protocol_name'):
            if self.outer_entity_type == 'files':
                # FIXME: Resize accumulators and disallow overflow
                #        https://github.com/DataBiosphere/azul/issues/8237
                return SetAccumulator(max_size=100, allow_overflow=True)
            else:
                return None
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return super()._accumulator(field)


class HCAHotEntityAggregator(HCAEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        # Hot entity types must always accumulate `document_id` so that
        # none are omitted during the verbatim handover. We can't track
        # their hubs in replica documents, so we rely on the inner entity
        # IDs instead.
        if field == 'document_id':
            return self._id_field_accumulator(field)
        else:
            return super()._accumulator(field)


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        fqid = entity['uuid'], entity['version']
        return dict(size=(fqid, entity['size']),
                    file_format=entity['file_format'],
                    file_source=entity['file_source'],
                    is_intermediate=entity['is_intermediate'],
                    count=(fqid, 1),
                    content_description=entity['content_description'],
                    matrix_cell_count=(fqid, entity.get('matrix_cell_count')))

    def _group_keys(self, entity) -> tuple[Any, ...]:
        return (
            frozenset(entity['content_description']),
            entity['file_format'],
            entity['is_intermediate']
        )

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('content_description', 'file_format', 'is_intermediate'):
            return SingleValueAccumulator()
        elif field == 'file_source':
            return SetAccumulator(max_size=100)
        elif field in ('size', 'count', 'matrix_cell_count'):
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._accumulator(field)

    def _default_accumulator(self) -> Accumulator | None:
        return None


class SampleAggregator(HCAEntityAggregator):

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(1209 * 1.25))


class SpecimenAggregator(HCAEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        # `document_id` is included in the sample aggregate so that the
        # summary response field `specimenCount` can be calculated. This
        # should not be a problem since there should only ever be one
        # specimen inner entity in a samples outer entity.
        if field == 'document_id' and self.outer_entity_type == 'samples':
            return self._default_accumulator()
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(1209 * 1.25))


class CellSuspensionAggregator(HCAEntityAggregator, GroupingAggregator):
    cell_count_fields = frozenset([
        'total_estimated_cells',
        'total_estimated_cells_redundant'
    ])

    def _transform_entity(self, entity: JSON) -> JSON:
        cell_count_fields = {
            field: (entity['document_id'], entity[field])
            for field in self.cell_count_fields
        }
        return {
            **entity,
            **cell_count_fields
        }

    def _group_keys(self, entity) -> tuple[Any, ...]:
        return frozenset(entity['organ']),

    def _accumulator(self, field) -> Accumulator | None:
        if field in self.cell_count_fields:
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(9766 * 1.25))


class CellLineAggregator(HCAEntityAggregator):
    pass


class DonorOrganismAggregator(HCAHotEntityAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'donor_count': entity['biomaterial_id']
        }

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'development_stage':
            return SetAccumulator(max_size=int(124 * 1.25))
        elif field == 'organism_age_range':
            return SetAccumulator(max_size=int(107 * 1.25))
        elif field == 'organism_age':
            return SetOfDictAccumulator(max_size=int(107 * 1.25),
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         none_safe_itemgetter('value', 'unit')))
        elif field == 'donor_count':
            return UniqueValueCountAccumulator()
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(931 * 1.25))


class OrganoidAggregator(HCAEntityAggregator):
    pass


class ProjectAggregator(HCAHotEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('project_description',
                     'contact_names',
                     'contributors',
                     'publications'):
            return None
        elif field == 'estimated_cell_count':
            return MaxAccumulator()
        elif field == 'accessions':
            return SetOfDictAccumulator(key=compose_keys(none_safe_key(),
                                                         none_safe_itemgetter('accession')))
        elif field == 'tissue_atlas':
            return SetOfDictAccumulator(key=compose_keys(none_safe_tuple_key(),
                                                         none_safe_itemgetter('atlas', 'version')))
        else:
            return super()._accumulator(field)


class ProtocolAggregator(HCAHotEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=100)

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator()


class SequencingInputAggregator(HCAEntityAggregator):

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(7302 * 1.25))


class SequencingProcessAggregator(HCAEntityAggregator):

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return SetAccumulator(max_size=int(6357 * 1.25))

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator(max_size=10)


class MatricesAggregator(HCAEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'file':
            return DictAccumulator(max_size=int(515 * 1.25), key=itemgetter('uuid'))
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return None

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator()


class DateAggregator(HCAEntityAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('submission_date', 'aggregate_submission_date'):
            return MinAccumulator()
        elif field in ('update_date', 'aggregate_update_date'):
            return MaxAccumulator()
        elif field in ('last_modified_date', 'aggregate_last_modified_date'):
            return MaxAccumulator()
        else:
            return super()._accumulator(field)

    def _id_field_accumulator(self, field) -> Accumulator | None:
        return None
