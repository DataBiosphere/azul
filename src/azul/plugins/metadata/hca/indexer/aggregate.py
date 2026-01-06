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


class SampleAggregator(SimpleAggregator):
    pass


class SpecimenAggregator(SimpleAggregator):
    pass


class CellSuspensionAggregator(GroupingAggregator):
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
        if field in ('biomaterial_id', 'document_id'):
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        elif field in self.cell_count_fields:
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._accumulator(field)


class CellLineAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == ('biomaterial_id', 'document_id'):
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        else:
            return super()._accumulator(field)


class DonorOrganismAggregator(SimpleAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return {
            **entity,
            'donor_count': entity['biomaterial_id']
        }

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'biomaterial_id':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        elif field == 'organism_age_range':
            return SetAccumulator(max_size=100)
        elif field == 'organism_age':
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         none_safe_itemgetter('value', 'unit')))
        elif field == 'donor_count':
            return UniqueValueCountAccumulator()
        elif field == 'document_id':
            # If any donor IDs are missing from the aggregate, those donors will
            # be omitted during the verbatim handover. Donors are a "hot" entity
            # type, and we can't track their hubs in replica documents, so we
            # rely on the inner entity IDs instead.
            #
            # FIXME: Enforce that hot entity types are completely aggregated
            #        https://github.com/DataBiosphere/azul/issues/6793
            return SetAccumulator(max_size=100)
        else:
            return super()._accumulator(field)


class OrganoidAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('biomaterial_id', 'document_id'):
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        else:
            return super()._accumulator(field)


class ProjectAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'document_id':
            # If any project IDs are missing from the aggregate, those projects
            # will be omitted during the verbatim handover. Projects are a "hot"
            # entity type, and we can't track their hubs in replica documents,
            # so we rely on the inner entity IDs instead. We also need to
            # aggregate `document_id` to allow filtering by `projectId` on
            # non-project endpoints.
            return SetAccumulator(max_size=100)
        elif field in ('project_description',
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


class ProtocolAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
        elif field == 'document_id':
            # If any protocol IDs are missing from the aggregate, those
            # protocols may be omitted during the verbatim handover. Some
            # protocols are "hot" entity types, and we can't track their hubs in
            # replicas, so we rely on the inner entity IDs instead.
            #
            # FIXME: Enforce that hot entity types are completely aggregated
            #        https://github.com/DataBiosphere/azul/issues/6793
            return SetAccumulator(max_size=100)
        else:
            return super()._accumulator(field)

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator()


class SequencingInputAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field in ('biomaterial_id', 'document_id'):
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        else:
            return super()._accumulator(field)


class SequencingProcessAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'document_id':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            if self.outer_entity_type == 'files':
                return super()._accumulator(field)
            else:
                return None
        else:
            return super()._accumulator(field)

    def _default_accumulator(self) -> Accumulator | None:
        return SetAccumulator(max_size=10)


class MatricesAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'document_id':
            return None
        elif field == 'file':
            return DictAccumulator(max_size=100, key=itemgetter('uuid'))
        else:
            return SetAccumulator()


class DateAggregator(SimpleAggregator):

    def _accumulator(self, field) -> Accumulator | None:
        if field == 'document_id':
            return None
        elif field in ('submission_date', 'aggregate_submission_date'):
            return MinAccumulator()
        elif field in ('update_date', 'aggregate_update_date'):
            return MaxAccumulator()
        elif field in ('last_modified_date', 'aggregate_last_modified_date'):
            return MaxAccumulator()
        else:
            return super()._accumulator(field)
