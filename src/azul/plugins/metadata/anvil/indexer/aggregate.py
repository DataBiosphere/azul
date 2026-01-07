from operator import (
    itemgetter,
)
from typing import (
    Any,
)

from azul.indexer.aggregate import (
    Accumulator,
    DistinctAccumulator,
    GroupingAggregator,
    SetOfDictAccumulator,
    SimpleAggregator,
    SumAccumulator,
)
from azul.lib.collections import (
    compose_keys,
    none_safe_tuple_key,
)
from azul.lib.types import (
    JSON,
)


class ActivityAggregator(SimpleAggregator):

    def _accumulator(self, field: str) -> Accumulator | None:
        if field in {
            'activity_id',
            'document_id',
            'source_datarepo_row_ids'
        } and self.outer_entity_type != 'files':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            return None
        else:
            return super()._accumulator(field)


class BiosampleAggregator(SimpleAggregator):

    def _accumulator(self, field: str) -> Accumulator | None:
        if field in {
            'biosample_id',
            'document_id',
            'source_datarepo_row_ids'
        } and self.outer_entity_type != 'files':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            return None
        elif field == 'donor_age_at_collection':
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         itemgetter('lte', 'gte')))
        else:
            return super()._accumulator(field)


class DatasetAggregator(SimpleAggregator):

    def _accumulator(self, field: str) -> Accumulator | None:
        if field == 'document_id':
            # If any dataset IDs are missing from the aggregate, those datasets
            # will be omitted during the verbatim handover. Datasets are a "hot"
            # entity type, and we can't track their hubs in replica documents,
            # so we rely on the inner entity IDs instead. We also need to
            # aggregate document_id to allow filtering by the value on
            # non-dataset endpoints.
            return super()._accumulator(field)
        elif field == 'source_datarepo_row_ids' and self.outer_entity_type != 'files':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            return None
        else:
            return super()._accumulator(field)


class DiagnosisAggregator(SimpleAggregator):

    def _accumulator(self, field: str) -> Accumulator | None:
        if field in {
            'diagnosis_id',
            'document_id',
            'source_datarepo_row_ids'
        } and self.outer_entity_type != 'files':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            return None
        elif field in ('diagnosis_age', 'onset_age'):
            return SetOfDictAccumulator(max_size=100,
                                        key=compose_keys(none_safe_tuple_key(none_last=True),
                                                         itemgetter('lte', 'gte')))
        else:
            return super()._accumulator(field)


class DonorAggregator(SimpleAggregator):

    def _accumulator(self, field: str) -> Accumulator | None:
        if field in {
            'document_id',
            'donor_id',
            'source_datarepo_row_ids'
        } and self.outer_entity_type != 'files':
            # These fields are only aggregated for files, where they are needed
            # for compact and PFB manifests
            return None
        else:
            return super()._accumulator(field)


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        file_aggregate_fields = {
            'file_size': (entity['document_id'], entity['file_size']),
            'count': (entity['document_id'], 1)
        }
        return {
            **super()._transform_entity(entity),
            **file_aggregate_fields
        }

    def _group_keys(self, entity) -> tuple[Any, ...]:
        return entity['file_format'],

    def _accumulator(self, field: str) -> Accumulator | None:
        if field in {
            'document_id',
            'drs_uri',
            'file_id',
            'file_md5sum',
            'file_name',
            'source_datarepo_row_ids',
            'version'
        }:
            return None
        elif field in ('count', 'file_size'):
            return DistinctAccumulator(SumAccumulator())
        else:
            return super()._accumulator(field)
