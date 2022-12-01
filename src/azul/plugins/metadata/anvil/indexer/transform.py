from abc import (
    ABC,
    abstractmethod,
)
from collections import (
    ChainMap,
    defaultdict,
)
from functools import (
    cached_property,
    partial,
)
from itertools import (
    chain,
)
from typing import (
    Callable,
    Iterable,
    Mapping,
    Optional,
)
from uuid import (
    UUID,
)

import attr
from more_itertools import (
    one,
)

from azul.indexer import (
    Bundle,
    BundlePartition,
)
from azul.indexer.aggregate import (
    EntityAggregator,
)
from azul.indexer.document import (
    Contribution,
    ContributionCoordinates,
    EntityID,
    EntityReference,
    EntityType,
    FieldTypes,
    null_datetime,
    null_int,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins.metadata.anvil.indexer.aggregate import (
    ActivityAggregator,
    BiosampleAggregator,
    DatasetAggregator,
    DonorAggregator,
    FileAggregator,
)
from azul.plugins.repository.tdr_hca import (
    EntitiesByType,
)
from azul.strings import (
    pluralize,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class LinkedEntities:
    origin: EntityReference
    ancestors: EntitiesByType
    descendants: EntitiesByType

    def __getitem__(self, item: EntityType) -> set[EntityID]:
        return self.ancestors[item] | self.descendants[item]

    @classmethod
    def from_links(cls, origin: EntityReference, links: JSONs) -> 'LinkedEntities':
        return cls(origin=origin,
                   ancestors=cls._search(origin, links, from_='outputs', to='inputs'),
                   descendants=cls._search(origin, links, from_='inputs', to='outputs'))

    @classmethod
    def _search(cls,
                entity_ref: EntityReference,
                links: JSONs,
                entities: Optional[EntitiesByType] = None,
                *,
                from_: str,
                to: str
                ) -> EntitiesByType:
        entities = defaultdict(set) if entities is None else entities
        if entity_ref.entity_type.endswith('activity'):
            follow = [one(link for link in links if str(entity_ref) == link['activity'])]
        else:
            follow = [link for link in links if str(entity_ref) in link[from_]]
        for link in follow:
            for relative in [link['activity'], *link[to]]:
                if relative is not None:
                    relative = EntityReference.parse(relative)
                    if relative != entity_ref and relative.entity_id not in entities[relative.entity_type]:
                        entities[relative.entity_type].add(relative.entity_id)
                        cls._search(relative, links, entities, from_=from_, to=to)
        return entities


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class BaseTransformer(Transformer, ABC):
    bundle: Bundle
    deleted: bool

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            'activities': cls._activity_types(),
            'biosamples': cls._biosample_types(),
            'datasets': cls._dataset_types(),
            'donors': cls._donor_types(),
            'files': cls._aggregate_file_types(),
        }

    @classmethod
    def get_aggregator(cls, entity_type) -> EntityAggregator:
        if entity_type == 'activities':
            return ActivityAggregator()
        if entity_type == 'biosamples':
            return BiosampleAggregator()
        if entity_type == 'datasets':
            return DatasetAggregator()
        if entity_type == 'donors':
            return DonorAggregator()
        if entity_type == 'files':
            return FileAggregator()
        else:
            assert False, entity_type

    def estimate(self, partition: BundlePartition) -> int:
        return sum(map(partial(self._contains, partition), self.bundle.manifest))

    def transform(self, partition: BundlePartition) -> Iterable[Contribution]:
        return map(self._transform,
                   filter(partial(self._contains, partition), self.bundle.manifest))

    @abstractmethod
    def _transform(self, manifest_entry: JSON) -> Contribution:
        raise NotImplementedError

    def _contains(self, partition: BundlePartition, manifest_entry: JSON) -> bool:
        return (
            pluralize(self._entity_type(manifest_entry)).endswith(self.entity_type())
            and partition.contains(UUID(manifest_entry['uuid']))
        )

    def _entity_type(self, manifest_entry: JSON) -> EntityType:
        return manifest_entry['name'].split('_')[0]

    @cached_property
    def _entries_by_entity_id(self) -> Mapping[EntityID, JSON]:
        return {
            manifest_entry['uuid']: manifest_entry
            for manifest_entry in self.bundle.manifest
        }

    @cached_property
    def _entities_by_type(self) -> EntitiesByType:
        entries = defaultdict(set)
        for e in self.bundle.manifest:
            entries[self._entity_type(e)].add(e['uuid'])
        return entries

    def _linked_entities(self, manifest_entry: JSON) -> LinkedEntities:
        entity_ref = EntityReference(entity_type=self._entity_type(manifest_entry),
                                     entity_id=manifest_entry['uuid'])
        links = self.bundle.metadata_files['links']
        return LinkedEntities.from_links(entity_ref, links)

    @classmethod
    def _entity_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'source_datarepo_row_ids': [null_str]
        }

    @classmethod
    def _activity_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'activity_id': null_str,
            'activity_table': null_str,
            'activity_type': null_str,
            'assay_category': null_str,
            'data_modality': null_str,
            'date_created': null_datetime,
        }

    @classmethod
    def _biosample_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'anatomical_site': null_str,
            'biosample_id': null_str,
            'biosample_type': null_str,
            'donor_age_at_collection_age_range': pass_thru_json,
            'donor_age_at_collection_unit': null_str,
            'disease': null_str,
        }

    @classmethod
    def _dataset_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'dataset_id': null_str,
            'consent_group': [null_str],
            'data_use_permission': [null_str],
            'registered_identifier': [null_str],
            'title': null_str,
        }

    @classmethod
    def _donor_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'donor_id': null_str,
            'organism_type': null_str,
            'phenotypic_sex': null_str,
            'reported_ethnicity': null_str,
        }

    @classmethod
    def _file_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'version': null_str,
            'uuid': null_str,
            'data_modality': [null_str],
            'file_format': null_str,
            'file_id': null_str,
            'byte_size': null_int,
            'size': null_int,
            'name': null_str,
            'reference_assembly': [null_str],
            'crc32': null_str,
            'sha256': null_str,
            'drs_path': null_str
        }

    @classmethod
    def _aggregate_file_types(cls) -> FieldTypes:
        return {
            **cls._file_types(),
            'count': pass_thru_int  # Added by FileAggregator, ever null
        }

    def _contribution(self,
                      contents: MutableJSON,
                      entity_id: EntityID
                      ) -> Contribution:
        entity = EntityReference(entity_type=self.entity_type(),
                                 entity_id=entity_id)
        coordinates = ContributionCoordinates(entity=entity,
                                              bundle=self.bundle.fqid.upcast(),
                                              deleted=self.deleted)
        return Contribution(coordinates=coordinates,
                            version=None,
                            source=self.bundle.fqid.source,
                            contents=contents)

    def _entity(self,
                manifest_entry: JSON,
                field_types: FieldTypes,
                **additional_fields
                ) -> MutableJSON:
        metadata = self.bundle.metadata_files[manifest_entry['name']]
        field_values = ChainMap(metadata,
                                {'document_id': manifest_entry['uuid']},
                                manifest_entry,
                                additional_fields)
        return {
            field: field_values[field]
            for field in field_types
        }

    def _entities(self,
                  factory: Callable[[JSON], MutableJSON],
                  entity_ids: Iterable[EntityID],
                  ) -> MutableJSONs:
        entities = []
        for entity_id in sorted(entity_ids):
            manifest_entry = self._entries_by_entity_id[entity_id]
            entities.append(factory(manifest_entry))
        return entities

    def _activity(self, manifest_entry: JSON) -> MutableJSON:
        activity_table = self._entity_type(manifest_entry)
        metadata = self.bundle.metadata_files[manifest_entry['name']]
        field_types = self._activity_types()
        common_fields = {
            'activity_table': activity_table,
            'activity_id': metadata[f'{activity_table}_id']
        }
        # Activities are unique in that they may not contain every field defined
        # in their field types due to polymorphism, so we need to pad the field
        # values with nulls.
        union_fields = {
            field_name: [None] if isinstance(field_type, list) else None
            for field_name, field_type in field_types.items()
            if field_name not in common_fields
        }
        activity = self._entity(manifest_entry,
                                self._activity_types(),
                                **common_fields,
                                **union_fields)

        return activity

    def _biosample(self, manifest_entry: JSON) -> MutableJSON:
        metadata = self.bundle.metadata_files[manifest_entry['name']]
        age_gte = metadata['donor_age_at_collection_lower_bound']
        age_lte = metadata['donor_age_at_collection_upper_bound']
        return self._entity(manifest_entry,
                            self._biosample_types(),
                            donor_age_at_collection_age_range={
                                'gte': None if age_gte is None else float(age_gte),
                                'lte': None if age_lte is None else float(age_lte)
                            })

    def _dataset(self, manifest_entry: JSON) -> MutableJSON:
        return self._entity(manifest_entry, self._dataset_types())

    def _donor(self, manifest_entry: JSON) -> MutableJSON:
        return self._entity(manifest_entry, self._donor_types())

    def _file(self, manifest_entry: JSON) -> MutableJSON:
        metadata = self.bundle.metadata_files[manifest_entry['name']]
        return self._entity(manifest_entry,
                            self._file_types(),
                            size=metadata['byte_size'])

    def _only_dataset(self) -> MutableJSON:
        return self._dataset(self._entries_by_entity_id[one(self._entities_by_type['dataset'])])

    _activity_polymorphic_types = {
        'activity',
        'alignmentactivity',
        'assayactivity',
        'sequencingactivity'
    }


class ActivityTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'activities'

    def _transform(self, manifest_entry: JSON) -> Contribution:
        linked = self._linked_entities(manifest_entry)
        contents = dict(
            activities=[self._activity(manifest_entry)],
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            donors=self._entities(self._donor, linked['donor']),
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, manifest_entry['uuid'])


class BiosampleTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'biosamples'

    def _transform(self, manifest_entry: JSON) -> Contribution:
        linked = self._linked_entities(manifest_entry)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=[self._biosample(manifest_entry)],
            datasets=[self._only_dataset()],
            donors=self._entities(self._donor, linked['donor']),
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, manifest_entry['uuid'])


class DatasetTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'datasets'

    def _transform(self, manifest_entry: JSON) -> Contribution:
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                self._entities_by_type[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, self._entities_by_type['biosample']),
            datasets=[self._dataset(manifest_entry)],
            donors=self._entities(self._donor, self._entities_by_type['donor']),
            files=self._entities(self._file, self._entities_by_type['file']),
        )
        return self._contribution(contents, manifest_entry['uuid'])


class DonorTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'donors'

    def _transform(self, manifest_entry: JSON) -> Contribution:
        linked = self._linked_entities(manifest_entry)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            donors=[self._donor(manifest_entry)],
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, manifest_entry['uuid'])


class FileTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    def _transform(self, manifest_entry: JSON) -> Contribution:
        linked = self._linked_entities(manifest_entry)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            files=[self._file(manifest_entry)],
            donors=self._entities(self._donor, linked['donor']),
        )
        return self._contribution(contents, manifest_entry['uuid'])
