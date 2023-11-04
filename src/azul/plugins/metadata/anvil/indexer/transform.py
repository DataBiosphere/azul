from abc import (
    ABCMeta,
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
from operator import (
    attrgetter,
)
from typing import (
    Callable,
    Collection,
    Iterable,
    Optional,
)
from uuid import (
    UUID,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    JSON,
)
from azul.indexer import (
    BundlePartition,
)
from azul.indexer.aggregate import (
    EntityAggregator,
)
from azul.indexer.document import (
    Contribution,
    EntityID,
    EntityReference,
    EntityType,
    FieldTypes,
    null_bool,
    null_datetime,
    null_int,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    Link,
)
from azul.plugins.metadata.anvil.indexer.aggregate import (
    ActivityAggregator,
    BiosampleAggregator,
    DatasetAggregator,
    DiagnosisAggregator,
    DonorAggregator,
    FileAggregator,
)
from azul.strings import (
    pluralize,
)
from azul.types import (
    MutableJSON,
    MutableJSONs,
)

EntityRefsByType = dict[EntityType, set[EntityReference]]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class LinkedEntities:
    origin: EntityReference
    ancestors: EntityRefsByType
    descendants: EntityRefsByType

    def __getitem__(self, item: EntityType) -> set[EntityReference]:
        return self.ancestors[item] | self.descendants[item]

    @classmethod
    def from_links(cls,
                   origin: EntityReference,
                   links: Collection[Link[EntityReference]]
                   ) -> 'LinkedEntities':
        return cls(origin=origin,
                   ancestors=cls._search(origin, links, from_='outputs', to='inputs'),
                   descendants=cls._search(origin, links, from_='inputs', to='outputs'))

    @classmethod
    def _search(cls,
                entity_ref: EntityReference,
                links: Collection[Link[EntityReference]],
                entities: Optional[EntityRefsByType] = None,
                *,
                from_: str,
                to: str
                ) -> EntityRefsByType:
        entities = defaultdict(set) if entities is None else entities
        if entity_ref.entity_type.endswith('activity'):
            follow = [one(link for link in links if entity_ref == link.activity)]
        else:
            follow = [link for link in links if entity_ref in getattr(link, from_)]
        for link in follow:
            for relative in [link.activity, *getattr(link, to)]:
                if relative is not None:
                    if relative != entity_ref and relative.entity_id not in entities[relative.entity_type]:
                        entities[relative.entity_type].add(relative)
                        cls._search(relative, links, entities, from_=from_, to=to)
        return entities


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class BaseTransformer(Transformer, metaclass=ABCMeta):
    bundle: AnvilBundle

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            'activities': cls._activity_types(),
            'biosamples': cls._biosample_types(),
            'datasets': cls._dataset_types(),
            'diagnoses': cls._diagnosis_types(),
            'donors': cls._donor_types(),
            'files': cls._aggregate_file_types(),
        }

    @classmethod
    def aggregator(cls, entity_type) -> EntityAggregator:
        if entity_type == 'activities':
            return ActivityAggregator()
        elif entity_type == 'biosamples':
            return BiosampleAggregator()
        elif entity_type == 'datasets':
            return DatasetAggregator()
        elif entity_type == 'diagnoses':
            return DiagnosisAggregator()
        elif entity_type == 'donors':
            return DonorAggregator()
        elif entity_type == 'files':
            return FileAggregator()
        else:
            assert False, entity_type

    def estimate(self, partition: BundlePartition) -> int:
        return sum(map(partial(self._contains, partition), self.bundle.entities))

    def transform(self, partition: BundlePartition) -> Iterable[Contribution]:
        return (
            self._transform(entity)
            for entity in self.bundle.entities
            if self._contains(partition, entity)
        )

    @abstractmethod
    def _transform(self, entity: EntityReference) -> Contribution:
        raise NotImplementedError

    def _pluralize(self, entity_type: str) -> str:
        if entity_type == 'diagnosis':
            return 'diagnoses'
        else:
            return pluralize(entity_type)

    def _contains(self,
                  partition: BundlePartition,
                  entity: EntityReference
                  ) -> bool:
        return (
            self._pluralize(entity.entity_type).endswith(self.entity_type())
            and partition.contains(UUID(entity.entity_id))
        )

    @cached_property
    def _entities_by_type(self) -> dict[EntityType, set[EntityReference]]:
        entries = defaultdict(set)
        for e in self.bundle.entities:
            entries[e.entity_type].add(e)
        return entries

    def _linked_entities(self, entity: EntityReference) -> LinkedEntities:
        return LinkedEntities.from_links(entity, self.bundle.links)

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
            'assay_type': null_str,
            'data_modality': null_str,
            'reference_assembly': [null_str],
            # Not in schema
            'date_created': null_datetime,
        }

    @classmethod
    def _biosample_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'biosample_id': null_str,
            'anatomical_site': null_str,
            'apriori_cell_type': [null_str],
            'biosample_type': null_str,
            'disease': null_str,
            'donor_age_at_collection_unit': null_str,
            'donor_age_at_collection': pass_thru_json,
        }

    @classmethod
    def _dataset_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'dataset_id': null_str,
            'consent_group': [null_str],
            'data_use_permission': [null_str],
            'owner': [null_str],
            'principal_investigator': [null_str],
            'registered_identifier': [null_str],
            'title': null_str,
            'data_modality': [null_str],
        }

    @classmethod
    def _diagnosis_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'diagnosis_id': null_str,
            'disease': [null_str],
            'diagnosis_age_unit': null_str,
            'diagnosis_age': pass_thru_json,
            'onset_age_unit': null_str,
            'onset_age': pass_thru_json,
            'phenotype': [null_str],
            'phenopacket': [null_str]
        }

    @classmethod
    def _donor_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'donor_id': null_str,
            'organism_type': null_str,
            'phenotypic_sex': null_str,
            'reported_ethnicity': null_str,
            'genetic_ancestry': [null_str],
        }

    @classmethod
    def _file_types(cls) -> FieldTypes:
        return {
            **cls._entity_types(),
            'file_id': null_str,
            'data_modality': [null_str],
            'file_format': null_str,
            'file_size': null_int,
            'file_md5sum': null_str,
            'reference_assembly': [null_str],
            'file_name': null_str,
            'is_supplementary': null_bool,
            # Not in schema
            'version': null_str,
            'uuid': null_str,
            'size': null_int,
            'name': null_str,
            'crc32': null_str,
            'sha256': null_str,
            'drs_uri': null_str
        }

    @classmethod
    def _aggregate_file_types(cls) -> FieldTypes:
        return {
            **cls._file_types(),
            'count': pass_thru_int  # Added by FileAggregator, ever null
        }

    def _range(self, entity: EntityReference, *field_prefixes: str) -> MutableJSON:
        metadata = self.bundle.entities[entity]

        def get_bound(field_name: str) -> Optional[float]:
            val = metadata[field_name]
            return None if val is None else float(val)

        return {
            field_prefix: {
                'gte': get_bound(field_prefix + '_lower_bound'),
                'lte': get_bound(field_prefix + '_upper_bound')
            }
            for field_prefix in field_prefixes
        }

    def _contribution(self,
                      contents: MutableJSON,
                      entity: EntityReference,
                      ) -> Contribution:
        # The entity type is used to determine the index name.
        # All activities go into the same index, regardless of their polymorphic type.
        # Index names use plural forms.
        entity_type = pluralize('activity'
                                if entity.entity_type.endswith('activity') else
                                entity.entity_type)
        entity = attr.evolve(entity, entity_type=entity_type)
        return super()._contribution(contents, entity)

    def _entity(self,
                entity: EntityReference,
                field_types: FieldTypes,
                **additional_fields
                ) -> MutableJSON:
        metadata = self.bundle.entities[entity]
        field_values = ChainMap(metadata,
                                {'document_id': entity.entity_id},
                                additional_fields)
        return {
            field: field_values[field]
            for field in field_types
        }

    def _entities(self,
                  factory: Callable[[EntityReference], MutableJSON],
                  entities: Iterable[EntityReference],
                  ) -> MutableJSONs:
        return [
            factory(entity)
            for entity in sorted(entities, key=attrgetter('entity_id'))
        ]

    def _activity(self, activity: EntityReference) -> MutableJSON:
        metadata = self.bundle.entities[activity]
        field_types = self._activity_types()
        common_fields = {
            'activity_table': activity.entity_type,
            'activity_id': metadata[f'{activity.entity_type}_id']
        }
        # Activities are unique in that they may not contain every field defined
        # in their field types due to polymorphism, so we need to pad the field
        # values with nulls.
        union_fields = {
            field_name: [None] if isinstance(field_type, list) else None
            for field_name, field_type in field_types.items()
            if field_name not in common_fields
        }
        activity = self._entity(activity,
                                self._activity_types(),
                                **common_fields,
                                **union_fields)

        return activity

    def _biosample(self, biosample: EntityReference) -> MutableJSON:
        return self._entity(biosample,
                            self._biosample_types(),
                            **self._range(biosample, 'donor_age_at_collection'))

    def _dataset(self, dataset: EntityReference) -> MutableJSON:
        return self._entity(dataset, self._dataset_types())

    def _diagnosis(self, diagnosis: EntityReference) -> MutableJSON:
        return self._entity(diagnosis,
                            self._diagnosis_types(),
                            **self._range(diagnosis, 'diagnosis_age', 'onset_age'))

    def _donor(self, donor: EntityReference) -> MutableJSON:
        return self._entity(donor, self._donor_types())

    def _file(self, file: EntityReference) -> MutableJSON:
        metadata = self.bundle.entities[file]
        return self._entity(file,
                            self._file_types(),
                            size=metadata['file_size'],
                            name=metadata['file_name'],
                            uuid=file.entity_id)

    def _only_dataset(self) -> MutableJSON:
        return self._dataset(one(self._entities_by_type['dataset']))

    _activity_polymorphic_types = {
        'activity',
        'alignmentactivity',
        'assayactivity',
        'sequencingactivity',
        'variantcallingactivity'
    }

    @classmethod
    def inner_entity_id(cls, entity_type: EntityType, entity: JSON) -> EntityID:
        return entity['document_id']


class ActivityTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'activities'

    def _transform(self, entity: EntityReference) -> Contribution:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=[self._activity(entity)],
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            diagnoses=self._entities(self._diagnosis, linked['diagnosis']),
            donors=self._entities(self._donor, linked['donor']),
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, entity)


class BiosampleTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'biosamples'

    def _transform(self, entity: EntityReference) -> Contribution:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=[self._biosample(entity)],
            datasets=[self._only_dataset()],
            diagnoses=self._entities(self._diagnosis, linked['diagnosis']),
            donors=self._entities(self._donor, linked['donor']),
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, entity)


class DatasetTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'datasets'

    def _transform(self, entity: EntityReference) -> Contribution:
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                self._entities_by_type[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, self._entities_by_type['biosample']),
            datasets=[self._dataset(entity)],
            diagnoses=self._entities(self._diagnosis, self._entities_by_type['diagnosis']),
            donors=self._entities(self._donor, self._entities_by_type['donor']),
            files=self._entities(self._file, self._entities_by_type['file']),
        )
        return self._contribution(contents, entity)


class DonorTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'donors'

    def _transform(self, entity: EntityReference) -> Contribution:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            diagnoses=self._entities(self._diagnosis, linked['diagnosis']),
            donors=[self._donor(entity)],
            files=self._entities(self._file, linked['file']),
        )
        return self._contribution(contents, entity)


class FileTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    def _transform(self, entity: EntityReference) -> Contribution:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['biosample']),
            datasets=[self._only_dataset()],
            diagnoses=self._entities(self._diagnosis, linked['diagnosis']),
            donors=self._entities(self._donor, linked['donor']),
            files=[self._file(entity)],
        )
        return self._contribution(contents, entity)
