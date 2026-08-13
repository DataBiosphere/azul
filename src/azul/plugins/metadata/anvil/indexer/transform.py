from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
    defaultdict,
)
from collections.abc import (
    Collection,
    Set,
)
from functools import (
    cached_property,
    partial,
)
from itertools import (
    chain,
)
import logging
from operator import (
    attrgetter,
)
from typing import (
    Callable,
    Iterable,
    Iterator,
    Self,
)
from uuid import (
    UUID,
)

import attr
from more_itertools import (
    one,
)

from azul import (
    config,
)
from azul.field_type import (
    FieldTypes,
    null_bool,
    null_int,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer import (
    BundleFQID,
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
    Replica,
)
from azul.indexer.transform import (
    ReplicaTransformer,
    Transformer,
)
from azul.lib.strings import (
    pluralize,
)
from azul.lib.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
    MutableJSONs,
    json_element_mappings,
    json_sequence_of_optional_strings,
    json_sorted,
    json_str,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    EntityLink,
)
from azul.plugins.metadata.anvil.indexer.aggregate import (
    ActivityAggregator,
    BiosampleAggregator,
    DatasetAggregator,
    DiagnosisAggregator,
    DonorAggregator,
    FileAggregator,
)

log = logging.getLogger(__name__)

EntityRefsByType = dict[EntityType, set[EntityReference]]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class LinkedEntities(Iterable[EntityReference]):
    origin: EntityReference
    ancestors: EntityRefsByType
    descendants: EntityRefsByType

    def __getitem__(self, item: EntityType) -> set[EntityReference]:
        return self.ancestors[item] | self.descendants[item]

    def __iter__(self) -> Iterator[EntityReference]:
        for entities in self.ancestors.values():
            yield from entities
        for entities in self.descendants.values():
            yield from entities

    @classmethod
    def from_links(cls,
                   origin: EntityReference,
                   links: Collection[EntityLink]
                   ) -> Self:
        return cls(origin=origin,
                   ancestors=cls._search(origin, links, from_='outputs', to='inputs'),
                   descendants=cls._search(origin, links, from_='inputs', to='outputs'))

    @classmethod
    def _search(cls,
                entity_ref: EntityReference,
                links: Collection[EntityLink],
                entities: EntityRefsByType | None = None,
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
    def _aggregator_cls(cls, entity_type: str) -> type[EntityAggregator]:
        if entity_type == 'activities':
            return ActivityAggregator
        elif entity_type == 'biosamples':
            return BiosampleAggregator
        elif entity_type == 'datasets':
            return DatasetAggregator
        elif entity_type == 'diagnoses':
            return DiagnosisAggregator
        elif entity_type == 'donors':
            return DonorAggregator
        elif entity_type == 'files':
            return FileAggregator
        else:
            assert False, entity_type

    def estimate(self, partition: BundlePartition) -> int:
        # Orphans are not considered when deciding whether to partition the
        # bundle, but if the bundle is partitioned then each partition will only
        # emit replicas for the orphans that it contains
        return sum(map(partial(self._contains, partition), self.bundle.entities))

    def transform(self,
                  partition: BundlePartition
                  ) -> Iterable[Contribution | Replica]:
        for entity in self._list_entities():
            if self._contains(partition, entity):
                yield from self._transform(entity)

    def _list_entities(self) -> Iterable[EntityReference]:
        return self.bundle.entities

    @abstractmethod
    def _transform(self,
                   entity: EntityReference
                   ) -> Iterable[Contribution | Replica]:
        raise NotImplementedError

    def _replica_contents(self, entity: EntityReference) -> JSON:
        return ChainMap(self.bundle.entities, self.bundle.orphans)[entity]

    def _convert_entity_type(self, entity_type: str) -> str:
        assert entity_type == 'bundle' or entity_type.startswith('anvil_'), entity_type
        if entity_type == 'anvil_diagnosis':
            # Irregular plural form
            return 'diagnoses'
        elif entity_type.endswith('activity'):
            # Polymorphic. Could be `anvil_sequencingactivity`,
            # `anvil_assayactivity`, `anvil_activity`, etc
            return 'activities'
        else:
            return pluralize(entity_type.removeprefix('anvil_'))

    def _contains(self,
                  partition: BundlePartition,
                  entity: EntityReference
                  ) -> bool:
        return (
            self._convert_entity_type(entity.entity_type) == self.entity_type()
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
        return {
            field_prefix: {
                'gte': metadata[field_prefix + '_lower_bound'],
                'lte': metadata[field_prefix + '_upper_bound']
            }
            for field_prefix in field_prefixes
        }

    def _entity(self,
                ref: EntityReference,
                field_types: FieldTypes,
                **additional_fields
                ) -> MutableJSON:
        metadata = self.bundle.entities[ref]
        entity: MutableJSON = {}
        for field in field_types:
            value: AnyMutableJSON
            if field == 'document_id':
                value = ref.entity_id
            else:
                try:
                    value = metadata[field]
                except KeyError:
                    value = additional_fields[field]
            if isinstance(value, list):
                value = json_sorted(json_sequence_of_optional_strings(value))
            entity[field] = value
        return entity

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
            'activity_id': metadata[f'{activity.entity_type.removeprefix("anvil_")}_id']
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
        return self._entity(file, self._file_types())

    def _only_dataset(self) -> EntityReference:
        try:
            return one(self._entities_by_type['anvil_dataset'])
        except ValueError:
            return one(o for o in self.bundle.orphans if o.entity_type == 'anvil_dataset')

    @cached_property
    def _activity_polymorphic_types(self) -> Set[str]:
        from azul.plugins.metadata.anvil.schema import (
            anvil_schema,
        )
        return {
            json_str(table['name'])
            for table in json_element_mappings(anvil_schema['tables'])
            if json_str(table['name']).endswith('activity')
        }

    @classmethod
    def inner_entity_id(cls, entity_type: EntityType, entity: JSON) -> EntityID:
        return json_str(entity['document_id'])

    @classmethod
    def reconcile_inner_entities(cls,
                                 entity_type: EntityType,
                                 *,
                                 this: tuple[JSON, BundleFQID],
                                 that: tuple[JSON, BundleFQID]
                                 ) -> tuple[JSON, BundleFQID]:
        this_entity, this_bundle = this
        that_entity, that_bundle = that
        # All AnVIL bundles use a fixed known version
        assert this_bundle.version == that_bundle.version, (this, that)
        assert this_entity.keys() == that_entity.keys(), (this, that)
        return this


class SingletonTransformer(BaseTransformer, metaclass=ABCMeta):

    def _transform(self, entity: EntityReference) -> Iterable[Contribution]:
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                self._entities_by_type[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, self._entities_by_type['anvil_biosample']),
            datasets=[self._dataset(self._only_dataset())],
            diagnoses=self._entities(self._diagnosis, self._entities_by_type['anvil_diagnosis']),
            donors=self._entities(self._donor, self._entities_by_type['anvil_donor']),
            files=self._entities(self._file, self._entities_by_type['anvil_file'])
        )
        yield self._contribution(contents, entity.entity_id)

    def _list_entities(self) -> Iterable[EntityReference]:
        # Suppress contributions for bundles that only contain orphans
        if self.bundle.entities:
            yield self._singleton()

    @abstractmethod
    def _singleton(self) -> EntityReference:
        raise NotImplementedError


class ActivityTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'activities'

    def _transform(self, entity: EntityReference) -> Iterable[Contribution]:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=[self._activity(entity)],
            biosamples=self._entities(self._biosample, linked['anvil_biosample']),
            datasets=[self._dataset(self._only_dataset())],
            diagnoses=self._entities(self._diagnosis, linked['anvil_diagnosis']),
            donors=self._entities(self._donor, linked['anvil_donor']),
            files=self._entities(self._file, linked['anvil_file'])
        )
        yield self._contribution(contents, entity.entity_id)


class BiosampleTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'biosamples'

    def _transform(self, entity: EntityReference) -> Iterable[Contribution]:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=[self._biosample(entity)],
            datasets=[self._dataset(self._only_dataset())],
            diagnoses=self._entities(self._diagnosis, linked['anvil_diagnosis']),
            donors=self._entities(self._donor, linked['anvil_donor']),
            files=self._entities(self._file, linked['anvil_file']),
        )
        yield self._contribution(contents, entity.entity_id)


class BundleTransformer(SingletonTransformer):

    @classmethod
    def entity_type(cls) -> EntityType:
        return 'bundles'

    def _singleton(self) -> EntityReference:
        return EntityReference(entity_type='bundle',
                               entity_id=self.bundle.uuid)

    def transform(self,
                  partition: BundlePartition
                  ) -> Iterable[Contribution | Replica]:
        yield from super().transform(partition)
        if config.enable_replicas:
            # The file transformer only emits replicas for entities that are
            # linked to at least one file. This excludes all orphans, and a
            # small number of linked entities, usually from primary bundles
            # don't include any files. Some of the replicas we emit here will be
            # redundant with those emitted by the file transformer, but these
            # will be consolidated by the index service before they are written
            # to OpenSearch.
            dataset = self._only_dataset()
            for entity in chain(self.bundle.orphans, self.bundle.entities):
                if partition.contains(UUID(entity.entity_id)):
                    yield self._replica(entity, file_hub=None, root_hub=dataset.entity_id)


class DatasetTransformer(SingletonTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'datasets'

    @classmethod
    def _detailed_dataset_types(cls) -> FieldTypes:
        return {
            **cls._dataset_types(),
            'description': null_str,
            'duos_id': null_str,
        }

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            **super().field_types(),
            'datasets': cls._detailed_dataset_types(),
        }

    def _dataset(self, dataset: EntityReference) -> MutableJSON:
        return self._entity(dataset, self._detailed_dataset_types())

    def _singleton(self) -> EntityReference:
        return self._only_dataset()


class DonorTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'donors'

    def _transform(self, entity: EntityReference) -> Iterable[Contribution]:
        linked = self._linked_entities(entity)
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['anvil_biosample']),
            datasets=[self._dataset(self._only_dataset())],
            diagnoses=self._entities(self._diagnosis, linked['anvil_diagnosis']),
            donors=[self._donor(entity)],
            files=self._entities(self._file, linked['anvil_file']),
        )
        yield self._contribution(contents, entity.entity_id)


class FileTransformer(BaseTransformer, ReplicaTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    @classmethod
    def hot_entity_types(cls) -> dict[EntityType, EntityType]:
        return {
            'anvil_dataset': 'datasets',
        }

    def _transform(self,
                   entity: EntityReference
                   ) -> Iterable[Contribution | Replica]:
        linked = self._linked_entities(entity)
        dataset = self._only_dataset()
        contents = dict(
            activities=self._entities(self._activity, chain.from_iterable(
                linked[activity_type]
                for activity_type in self._activity_polymorphic_types
            )),
            biosamples=self._entities(self._biosample, linked['anvil_biosample']),
            datasets=[self._dataset(self._only_dataset())],
            diagnoses=self._entities(self._diagnosis, linked['anvil_diagnosis']),
            donors=self._entities(self._donor, linked['anvil_donor']),
            files=[self._file(entity)],
        )
        file_id = entity.entity_id
        yield self._contribution(contents, file_id)
        if config.enable_replicas:
            dataset_id = dataset.entity_id
            yield self._replica(entity, file_hub=file_id, root_hub=dataset_id)
            for linked_entity in linked:
                yield self._replica(
                    linked_entity,
                    file_hub=None if linked_entity.entity_type in self.hot_entity_types() else file_id,
                    root_hub=dataset_id
                )
