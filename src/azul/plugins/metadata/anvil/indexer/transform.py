from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
    defaultdict,
)
from collections.abc import (
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
from azul.lib import (
    R,
)
from azul.lib.strings import (
    pluralize,
)
from azul.lib.types import (
    AnyMutableJSON,
    JSON,
    MutableJSON,
    MutableJSONs,
    json_bool,
    json_element_mappings,
    json_sequence_of_optional_strings,
    json_sorted,
    json_str,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
    LinkedEntities,
    snapshot_without_md5s,
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

#: The version of the AnVIL schema that this module was written against
#:
anvil_schema_version = 6


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
            'duos_id': null_str,
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
            'file_path': null_str,
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
            anvil_schemas,
        )
        anvil_schema = anvil_schemas[anvil_schema_version]
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
        # A file's backfilled path may have been lengthened, to set it apart
        # from the files it would otherwise collide with on a user's disk.
        # Only the bundle that holds every file a given one could collide
        # with can make that decision, so the longer of two paths is the
        # considered one, and the copy carrying it wins. Copies of any other
        # entity are equal. See :py:meth:`FileTransformer._file_paths`.
        if entity_type == 'files':
            this_path = json_str(this_entity['file_path'])
            that_path = json_str(that_entity['file_path'])
            result = that if len(that_path) > len(this_path) else this
        else:
            result = this
        return result


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

    def _file(self, file: EntityReference) -> MutableJSON:
        entity = super()._file(file)
        if entity['file_path'] is None:
            entity['file_path'] = self._file_paths[file]
        return entity

    @cached_property
    def _file_paths(self) -> dict[EntityReference, str]:
        """
        A path for each file in this bundle that lacks an explicitly provided
        path, relative to an imagined directory that holds all the files in the
        dataset containing the bundle. Version 5 of the schema records only a
        file's name, which is not even unique within a bundle, let alone the
        dataset. Based on the file's name, this method builds a path that's
        unique within the dataset.
        """
        # Group files by intermediate directory and file name. Every file in the
        # bundle is grouped, not just those in the current partition, otherwise
        # the set of files that need deduplication would be incomplete. Normal
        # files go into a directory named after one of the biosamples they are
        # derived from while supplementary files are collected in a single,
        # statically named directory.
        groups = defaultdict(list)
        for file in self._entities_by_type['anvil_file']:
            metadata = self.bundle.entities[file]
            if metadata['file_path'] is None:
                if json_bool(metadata['is_supplementary']):
                    dir = self._supplementary_dir
                else:
                    dir = self._biosample_dir(file)
                groups[dir, json_str(metadata['file_name'])].append(file)

        # Only a bundle that holds every file in a group can tell whether the
        # group needs deduplication, and a file can be part of more than one
        # bundle. The bundle rooted at the biosample that names a group's
        # directory is the one that holds the complete group, because every
        # file in the group is derived from that biosample, and a bundle
        # rooted at a biosample holds every file derived from it. Any other
        # bundle holds a subset, so it leaves the group alone and emits the
        # file's name unchanged. Since a deduplicated name is longer, keeping
        # the longer of two paths in reconcile_inner_entities() settles which
        # of the two decisions prevails.
        #
        # This assumes that every bundle containing a file also contains all
        # of that file's biosample ancestors, otherwise we wouldn't be able to
        # deterministically pick a biosample for naming the file's directory.
        # The repository plugin is expected to satisfy this assumption.
        #
        # Neither of the two directories that aren't named after a biosample
        # has a bundle that holds its complete group. A supplementary bundle
        # is a batch, and a detached file has no biosample for a bundle to be
        # rooted at. Files in them are therefore deduplicated unconditionally,
        # which also makes every bundle agree on their paths without
        # reconciling them.
        #
        paths = {}
        # A batched bundle is rooted at nothing, and only a bundle rooted at a
        # biosample can be the reliable one for any of the groups below
        entity = self.bundle.bundle_entity
        if entity is None or entity.entity_type != 'anvil_biosample':
            root_biosample_dir = None
        else:
            root_biosample = self.bundle.entities[entity]
            root_biosample_dir = json_str(root_biosample['biosample_id'])
        for (dir, name), files in groups.items():
            deduplicate = (
                len(files) > 1 and dir == root_biosample_dir
                or dir in (self._supplementary_dir, self._detached_dir)
            )
            for file in files:
                file_name = self._deduplicate_file_name(file) if deduplicate else name
                paths[file] = '/'.join([dir, file_name])
        return paths

    #: The directory holding the files that belong to a dataset as a whole,
    #: rather than to any one sample taken for it
    #:
    _supplementary_dir = 'supplementary'

    #: The directory holding the files that belong to neither, being linked to
    #: no biosample at all. The existence of such a file is a metadata defect.
    # It should either be declared to be supplementary or linked to a biosample.
    #:
    _detached_dir = 'detached'

    def _biosample_dir(self, file: EntityReference) -> str:
        # Of the biosamples a file was derived from, the one with lowest ID is
        # picked, so that every bundle containing the file agrees on the
        # directory, even though that biosample may not be the one the bundle is
        # rooted at.
        biosamples = self._linked_entities(file)['anvil_biosample']
        biosample_ids = (
            json_str(self.bundle.entities[biosample]['biosample_id'])
            for biosample in biosamples
        )
        # FIXME: Remove the default
        #        https://github.com/DataBiosphere/azul/issues/8386
        return min(biosample_ids, default=self._detached_dir)

    #: The number of hexadecimal digits of a file's digest that distinguish it
    #: from the files it shares a name with. Sixteen digits carry 64 bits, so
    #: against the 1.9e7 pairs of same-name files in one dataset observed in
    #: `anvil15`, the chance of any two of them colliding is around 1e-12.
    #:
    _digest_length = 16

    def _deduplicate_file_name(self, file: EntityReference) -> str:
        metadata = self.bundle.entities[file]
        # The digest goes after the stem, not in front of the name, so that a
        # listing sorted by name keeps a file next to similarly named ones
        stem, extension = _split_extension(json_str(metadata['file_name']))
        digest = metadata['file_md5sum']
        if digest is None:
            # FIXME: Files from 1000G snapshot in anvildev can't be mirrored
            #        https://github.com/DataBiosphere/azul/issues/7634
            assert self.bundle.fqid.source.spec.name == snapshot_without_md5s, R(
                'File lacks MD5 digest', file, self.bundle.fqid)
            digest = json_str(metadata['file_id']).replace('-', '')
        infix = json_str(digest)[:self._digest_length]
        return ''.join([stem, '-', infix, extension])


#: The dot-separated components of a file name that belong to its extension
#: rather than to its stem. A component such as `md`, `final` or `stats`, which
#: frequently precedes a real extension, is deliberately absent.
#:
#: The set was compiled by sampling the file names in `anvil15`. The
#: `azul-anvil-extensions` skill describes how to redo that analysis against a
#: later release.
#:
_extensions = frozenset([
    # Compression
    'bz2', 'gz', 'xz', 'zst',
    # Content. A lone `g` is how a gVCF is conventionally marked, as in
    # `.g.vcf.gz`, and it occurs in no other role in `anvil15`
    'bam', 'bed', 'cram', 'csv', 'fa', 'fasta', 'fastq', 'fq', 'g', 'gff3',
    'gvcf', 'idat', 'junc', 'sam', 'svs', 'tab', 'tar', 'tsv', 'txt', 'vcf',
    'yaml',
    # Indices and checksums
    'bai', 'crai', 'csi', 'idx', 'md5', 'tbi'
])


def _split_extension(file_name: str) -> tuple[str, str]:
    """
    Split the given file name into its stem and its extension, the latter
    including the leading dot, or empty if the name has none.

    The last component is taken to be an extension even if it isn't a known
    one, because most are. Further components are taken only while they are
    known, so that a name ending in a component that merely looks like an
    extension is left intact.

    >>> _split_extension('NA19189.chr2.hc.vcf.gz')
    ('NA19189.chr2.hc', '.vcf.gz')

    An index sorts next to what it indexes, so its extension is taken whole:

    >>> _split_extension('A0-03569.cram.crai')
    ('A0-03569', '.cram.crai')

    >>> _split_extension('NWD1234.vcf.gz.tbi')
    ('NWD1234', '.vcf.gz.tbi')

    >>> _split_extension('RES03249.rb.g.vcf.gz')
    ('RES03249.rb', '.g.vcf.gz')

    A component that merely precedes the extension stays with the stem:

    >>> _split_extension('NWD1234.md.bam')
    ('NWD1234.md', '.bam')

    >>> _split_extension('chr5.136400001_136500001.tar')
    ('chr5.136400001_136500001', '.tar')

    An unknown extension is still an extension:

    >>> _split_extension('sample.crosscheck')
    ('sample', '.crosscheck')

    A name without a dot is all stem:

    >>> _split_extension('README')
    ('README', '')

    The first component is never consumed, so a name that is all extension
    keeps an empty stem:

    >>> _split_extension('.bashrc')
    ('', '.bashrc')
    """
    components = file_name.split('.')
    i = len(components) - 1
    while i > 1 and components[i - 1].lower() in _extensions:
        i -= 1
    if len(components) == 1:
        stem, extension = file_name, ''
    else:
        stem = '.'.join(components[:i])
        extension = '.' + '.'.join(components[i:])
    return stem, extension
