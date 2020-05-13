from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    ChainMap,
    Counter,
    defaultdict,
)
import logging
from operator import itemgetter
from typing import (
    Any,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from humancellatlas.data.metadata import api

from azul import (
    reject,
    require,
)
from azul.collections import (
    compose_keys,
    none_safe_key,
    none_safe_tuple_key,
)
from azul.indexer.transformer import (
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
    Transformer,
    UniqueValueCountAccumulator,
)
from azul.indexer.document import (
    BundleUUID,
    BundleVersion,
    Contribution,
    EntityReference,
    FieldTypes,
)
from azul.plugins.metadata.hca.metadata_generator import MetadataGenerator
from azul.types import JSON

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]
sample_types = api.CellLine, api.Organoid, api.SpecimenFromOrganism
assert Sample.__args__ == sample_types  # since we can't use * in generic types


class BaseTransformer(Transformer, metaclass=ABCMeta):

    @abstractmethod
    def _transform(self, bundle: api.Bundle, deleted: bool) -> Iterable[Contribution]:
        raise NotImplementedError()

    def transform(self,
                  uuid: BundleUUID,
                  version: BundleVersion,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]) -> Iterable[Contribution]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        for file in bundle.files.values():
            # Note that this only patches the file name in a manifest entry.
            # It does not modify the `file_core.file_name` metadata property,
            # thereby breaking the important invariant that the two be the
            # same. There are two places where this invariant matters: in the
            # `api.File` constructor and in `metadata_generator.py`. The
            # former has already been invoked at this point and the latter is
            # not affected by this patch because the patch occurs on copies of
            # the manifest entries whereas `metadata_generator` consumes the
            # originals for which the invariant still holds. Furthermore,
            # `metadata_generator` performs it's own ! to / conversion.
            file.manifest_entry.name = file.manifest_entry.name.replace('!', '/')
        return self._transform(bundle, deleted)

    def get_aggregator(self, entity_type):
        if entity_type == 'files':
            return FileAggregator()
        elif entity_type == 'samples':
            return SampleAggregator()
        elif entity_type == 'specimens':
            return SpecimenAggregator()
        elif entity_type == 'cell_suspensions':
            return CellSuspensionAggregator()
        elif entity_type == 'cell_lines':
            return CellLineAggregator()
        elif entity_type == 'donors':
            return DonorOrganismAggregator()
        elif entity_type == 'organoids':
            return OrganoidAggregator()
        elif entity_type == 'projects':
            return ProjectAggregator()
        elif entity_type == 'protocols':
            return ProtocolAggregator()
        else:
            return SimpleAggregator()

    def _find_ancestor_samples(self, entity: api.LinkedEntity, samples: MutableMapping[str, Sample]):
        """
        Populate the `samples` argument with the sample ancestors of the given entity. A sample is any biomaterial
        that is neither a cell suspension nor an ancestor of another sample.
        """
        if isinstance(entity, sample_types):
            samples[str(entity.document_id)] = entity
        else:
            for parent in entity.parents.values():
                self._find_ancestor_samples(parent, samples)

    def _contact_types(self) -> FieldTypes:
        return {
            'contact_name': str,
            'corresponding_contributor': bool,
            'email': str,
            'institution': str,
            'laboratory': str,
            'project_role': str
        }

    def _contact(self, p: api.ProjectContact):
        # noinspection PyDeprecation
        return {
            "contact_name": p.contact_name,
            "corresponding_contributor": p.corresponding_contributor,
            "email": p.email,
            "institution": p.institution,
            "laboratory": p.laboratory,
            "project_role": p.project_role
        }

    def _publication_types(self) -> FieldTypes:
        return {
            'publication_title': str,
            'publication_url': str
        }

    def _publication(self, p: api.ProjectPublication):
        # noinspection PyDeprecation
        return {
            "publication_title": p.publication_title,
            "publication_url": p.publication_url
        }

    def _project_types(self) -> FieldTypes:
        return {
            'project_title': str,
            'project_description': str,
            'project_short_name': str,
            'laboratory': str,
            'institutions': str,
            'contact_names': str,
            'contributors': self._contact_types(),
            'document_id': str,
            'publication_titles': str,
            'publications': self._publication_types(),
            'insdc_project_accessions': str,
            'geo_series_accessions': str,
            'array_express_accessions': str,
            'insdc_study_accessions': str,
            'supplementary_links': str,
            '_type': str
        }

    def _project(self, project: api.Project) -> JSON:
        # Store lists of all values of each of these facets to allow facet filtering
        # and term counting on the webservice
        laboratories: Set[str] = set()
        institutions: Set[str] = set()
        contact_names: Set[str] = set()
        publication_titles: Set[str] = set()

        for contributor in project.contributors:
            if contributor.laboratory:
                laboratories.add(contributor.laboratory)
            # noinspection PyDeprecation
            if contributor.contact_name:
                # noinspection PyDeprecation
                contact_names.add(contributor.contact_name)
            if contributor.institution:
                institutions.add(contributor.institution)

        for publication in project.publications:
            # noinspection PyDeprecation
            if publication.publication_title:
                # noinspection PyDeprecation
                publication_titles.add(publication.publication_title)

        return {
            'project_title': project.project_title,
            'project_description': project.project_description,
            'project_short_name': project.project_short_name,
            'laboratory': sorted(laboratories),
            'institutions': sorted(institutions),
            'contact_names': sorted(contact_names),
            'contributors': list(map(self._contact, project.contributors)),
            'document_id': str(project.document_id),
            'publication_titles': sorted(publication_titles),
            'publications': list(map(self._publication, project.publications)),
            'insdc_project_accessions': sorted(project.insdc_project_accessions),
            'geo_series_accessions': sorted(project.geo_series_accessions),
            'array_express_accessions': sorted(project.array_express_accessions),
            'insdc_study_accessions': sorted(project.insdc_study_accessions),
            'supplementary_links': sorted(project.supplementary_links),
            '_type': 'project'
        }

    def _specimen_types(self) -> FieldTypes:
        return {
            'has_input_biomaterial': str,
            '_source': str,
            'document_id': str,
            'biomaterial_id': str,
            'disease': str,
            'organ': str,
            'organ_part': str,
            'storage_method': str,
            'preservation_method': str,
            '_type': str
        }

    def _specimen(self, specimen: api.SpecimenFromOrganism) -> JSON:
        return {
            'has_input_biomaterial': specimen.has_input_biomaterial,
            '_source': api.schema_names[type(specimen)],
            'document_id': str(specimen.document_id),
            'biomaterial_id': specimen.biomaterial_id,
            'disease': sorted(specimen.diseases),
            'organ': specimen.organ,
            'organ_part': sorted(specimen.organ_parts),
            'storage_method': specimen.storage_method,
            'preservation_method': specimen.preservation_method,
            '_type': 'specimen'
        }

    def _cell_suspension_types(self) -> FieldTypes:
        return {
            'document_id': str,
            'total_estimated_cells': int,
            'selected_cell_type': str,
            'organ': str,
            'organ_part': str
        }

    def _cell_suspension(self, cell_suspension: api.CellSuspension) -> JSON:
        organs = set()
        organ_parts = set()
        samples: MutableMapping[str, Sample] = dict()
        self._find_ancestor_samples(cell_suspension, samples)
        for sample in samples.values():
            if isinstance(sample, api.SpecimenFromOrganism):
                organs.add(sample.organ)
                organ_parts.update(sample.organ_parts)
            elif isinstance(sample, api.CellLine):
                organs.add(sample.model_organ)
                organ_parts.add(None)
            elif isinstance(sample, api.Organoid):
                organs.add(sample.model_organ)
                organ_parts.add(sample.model_organ_part)
            else:
                assert False
        return {
            'document_id': str(cell_suspension.document_id),
            'total_estimated_cells': cell_suspension.estimated_cell_count,
            'selected_cell_type': sorted(cell_suspension.selected_cell_types),
            'organ': sorted(organs),
            # With multiple samples it is possible to have str and None values
            'organ_part': sorted(organ_parts, key=none_safe_key(none_last=True))
        }

    def _cell_line_types(self) -> FieldTypes:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'cell_line_type': str,
            'model_organ': str
        }

    def _cell_line(self, cell_line: api.CellLine) -> JSON:
        # noinspection PyDeprecation
        return {
            'document_id': str(cell_line.document_id),
            'biomaterial_id': cell_line.biomaterial_id,
            'cell_line_type': cell_line.cell_line_type,
            'model_organ': cell_line.model_organ
        }

    def _donor_types(self) -> FieldTypes:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'biological_sex': str,
            'genus_species': str,
            'diseases': str,
            'organism_age': str,
            'organism_age_unit': str,
            'organism_age_range': None,  # Exclude ranged values from translation, prevents problem due to shadow copies
            'donor_count': None  # Exclude this field added by DonorOrganismAggregator from translation
        }

    def _donor(self, donor: api.DonorOrganism) -> JSON:
        return {
            'document_id': str(donor.document_id),
            'biomaterial_id': donor.biomaterial_id,
            'biological_sex': donor.sex,
            'genus_species': sorted(donor.genus_species),
            'diseases': sorted(donor.diseases),
            'organism_age': donor.organism_age,
            'organism_age_unit': donor.organism_age_unit,
            **(
                {
                    'organism_age_range': {
                        'gte': donor.organism_age_in_seconds.min,
                        'lte': donor.organism_age_in_seconds.max
                    }
                } if donor.organism_age_in_seconds else {
                }
            )
        }

    def _organoid_types(self) -> FieldTypes:
        return {
            'document_id': str,
            'biomaterial_id': str,
            'model_organ': str,
            'model_organ_part': str
        }

    def _organoid(self, organoid: api.Organoid) -> JSON:
        return {
            'document_id': str(organoid.document_id),
            'biomaterial_id': organoid.biomaterial_id,
            'model_organ': organoid.model_organ,
            'model_organ_part': organoid.model_organ_part
        }

    def _file_types(self) -> FieldTypes:
        return {
            'content-type': str,
            'indexed': bool,
            'name': str,
            'sha256': str,
            'size': int,
            'count': None,  # Exclude this field added by FileAggregator from translation, field will never be None
            'uuid': api.UUID4,
            'version': str,
            'document_id': str,
            'file_format': str,
            'content_description': str,
            '_type': str,
            'related_files': self._related_file_types(),
            'read_index': str,
            'lane_index': int
        }

    def _file(self, file: api.File, related_files: Iterable[api.File] = ()) -> JSON:
        # noinspection PyDeprecation
        return {
            'content-type': file.manifest_entry.content_type,
            'indexed': file.manifest_entry.indexed,
            'name': file.manifest_entry.name,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'version': file.manifest_entry.version,
            'document_id': str(file.document_id),
            'file_format': file.file_format,
            'content_description': sorted(file.content_description),
            '_type': 'file',
            'related_files': list(map(self._related_file, related_files)),
            **(
                {
                    'read_index': file.read_index,
                    'lane_index': file.lane_index
                } if isinstance(file, api.SequenceFile) else {
                }
            ),
        }

    def _related_file_types(self) -> FieldTypes:
        return {
            'name': str,
            'sha256': str,
            'size': int,
            'uuid': api.UUID4,
            'version': str,
        }

    def _related_file(self, file: api.File) -> JSON:
        return {
            'name': file.manifest_entry.name,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'version': file.manifest_entry.version,
        }

    def _protocol_types(self) -> FieldTypes:
        return {
            'document_id': str,
            'library_construction_approach': str,
            'instrument_manufacturer_model': str,
            'paired_end': bool,
            'workflow': str,
            'assay_type': None  # Exclude counter dict used to produce a FrequencySetAccumulator from translation
        }

    def _protocol(self, protocol: api.Protocol) -> JSON:
        # Note that `protocol` inner entities are constructed with all possible
        # protocol fields, and not just the fields relating to one specific
        # protocol. This is required for Elasticsearch searching and sorting.
        return {
            'document_id': protocol.document_id,
            'library_construction_approach': protocol.library_construction_method
            if isinstance(protocol, api.LibraryPreparationProtocol) else None,
            'instrument_manufacturer_model': protocol.instrument_manufacturer_model
            if isinstance(protocol, api.SequencingProtocol) else None,
            'paired_end': protocol.paired_end
            if isinstance(protocol, api.SequencingProtocol) else None,
            'workflow': protocol.protocol_id
            if isinstance(protocol, api.AnalysisProtocol) else None,
            'assay_type': dict(Counter(target.assay_type for target in protocol.target))
            if isinstance(protocol, api.ImagingProtocol) else None,
        }

    def _sample_types(self) -> FieldTypes:
        return {
            'entity_type': str,
            'effective_organ': str,
            **self._cell_line_types(),
            **self._organoid_types(),
            **self._specimen_types()
        }

    def _sample(self, sample: api.Biomaterial) -> JSON:
        # Start construction of a `sample` inner entity by including all fields
        # possible from any entities that can be a sample. This is done to
        # have consistency of fields between various sample inner entities
        # to allow Elasticsearch to search and sort against these entities.
        sample_ = dict.fromkeys(ChainMap(
            self._cell_line_types(),
            self._organoid_types(),
            self._specimen_types()
        ).keys())
        entity_type, entity_dict = (
            'cell_lines', self._cell_line(sample)
        ) if isinstance(sample, api.CellLine) else (
            'organoids', self._organoid(sample)
        ) if isinstance(sample, api.Organoid) else (
            'specimens', self._specimen(sample)
        ) if isinstance(sample, api.SpecimenFromOrganism) else (
            require(False, sample), None
        )
        sample_.update(entity_dict)
        sample_['entity_type'] = entity_type
        assert hasattr(sample, 'organ') != hasattr(sample, 'model_organ')
        sample_['effective_organ'] = sample.organ if hasattr(sample, 'organ') else sample.model_organ
        assert sample_['document_id'] == str(sample.document_id)
        assert sample_['biomaterial_id'] == sample.biomaterial_id
        return sample_

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project

    def _contribution(self, bundle: api.Bundle, contents: JSON, entity_id: api.UUID4, deleted: bool) -> Contribution:
        entity_reference = EntityReference(entity_type=self.entity_type(),
                                           entity_id=str(entity_id))
        return Contribution(entity=entity_reference,
                            version=None,
                            contents=contents,
                            bundle_uuid=str(bundle.uuid),
                            bundle_version=bundle.version,
                            bundle_deleted=deleted)

    def field_types(self) -> FieldTypes:
        return {
            'samples': self._sample_types(),
            'specimens': self._specimen_types(),
            'cell_suspensions': self._cell_suspension_types(),
            'cell_lines': self._cell_line_types(),
            'donors': self._donor_types(),
            'organoids': self._organoid_types(),
            'files': self._file_types(),
            'protocols': self._protocol_types(),
            'projects': self._project_types()
        }


def _parse_zarr_file_name(file_name: str) -> Tuple[bool, Optional[str], Optional[str]]:
    file_name = file_name.split('.zarr/')
    if len(file_name) == 1:
        return False, None, None
    elif len(file_name) == 2:
        zarr_name, sub_name = file_name
        return True, zarr_name, sub_name
    else:
        assert False


class TransformerVisitor(api.EntityVisitor):
    # Entities are tracked by ID to ensure uniqueness if an entity is visited twice while descending the entity DAG
    specimens: MutableMapping[api.UUID4, api.SpecimenFromOrganism]
    cell_suspensions: MutableMapping[api.UUID4, api.CellSuspension]
    cell_lines: MutableMapping[api.UUID4, api.CellLine]
    donors: MutableMapping[api.UUID4, api.DonorOrganism]
    organoids: MutableMapping[api.UUID4, api.Organoid]
    protocols: MutableMapping[api.UUID4, api.Protocol]
    files: MutableMapping[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.cell_lines = {}
        self.donors = {}
        self.organoids = {}
        self.protocols = {}
        self.files = {}

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.SpecimenFromOrganism):
            self.specimens[entity.document_id] = entity
        elif isinstance(entity, api.CellSuspension):
            self.cell_suspensions[entity.document_id] = entity
        elif isinstance(entity, api.CellLine):
            self.cell_lines[entity.document_id] = entity
        elif isinstance(entity, api.DonorOrganism):
            self.donors[entity.document_id] = entity
        elif isinstance(entity, api.Organoid):
            self.organoids[entity.document_id] = entity
        elif isinstance(entity, api.Process):
            for protocol in entity.protocols.values():
                if isinstance(protocol, (api.SequencingProtocol,
                                         api.LibraryPreparationProtocol,
                                         api.AnalysisProtocol,
                                         api.ImagingProtocol)):
                    self.protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            # noinspection PyDeprecation
            file_name = entity.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            # FIXME: Remove condition once https://github.com/HumanCellAtlas/metadata-schema/issues/623 is resolved
            if not is_zarr or sub_name.endswith('.zattrs'):
                self.files[entity.document_id] = entity


class FileTransformer(BaseTransformer):

    def entity_type(self) -> str:
        return 'files'

    def _transform(self, bundle: api.Bundle, deleted: bool) -> Iterable[Contribution]:
        project = self._get_project(bundle)
        zarr_stores: Mapping[str, List[api.File]] = self.group_zarrs(bundle.files.values())
        for file in bundle.files.values():
            file_name = file.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            # FIXME: Remove condition once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
            if not is_zarr or sub_name.endswith('.zattrs'):
                if is_zarr:
                    # This is the representative file, so add the related files
                    related_files = zarr_stores[zarr_name]
                else:
                    related_files = ()
                visitor = TransformerVisitor()
                file.accept(visitor)
                file.ancestors(visitor)
                samples: MutableMapping[str, Sample] = dict()
                self._find_ancestor_samples(file, samples)
                contents = dict(samples=list(map(self._sample, samples.values())),
                                specimens=list(map(self._specimen, visitor.specimens.values())),
                                cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                                cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                                donors=list(map(self._donor, visitor.donors.values())),
                                organoids=list(map(self._organoid, visitor.organoids.values())),
                                files=[self._file(file, related_files=related_files)],
                                protocols=list(map(self._protocol, visitor.protocols.values())),
                                projects=[self._project(project)])
                yield self._contribution(bundle, contents, file.document_id, deleted)

    def group_zarrs(self, files: Iterable[api.File]) -> Mapping[str, List[api.File]]:
        zarr_stores = defaultdict(list)
        for file in files:
            file_name = file.manifest_entry.name
            is_zarr, zarr_name, sub_name = _parse_zarr_file_name(file_name)
            if is_zarr:
                # Leave the representative file out of the list since it's already in the manifest
                if not sub_name.startswith('.zattrs'):
                    zarr_stores[zarr_name].append(file)
        return zarr_stores


class CellSuspensionTransformer(BaseTransformer):

    def entity_type(self) -> str:
        return 'cell_suspensions'

    def _transform(self, bundle: api.Bundle, deleted: bool) -> Iterable[Contribution]:
        project = self._get_project(bundle)
        for cell_suspension in bundle.biomaterials.values():
            if isinstance(cell_suspension, api.CellSuspension):
                samples: MutableMapping[str, Sample] = dict()
                self._find_ancestor_samples(cell_suspension, samples)
                visitor = TransformerVisitor()
                cell_suspension.accept(visitor)
                cell_suspension.ancestors(visitor)
                contents = dict(samples=list(map(self._sample, samples.values())),
                                specimens=list(map(self._specimen, visitor.specimens.values())),
                                cell_suspensions=[self._cell_suspension(cell_suspension)],
                                cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                                donors=list(map(self._donor, visitor.donors.values())),
                                organoids=list(map(self._organoid, visitor.organoids.values())),
                                files=list(map(self._file, visitor.files.values())),
                                protocols=list(map(self._protocol, visitor.protocols.values())),
                                projects=[self._project(project)])
                yield self._contribution(bundle, contents, cell_suspension.document_id, deleted)


class SampleTransformer(BaseTransformer):

    def entity_type(self) -> str:
        return 'samples'

    def _transform(self, bundle: api.Bundle, deleted: bool) -> Iterable[Contribution]:
        project = self._get_project(bundle)
        samples: MutableMapping[str, Sample] = dict()
        for file in bundle.files.values():
            self._find_ancestor_samples(file, samples)
        for sample in samples.values():
            visitor = TransformerVisitor()
            sample.accept(visitor)
            sample.ancestors(visitor)
            contents = dict(samples=[self._sample(sample)],
                            specimens=list(map(self._specimen, visitor.specimens.values())),
                            cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                            cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                            donors=list(map(self._donor, visitor.donors.values())),
                            organoids=list(map(self._organoid, visitor.organoids.values())),
                            files=list(map(self._file, visitor.files.values())),
                            protocols=list(map(self._protocol, visitor.protocols.values())),
                            projects=[self._project(project)])
            yield self._contribution(bundle, contents, sample.document_id, deleted)


class BundleProjectTransformer(BaseTransformer, metaclass=ABCMeta):

    @abstractmethod
    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        raise NotImplementedError()

    def _transform(self, bundle: api.Bundle, deleted: bool) -> Iterable[Contribution]:
        # Project entities are not explicitly linked in the graph. The mere presence of project metadata in a bundle
        # indicates that all other entities in that bundle belong to that project. Because of that we can't rely on a
        # visitor to collect the related entities but have to enumerate the explicitly:
        #
        visitor = TransformerVisitor()
        for specimen in bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        samples: MutableMapping[str, Sample] = dict()
        for file in bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
            self._find_ancestor_samples(file, samples)
        project = self._get_project(bundle)

        contents = dict(samples=list(map(self._sample, samples.values())),
                        specimens=list(map(self._specimen, visitor.specimens.values())),
                        cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                        cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                        donors=list(map(self._donor, visitor.donors.values())),
                        organoids=list(map(self._organoid, visitor.organoids.values())),
                        files=list(map(self._file, visitor.files.values())),
                        protocols=list(map(self._protocol, visitor.protocols.values())),
                        projects=[self._project(project)])

        yield self._contribution(bundle, contents, self._get_entity_id(bundle, project), deleted)


class ProjectTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return project.document_id

    def entity_type(self) -> str:
        return 'projects'


class BundleTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return bundle.uuid

    def get_aggregator(self, entity_type):
        if entity_type in ('files', 'metadata'):
            return None
        else:
            return super().get_aggregator(entity_type)

    def entity_type(self) -> str:
        return 'bundles'

    def transform(self,
                  uuid: BundleUUID,
                  version: BundleVersion,
                  deleted: bool,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Iterable[Contribution]:
        for contrib in super().transform(uuid, version, deleted, manifest, metadata_files):
            # noinspection PyArgumentList
            if 'project.json' in metadata_files:
                # we can't handle v5 bundles
                metadata = []
            else:
                generator = MetadataGenerator()
                generator.add_bundle(uuid, version, manifest, metadata_files)
                metadata = generator.dump()
            contrib.contents['metadata'] = metadata
            yield contrib

    def field_types(self) -> FieldTypes:
        return {
            **super().field_types(),
            'metadata': None  # Exclude fields that came from MetadataGenerator() from translation
        }


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return dict(size=((entity['uuid'], entity['version']), entity['size']),
                    file_format=entity['file_format'],
                    count=((entity['uuid'], entity['version']), 1),
                    content_description=entity['content_description'])

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['file_format']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'file_format':
            return SingleValueAccumulator()
        elif field == 'content_description':
            return SetAccumulator(max_size=100)
        elif field in ('size', 'count'):
            return DistinctAccumulator(SumAccumulator(0))
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

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['organ']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return DistinctAccumulator(SumAccumulator(0))
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
                                        key=compose_keys(none_safe_tuple_key(none_last=True), itemgetter('lte', 'gte')))
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
                       'publication_titles',
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
