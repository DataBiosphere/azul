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
from typing import (
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from humancellatlas.data.metadata import (
    api,
)

from azul import (
    reject,
    require,
)
from azul.collections import (
    none_safe_key,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.indexer.aggregate import (
    SimpleAggregator,
)
from azul.indexer.document import (
    Contribution,
    ContributionCoordinates,
    EntityReference,
    FieldTypes,
    PassThrough,
    null_bool,
    null_int,
    null_str,
    pass_thru_int,
    pass_thru_json,
)
from azul.indexer.transform import (
    Transformer,
)
from azul.plugins.metadata.hca.aggregate import (
    CellLineAggregator,
    CellSuspensionAggregator,
    DonorOrganismAggregator,
    FileAggregator,
    OrganoidAggregator,
    ProjectAggregator,
    ProtocolAggregator,
    SampleAggregator,
    SequencingProcessAggregator,
    SpecimenAggregator,
)
from azul.plugins.metadata.hca.full_metadata import (
    FullMetadata,
)
from azul.types import (
    MutableJSON,
)

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]
sample_types = api.CellLine, api.Organoid, api.SpecimenFromOrganism
assert Sample.__args__ == sample_types  # since we can't use * in generic types

pass_thru_uuid4: PassThrough[api.UUID4] = PassThrough()


class BaseTransformer(Transformer, metaclass=ABCMeta):

    @classmethod
    def create(cls, bundle: Bundle, deleted: bool) -> 'Transformer':
        return cls(bundle, deleted)

    def __init__(self, bundle: Bundle, deleted: bool) -> None:
        super().__init__()
        self.deleted = deleted
        self.bundle = api.Bundle(uuid=bundle.uuid,
                                 version=bundle.version,
                                 manifest=bundle.manifest,
                                 metadata_files=bundle.metadata_files)
        for file in self.bundle.files.values():
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

    @classmethod
    def get_aggregator(cls, entity_type):
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
        elif entity_type == 'sequencing_processes':
            return SequencingProcessAggregator()
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

    @classmethod
    def _contact_types(cls) -> FieldTypes:
        return {
            'contact_name': null_str,
            'corresponding_contributor': null_bool,
            'email': null_str,
            'institution': null_str,
            'laboratory': null_str,
            'project_role': null_str
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

    @classmethod
    def _publication_types(cls) -> FieldTypes:
        return {
            'publication_title': null_str,
            'publication_url': null_str
        }

    def _publication(self, p: api.ProjectPublication):
        # noinspection PyDeprecation
        return {
            "publication_title": p.publication_title,
            "publication_url": p.publication_url
        }

    @classmethod
    def _project_types(cls) -> FieldTypes:
        return {
            'project_title': null_str,
            'project_description': null_str,
            'project_short_name': null_str,
            'laboratory': null_str,
            'institutions': null_str,
            'contact_names': null_str,
            'contributors': cls._contact_types(),
            'document_id': null_str,
            'publication_titles': null_str,
            'publications': cls._publication_types(),
            'insdc_project_accessions': null_str,
            'geo_series_accessions': null_str,
            'array_express_accessions': null_str,
            'insdc_study_accessions': null_str,
            'supplementary_links': null_str,
            '_type': null_str
        }

    def _project(self, project: api.Project) -> MutableJSON:
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

    @classmethod
    def _specimen_types(cls) -> FieldTypes:
        return {
            'has_input_biomaterial': null_str,
            '_source': null_str,
            'document_id': null_str,
            'biomaterial_id': null_str,
            'disease': null_str,
            'organ': null_str,
            'organ_part': null_str,
            'storage_method': null_str,
            'preservation_method': null_str,
            '_type': null_str
        }

    def _specimen(self, specimen: api.SpecimenFromOrganism) -> MutableJSON:
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

    @classmethod
    def _cell_suspension_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'total_estimated_cells': null_int,
            'selected_cell_type': null_str,
            'organ': null_str,
            'organ_part': null_str
        }

    def _cell_suspension(self, cell_suspension: api.CellSuspension) -> MutableJSON:
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
            'biomaterial_id': str(cell_suspension.biomaterial_id),
            'total_estimated_cells': cell_suspension.estimated_cell_count,
            'selected_cell_type': sorted(cell_suspension.selected_cell_types),
            'organ': sorted(organs),
            # With multiple samples it is possible to have str and None values
            'organ_part': sorted(organ_parts, key=none_safe_key(none_last=True))
        }

    @classmethod
    def _cell_line_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'cell_line_type': null_str,
            'model_organ': null_str
        }

    def _cell_line(self, cell_line: api.CellLine) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            'document_id': str(cell_line.document_id),
            'biomaterial_id': cell_line.biomaterial_id,
            'cell_line_type': cell_line.cell_line_type,
            'model_organ': cell_line.model_organ
        }

    @classmethod
    def _donor_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'biological_sex': null_str,
            'genus_species': null_str,
            'diseases': null_str,
            'organism_age': null_str,
            'organism_age_unit': null_str,
            # Prevent problem due to shadow copies on numeric ranges
            'organism_age_range': pass_thru_json,
            # Pass through field added by DonorOrganismAggregator
            'donor_count': pass_thru_int
        }

    def _donor(self, donor: api.DonorOrganism) -> MutableJSON:
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

    @classmethod
    def _organoid_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'biomaterial_id': null_str,
            'model_organ': null_str,
            'model_organ_part': null_str
        }

    def _organoid(self, organoid: api.Organoid) -> MutableJSON:
        return {
            'document_id': str(organoid.document_id),
            'biomaterial_id': organoid.biomaterial_id,
            'model_organ': organoid.model_organ,
            'model_organ_part': organoid.model_organ_part
        }

    @classmethod
    def _file_types(cls) -> FieldTypes:
        return {
            'content-type': null_str,
            'indexed': null_bool,
            'name': null_str,
            'crc32c': null_str,
            'sha256': null_str,
            'size': null_int,
            # Pass through field added by FileAggregator, will never be None
            'count': pass_thru_int,
            'uuid': pass_thru_uuid4,
            'version': null_str,
            'document_id': null_str,
            'file_format': null_str,
            'content_description': null_str,
            '_type': null_str,
            'related_files': cls._related_file_types(),
            'read_index': null_str,
            'lane_index': null_int
        }

    def _file(self, file: api.File, related_files: Iterable[api.File] = ()) -> MutableJSON:
        # noinspection PyDeprecation
        return {
            'content-type': file.manifest_entry.content_type,
            'indexed': file.manifest_entry.indexed,
            'name': file.manifest_entry.name,
            'crc32c': file.manifest_entry.crc32c,
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

    @classmethod
    def _related_file_types(cls) -> FieldTypes:
        return {
            'name': null_str,
            'crc32c': null_str,
            'sha256': null_str,
            'size': null_int,
            'uuid': pass_thru_uuid4,
            'version': null_str,
        }

    def _related_file(self, file: api.File) -> MutableJSON:
        return {
            'name': file.manifest_entry.name,
            'crc32c': file.manifest_entry.crc32c,
            'sha256': file.manifest_entry.sha256,
            'size': file.manifest_entry.size,
            'uuid': file.manifest_entry.uuid,
            'version': file.manifest_entry.version,
        }

    @classmethod
    def _protocol_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
            'library_construction_approach': null_str,
            'instrument_manufacturer_model': null_str,
            'paired_end': null_bool,
            'workflow': null_str,
            # Pass through counter used to produce a FrequencySetAccumulator
            'assay_type': pass_thru_json
        }

    def _protocol(self, protocol: api.Protocol) -> MutableJSON:
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

    @classmethod
    def _sequencing_process_types(cls) -> FieldTypes:
        return {
            'document_id': null_str,
        }

    def _sequencing_process(self, process: api.Process) -> MutableJSON:
        return {
            'document_id': str(process.document_id),
        }

    @classmethod
    def _sample_types(cls) -> FieldTypes:
        return {
            'entity_type': null_str,
            'effective_organ': null_str,
            **cls._cell_line_types(),
            **cls._organoid_types(),
            **cls._specimen_types()
        }

    def _sample(self, sample: api.Biomaterial) -> MutableJSON:
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

    def _contribution(self, contents: MutableJSON, entity_id: api.UUID4) -> Contribution:
        entity = EntityReference(entity_type=self.entity_type(),
                                 entity_id=str(entity_id))
        bundle_fqid = BundleFQID(uuid=str(self.bundle.uuid),
                                 version=self.bundle.version)
        coordinates = ContributionCoordinates(entity=entity,
                                              bundle=bundle_fqid,
                                              deleted=self.deleted)
        return Contribution(coordinates=coordinates,
                            version=None,
                            contents=contents)

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            'samples': cls._sample_types(),
            'specimens': cls._specimen_types(),
            'cell_suspensions': cls._cell_suspension_types(),
            'cell_lines': cls._cell_line_types(),
            'donors': cls._donor_types(),
            'organoids': cls._organoid_types(),
            'files': cls._file_types(),
            'protocols': cls._protocol_types(),
            'sequencing_processes': cls._sequencing_process_types(),
            'projects': cls._project_types()
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
    sequencing_processes: MutableMapping[api.UUID4, api.Process]
    files: MutableMapping[api.UUID4, api.File]

    def __init__(self) -> None:
        self.specimens = {}
        self.cell_suspensions = {}
        self.cell_lines = {}
        self.donors = {}
        self.organoids = {}
        self.protocols = {}
        self.sequencing_processes = {}
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
            if entity.is_sequencing_process():
                self.sequencing_processes[entity.document_id] = entity
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

    @classmethod
    def entity_type(cls) -> str:
        return 'files'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.bundle)
        zarr_stores: Mapping[str, List[api.File]] = self.group_zarrs(self.bundle.files.values())
        for file in self.bundle.files.values():
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
                                sequencing_processes=list(
                                    map(self._sequencing_process, visitor.sequencing_processes.values())
                                ),
                                projects=[self._project(project)])
                yield self._contribution(contents, file.document_id)

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

    @classmethod
    def entity_type(cls) -> str:
        return 'cell_suspensions'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.bundle)
        for cell_suspension in self.bundle.biomaterials.values():
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
                                sequencing_processes=list(
                                    map(self._sequencing_process, visitor.sequencing_processes.values())
                                ),
                                projects=[self._project(project)])
                yield self._contribution(contents, cell_suspension.document_id)


class SampleTransformer(BaseTransformer):

    @classmethod
    def entity_type(cls) -> str:
        return 'samples'

    def transform(self) -> Iterable[Contribution]:
        project = self._get_project(self.bundle)
        samples: MutableMapping[str, Sample] = dict()
        for file in self.bundle.files.values():
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
                            sequencing_processes=list(
                                map(self._sequencing_process, visitor.sequencing_processes.values())
                            ),
                            projects=[self._project(project)])
            yield self._contribution(contents, sample.document_id)


class BundleProjectTransformer(BaseTransformer, metaclass=ABCMeta):

    @abstractmethod
    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        raise NotImplementedError()

    def transform(self) -> Iterable[Contribution]:
        # Project entities are not explicitly linked in the graph. The mere presence of project metadata in a bundle
        # indicates that all other entities in that bundle belong to that project. Because of that we can't rely on a
        # visitor to collect the related entities but have to enumerate the explicitly:
        #
        visitor = TransformerVisitor()
        for specimen in self.bundle.specimens:
            specimen.accept(visitor)
            specimen.ancestors(visitor)
        samples: MutableMapping[str, Sample] = dict()
        for file in self.bundle.files.values():
            file.accept(visitor)
            file.ancestors(visitor)
            self._find_ancestor_samples(file, samples)
        project = self._get_project(self.bundle)

        contents = dict(samples=list(map(self._sample, samples.values())),
                        specimens=list(map(self._specimen, visitor.specimens.values())),
                        cell_suspensions=list(map(self._cell_suspension, visitor.cell_suspensions.values())),
                        cell_lines=list(map(self._cell_line, visitor.cell_lines.values())),
                        donors=list(map(self._donor, visitor.donors.values())),
                        organoids=list(map(self._organoid, visitor.organoids.values())),
                        files=list(map(self._file, visitor.files.values())),
                        protocols=list(map(self._protocol, visitor.protocols.values())),
                        sequencing_processes=list(
                            map(self._sequencing_process, visitor.sequencing_processes.values())
                        ),
                        projects=[self._project(project)])

        yield self._contribution(contents, self._get_entity_id(project))


class ProjectTransformer(BundleProjectTransformer):

    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        return project.document_id

    @classmethod
    def entity_type(cls) -> str:
        return 'projects'


class BundleTransformer(BundleProjectTransformer):

    def __init__(self, bundle: Bundle, deleted: bool) -> None:
        super().__init__(bundle, deleted)
        if 'project.json' in bundle.metadata_files:
            # we can't handle v5 bundles
            self.metadata = []
        else:
            full_metadata = FullMetadata()
            full_metadata.add_bundle(bundle)
            self.metadata = full_metadata.dump()

    def _get_entity_id(self, project: api.Project) -> api.UUID4:
        return self.bundle.uuid

    @classmethod
    def get_aggregator(cls, entity_type):
        if entity_type in ('files', 'metadata'):
            return None
        else:
            return super().get_aggregator(entity_type)

    @classmethod
    def entity_type(cls) -> str:
        return 'bundles'

    def _contribution(self, contents: MutableJSON, entity_id: api.UUID4) -> Contribution:
        contents['metadata'] = self.metadata
        return super()._contribution(contents, entity_id)

    @classmethod
    def field_types(cls) -> FieldTypes:
        return {
            **super().field_types(),
            'metadata': pass_thru_json  # Exclude full metadata from translation
        }
