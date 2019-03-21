from abc import ABCMeta, abstractmethod
import logging
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple, Union
from more_itertools import one

from humancellatlas.data.metadata import api
from humancellatlas.data.metadata.helpers.json import as_json

from azul import reject
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Contribution,
                              DistinctAccumulator,
                              Document,
                              EntityReference,
                              GroupingAggregator,
                              ListAccumulator,
                              MandatoryValueAccumulator,
                              OptionalValueAccumulator,
                              SetAccumulator,
                              PrioritySetAccumulator,
                              SimpleAggregator,
                              SumAccumulator)
from azul.types import JSON

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]


def _project_dict(project: api.Project) -> JSON:
    # Store lists of all values of each of these facets to allow facet filtering
    # and term counting on the webservice
    laboratories: Set[str] = set()
    institutions: Set[str] = set()
    contact_names: Set[str] = set()
    publication_titles: Set[str] = set()

    for contributor in project.contributors:
        if contributor.laboratory:
            laboratories.add(contributor.laboratory)
        if contributor.contact_name:
            contact_names.add(contributor.contact_name)
        if contributor.institution:
            institutions.add(contributor.institution)

    for publication in project.publications:
        if publication.publication_title:
            publication_titles.add(publication.publication_title)

    return {
        'project_title': project.project_title,
        'project_description': project.project_description,
        'project_shortname': project.project_short_name,
        'laboratory': list(laboratories),
        'institutions': list(institutions),
        'contact_names': list(contact_names),
        'contributors': as_json(project.contributors),
        'document_id': str(project.document_id),
        'publication_titles': list(publication_titles),
        'publications': as_json(project.publications),
        'insdc_project_accessions': as_json(project.insdc_project_accessions),
        'geo_series_accessions': as_json(project.geo_series_accessions),
        'array_express_accessions': as_json(project.array_express_accessions),
        'insdc_study_accessions': as_json(project.insdc_study_accessions),
        '_type': 'project'
    }


def _specimen_dict(specimen: api.SpecimenFromOrganism) -> JSON:
    return {
        'has_input_biomaterial': specimen.has_input_biomaterial,
        '_source': api.schema_names[type(specimen)],
        'document_id': str(specimen.document_id),
        'biomaterial_id': specimen.biomaterial_id,
        'disease': as_json(specimen.diseases),
        'organ': specimen.organ,
        'organ_part': as_json(specimen.organ_parts),
        'storage_method': specimen.storage_method,
        'preservation_method': specimen.preservation_method,
        '_type': 'specimen',
    }


def _cell_suspension_dict(cell_suspension: api.CellSuspension) -> JSON:
    visitor = CellSuspensionVisitor()
    # Visit the cell suspension but don't descend. We're only interested in parent biomaterials
    visitor.visit(cell_suspension)
    cell_suspension.ancestors(visitor)
    return visitor.merged_cell_suspension


def _cell_line_dict(cell_line: api.CellLine) -> JSON:
    return {
        'document_id': str(cell_line.document_id),
        'biomaterial_id': cell_line.biomaterial_id,
    }


def _donor_dict(donor: api.DonorOrganism) -> JSON:
    return {
        'document_id': str(donor.document_id),
        'biomaterial_id': donor.biomaterial_id,
        'biological_sex': donor.sex,
        'genus_species': as_json(donor.genus_species),
        'diseases': as_json(donor.diseases),
        'organism_age': donor.organism_age,
        'organism_age_unit': donor.organism_age_unit,
        **(
            {
                'min_organism_age_in_seconds': donor.organism_age_in_seconds.min,
                'max_organism_age_in_seconds': donor.organism_age_in_seconds.max,
            } if donor.organism_age_in_seconds else {
            }
        ),
    }


def _organoid_dict(organoid: api.Organoid) -> JSON:
    return {
        'document_id': str(organoid.document_id),
        'biomaterial_id': organoid.biomaterial_id,
        'model_organ': organoid.model_organ,
        'model_organ_part': organoid.model_organ_part,
    }


def _file_dict(file: api.File) -> JSON:
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
        '_type': 'file',
        **(
            {
                'read_index': file.read_index,
                'lane_index': file.lane_index
            } if isinstance(file, api.SequenceFile) else {
            }
        )
    }


def _protocol_dict(protocol: api.Protocol) -> JSON:
    protocol_dict = {"document_id": protocol.document_id}
    if isinstance(protocol, api.LibraryPreparationProtocol):
        protocol_dict['library_construction_approach'] = protocol.library_construction_approach
    elif isinstance(protocol, api.SequencingProtocol):
        protocol_dict['instrument_manufacturer_model'] = protocol.instrument_manufacturer_model
        protocol_dict['paired_end'] = protocol.paired_end
    else:
        assert False
    return protocol_dict


# map Sample api.schema_names to keys in the contents dict
sample_entity_types = {
    'cell_line': 'cell_lines',
    'organoid': 'organoids',
    'specimen_from_organism': 'specimens',
}

# map Sample api.schema_names to the related ..._dict function
sample_entity_dict_functions = {
    'cell_line': _cell_line_dict,
    'organoid': _organoid_dict,
    'specimen_from_organism': _specimen_dict,
}


def _sample_dict(sample: api.Biomaterial) -> JSON:
    schema_type = api.schema_names[type(sample)]
    sample_dict = {
        'entity_type': sample_entity_types[schema_type],
        **sample_entity_dict_functions[schema_type](sample)
    }
    assert sample_dict['document_id'] == str(sample.document_id)
    assert sample_dict['biomaterial_id'] == sample.biomaterial_id
    return sample_dict


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
                if isinstance(protocol, (api.SequencingProtocol, api.LibraryPreparationProtocol)):
                    self.protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            if entity.file_format == 'unknown' and '.zarr!' in entity.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = entity


class BiomaterialVisitor(api.EntityVisitor, metaclass=ABCMeta):

    def __init__(self) -> None:
        self._accumulators: MutableMapping[str, Accumulator] = {}
        self._biomaterials: MutableMapping[api.UUID4, api.Biomaterial] = dict()

    def _set(self, field: str, accumulator_factory: Callable[[], Accumulator], value: Any):
        try:
            accumulator = self._accumulators[field]
        except KeyError:
            self._accumulators[field] = accumulator = accumulator_factory()
        accumulator.accumulate(value)

    def visit(self, entity: api.Entity) -> None:
        if isinstance(entity, api.Biomaterial) and entity.document_id not in self._biomaterials:
            self._biomaterials[entity.document_id] = entity
            self._visit(entity)

    @abstractmethod
    def _visit(self, entity: api.Biomaterial) -> None:
        raise NotImplementedError()


class CellSuspensionVisitor(BiomaterialVisitor):

    def _visit(self, entity: api.Biomaterial) -> None:

        if isinstance(entity, api.CellSuspension):
            self._set('document_id', MandatoryValueAccumulator, str(entity.document_id))
            self._set('total_estimated_cells', OptionalValueAccumulator, entity.total_estimated_cells)
            self._set('selected_cell_type', SetAccumulator, entity.selected_cell_type)
        elif isinstance(entity, api.SpecimenFromOrganism):
            self._set('organ', PrioritySetAccumulator, (0, entity.organ))
            self._set('organ_part', PrioritySetAccumulator, (0, entity.organ_parts))
        elif isinstance(entity, api.Organoid):
            self._set('organ', PrioritySetAccumulator, (1, entity.model_organ))
            self._set('organ_part', PrioritySetAccumulator, (1, entity.model_organ_part))

    @property
    def merged_cell_suspension(self) -> JSON:
        assert 'document_id' in self._accumulators
        return {field: accumulator.get() for field, accumulator in self._accumulators.items()}


class FileAggregator(GroupingAggregator):

    def _transform_entity(self, entity: JSON) -> JSON:
        return dict(size=((entity['uuid'], entity['version']), entity['size']),
                    file_format=entity['file_format'],
                    count=((entity['uuid'], entity['version']), 1))

    def _group_keys(self, entity) -> Iterable[Any]:
        return [entity['file_format']]

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'file_format':
            return SetAccumulator()
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

    def _group_keys(self, entity) -> Iterable[Any]:
        return entity['organ']

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        if field == 'total_estimated_cells':
            return SumAccumulator(0)
        else:
            return SetAccumulator(max_size=100)


class CellLineAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
        return SetAccumulator(max_size=100)


class DonorOrganismAggregator(SimpleAggregator):

    def _get_accumulator(self, field) -> Optional[Accumulator]:
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
        else:
            return SetAccumulator()


class Transformer(AggregatingTransformer, metaclass=ABCMeta):

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
            return super().get_aggregator(entity_type)

    def _get_project(self, bundle) -> api.Project:
        project, *additional_projects = bundle.projects.values()
        reject(additional_projects, "Azul can currently only handle a single project per bundle")
        assert isinstance(project, api.Project)
        return project

    def _contribution(self, bundle, contents, entity):
        entity_reference = EntityReference(entity_type=self.entity_type(),
                                           entity_id=str(entity.document_id))
        # noinspection PyArgumentList
        # https://youtrack.jetbrains.com/issue/PY-28506
        return Contribution(entity=entity_reference,
                            version=None,
                            contents=contents,
                            bundle_uuid=str(bundle.uuid),
                            bundle_version=bundle.version)


class FileTransformer(Transformer):

    def entity_type(self) -> str:
        return 'files'

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Iterable[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        for file in bundle.files.values():
            if file.file_format == 'unknown' and '.zarr!' in file.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                continue
            visitor = TransformerVisitor()
            file.accept(visitor)
            file.ancestors(visitor)
            samples: MutableMapping[str, Sample] = dict()
            SampleTransformer.get_ancestor_samples(file, samples)
            contents = dict(samples=[_sample_dict(s) for s in samples.values()],
                            specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            cell_lines=[_cell_line_dict(cl) for cl in visitor.cell_lines.values()],
                            donors=[_donor_dict(d) for d in visitor.donors.values()],
                            organoids=[_organoid_dict(o) for o in visitor.organoids.values()],
                            files=[_file_dict(file)],
                            protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                            projects=[_project_dict(project)])
            yield self._contribution(bundle, contents, file)


class SampleTransformer(Transformer):

    def entity_type(self) -> str:
        return 'samples'

    @classmethod
    def get_ancestor_samples(cls, entity: api.LinkedEntity, samples: MutableMapping[str, Sample]):
        """
        Fill samples dict with the first Sample found up each ancestor tree
        """
        if isinstance(entity, Sample.__args__):
            samples[str(entity.document_id)] = entity
        else:
            for parent in entity.parents.values():
                cls.get_ancestor_samples(parent, samples)

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
        project = self._get_project(bundle)
        samples: MutableMapping[str, Sample] = dict()
        for file in bundle.files.values():
            self.get_ancestor_samples(file, samples)
        for sample in samples.values():
            visitor = TransformerVisitor()
            sample.accept(visitor)
            sample.ancestors(visitor)
            contents = dict(samples=[_sample_dict(sample)],
                            specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                            cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                            cell_lines=[_cell_line_dict(cl) for cl in visitor.cell_lines.values()],
                            donors=[_donor_dict(d) for d in visitor.donors.values()],
                            organoids=[_organoid_dict(o) for o in visitor.organoids.values()],
                            files=[_file_dict(f) for f in visitor.files.values()],
                            protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                            projects=[_project_dict(project)])
            yield self._contribution(bundle, contents, sample)


class ProjectTransformer(Transformer):

    def entity_type(self) -> str:
        return 'projects'

    def transform(self,
                  uuid: str,
                  version: str,
                  manifest: List[JSON],
                  metadata_files: Mapping[str, JSON]
                  ) -> Sequence[Document]:
        bundle = api.Bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files)
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
            SampleTransformer.get_ancestor_samples(file, samples)
        project = self._get_project(bundle)

        contents = dict(samples=[_sample_dict(s) for s in samples.values()],
                        specimens=[_specimen_dict(s) for s in visitor.specimens.values()],
                        cell_suspensions=[_cell_suspension_dict(cs) for cs in visitor.cell_suspensions.values()],
                        cell_lines=[_cell_line_dict(cl) for cl in visitor.cell_lines.values()],
                        donors=[_donor_dict(d) for d in visitor.donors.values()],
                        organoids=[_organoid_dict(o) for o in visitor.organoids.values()],
                        files=[_file_dict(f) for f in visitor.files.values()],
                        protocols=[_protocol_dict(pl) for pl in visitor.protocols.values()],
                        projects=[_project_dict(project)])
        yield self._contribution(bundle, contents, project)
