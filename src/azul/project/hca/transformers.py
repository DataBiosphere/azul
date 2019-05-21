from abc import ABCMeta, abstractmethod
from collections import Counter
import logging
from typing import Any, Callable, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Union

from humancellatlas.data.metadata import api

from azul import reject
from azul.transformer import (Accumulator,
                              AggregatingTransformer,
                              Contribution,
                              DistinctAccumulator,
                              Document,
                              EntityReference,
                              GroupingAggregator,
                              ListAccumulator,
                              SetAccumulator,
                              FrequencySetAccumulator,
                              SimpleAggregator,
                              SumAccumulator)
from azul.types import JSON

log = logging.getLogger(__name__)

Sample = Union[api.CellLine, api.Organoid, api.SpecimenFromOrganism]


def _contact_dict(p: api.ProjectContact):
    return {
        "contact_name": p.contact_name,
        "corresponding_contributor": p.corresponding_contributor,
        "email": p.email,
        "institution": p.institution,
        "laboratory": p.laboratory,
        "project_role": p.project_role
    }


def _publication_dict(p: api.ProjectPublication):
    return {
        "publication_title": p.publication_title,
        "publication_url": p.publication_url
    }


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
        'contributors': [_contact_dict(c) for c in project.contributors],
        'document_id': str(project.document_id),
        'publication_titles': list(publication_titles),
        'publications': [_publication_dict(p) for p in project.publications],
        'insdc_project_accessions': list(project.insdc_project_accessions),
        'geo_series_accessions': list(project.geo_series_accessions),
        'array_express_accessions': list(project.array_express_accessions),
        'insdc_study_accessions': list(project.insdc_study_accessions),
        '_type': 'project'
    }


def _specimen_dict(specimen: api.SpecimenFromOrganism) -> JSON:
    return {
        'has_input_biomaterial': specimen.has_input_biomaterial,
        '_source': api.schema_names[type(specimen)],
        'document_id': str(specimen.document_id),
        'biomaterial_id': specimen.biomaterial_id,
        'disease': list(specimen.diseases),
        'organ': specimen.organ,
        'organ_part': list(specimen.organ_parts),
        'storage_method': specimen.storage_method,
        'preservation_method': specimen.preservation_method,
        '_type': 'specimen',
    }

def _cell_suspension_dict(cell_suspension: api.CellSuspension) -> JSON:
    organs = set()
    organ_parts = set()
    samples: MutableMapping[str, Sample] = dict()
    SampleTransformer.get_ancestor_samples(cell_suspension, samples)
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
        'selected_cell_type': list( cell_suspension.selected_cell_types),
        'organ': list(organs),
        'organ_part': list(organ_parts),
    }


def _cell_line_dict(cell_line: api.CellLine) -> JSON:
    return {
        'document_id': str(cell_line.document_id),
        'biomaterial_id': cell_line.biomaterial_id,
        'cell_line_type': cell_line.cell_line_type,
        'model_organ': cell_line.model_organ
    }


def _donor_dict(donor: api.DonorOrganism) -> JSON:
    return {
        'document_id': str(donor.document_id),
        'biomaterial_id': donor.biomaterial_id,
        'biological_sex': donor.sex,
        'genus_species': list(donor.genus_species),
        'diseases': list(donor.diseases),
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
    elif isinstance(protocol, api.AnalysisProtocol):
        protocol_dict['workflow'] = protocol.protocol_id
    elif isinstance(protocol, api.ImagingProtocol):
        protocol_dict['assay_type'] = dict(Counter(target.assay_type for target in protocol.target))
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
    assert hasattr(sample, 'organ') != hasattr(sample, 'model_organ')
    sample_dict['effective_organ'] = sample.organ if hasattr(sample, 'organ') else sample.model_organ
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
                if isinstance(protocol, (api.SequencingProtocol,
                                         api.LibraryPreparationProtocol,
                                         api.AnalysisProtocol,
                                         api.ImagingProtocol)):
                    self.protocols[protocol.document_id] = protocol
        elif isinstance(entity, api.File):
            if entity.file_format == 'unknown' and '.zarr!' in entity.manifest_entry.name:
                # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
                #
                return
            self.files[entity.document_id] = entity


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
        elif field == 'assay_type':
            return FrequencySetAccumulator(max_size=100)
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

    def _contribution(self, bundle: api.Bundle, contents: JSON, entity_id: api.UUID4) -> Contribution:
        entity_reference = EntityReference(entity_type=self.entity_type(),
                                           entity_id=str(entity_id))
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
            yield self._contribution(bundle, contents, file.document_id)


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
            yield self._contribution(bundle, contents, sample.document_id)


class BundleProjectTransformer(Transformer, metaclass=ABCMeta):

    @abstractmethod
    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        raise NotImplementedError()

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

        yield self._contribution(bundle, contents, self._get_entity_id(bundle, project))


class ProjectTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return project.document_id

    def entity_type(self) -> str:
        return 'projects'


class BundleTransformer(BundleProjectTransformer):

    def _get_entity_id(self, bundle: api.Bundle, project: api.Project) -> api.UUID4:
        return bundle.uuid

    def get_aggregator(self, entity_type):
        if entity_type == 'files':
            return None
        else:
            return super().get_aggregator(entity_type)

    def entity_type(self) -> str:
        return 'bundles'
