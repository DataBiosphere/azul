from collections.abc import (
    Mapping,
)
from itertools import (
    permutations,
    product,
)
import logging
from typing import (
    Callable,
    Optional,
    Sequence,
    TypeVar,
    TypedDict,
    Union,
    cast,
)

from more_itertools import (
    one,
)

from azul import (
    CatalogName,
)
from azul.plugins.metadata.hca.service.contributor_matrices import (
    make_stratification_tree,
)
from azul.service.elasticsearch_service import (
    ResponsePagination,
    ResponseTriple,
)
from azul.service.repository_service import (
    SearchResponseStage,
    SummaryResponseStage,
)
from azul.strings import (
    to_camel_case,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
)

logger = logging.getLogger(__name__)


class ValueAndUnit(TypedDict):
    value: str
    unit: str


class Term(TypedDict):
    count: int
    term: Union[str, ValueAndUnit, None]


class ProjectTerm(Term):
    projectId: list[str]


class Terms(TypedDict):
    terms: list[Term]
    total: int
    # FIXME: Remove type from termsFacets in /index responses
    #        https://github.com/DataBiosphere/azul/issues/2460
    type: str


class FileTypeSummary(TypedDict):
    format: str
    count: int
    totalSize: float
    matrixCellCount: float


class FileTypeSummaryForHit(FileTypeSummary):
    fileSource: list[Optional[str]]
    isIntermediate: bool
    contentDescription: list[Optional[str]]


class OrganCellCountSummary(TypedDict):
    organType: list[Optional[str]]
    countOfDocsWithOrganType: int
    totalCellCountByOrgan: float


class Hit(TypedDict):
    protocols: JSONs
    entryId: str
    sources: JSONs
    projects: JSONs
    samples: JSONs
    specimens: JSONs
    cellLines: JSONs
    donorOrganisms: JSONs
    organoids: JSONs
    cellSuspensions: JSONs
    dates: JSONs


class CompleteHit(Hit):
    bundles: JSONs
    files: JSONs


class SummarizedHit(Hit):
    fileTypeSummaries: list[FileTypeSummary]


class SearchResponse(TypedDict):
    hits: list[Union[SummarizedHit, CompleteHit]]
    pagination: ResponsePagination
    termFacets: dict[str, Terms]


class SummaryResponse(TypedDict):
    projectCount: int
    specimenCount: int
    speciesCount: int
    fileCount: int
    totalFileSize: float
    donorCount: int
    labCount: int
    organTypes: list[str]
    fileTypeSummaries: list[FileTypeSummary]
    cellCountSummaries: list[OrganCellCountSummary]
    projects: JSONs


class HCASummaryResponseStage(SummaryResponseStage):

    @property
    def aggs_by_authority(self) -> Mapping[str, Sequence[str]]:
        return {
            'files': [
                'totalFileSize',
                'fileFormat',
            ],
            'samples': [
                'organTypes',
                'donorCount',
                'specimenCount',
                'speciesCount'
            ],
            'projects': [
                'project',
                'labCount',
                'cellSuspensionCellCount',
                'projectCellCount',
            ],
            'cell_suspensions': [
                'cellCountSummaries',
            ]
        }

    def process_response(self, response: JSON) -> SummaryResponse:
        factory = SummaryResponseFactory(response)
        response = factory.make_response()
        self._validate_response(cast(JSON, response))
        return response

    def _validate_response(self, response: JSON):
        for field, summary_field in (
            ('totalFileSize', 'totalSize'),
            ('fileCount', 'count')
        ):
            total = response[field]
            summaries = cast(JSONs, response['fileTypeSummaries'])
            summary_total = sum(summary[summary_field] for summary in summaries)
            assert total == summary_total, (total, summary_total)


T = TypeVar('T')


# FIXME: Merge into HCASummaryResponseStage
#        https://github.com/DataBiosphere/azul/issues/4135

class SummaryResponseFactory:

    def __init__(self, aggs: JSON):
        super().__init__()
        self.aggs = aggs

    def make_response(self):
        def agg_value(*path: str) -> AnyJSON:
            agg = self.aggs
            for name in path:
                agg = agg[name]
            return agg

        def agg_values(function: Callable[[JSON], T], *path: str) -> list[T]:
            values = agg_value(*path)
            assert isinstance(values, list)
            return list(map(function, values))

        bools = [False, True]
        cell_counts = {
            child: {
                (parent, present): agg_value(parent + 'CellCount',
                                             'buckets',
                                             'hasSome' if present else 'hasNone',
                                             child + 'CellCount',
                                             'value')
                for present in bools
            }
            for parent, child in permutations(['project', 'cellSuspension'])
        }

        def file_type_summary(bucket: JSON) -> FileTypeSummary:
            return FileTypeSummary(
                count=bucket['doc_count'],
                totalSize=bucket['size_by_type']['value'],
                matrixCellCount=bucket['matrix_cell_count_by_type']['value'],
                format=bucket['key']
            )

        def organ_cell_count_summary(bucket: JSON) -> OrganCellCountSummary:
            return OrganCellCountSummary(
                organType=[bucket['key']],
                countOfDocsWithOrganType=bucket['doc_count'],
                totalCellCountByOrgan=bucket['cellCount']['value']
            )

        def organ_type(bucket: JSON) -> str:
            return bucket['key']

        return SummaryResponse(projectCount=agg_value('project', 'doc_count'),
                               specimenCount=agg_value('specimenCount', 'value'),
                               speciesCount=agg_value('speciesCount', 'value'),
                               fileCount=agg_value('fileFormat', 'doc_count'),
                               totalFileSize=agg_value('totalFileSize', 'value'),
                               donorCount=agg_value('donorCount', 'value'),
                               labCount=agg_value('labCount', 'value'),
                               organTypes=agg_values(organ_type, 'organTypes', 'buckets'),
                               fileTypeSummaries=agg_values(file_type_summary,
                                                            'fileFormat',
                                                            'myTerms',
                                                            'buckets'),
                               cellCountSummaries=agg_values(organ_cell_count_summary,
                                                             'cellCountSummaries',
                                                             'buckets'),
                               projects=[
                                   {
                                       'projects': {
                                           'estimatedCellCount': (
                                               cell_counts['project']['cellSuspension', project_present]
                                               if cs_present else None
                                           )
                                       },
                                       'cellSuspensions': {
                                           'totalCells': (
                                               cell_counts['cellSuspension']['project', cs_present]
                                               if project_present else None
                                           )
                                       }
                                   }
                                   for project_present, cs_present in product(bools, bools)
                                   if project_present or cs_present
                               ])


class HCASearchResponseStage(SearchResponseStage):

    def process_response(self, response: ResponseTriple) -> SearchResponse:
        hits, pagination, aggs = response
        factory = SearchResponseFactory(hits=hits,
                                        pagination=pagination,
                                        aggs=aggs,
                                        entity_type=self.entity_type,
                                        catalog=self.catalog)
        return factory.make_response()


# FIXME: Merge into HCASearchResponseStage
#        https://github.com/DataBiosphere/azul/issues/4135

class SearchResponseFactory:

    def __init__(self,
                 *,
                 hits: JSONs,
                 pagination: ResponsePagination,
                 aggs: JSON,
                 entity_type: str,
                 catalog: CatalogName):
        super().__init__()
        self.hits = hits
        self.pagination = pagination
        self.aggs = aggs
        self.entity_type = entity_type
        self.catalog = catalog

    def make_response(self):
        return SearchResponse(pagination=self.pagination,
                              termFacets=self.make_facets(),
                              hits=self.make_hits())

    def make_bundles(self, entry):
        return [
            {"bundleUuid": b["uuid"], "bundleVersion": b["version"]}
            for b in entry["bundles"]
        ]

    def make_sources(self, entry):
        return [
            {'sourceId': s['id'], 'sourceSpec': s['spec']}
            for s in entry['sources']
        ]

    def make_protocols(self, entry):
        return [
            *(
                {
                    'workflow': p.get('workflow', None),
                }
                for p in entry['contents']['analysis_protocols']
            ),
            *(
                {
                    'assayType': p.get('assay_type', None),
                }
                for p in entry['contents']['imaging_protocols']
            ),
            *(
                {
                    'libraryConstructionApproach': p.get('library_construction_approach', None),
                    'nucleicAcidSource': p.get('nucleic_acid_source', None),
                }
                for p in entry['contents']['library_preparation_protocols']),
            *(
                {
                    'instrumentManufacturerModel': p.get('instrument_manufacturer_model', None),
                    'pairedEnd': p.get('paired_end', None),
                }
                for p in entry['contents']['sequencing_protocols']
            )
        ]

    def make_dates(self, entry):
        return [
            {
                'aggregateLastModifiedDate': dates['aggregate_last_modified_date'],
                'aggregateSubmissionDate': dates['aggregate_submission_date'],
                'aggregateUpdateDate': dates['aggregate_update_date'],
                'lastModifiedDate': dates['last_modified_date'],
                'submissionDate': dates['submission_date'],
                'updateDate': dates['update_date'],
            }
            for dates in entry['contents']['dates']
        ]

    def make_projects(self, entry):
        projects = []
        contents = entry['contents']
        for project in contents["projects"]:
            translated_project = {
                'projectId': project['document_id'],
                'projectTitle': project.get('project_title'),
                'projectShortname': project['project_short_name'],
                'laboratory': sorted(set(project.get('laboratory', [None]))),
                'estimatedCellCount': project['estimated_cell_count'],
            }
            if self.entity_type == 'projects':
                translated_project['projectDescription'] = project.get('project_description', [])
                contributors = project.get('contributors', [])  # list of dict
                translated_project['contributors'] = contributors
                publications = project.get('publications', [])  # list of dict
                translated_project['publications'] = publications
                for contributor in contributors:
                    for key in list(contributor.keys()):
                        contributor[to_camel_case(key)] = contributor.pop(key)
                for publication in publications:
                    for key in list(publication.keys()):
                        publication[to_camel_case(key)] = publication.pop(key)
                translated_project['supplementaryLinks'] = project.get('supplementary_links', [None])
                translated_project['matrices'] = self.make_matrices_(contents['matrices'])
                translated_project['contributedAnalyses'] = self.make_matrices_(contents['contributed_analyses'])
                translated_project['accessions'] = project.get('accessions', [None])
            projects.append(translated_project)
        return projects

    # FIXME: Move this to during aggregation
    #        https://github.com/DataBiosphere/azul/issues/2415

    def make_matrices_(self, matrices: JSON) -> JSON:
        files = []
        if matrices:
            for _file in one(matrices)['file']:
                translated_file = {
                    **self.make_translated_file(_file),
                    'strata': _file['strata']
                }
                files.append(translated_file)
        return make_stratification_tree(files)

    def make_files(self, entry):
        files = []
        for _file in entry['contents']['files']:
            translated_file = self.make_translated_file(_file)
            files.append(translated_file)
        return files

    def make_translated_file(self, file):
        translated_file = {
            'contentDescription': file.get('content_description'),
            'format': file.get('file_format'),
            'isIntermediate': file.get('is_intermediate'),
            'name': file.get('name'),
            'sha256': file.get('sha256'),
            'size': file.get('size'),
            'fileSource': file.get('file_source'),
            'uuid': file.get('uuid'),
            'version': file.get('version'),
            'matrixCellCount': file.get('matrix_cell_count'),
            'url': None,  # to be injected later in post-processing
        }
        return translated_file

    def make_specimen(self, specimen):
        return {
            "id": specimen["biomaterial_id"],
            "organ": specimen.get("organ", None),
            "organPart": specimen.get("organ_part", None),
            "disease": specimen.get("disease", None),
            "preservationMethod": specimen.get("preservation_method", None),
            "source": specimen.get("_source", None)
        }

    def make_specimens(self, entry):
        return [self.make_specimen(specimen) for specimen in entry["contents"]["specimens"]]

    def make_cell_suspension(self, cell_suspension):
        return {
            "organ": cell_suspension.get("organ", None),
            "organPart": cell_suspension.get("organ_part", None),
            "selectedCellType": cell_suspension.get("selected_cell_type", None),
            "totalCells": cell_suspension.get("total_estimated_cells", None)
        }

    def make_cell_suspensions(self, entry):
        return [self.make_cell_suspension(cs) for cs in entry["contents"]["cell_suspensions"]]

    def make_cell_line(self, cell_line):
        return {
            "id": cell_line["biomaterial_id"],
            "cellLineType": cell_line.get("cell_line_type", None),
            "modelOrgan": cell_line.get("model_organ", None),
        }

    def make_cell_lines(self, entry):
        return [self.make_cell_line(cell_line) for cell_line in entry["contents"]["cell_lines"]]

    def make_donor(self, donor):
        return {
            "id": donor["biomaterial_id"],
            "donorCount": donor.get("donor_count", None),
            "developmentStage": donor.get("development_stage", None),
            "genusSpecies": donor.get("genus_species", None),
            "organismAge": donor.get("organism_age", None),
            "organismAgeRange": donor.get("organism_age_range", None),  # list of dict
            "biologicalSex": donor.get("biological_sex", None),
            "disease": donor.get("diseases", None)
        }

    def make_donors(self, entry):
        return [self.make_donor(donor) for donor in entry["contents"]["donors"]]

    def make_organoid(self, organoid):
        return {
            "id": organoid["biomaterial_id"],
            "modelOrgan": organoid.get("model_organ", None),
            "modelOrganPart": organoid.get("model_organ_part", None)
        }

    def make_organoids(self, entry):
        return [self.make_organoid(organoid) for organoid in entry["contents"]["organoids"]]

    def make_sample(self, sample, entity_dict, entity_type):
        is_aggregate = isinstance(sample['document_id'], list)
        organ_prop = 'organ' if entity_type == 'specimens' else 'model_organ'
        return {
            'sampleEntityType': [entity_type] if is_aggregate else entity_type,
            'effectiveOrgan': sample[organ_prop],
            **entity_dict
        }

    def make_samples(self, entry):
        pieces = [
            (self.make_cell_line, 'cellLines', 'sample_cell_lines'),
            (self.make_organoid, 'organoids', 'sample_organoids'),
            (self.make_specimen, 'specimens', 'sample_specimens'),
        ]
        return [
            self.make_sample(sample, entity_fn(sample), entity_type)
            for entity_fn, entity_type, sample_entity_type in pieces
            for sample in entry['contents'].get(sample_entity_type, [])
        ]

    def make_hits(self):
        return list(map(self.make_hit, self.hits))

    def make_hit(self, es_hit):
        hit = Hit(protocols=self.make_protocols(es_hit),
                  entryId=es_hit['entity_id'],
                  sources=self.make_sources(es_hit),
                  projects=self.make_projects(es_hit),
                  samples=self.make_samples(es_hit),
                  specimens=self.make_specimens(es_hit),
                  cellLines=self.make_cell_lines(es_hit),
                  donorOrganisms=self.make_donors(es_hit),
                  organoids=self.make_organoids(es_hit),
                  cellSuspensions=self.make_cell_suspensions(es_hit),
                  dates=self.make_dates(es_hit))
        if self.entity_type in ('files', 'bundles'):
            hit = cast(CompleteHit, hit)
            hit['bundles'] = self.make_bundles(es_hit)
            hit['files'] = self.make_files(es_hit)
        else:
            hit = cast(SummarizedHit, hit)

            def file_type_summary(aggregate_file: JSON) -> FileTypeSummaryForHit:
                summary = FileTypeSummaryForHit(
                    count=aggregate_file['count'],
                    fileSource=cast(list, aggregate_file['file_source']),
                    totalSize=aggregate_file['size'],
                    matrixCellCount=aggregate_file['matrix_cell_count'],
                    format=aggregate_file['file_format'],
                    isIntermediate=aggregate_file['is_intermediate'],
                    contentDescription=cast(list, aggregate_file['content_description'])
                )
                assert isinstance(summary['format'], str), type(str)
                # FIXME: Remove workaround
                #        https://github.com/DataBiosphere/azul/issues/4099
                if False:
                    assert summary['format']
                return summary

            hit['fileTypeSummaries'] = [
                file_type_summary(aggregate_file)
                for aggregate_file in es_hit['contents']['files']
            ]
        return hit

    def make_terms(self, agg) -> Terms:
        def choose_entry(_term):
            if 'key_as_string' in _term:
                return _term['key_as_string']
            elif (term_key := _term['key']) is None:
                return None
            elif isinstance(term_key, bool):
                return str(term_key).lower()
            elif isinstance(term_key, dict):
                return term_key
            else:
                return str(term_key)

        terms: list[Term] = []
        for bucket in agg['myTerms']['buckets']:
            term = Term(term=choose_entry(bucket),
                        count=bucket['doc_count'])
            try:
                sub_agg = bucket['myProjectIds']
            except KeyError:
                pass
            else:
                project_ids = [sub_bucket['key'] for sub_bucket in sub_agg['buckets']]
                term = cast(ProjectTerm, term)
                term['projectId'] = project_ids
            terms.append(term)

        untagged_count = agg['untagged']['doc_count']

        # Add the untagged_count to the existing termObj for a None value, or add a new one
        if untagged_count > 0:
            for term in terms:
                if term['term'] is None:
                    term['count'] += untagged_count
                    untagged_count = 0
                    break
        if untagged_count > 0:
            terms.append(Term(term=None, count=untagged_count))

        return Terms(terms=terms,
                     total=0 if len(agg['myTerms']['buckets']) == 0 else agg['doc_count'],
                     # FIXME: Remove type from termsFacets in /index responses
                     #        https://github.com/DataBiosphere/azul/issues/2460
                     type='terms')

    def make_facets(self):
        facets = {}
        for facet, agg in self.aggs.items():
            if facet != '_project_agg':  # Filter out project specific aggs
                facets[facet] = self.make_terms(agg)
        return facets
