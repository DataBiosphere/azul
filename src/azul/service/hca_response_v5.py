import abc
from itertools import (
    permutations,
    product,
)
import logging
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    TypeVar,
    TypedDict,
    Union,
    cast,
)

from more_itertools import (
    one,
)

from azul.plugins.metadata.hca.contributor_matrices import (
    make_stratification_tree,
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


class ValueAndUnitObj(TypedDict):
    value: str
    unit: str


class TermObj(TypedDict):
    count: int
    term: Union[str, ValueAndUnitObj, None]


class ProjectTermObj(TermObj):
    projectId: List[str]


class FacetObj(TypedDict):
    terms: List[TermObj]
    total: int
    # FIXME: Remove type from termsFacets in /index responses
    #        https://github.com/DataBiosphere/azul/issues/2460
    type: str


class PaginationObj(TypedDict):
    count: int
    total: int
    size: int
    pages: int
    next: Optional[str]
    previous: Optional[str]
    sort: str
    order: str


class FileTypeSummary(TypedDict):
    format: str
    count: int
    totalSize: float
    matrixCellCount: float


class FileTypeSummaryForHit(FileTypeSummary):
    fileSource: List[Optional[str]]
    isIntermediate: bool
    contentDescription: List[Optional[str]]


class OrganCellCountSummary(TypedDict):
    organType: List[Optional[str]]
    countOfDocsWithOrganType: int
    totalCellCountByOrgan: float


class HitEntry(TypedDict):
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


class CompleteHitEntry(HitEntry):
    bundles: JSONs
    files: JSONs


class SummarizedHitEntry(HitEntry):
    fileTypeSummaries: List[FileTypeSummary]


class ApiResponse(TypedDict):
    hits: List[Union[SummarizedHitEntry, CompleteHitEntry]]
    pagination: PaginationObj
    termFacets: Dict[str, FacetObj]


class SummaryRepresentation(TypedDict):
    projectCount: int
    specimenCount: int
    speciesCount: int
    fileCount: int
    totalFileSize: float
    donorCount: int
    labCount: int
    organTypes: List[str]
    fileTypeSummaries: List[FileTypeSummary]
    cellCountSummaries: List[OrganCellCountSummary]
    projects: List[dict]


class AbstractResponse(object, metaclass=abc.ABCMeta):
    """
    Abstract class to be used for each /files API response.
    """

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError


T = TypeVar('T')


class SummaryResponse(AbstractResponse):

    def __init__(self, aggregations):
        super().__init__()
        self.aggregations = aggregations

    def return_response(self):
        def agg_value(*path: str) -> AnyJSON:
            agg = self.aggregations
            for name in path:
                agg = agg[name]
            return agg

        def agg_values(function: Callable[[JSON], T], *path: str) -> List[T]:
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

        return SummaryRepresentation(projectCount=agg_value('project', 'doc_count'),
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


class SearchResponse(AbstractResponse):

    def return_response(self):
        return self.apiResponse

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

    def map_entries(self, entry):
        """
        Returns a HitEntry Object. Creates a single HitEntry object.
        :param entry: A dictionary corresponding to a single hit from
        ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """
        hit = HitEntry(protocols=self.make_protocols(entry),
                       entryId=entry['entity_id'],
                       sources=self.make_sources(entry),
                       projects=self.make_projects(entry),
                       samples=self.make_samples(entry),
                       specimens=self.make_specimens(entry),
                       cellLines=self.make_cell_lines(entry),
                       donorOrganisms=self.make_donors(entry),
                       organoids=self.make_organoids(entry),
                       cellSuspensions=self.make_cell_suspensions(entry),
                       dates=self.make_dates(entry))
        if self.entity_type in ('files', 'bundles'):
            hit = cast(CompleteHitEntry, hit)
            hit['bundles'] = self.make_bundles(entry)
            hit['files'] = self.make_files(entry)
        else:
            hit = cast(SummarizedHitEntry, hit)

            def file_type_summary(aggregate_file: JSON) -> FileTypeSummaryForHit:
                summary = FileTypeSummaryForHit(
                    count=aggregate_file['count'],
                    fileSource=cast(List, aggregate_file['file_source']),
                    totalSize=aggregate_file['size'],
                    matrixCellCount=aggregate_file['matrix_cell_count'],
                    format=aggregate_file['file_format'],
                    isIntermediate=aggregate_file['is_intermediate'],
                    contentDescription=cast(List, aggregate_file['content_description'])
                )
                assert isinstance(summary['format'], str), type(str)
                assert summary['format']
                return summary

            hit['fileTypeSummaries'] = [
                file_type_summary(aggregate_file)
                for aggregate_file in entry['contents']['files']
            ]
        return hit

    @staticmethod
    def create_facet(contents) -> FacetObj:
        """
        This function creates a FacetObj. It takes in the contents of a
        particular aggregate from ElasticSearch with the format
        '
        {
          "doc_count": 2,
          "myTerms": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
              {
                "key": "spinnaker:1.0.2",
                "doc_count": 2
              }
            ]
          }
        }
        '
        :param contents: A dictionary from a particular ElasticSearch aggregate
        :return: A FacetObj constructed out of the ElasticSearch aggregate
        """

        # HACK
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

        terms: List[TermObj] = []
        for bucket in contents['myTerms']['buckets']:
            term = TermObj(term=choose_entry(bucket),
                           count=bucket['doc_count'])
            try:
                sub_agg = bucket['myProjectIds']
            except KeyError:
                pass
            else:
                project_ids = [sub_bucket['key'] for sub_bucket in sub_agg['buckets']]
                term = cast(ProjectTermObj, term)
                term['projectId'] = project_ids
            terms.append(term)

        untagged_count = contents['untagged']['doc_count']

        # Add the untagged_count to the existing termObj for a None value, or add a new one
        if untagged_count > 0:
            for term in terms:
                if term['term'] is None:
                    term['count'] += untagged_count
                    untagged_count = 0
                    break
        if untagged_count > 0:
            terms.append(TermObj(term=None, count=untagged_count))

        facet = FacetObj(
            terms=terms,
            total=0 if len(
                contents['myTerms']['buckets']
            ) == 0 else contents['doc_count'],
            # FIXME: Remove type from termsFacets in /index responses
            #        https://github.com/DataBiosphere/azul/issues/2460
            type='terms'  # Change once we on-board more types of contents.
        )
        return facet

    @classmethod
    def add_facets(cls, facets_response):
        """
        This function takes the 'aggregations' dictionary from ElasticSearch
        Processes the aggregates and creates a dictionary of FacetObj
        :param facets_response: Facets response dictionary from ElasticSearch
        :return: A dictionary containing the FacetObj
        """
        facets = {}
        for facet, contents in facets_response.items():
            if facet != '_project_agg':  # Filter out project specific aggs
                facets[facet] = cls.create_facet(contents)
        return facets

    def __init__(self, hits, pagination: PaginationObj, facets, entity_type, catalog):
        """
        Constructs the object and initializes the apiResponse attribute

        :param hits: A list of hits from ElasticSearch
        :param pagination: A dict with pagination properties
        :param facets: The aggregations from the ElasticSearch response
        :param entity_type: The entity type used to get the ElasticSearch index
        :param catalog: The catalog searched against to produce the hits
        """
        self.entity_type = entity_type
        self.catalog = catalog
        self.apiResponse = ApiResponse(pagination=pagination,
                                       termFacets=self.add_facets(facets),
                                       hits=[self.map_entries(x) for x in hits])
