import abc
from collections import (
    ChainMap,
)
import logging
from typing import (
    Callable,
    List,
    TypeVar,
)

from jsonobject.api import (
    JsonObject,
)
from jsonobject.exceptions import (
    BadValueError,
)
from jsonobject.properties import (
    FloatProperty,
    IntegerProperty,
    ListProperty,
    ObjectProperty,
    StringProperty,
)
from more_itertools import (
    one,
)

from azul.plugins.metadata.hca.contributor_matrices import (
    make_stratification_tree,
)
from azul.service.utilities import (
    json_pp,
)
from azul.strings import (
    to_camel_case,
)
from azul.types import (
    JSON,
)

logger = logging.getLogger(__name__)


class AbstractTermObj(JsonObject):
    count = IntegerProperty()


class TermObj(AbstractTermObj):
    term = StringProperty()


class ValueAndUnitObj(JsonObject):
    value = StringProperty()
    unit = StringProperty()


class MeasuredTermObj(AbstractTermObj):
    term = ValueAndUnitObj()


class FacetObj(JsonObject):
    terms = ListProperty(AbstractTermObj)
    total = IntegerProperty()
    _type = StringProperty(name='type')


class PaginationObj(JsonObject):
    count = IntegerProperty()
    total = IntegerProperty()
    size = IntegerProperty()
    next = StringProperty()
    previous = StringProperty()
    sort = StringProperty()
    order = StringProperty(choices=['asc', 'desc'])


class FileTypeSummary(JsonObject):
    fileType = StringProperty()
    count = IntegerProperty()
    totalSize = IntegerProperty()

    @classmethod
    def for_bucket(cls, bucket: JSON) -> 'FileTypeSummary':
        self = cls()
        self.count = bucket['doc_count']
        self.totalSize = int(bucket['size_by_type']['value'])  # Casting to integer since ES returns a double
        self.fileType = bucket['key']
        return self

    @classmethod
    def for_aggregate(cls, aggregate_file: JSON) -> 'FileTypeSummary':
        self = cls()
        self.count = aggregate_file['count']
        self.totalSize = aggregate_file['size']
        self.fileType = aggregate_file['file_format']
        assert isinstance(self.fileType, str)
        assert len(self.fileType)
        return self


class OrganCellCountSummary(JsonObject):
    organType = ListProperty()  # List could have strings and/or None (eg. ['Brain', 'Skin', None])
    countOfDocsWithOrganType = IntegerProperty()
    totalCellCountByOrgan = FloatProperty()

    @classmethod
    def for_bucket(cls, bucket: JSON) -> 'OrganCellCountSummary':
        self = cls()
        self.organType = [bucket['key']]
        self.countOfDocsWithOrganType = bucket['doc_count']
        self.totalCellCountByOrgan = bucket['cell_count']['value']
        return self


class OrganType:

    @classmethod
    def for_bucket(cls, bucket: JSON):
        return bucket['key']


class HitEntry(JsonObject):
    pass


class ApiResponse(JsonObject):
    hits = ListProperty(HitEntry)
    pagination = ObjectProperty(
        PaginationObj, exclude_if_none=True, default=None)
    # termFacets = DictProperty(FacetObj, exclude_if_none=True)


class SummaryRepresentation(JsonObject):
    projectCount = IntegerProperty()
    specimenCount = IntegerProperty()
    speciesCount = IntegerProperty()
    fileCount = IntegerProperty()
    totalFileSize = FloatProperty()
    donorCount = IntegerProperty()
    labCount = IntegerProperty()
    totalCellCount = FloatProperty()
    organTypes = ListProperty(StringProperty(required=False))
    fileTypeSummaries = ListProperty(FileTypeSummary)
    cellCountSummaries = ListProperty(OrganCellCountSummary)


class FileIdAutoCompleteEntry(JsonObject):
    _id = StringProperty(name='id')
    dataType = StringProperty()
    donorId = ListProperty(StringProperty)
    fileBundleId = StringProperty()
    fileName = ListProperty(StringProperty)
    projectCode = ListProperty(StringProperty)
    _type = StringProperty(name='type', default='file')


class AutoCompleteRepresentation(JsonObject):
    hits = ListProperty()
    pagination = ObjectProperty(
        PaginationObj,
        exclude_if_none=True,
        default=None)


class AbstractResponse(object, metaclass=abc.ABCMeta):
    """
    Abstract class to be used for each /files API response.
    """

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError


class EntryFetcher:
    """
    Helper class containing helper methods
    """

    @staticmethod
    def fetch_entry_value(mapping, entry, key):
        """
        Helper method for getting the value of key on the mapping
        :param mapping: Mapping in question. Values should be at
        the root level
        :param entry: Dictionary where the contents are to be looking for in
        :param key: Key to be used to get the right value
        :return: Returns entry[mapping[key]] if present. Other
        """
        m = mapping[key]
        if m is not None:
            if isinstance(m, list):
                return entry[m[0]] if m[0] is not None else None
            else:
                _entry = entry[m] if m in entry else None
                _entry = _entry[0] if isinstance(
                    _entry, list) and len(_entry) == 1 else _entry
                return _entry
        else:
            return None

    @staticmethod
    def handle_list(value):
        return [value] if value is not None else []


T = TypeVar('T')


class SummaryResponse(AbstractResponse):

    def __init__(self, raw_response):
        super().__init__()
        self.aggregations = raw_response['aggregations']

    def return_response(self):
        def agg_value(path: str) -> JSON:
            agg = self.aggregations
            for name in path.split('.'):
                agg = agg[name]
            return agg

        def agg_values(path: str, function: Callable[[JSON], T]) -> List[T]:
            return list(map(function, agg_value(path)))

        return SummaryRepresentation(
            projectCount=agg_value('projectCount.value'),
            specimenCount=agg_value('specimenCount.value'),
            speciesCount=agg_value('speciesCount.value'),
            fileCount=agg_value('fileCount.value'),
            totalFileSize=agg_value('total_size.value'),
            donorCount=agg_value('donorCount.value'),
            labCount=agg_value('labCount.value'),
            totalCellCount=agg_value('total_cell_count.value'),
            organTypes=agg_values('organTypes.buckets', OrganType.for_bucket),
            fileTypeSummaries=agg_values('fileFormat.myTerms.buckets', FileTypeSummary.for_bucket),
            cellCountSummaries=agg_values('group_by_organ.buckets', OrganCellCountSummary.for_bucket)
        )


class KeywordSearchResponse(AbstractResponse, EntryFetcher):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """

    def return_response(self):
        return self.apiResponse

    def make_bundles(self, entry):
        return [{"bundleUuid": b["uuid"], "bundleVersion": b["version"]} for b in entry["bundles"]]

    def make_protocols(self, entry):
        return [
            *(
                {'workflow': p.get('workflow', None)}
                for p in entry['contents']['analysis_protocols']
            ),
            *(
                {'assayType': p.get('assay_type', None)}
                for p in entry['contents']['imaging_protocols']
            ),
            *(
                {
                    'libraryConstructionApproach': p.get('library_construction_approach', None),
                    'nucleicAcidSource': p.get('nucleic_acid_source', None)

                }
                for p in entry['contents']['library_preparation_protocols']),
            *(
                {
                    'instrumentManufacturerModel': p.get('instrument_manufacturer_model', None),
                    'pairedEnd': p.get('paired_end', None),
                } for p in entry['contents']['sequencing_protocols']
            )
        ]

    def make_projects(self, entry):
        projects = []
        contents = entry['contents']
        for project in contents["projects"]:
            translated_project = {
                "projectTitle": project.get("project_title"),
                "projectShortname": project["project_short_name"],
                "laboratory": sorted(set(project.get("laboratory", [None])))
            }
            if self.entity_type == 'projects':
                translated_project['projectDescription'] = project.get('project_description', [])
                translated_project['contributors'] = project.get('contributors', [])  # list of dict
                translated_project['publications'] = project.get('publications', [])  # list of dict
                for contributor in translated_project['contributors']:
                    for key in list(contributor.keys()):
                        contributor[to_camel_case(key)] = contributor.pop(key)
                for publication in translated_project['publications']:
                    for key in list(publication.keys()):
                        publication[to_camel_case(key)] = publication.pop(key)
                translated_project['arrayExpressAccessions'] = project.get('array_express_accessions', [None])
                translated_project['geoSeriesAccessions'] = project.get('geo_series_accessions', [None])
                translated_project['insdcProjectAccessions'] = project.get('insdc_project_accessions', [None])
                translated_project['insdcStudyAccessions'] = project.get('insdc_study_accessions', [None])
                translated_project['supplementaryLinks'] = project.get('supplementary_links', [None])
                translated_project['matrices'] = self.make_matrices_(contents['matrices'])
                translated_project['contributorMatrices'] = self.make_matrices_(contents['contributor_matrices'])
            projects.append(translated_project)
        return projects

    # FIXME: Move this to during aggregation
    #        https://github.com/DataBiosphere/azul/issues/2415

    def make_matrices_(self, matrices: JSON) -> JSON:
        files = one(matrices)['file'] if matrices else []
        return make_stratification_tree(files)

    def make_files(self, entry):
        files = []
        for _file in entry["contents"]["files"]:
            translated_file = {
                "content_description": _file.get("content_description"),
                "format": _file.get("file_format"),
                "name": _file.get("name"),
                "sha256": _file.get("sha256"),
                "size": _file.get("size"),
                "source": _file.get("source"),
                "uuid": _file.get("uuid"),
                "version": _file.get("version"),
            }
            files.append(translated_file)
        return files

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

    def make_sample(self, sample):
        lookup = {
            'cell_lines': ('cellLines', self.make_cell_line),
            'organoids': ('organoids', self.make_organoid),
            'specimens': ('specimens', self.make_specimen),
        }
        entity_type = sample['entity_type']
        if isinstance(entity_type, list):
            entity_type, make_functions = map(list, zip(*map(lookup.get, entity_type)))
            entity_dicts = (make_function(sample) for make_function in make_functions)
            entity_dict = ChainMap(*entity_dicts)
        else:
            entity_type, make_function = lookup[entity_type]
            entity_dict = make_function(sample)
        return {
            'sampleEntityType': entity_type,
            'effectiveOrgan': sample['effective_organ'],
            **entity_dict
        }

    def make_samples(self, entry):
        return [self.make_sample(sample) for sample in entry["contents"]["samples"]]

    def map_entries(self, entry):
        """
        Returns a HitEntry Object. Creates a single HitEntry object.
        :param entry: A dictionary corresponding to a single hit from
        ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """
        kwargs = {
            'bundles': self.make_bundles(entry),
            'files': self.make_files(entry)
        } if self.entity_type in ('files', 'bundles') else {
            'fileTypeSummaries': [FileTypeSummary.for_aggregate(aggregate_file).to_json()
                                  for aggregate_file in entry["contents"]["files"]]
        }
        return HitEntry(protocols=self.make_protocols(entry),
                        entryId=entry["entity_id"],
                        projects=self.make_projects(entry),
                        samples=self.make_samples(entry),
                        specimens=self.make_specimens(entry),
                        cellLines=self.make_cell_lines(entry),
                        donorOrganisms=self.make_donors(entry),
                        organoids=self.make_organoids(entry),
                        cellSuspensions=self.make_cell_suspensions(entry),
                        **kwargs)

    def __init__(self, hits, entity_type, catalog):
        """
        Constructs the object and initializes the apiResponse attribute

        :param hits: A list of hits from ElasticSearch
        :param entity_type: The entity type used to get the ElasticSearch index
        :param catalog: The catalog searched against to produce the hits
        """
        self.entity_type = entity_type
        self.catalog = catalog
        # TODO: This is actually wrong. The Response from a single fileId call
        # isn't under hits. It is actually not wrapped under anything
        super(KeywordSearchResponse, self).__init__()
        class_entries = {'hits': [
            self.map_entries(x) for x in hits], 'pagination': None}
        self.apiResponse = ApiResponse(**class_entries)


class FileSearchResponse(KeywordSearchResponse):
    """
    Class for the file search response. Inherits from KeywordSearchResponse
    """

    @staticmethod
    def create_facet(contents):
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

        term_list = []
        for term in contents['myTerms']['buckets']:
            term_object_params = {'term': choose_entry(term), 'count': term['doc_count']}
            if 'myProjectIds' in term:
                term_object_params['projectId'] = [bucket['key'] for bucket in term['myProjectIds']['buckets']]
            try:
                term_list.append(TermObj(**term_object_params))
            except BadValueError:
                # BadValueError is raised by the TermObj constructor if the
                # input doesn't have the required shape. If that is the case,
                # we try MeasuredTermObj instead.
                term_list.append(MeasuredTermObj(**term_object_params))

        untagged_count = contents['untagged']['doc_count']

        # Add the untagged_count to the existing termObj for a None value, or add a new one
        if untagged_count > 0:
            for term_obj in term_list:
                if term_obj.term is None:
                    term_obj.count += untagged_count
                    untagged_count = 0
                    break
        if untagged_count > 0:
            term_list.append(TermObj(term=None, count=untagged_count))

        facet = FacetObj(
            terms=term_list,
            total=0 if len(
                contents['myTerms']['buckets']
            ) == 0 else contents['doc_count'],
            # FIXME: consider removing `type` from API responses
            #        https://github.com/DataBiosphere/azul/issues/2460
            type='terms'  # Change once we on-board more types of contents.
        )
        return facet.to_json()

    @staticmethod
    def add_facets(facets_response):
        """
        This function takes the 'aggregations' dictionary from ElasticSearch
        Processes the aggregates and creates a dictionary of FacetObj
        :param facets_response: Facets response dictionary from ElasticSearch
        :return: A dictionary containing the FacetObj
        """
        facets = {}
        for facet, contents in facets_response.items():
            if facet != '_project_agg':  # Filter out project specific aggs
                facets[facet] = FileSearchResponse.create_facet(contents)
        return facets

    def __init__(self, hits, pagination, facets, entity_type, catalog):
        """
        Constructs the object and initializes the apiResponse attribute

        :param hits: A list of hits from ElasticSearch
        :param pagination: A dict with pagination properties
        :param facets: The aggregations from the ElasticSearch response
        :param entity_type: The entity type used to get the ElasticSearch index
        :param catalog: The catalog searched against to produce the hits
        """
        # This should initialize the self.apiResponse attribute of the object
        KeywordSearchResponse.__init__(self, hits, entity_type, catalog)
        # Add the paging via **kwargs of dictionary 'pagination'
        self.apiResponse.pagination = PaginationObj(**pagination)
        # Add the facets
        self.apiResponse.termFacets = self.add_facets(facets)


class AutoCompleteResponse(EntryFetcher):

    def map_entries(self, mapping, entry, _type='file'):
        """
        Returns a HitEntry Object. Takes the mapping and maps the appropriate
        fields from entry to the corresponding entry in the mapping
        :param mapping: Takes in a Json object with the mapping to the
        corresponding field in the entry object
        :param entry: A 1 dimensional dictionary corresponding to a single
        hit from ElasticSearch
        :param _type: The type of entry that will be used when constructing
        the entry
        :return: A HitEntry Object with the appropriate fields mapped
        """
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Entry to be mapped: \n%s', json_pp(entry))
        mapped_entry = {}
        if _type == 'file':
            # Create a file representation
            mapped_entry = FileIdAutoCompleteEntry(
                _id=self.fetch_entry_value(mapping, entry, 'id'),
                dataType=self.fetch_entry_value(mapping, entry, 'dataType'),
                donorId=self.handle_list(self.fetch_entry_value(
                    mapping, entry, 'donorId')),
                fileBundleId=self.fetch_entry_value(
                    mapping, entry, 'fileBundleId'),
                fileName=self.handle_list(self.fetch_entry_value(
                    mapping, entry, 'fileName')),
                projectCode=self.handle_list(self.fetch_entry_value(
                    mapping, entry, 'projectCode')),
                _type='file'
            )

        return mapped_entry.to_json()

    def __init__(self, mapping, hits, pagination, _type):
        """
        Constructs the object and initializes the apiResponse attribute
        :param mapping: A JSON with the mapping for the field
        :param hits: A list of hits from ElasticSearch
        """
        # Overriding the __init__ method of the parent class
        EntryFetcher.__init__(self)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Mapping: \n%s', json_pp(mapping))
        class_entries = {'hits': [self.map_entries(
            mapping, x, _type) for x in hits], 'pagination': None}
        self.apiResponse = AutoCompleteRepresentation(**class_entries)
        # Add the paging via **kwargs of dictionary 'pagination'
        self.apiResponse.pagination = PaginationObj(**pagination)
