#!/usr/bin/python
import abc
from collections import OrderedDict, defaultdict
import csv
from io import StringIO
from itertools import chain
import logging
import os
from uuid import uuid4

from chalice import Response
from jsonobject import (FloatProperty,
                        IntegerProperty,
                        JsonObject,
                        ListProperty,
                        ObjectProperty,
                        StringProperty)

from azul.service.responseobjects.storage_service import StorageService
from azul.service.responseobjects.utilities import json_pp
from azul.json_freeze import freeze, thaw
from azul.strings import to_camel_case
from azul.transformer import SetAccumulator

logger = logging.getLogger(__name__)
module_logger = logger  # FIXME: inline (https://github.com/DataBiosphere/azul/issues/419)


class TermObj(JsonObject):
    """
    Class defining a term object in the FacetObj
    """
    count = IntegerProperty()
    term = StringProperty()


class FacetObj(JsonObject):
    """
    Class defining the facet entry in the ApiResponse object
    """
    terms = ListProperty(TermObj)
    total = IntegerProperty()
    _type = StringProperty(name='type')


class PaginationObj(JsonObject):
    """
    Class defining the pagination attribute in the ApiResponse class
    """
    count = IntegerProperty()
    total = IntegerProperty()
    size = IntegerProperty()
    search_after = StringProperty()
    search_after_uid = StringProperty()
    search_before = StringProperty()
    search_before_uid = StringProperty()
    sort = StringProperty()
    order = StringProperty(choices=['asc', 'desc'])


class FileTypeSummary(JsonObject):
    fileType = StringProperty()
    count = IntegerProperty()
    totalSize = IntegerProperty()

    @classmethod
    def for_bucket(cls, bucket):
        self = cls()
        self.count = bucket['doc_count']
        self.totalSize = int(bucket['size_by_type']['value'])  # Casting to integer since ES returns a double
        self.fileType = bucket['key']
        return self

    @classmethod
    def for_aggregate(cls, aggregate_file):
        self = cls()
        self.count = aggregate_file['count']
        self.totalSize = aggregate_file['size']
        format = aggregate_file['file_format']
        assert isinstance(format, list)
        assert len(format)
        self.fileType = format[0]
        return self


class OrganCellCountSummary(JsonObject):
    organType = StringProperty()
    countOfDocsWithOrganType = IntegerProperty()
    totalCellCountByOrgan = FloatProperty()

    @classmethod
    def for_bucket(cls, bucket):
        self = cls()
        self.organType = bucket['key']
        self.countOfDocsWithOrganType = bucket['doc_count']
        self.totalCellCountByOrgan = bucket['cell_count']['value']
        return self

    @classmethod
    def create_object_from_simple_count(cls, count):
        self = cls()
        self.organType = count['key']
        self.countOfDocsWithOrganType = 1
        self.totalCellCountByOrgan = count['value']
        return self


class HitEntry(JsonObject):
    """
    Class defining a hit entry in the Api response
    """
    pass


class ApiResponse(JsonObject):
    """
    Class defining an API response
    """
    hits = ListProperty(HitEntry)
    pagination = ObjectProperty(
        PaginationObj, exclude_if_none=True, default=None)
    # termFacets = DictProperty(FacetObj, exclude_if_none=True)


class SummaryRepresentation(JsonObject):
    """
    Class defining the Summary Response
    """
    fileCount = IntegerProperty()
    totalFileSize = FloatProperty()
    fileTypeSummaries = ListProperty(FileTypeSummary)
    organSummaries = ListProperty(OrganCellCountSummary)


class ProjectSummaryRepresentation(JsonObject):
    """
    Class defining the Project Summary Response
    """
    totalCellCount = FloatProperty()
    donorCount = IntegerProperty()
    organSummaries = ListProperty(OrganCellCountSummary)
    genusSpecies = ListProperty(StringProperty)
    libraryConstructionApproach = ListProperty(StringProperty)
    disease = ListProperty(StringProperty)


class FileIdAutoCompleteEntry(JsonObject):
    """
    Class defining the File Id Auto Complete Entry
    """
    _id = StringProperty(name='id')
    dataType = StringProperty()
    donorId = ListProperty(StringProperty)
    fileBundleId = StringProperty()
    fileName = ListProperty(StringProperty)
    projectCode = ListProperty(StringProperty)
    _type = StringProperty(name='type', default='file')


class AutoCompleteRepresentation(JsonObject):
    """
    Class defining the Autocomplete Representation
    """
    hits = ListProperty()
    pagination = ObjectProperty(
        PaginationObj,
        exclude_if_none=True,
        default=None)


class AbstractResponse(object):
    """
    Abstract class to be used for each /files API response.
    """
    __metaclass__ = abc.ABCMeta
    DSS_URL = os.getenv("DSS_URL",
                        "https://dss.staging.data.humancellatlas.org/v1")

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError(
            'users must define return_response to use this base class')


class ManifestResponse(AbstractResponse):
    """
    Class for the Manifest response. Based on the AbstractionResponse class
    """

    def _translate(self, untranslated, keyname):
        m = self.manifest_entries[keyname]
        return [untranslated.get(es_name, "") for es_name in m.values()]

    def _construct_tsv_content(self):
        es_search = self.es_search

        output = StringIO()
        writer = csv.writer(output, dialect='excel-tab')

        writer.writerow(list(self.manifest_entries['bundles'].keys()) +
                        list(self.manifest_entries['contents.files'].keys()))
        for hit in es_search.scan():
            hit_dict = hit.to_dict()
            assert len(hit_dict['contents']['files']) == 1
            file = hit_dict['contents']['files'][0]
            file_fields = self._translate(file, 'contents.files')
            for bundle in hit_dict['bundles']:
                # FIXME: If a file is in multiple bundles, the manifest will list it twice. `hca dss download_manifest`
                # would download the file twice (https://github.com/DataBiosphere/azul/issues/423).
                bundle_fields = self._translate(bundle, 'bundles')
                writer.writerow(bundle_fields + file_fields)

        return output.getvalue()

    def return_response(self):
        parameters = dict(object_key=f'manifests/{uuid4()}.tsv',
                          data=self._construct_tsv_content().encode(),
                          content_type='text/tab-separated-values')
        object_key = self.storage_service.put(**parameters)
        presigned_url = self.storage_service.get_presigned_url(object_key)
        headers = {'Content-Type': 'application/json', 'Location': presigned_url}

        return Response(body='', headers=headers, status_code=302)

    def __init__(self, es_search, manifest_entries, mapping):
        """
        The constructor takes the raw response from ElasticSearch and creates
        a csv file based on the columns from the manifest_entries
        :param raw_response: The raw response from ElasticSearch
        :param mapping: The mapping between the columns to values within ES
        :param manifest_entries: The columns that will be present in the tsv
        :param storage_service: The storage service used to store temporary downloadable content
        """
        self.es_search = es_search
        self.manifest_entries = OrderedDict(manifest_entries)
        self.mapping = mapping
        self.storage_service = StorageService()


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

    def __init__(self):
        # Setting up logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.EntryFetcher')


class BaseSummaryResponse(AbstractResponse):
    def return_response(self):
        return self.apiResponse

    @staticmethod
    def agg_contents(aggs_dict, agg_name, agg_form="buckets"):
        """
        Helper function for parsing aggregate dictionary and returning the
        contents of the aggregation
        :param aggs_dict: ES dictionary response containing the aggregates
        :param agg_name: Name of aggregate to inspect
        :param agg_form: Part of the aggregate to return.
        :return: Returns the agg_form within the aggregate agg_name
        """
        # Return the specified content of the aggregate. Otherwise return
        # an empty string
        try:
            contents = aggs_dict[agg_name][agg_form]
            if agg_form == "buckets":
                contents = len(contents)
        except Exception:
            # FIXME: Eliminate this except clause (https://github.com/DataBiosphere/azul/issues/421)
            logger.warning('Exception occurred trying to extract aggregation bucket', exc_info=True)
            contents = -1
        return contents

    def __init__(self, raw_response):
        # Separate the raw_response into hits and aggregates
        self.hits = raw_response['hits']
        self.aggregates = raw_response['aggregations']


class SummaryResponse(BaseSummaryResponse):
    """
    Build response for the summary endpoint
    """

    def __init__(self, raw_response):
        super().__init__(raw_response)

        _sum = raw_response['aggregations']['fileFormat']["myTerms"]
        _organ_group = raw_response['aggregations']['group_by_organ']

        # Create a SummaryRepresentation object
        kwargs = dict(
            projectCount=self.agg_contents(self.aggregates, 'projectCount', agg_form='value'),
            totalFileSize=self.agg_contents(self.aggregates, 'total_size', agg_form='value'),
            specimenCount=self.agg_contents(self.aggregates, 'specimenCount', agg_form='value'),
            fileCount=self.agg_contents(self.aggregates, 'fileCount', agg_form='value'),
            organCount=self.agg_contents(self.aggregates, 'organCount', agg_form='value'),
            donorCount=self.agg_contents(self.aggregates, 'donorCount', agg_form='value'),
            labCount=self.agg_contents(self.aggregates, 'labCount', agg_form='value'),
            totalCellCount=self.agg_contents(self.aggregates, 'total_cell_count', agg_form='value'),
            fileTypeSummaries=[FileTypeSummary.for_bucket(bucket) for bucket in _sum['buckets']],
            organSummaries=[OrganCellCountSummary.for_bucket(bucket) for bucket in _organ_group['buckets']])

        self.apiResponse = SummaryRepresentation(**kwargs)


class ProjectSummaryResponse(AbstractResponse):
    """
    Build summary field for each project in projects endpoint
    """
    def return_response(self):
        return self.apiResponse

    @classmethod
    def get_cell_count(cls, hit):
        """
        Iterate through cell suspensions to get overall and per organ cell count. Expects cell suspensions to already
        be grouped and aggregated by organ.

        :param hit: Project document hit from ES response
        :return: tuple where total_cell_count is the number of cells in the project and organ_cell_count is a dict
            where the key is an organ and the value is the number of cells for the organ in the project
        """
        organ_cell_count = defaultdict(int)
        for cell_suspension in hit['cell_suspensions']:
            assert len(cell_suspension['organ']) == 1
            organ_cell_count[cell_suspension['organ'][0]] += cell_suspension.get('total_estimated_cells', 0)
        total_cell_count = sum(organ_cell_count.values())
        organ_cell_count = [{'key': k, 'value': v} for k, v in organ_cell_count.items()]
        return total_cell_count, organ_cell_count

    def __init__(self, es_hit_contents):
        specimen_accumulators = {
            'donor_biomaterial_id': SetAccumulator(),
            'genus_species': SetAccumulator(),
            'disease': SetAccumulator()
        }
        for specimen in es_hit_contents['specimens']:
            for property_name, accumulator in specimen_accumulators.items():
                if property_name in specimen:
                    accumulator.accumulate(specimen[property_name])

        library_accumulator = SetAccumulator()
        for process in es_hit_contents['processes']:
            if 'library_construction_approach' in process:
                library_accumulator.accumulate(process['library_construction_approach'])

        total_cell_count, organ_cell_count = self.get_cell_count(es_hit_contents)

        self.apiResponse = ProjectSummaryRepresentation(
            donorCount=len(specimen_accumulators['donor_biomaterial_id'].get()),
            totalCellCount=total_cell_count,
            organSummaries=[OrganCellCountSummary.create_object_from_simple_count(count)
                            for count in organ_cell_count],
            genusSpecies=specimen_accumulators['genus_species'].get(),
            libraryConstructionApproach=library_accumulator.get(),
            disease=specimen_accumulators['disease'].get()
        )


class KeywordSearchResponse(AbstractResponse, EntryFetcher):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """

    def _merge(self, dict_1, dict_2, identifier):
        merged_dict = defaultdict(list)
        dict_id = dict_1.pop(identifier)
        dict_2.pop(identifier)
        for key, value in chain(dict_1.items(), dict_2.items()):
            if isinstance(value, str):
                merged_dict[key] = list(set(merged_dict[key] + [value]))
            elif isinstance(value, int):
                merged_dict[key] = list(merged_dict[key] + [value])
            elif isinstance(value, list):
                cleaned_list = list(filter(None, chain(value, merged_dict[key])))
                if len(cleaned_list) > 0 and isinstance(cleaned_list[0], dict):
                    # Make each dict hashable so we can deduplicate the list
                    merged_dict[key] = thaw(list(set(freeze(cleaned_list))))
                else:
                    merged_dict[key] = list(set(cleaned_list))
            elif value is None:
                merged_dict[key] = []
        merged_dict[identifier] = dict_id
        return dict(merged_dict)

    def return_response(self):
        return self.apiResponse

    def make_bundles(self, entry):
        return [{"bundleUuid": b["uuid"], "bundleVersion": b["version"]} for b in entry["bundles"]]

    def make_processes(self, entry):
        processes = []
        for process in entry["contents"]["processes"]:
            translated_process = {
                "processId": process["process_id"],
                "processName": process.get("process_name", None),
                "libraryConstructionApproach": process.get("library_construction_approach", None),
                "instrumentManufacturerModel": process.get("instrument_manufacturer_model", None),
                "protocolId": process.get("protocol_id", None),
                "protocol": process.get("protocol_name", None),
            }
            processes.append(translated_process)
        return processes

    def make_projects(self, entry):
        projects = []
        for project in entry["contents"]["projects"]:
            translated_project = {
                "projectTitle": project.get("project_title"),
                "projectShortname": project["project_shortname"],
                "laboratory": list(set(project.get("laboratory", [])))
            }
            if self.entity_type == 'projects':
                translated_project['projectDescription'] = project.get('project_description', [])
                translated_project['contributors'] = project.get('contributors', [])
                translated_project['publications'] = project.get('publications', [])
                for contributor in translated_project['contributors']:
                    for key in list(contributor.keys()):
                        contributor[to_camel_case(key)] = contributor.pop(key)
                for publication in translated_project['publications']:
                    for key in list(publication.keys()):
                        publication[to_camel_case(key)] = publication.pop(key)
            projects.append(translated_project)
        return projects

    def make_files(self, entry):
        files = []
        for _file in entry["contents"]["files"]:
            translated_file = {
                "format": _file.get("file_format"),
                "name": _file.get("name"),
                "sha256": _file.get("sha256"),
                "size": _file.get("size"),
                "uuid": _file.get("uuid"),
                "version": _file.get("version"),
            }
            files.append(translated_file)
        return files

    def make_specimens(self, entry):
        specimens = []
        for specimen in entry["contents"]["specimens"]:
            translated_specimen = {
                "id": (specimen["biomaterial_id"]),
                "genusSpecies": specimen.get("genus_species", None),
                "organ": specimen.get("organ", None),
                "organPart": specimen.get("organ_part", None),
                "organismAge": specimen.get("organism_age", None),
                "organismAgeUnit": specimen.get("organism_age_unit", None),
                "biologicalSex": specimen.get("biological_sex", None),
                "disease": specimen.get("disease", None),
                "preservationMethod": specimen.get("preservation_method", None),
                "source": specimen.get("_source", None)
            }
            specimens.append(translated_specimen)
        return specimens

    def make_cell_suspensions(self, entry):
        specimens = []
        for cell_suspension in entry["contents"]["cell_suspensions"]:
            translated_specimen = {
                "organ": cell_suspension.get("organ", None),
                "organPart": cell_suspension.get("organ_part", None),
                "totalCells": cell_suspension.get("total_estimated_cells", None)
            }
            specimens.append(translated_specimen)
        return specimens

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
        } if self.entity_type == 'files' else {
            'fileTypeSummaries': [FileTypeSummary.for_aggregate(aggregate_file).to_json()
                                  for aggregate_file in entry["contents"]["files"]]
        }
        return HitEntry(processes=self.make_processes(entry),
                        entryId=entry["entity_id"],
                        projects=self.make_projects(entry),
                        specimens=self.make_specimens(entry),
                        cellSuspensions=self.make_cell_suspensions(entry),
                        **kwargs)

    def __init__(self, hits, entity_type):
        """
        Constructs the object and initializes the apiResponse attribute

        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.KeywordSearchResponse')
        self.entity_type = entity_type
        # TODO: This is actually wrong. The Response from a single fileId call
        # isn't under hits. It is actually not wrapped under anything
        super(KeywordSearchResponse, self).__init__()
        self.logger.info('Creating the entries in ApiResponse')
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
            else:
                return _term['key']

        term_list = [TermObj(**{'term': choose_entry(term),
                                'count': term['doc_count']})
                     for term in contents['myTerms']['buckets']]

        # Add 'unspecified' term if there is at least one unlabelled document
        untagged_count = contents['untagged']['doc_count']
        if untagged_count > 0:
            term_list.append(TermObj(term=None, count=untagged_count))

        facet = FacetObj(
            terms=term_list,
            total=0 if len(
                contents['myTerms']['buckets']
            ) == 0 else contents['doc_count'],
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

    def __init__(self, hits, pagination, facets, entity_type):
        """
        Constructs the object and initializes the apiResponse attribute
        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.FileSearchResponse')
        # This should initialize the self.apiResponse attribute of the object
        KeywordSearchResponse.__init__(self, hits, entity_type)
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
        self.logger.debug("Entry to be mapped: \n{}".format(json_pp(entry)))
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
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.AutoCompleteResponse')
        # Overriding the __init__ method of the parent class
        EntryFetcher.__init__(self)
        self.logger.info("Mapping entries")
        self.logger.debug("Mapping: \n{}".format(json_pp(mapping)))
        class_entries = {'hits': [self.map_entries(
            mapping, x, _type) for x in hits], 'pagination': None}
        self.apiResponse = AutoCompleteRepresentation(**class_entries)
        # Add the paging via **kwargs of dictionary 'pagination'
        self.apiResponse.pagination = PaginationObj(**pagination)
