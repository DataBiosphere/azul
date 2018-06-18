#!/usr/bin/python
import abc
from chalicelib.responseobjects.utilities import json_pp
from chalice import Response
from collections import defaultdict
from collections import OrderedDict
from itertools import chain
from io import StringIO
import logging
import jmespath
from jsonobject import JsonObject, StringProperty, FloatProperty, \
    IntegerProperty, ListProperty, ObjectProperty
import os
import csv

module_logger = logging.getLogger("dashboardService.elastic_request_builder")


class FileCopyObj(JsonObject):
    """
    Class defining a FileCopy Object in the HitEntry Object
    """
    fileName = StringProperty()
    fileFormat = StringProperty()
    fileSize = IntegerProperty()
    fileSha1 = StringProperty()
    fileVersion = StringProperty()
    fileUuid = StringProperty()


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
    _from = IntegerProperty(name='from')
    # page = IntegerProperty()
    sort = StringProperty()
    order = StringProperty(choices=['asc', 'desc'])


class HitEntry(JsonObject):
    """
    Class defining a hit entry in the Api response
    """
    # entity_id = StringProperty()
    # entity_version = StringProperty()
    # bundleUuid = StringProperty()
    # bundleVersion = StringProperty()


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

    def return_response(self):
        es_search = self.es_search

        headers = {'Content-Disposition': 'attachment; filename="export.tsv"',
                   'Content-Type': 'text/tab-separated-values'}

        output = StringIO()

        writer = csv.writer(output, dialect='excel-tab')

        writer.writerow(list(self.manifest_entries['bundles'].keys()) + list(self.manifest_entries['files'].keys()))
        for hit in es_search.scan():
            hit_dict = hit.to_dict()
            for bundle in hit_dict['bundles']:
                bundle_fields = self._translate(bundle, 'bundles')
                for file in bundle['contents']['files']:
                    file_fields = self._translate(file, 'files')
                    writer.writerow(bundle_fields + file_fields)
        return Response(body=output.getvalue(), headers=headers, status_code=200)

    def __init__(self, es_search, manifest_entries, mapping):
        """
        The constructor takes the raw response from ElasticSearch and creates
        a csv file based on the columns from the manifest_entries
        :param raw_response: The raw response from ElasticSearch
        :param mapping: The mapping between the columns to values within ES
        :param manifest_entries: The columns that will be present in the tsv
        """
        self.es_search = es_search
        self.manifest_entries = OrderedDict(manifest_entries)
        self.mapping = mapping


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


class SummaryResponse(AbstractResponse):
    """
    Class for the summary response. Based on the AbstractResponse class
    """

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
        except Exception as e:
            print(e)
            # If for whatever reason it can't do it, just return
            # a negative number
            contents = -1
        return contents

    def __init__(self, raw_response):
        # Separate the raw_response into hits and aggregates
        hits = raw_response['hits']
        aggregates = raw_response['aggregations']
        # Create a SummaryRepresentation object
        self.apiResponse = SummaryRepresentation(
            fileCount=hits['total'],
            specimenCount=self.agg_contents(
                aggregates, 'specimenCount', agg_form='value'),
            projectCount=self.agg_contents(
                aggregates, 'projectCode', agg_form='value'),
            totalFileSize=self.agg_contents(
                aggregates, 'total_size', agg_form='value'),
            organCount=self.agg_contents(
                aggregates, 'organCount', agg_form='value')
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
                merged_dict[key] = list(set(cleaned_list))
            elif value is None:
                merged_dict[key] = []
        merged_dict[identifier] = dict_id
        return dict(merged_dict)

    def return_response(self):
        return self.apiResponse

    def make_file_copy(self, entry):
        """
        Returns a FileCopyObj based on the mapping entry params
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a FileCopyObj
        """
        _entry = entry['files']
        return FileCopyObj(
            fileUuid=jmespath.search("uuid", _entry),
            fileVersion=jmespath.search("version", _entry),
            fileSha1=jmespath.search("sha1", _entry),
            fileSize=jmespath.search("size", _entry),
            fileFormat=jmespath.search("format", _entry),
            fileName=jmespath.search("name", _entry)
        )

    def make_bundles(self, entry):
        return [{"bundleUuid": b["uuid"], "bundleVersion": b["version"]} for b in entry["bundles"]]

    def make_processes(self, entry):
        processes = {}
        for bundle in entry["bundles"]:
            for process in bundle["contents"]["processes"]:
                # HACK: need to check on the indexer why raw protocols are being added
                if "process_id" not in process:
                    continue
                process.pop("_type")
                process_id = process["process_id"]
                translated_process = {
                    "processId": process_id,
                    "processName": process.get("process_name", None),
                    "libraryConstructionApproach": process.get("library_construction", None),
                    "instrument": process.get("instrument", None),
                    "protocolId": process.get("protocol_id", None),
                    "protocol": process.get("protocol", None),
                }
                if process_id not in processes:
                    processes[process_id] = translated_process
                else:
                    merged_process = self._merge(processes[process_id], translated_process, "processId")
                    processes[process_id] = merged_process
        return list(processes.values())

    def make_projects(self, entry):
        projects = {}
        for bundle in entry["bundles"]:
            project = bundle["contents"]["project"]
            project.pop("_type")
            project_id = project["project"]
            translated_project = {
                "shortname": project_id,
                "laboratory": list(set(project.get("laboratory")) if project.get("laboratory") else [])
            }
            if project_id not in projects:
                projects[project_id] = translated_project
            else:
                merged_project = self._merge(projects[project_id], translated_project, "shortname")
                projects[project_id] = merged_project
        return list(projects.values())

    def make_files(self, entry):
        all_files = []
        for bundle in entry["bundles"]:
            for _file in bundle["contents"]["files"]:
                new_file = {
                    "format": _file.get("format"),
                    "name": _file.get("name"),
                    "sha1": _file.get("sha1"),
                    "size": _file.get("size"),
                    "uuid": _file.get("uuid"),
                    "version": _file.get("version"),
                }
                all_files.append(new_file)
        return all_files

    def make_specimens(self, entry):
        specimens = {}
        for bundle in entry["bundles"]:
            for specimen in bundle["contents"]["specimens"]:
                specimen.pop("_type")
                specimen_id = specimen["biomaterial_id"]
                translated_specimen = {
                    "id": specimen_id,
                    "genusSpecies": specimen.get("species", None),
                    "organ": specimen.get("organ", None),
                    "organPart": specimen.get("organ_part", None),
                    "organismAge": specimen.get("age", None),
                    "organismAgeUnit": specimen.get("age_unit", None),
                    "biologicalSex": specimen.get("sex", None),
                    "disease": specimen.get("disease", None),
                    "storageMethod": specimen.get("storage_method", None),
                    "source": specimen.get("source", None),
                    "totalCells": specimen.get("total_cells", None)
                }
                if specimen_id not in specimens:
                    for key, value in translated_specimen.items():
                        if key == "id":
                            continue
                        else:
                            translated_specimen[key] = list(set(filter(None, value)))
                    translated_specimen["totalCells"] = sum(translated_specimen["totalCells"])
                    specimens[specimen_id] = translated_specimen
                else:
                    merged_specimen = self._merge(specimens[specimen_id], translated_specimen, "id")
                    merged_specimen["totalCells"] = sum(merged_specimen["totalCells"])
                    specimens[specimen_id] = merged_specimen
        return list(specimens.values())

    def map_entries(self, entry):
        """
        Returns a HitEntry Object. Creates a single HitEntry object.
        :param entry: A dictionary corresponding to a single hit from
        ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """

        return HitEntry(
            processes=self.make_processes(entry),
            entryId=entry["entity_id"],
            files=self.make_files(entry),
            projects=self.make_projects(entry),
            specimens=self.make_specimens(entry),
            bundles=self.make_bundles(entry)
        )

    def __init__(self, hits):
        """
        Constructs the object and initializes the apiResponse attribute
        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.KeywordSearchResponse')
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

        term_list = [TermObj(**{"term": choose_entry(term),
                                "count": term['doc_count']})
                     for term in contents['myTerms']['buckets']]
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
            facets[facet] = FileSearchResponse.create_facet(contents)
        return facets

    def __init__(self, hits, pagination, facets):
        """
        Constructs the object and initializes the apiResponse attribute
        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.FileSearchResponse')
        # This should initialize the self.apiResponse attribute of the object
        KeywordSearchResponse.__init__(self, hits)
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
