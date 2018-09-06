#!/usr/bin/python
import abc
from collections import OrderedDict, defaultdict
import csv
from io import StringIO
from itertools import chain
import logging
import os

from chalice import Response
import jmespath
from jsonobject import (DictProperty,
                        FloatProperty,
                        IntegerProperty,
                        JsonObject,
                        ListProperty,
                        ObjectProperty,
                        StringProperty)

from azul.service.responseobjects.utilities import json_pp
from azul.json_freeze import freeze, thaw

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
    def create_object(cls, **kwargs):
        if "bucket" in kwargs:
            return cls._create_object_with_bucket(kwargs["bucket"])
        else:
            return cls._create_object_with_args(
                file_type=kwargs["file_type"], total_size=kwargs["total_size"], count=kwargs["count"]
            )

    @classmethod
    def _create_object_with_bucket(cls, bucket):
        new_object = cls()
        new_object.count = bucket['doc_count']
        new_object.totalSize = int(bucket['size_by_type']['value'])  # Casting to integer since ES returns a double
        new_object.fileType = bucket['key']
        return new_object

    @classmethod
    def _create_object_with_args(cls, file_type, total_size, count):
        new_object = cls()
        new_object.count = count
        new_object.totalSize = total_size
        new_object.fileType = file_type
        return new_object


class OrganCellCountSummary(JsonObject):
    organType = StringProperty()
    countOfDocsWithOrganType = IntegerProperty()
    totalCellCountByOrgan = FloatProperty()

    @classmethod
    def create_object(cls, bucket):
        new_object = cls()
        new_object.organType = bucket['key']
        new_object.countOfDocsWithOrganType = bucket['doc_count']
        new_object.totalCellCountByOrgan = bucket['cell_count']['value']
        return new_object

    @classmethod
    def create_object_from_simple_count(cls, count):
        new_object = cls()
        new_object.organType = count['key']
        new_object.countOfDocsWithOrganType = 1
        new_object.totalCellCountByOrgan = count['value']
        return new_object


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


class BaseSummaryResponse(AbstractResponse):
    """
    Base class for the summary response. Based on the AbstractResponse class
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
        self.hits = raw_response['hits']
        self.aggregates = raw_response['aggregations']


class SummaryResponse(BaseSummaryResponse):
    """
    Class for the summary response. Based on the BaseSummaryResponse class
    """

    def __init__(self, raw_response):
        super().__init__(raw_response)

        _sum = raw_response['aggregations']['by_type']
        _organ_group = raw_response['aggregations']['group_by_organ']

        # Create a SummaryRepresentation object
        kwargs = dict(
            projectCount=self.agg_contents(self.aggregates, 'projectCode', agg_form='value'),
            totalFileSize=self.agg_contents(self.aggregates, 'total_size', agg_form='value'),
            organCount=self.agg_contents(self.aggregates, 'organCount', agg_form='value'),
            donorCount=self.agg_contents(self.aggregates, 'donorCount', agg_form='value'),
            labCount=self.agg_contents(self.aggregates, 'labCount', agg_form='value'),
            totalCellCount=self.agg_contents(self.aggregates, 'total_cell_count', agg_form='value'),
            fileTypeSummaries=[FileTypeSummary.create_object(bucket=bucket) for bucket in _sum['buckets']],
            organSummaries=[OrganCellCountSummary.create_object(bucket) for bucket in _organ_group['buckets']])

        if 'specimenCount' in self.aggregates:
            kwargs['fileCount'] = self.hits['total']
            kwargs['specimenCount'] = self.agg_contents(self.aggregates, 'specimenCount', agg_form='value')
        elif 'fileCount' in self.aggregates:
            kwargs['fileCount'] = self.agg_contents(self.aggregates, 'fileCount', agg_form='value')
            kwargs['specimenCount'] = self.hits['total']

        self.apiResponse = SummaryRepresentation(**kwargs)


class ProjectSummaryResponse(BaseSummaryResponse):

    @staticmethod
    def get_bucket_terms(project_id, project_buckets, agg_key):
        """
        Return a list of the keys of the buckets from an ElasticSearch aggregate
        of a given project with the format:
        {
          "buckets": [
            {
              "key": $project_id,
              $agg_key: {
                "buckets": [
                  {
                    "key": "a",
                    "doc_count": 2
                  }
                ]
              }
            },
            ...
          ]
        }

        :param project_id: string UUID of the project info to retrieve
        :param project_buckets: A dictionary from an ElasticSearch aggregate
        :param agg_key: Key of aggregation to use
        :return: list of bucket keys
        """
        for project_bucket in project_buckets['buckets']:
            if project_bucket['key'] != project_id:
                continue
            return [bucket['key'] for bucket in project_bucket[agg_key]['buckets']]
        return []

    @staticmethod
    def get_bucket_value(project_id, project_buckets, agg_key):
        """
        Return a value of the bucket of the given project from an
        ElasticSearch aggregate with the format:
        {
          "buckets": [
            {
              "key": $project_id,
              $agg_key: {
                "value" : value
              }
            },
            ...
          ]
        }

        :param project_id: string UUID of the project info to retrieve
        :param project_buckets: A dictionary from an ElasticSearch aggregate
        :param agg_key: Key of aggregation to use
        :return: value in given project
        """
        for project_bucket in project_buckets['buckets']:
            if project_bucket['key'] != project_id:
                continue
            return project_bucket[agg_key]['value']
        return -1

    @staticmethod
    def get_cell_count(hit):
        """Iterate through specimens to get per organ cell count"""
        # FIXME: This should ideally be done through elasticsearch
        specimen_ids = set()
        organ_cell_count = dict()
        total_cell_count = 0
        for bundle in hit['_source']['bundles']:
            specimens = bundle['contents']['specimens']
            for specimen in specimens:
                if specimen['biomaterial_id'] in specimen_ids:
                    continue
                specimen_ids.add(specimen['biomaterial_id'])
                if len(list(filter(None, specimen['organ']))) == 0:  # check if specimen has no organ
                    continue
                if 'total_estimated_cells' not in specimen:
                    estimated_cells = 0
                else:
                    estimated_cells = sum(list(filter(None, specimen['total_estimated_cells'])))
                total_cell_count += estimated_cells
                if specimen['organ'][0] in organ_cell_count:
                    organ_cell_count[specimen['organ'][0]] += estimated_cells
                else:
                    organ_cell_count[specimen['organ'][0]] = estimated_cells

        organ_cell_count = [{'key': k, 'value': v} for k, v in organ_cell_count.items()]
        return total_cell_count, organ_cell_count

    def __init__(self, project_id, raw_response):
        super().__init__(raw_response)

        total_cell_count = 0
        organ_cell_count = []
        for hit in raw_response['hits']['hits']:
            if hit['_id'] == project_id:
                total_cell_count, organ_cell_count = (
                    ProjectSummaryResponse.get_cell_count(hit))
                break

        project_aggregates = self.aggregates['_project_agg']

        # Create a ProjectSummaryRepresentation object
        kwargs = dict(
            donorCount=ProjectSummaryResponse.get_bucket_value(
                project_id, project_aggregates, 'donor_count'),
            totalCellCount=total_cell_count,
            organSummaries=[OrganCellCountSummary.create_object_from_simple_count(count)
                            for count in organ_cell_count],
            genusSpecies=ProjectSummaryResponse.get_bucket_terms(
                project_id, project_aggregates, 'species'),
            libraryConstructionApproach=ProjectSummaryResponse.get_bucket_terms(
                project_id, project_aggregates, 'libraryConstructionApproach'),
            disease=ProjectSummaryResponse.get_bucket_terms(
                project_id, project_aggregates, 'disease')
        )

        self.apiResponse = ProjectSummaryRepresentation(**kwargs)


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
                if len(cleaned_list) > 0 and isinstance(cleaned_list[0], dict):  # make dicts hashable
                    hashable_dicts = [freeze(d) for d in cleaned_list]
                    merged_dict[key] = [thaw(d) for d in list(set(hashable_dicts))]
                else:
                    merged_dict[key] = list(set(cleaned_list))
            elif value is None:
                merged_dict[key] = []
        merged_dict[identifier] = dict_id
        return dict(merged_dict)

    def return_response(self):
        return self.apiResponse

    @staticmethod
    def to_camel_case(text: str):
        camel_cased = ''.join(part.title() for part in text.split('_'))
        return camel_cased[0].lower() + camel_cased[1:]

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
                    "libraryConstructionApproach": process.get("library_construction_approach", None),
                    "instrument": process.get("instrument_manufacturer_model", None),
                    "protocolId": process.get("protocol_id", None),
                    "protocol": process.get("protocol_name", None),
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
            project_shortname = project["project_shortname"]
            translated_project = {
                "projectTitle": project.get("project_title"),
                "projectShortname": project_shortname,
                "laboratory": list(set(project.get("laboratory", [])))
            }

            if self.projects_response:
                translated_project["projectDescription"] = project.get("project_description", [])
                translated_project["contributors"] = project.get("contributors", [])
                translated_project["publications"] = project.get("publications", [])

                for contributor in translated_project['contributors']:
                    for key in list(contributor.keys()):
                        contributor[KeywordSearchResponse.to_camel_case(key)] = contributor.pop(key)

                for publication in translated_project['publications']:
                    for key in list(publication.keys()):
                        publication[KeywordSearchResponse.to_camel_case(key)] = publication.pop(key)

            if project_shortname not in projects:
                projects[project_shortname] = translated_project
            else:
                merged_project = self._merge(projects[project_shortname], translated_project, "projectShortname")
                projects[project_shortname] = merged_project
        return list(projects.values())

    def make_files(self, entry):
        all_files = []
        for bundle in entry["bundles"]:
            for _file in bundle["contents"]["files"]:
                new_file = {
                    "format": _file.get("file_format"),
                    "name": _file.get("name"),
                    "sha1": _file.get("sha1"),
                    "size": _file.get("size"),
                    "uuid": _file.get("uuid"),
                    "version": _file.get("version"),
                }
                all_files.append(new_file)
        return all_files

    def make_file_type_summaries(self, files):
        file_type_summaries = {}
        for file in files:
            if not file['format'] in file_type_summaries:
                file_type_summaries[file['format']] = {}
            file_type_summaries[file['format']]['size'] = file_type_summaries[file['format']].get('size', 0) + file['size']
            file_type_summaries[file['format']]['count'] = file_type_summaries[file['format']].get('count', 0) + 1
        return file_type_summaries

    def make_specimens(self, entry):
        specimens = {}
        for bundle in entry["bundles"]:
            for specimen in bundle["contents"]["specimens"]:
                specimen.pop("_type")
                specimen_id = specimen["biomaterial_id"]
                translated_specimen = {
                    "id": specimen_id,
                    "genusSpecies": specimen.get("genus_species", None),
                    "organ": specimen.get("organ", None),
                    "organPart": specimen.get("organ_part", None),
                    "organismAge": specimen.get("organism_age", None),
                    "organismAgeUnit": specimen.get("organism_age_unit", None),
                    "biologicalSex": specimen.get("biological_sex", None),
                    "disease": specimen.get("disease", None),
                    "storageMethod": specimen.get("storage_method", None),
                    "source": specimen.get("_source", None),
                    "totalCells": specimen.get("total_estimated_cells", None)
                }
                if specimen_id not in specimens:
                    for key, value in translated_specimen.items():
                        if key == "id":
                            continue
                        else:
                            translated_specimen[key] = [] if value is None else list(set(filter(None, value)))
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
        if self.entity_type == 'files':
            files = {
                'files': self.make_files(entry)
            }
        else:
            file_type_summaries = self.make_file_type_summaries(self.make_files(entry))
            files = {
                'fileTypeSummaries': [FileTypeSummary.create_object(file_type=file_type,
                                                                    total_size=file_type_summary['size'],
                                                                    count=file_type_summary['count']).to_json()
                                      for file_type, file_type_summary in file_type_summaries.items()]
            }

        return HitEntry(
            processes=self.make_processes(entry),
            entryId=entry["entity_id"],
            projects=self.make_projects(entry),
            specimens=self.make_specimens(entry),
            bundles=self.make_bundles(entry),
            **files
        )

    def __init__(self, hits, entity_type):
        """
        Constructs the object and initializes the apiResponse attribute
        :param hits: A list of hits from ElasticSearch
        :param projects_response: True if creating response for projects endpoint
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.KeywordSearchResponse')
        self.entity_type = entity_type
        # TODO: This is actually wrong. The Response from a single fileId call
        # isn't under hits. It is actually not wrapped under anything
        super(KeywordSearchResponse, self).__init__()
        self.projects_response = projects_response
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
        :param projects_response: True if creating response for projects endpoint
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
