#!/usr/bin/python
import abc
import time
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
        logger.info('return_response/_construct_tsv_content.begin')
        es_search = self.es_search

        output = StringIO()
        writer = csv.writer(output, dialect='excel-tab')

        writer.writerow(list(self.manifest_entries['bundles'].keys()) + list(self.manifest_entries['files'].keys()))
        scanning_0_ts = time.time()
        scanning_round = 0
        logger.info("Elasticsearch request: %r", es_search.to_dict())

        # Override es_search.scan
        from elasticsearch.helpers import scan
        from elasticsearch_dsl.connections import connections
        from elasticsearch_dsl.response import Hit
        from elasticsearch_dsl.response import Response as ESResponse

        def override_es_search__resolve_nested(s, hit, parent_class=None):
            doc_class = Hit
            nested_field = None

            nested_path = []
            nesting = hit['_nested']
            while nesting and 'field' in nesting:
                nested_path.append(nesting['field'])
                nesting = nesting.get('_nested')
            nested_path = '.'.join(nested_path)

            if hasattr(parent_class, '_index'):
                nested_field = parent_class._index.resolve_field(nested_path)
            else:
                nested_field = s._resolve_field(nested_path)

            if nested_field is not None:
                return nested_field._doc_class

            return doc_class

        def override_es_search__get_result(s, hit, parent_class=None):
            doc_class = Hit
            dt = hit.get('_type')

            if '_nested' in hit:
                doc_class = override_es_search__resolve_nested(s, hit, parent_class)

            elif dt in s._doc_type_map:
                doc_class = s._doc_type_map[dt]

            else:
                for doc_type in s._doc_type:
                    if hasattr(doc_type, '_matches') and doc_type._matches(hit):
                        doc_class = doc_type
                        break

            for t in hit.get('inner_hits', ()):
                hit['inner_hits'][t] = ESResponse(s, hit['inner_hits'][t], doc_class=doc_class)

            callback = getattr(doc_class, 'from_es', doc_class)
            return callback(hit)

        def override_es_search_scan(s):
            """
            Turn the search into a scan search and return a generator that will
            iterate over all the documents matching the query.
            Use ``params`` method to specify any additional arguments you with to
            pass to the underlying ``scan`` helper from ``elasticsearch-py`` -
            https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.scan
            """
            logger.info(f'{s.__module__}.{s.__class__.__name__}: {sorted(dir(s))}')
            es = connections.get_connection(s._using)

            for hit in scan(
                    es,
                    query=s.to_dict(),
                    index=s._index,
                    doc_type=s._doc_type,
                    size=10000,
                    **s._params
            ):
                yield override_es_search__get_result(s, hit)

        for hit in override_es_search_scan(es_search):
            hit_dict = hit.to_dict()
            scanning_round += 1
            if scanning_round == 1: logger.info(f'***** SCANNING: Max size: {len(hit_dict["bundles"])}')
            assert len(hit_dict['contents']['files']) == 1
            file = hit_dict['contents']['files'][0]
            file_fields = self._translate(file, 'files')
            for bundle in hit_dict['bundles']:
                # FIXME: If a file is in multiple bundles, the manifest will list it twice. `hca dss download_manifest`
                # would download the file twice (https://github.com/DataBiosphere/azul/issues/423).
                bundle_fields = self._translate(bundle, 'bundles')
                writer.writerow(bundle_fields + file_fields)
                if scanning_round % 10000 == 0: logger.info(f'***** SCANNING: End of iteration (Elapsed Time: {time.time() - scanning_0_ts:.3f}s)')

        logger.info(f'return_response/scanning_round ({scanning_round})')
        logger.info('return_response/_construct_tsv_content.end')

        return output.getvalue()

    def return_response(self):
        from azul.profilier import profiler
        profiler.record('return_response.begin')
        parameters = dict(object_key=f'manifests/{uuid4()}.tsv',
                          data=self._construct_tsv_content().encode(),
                          content_type='text/tab-separated-values')
        profiler.record('return_response.tsv.ready')
        object_key = self.storage_service.put(**parameters)
        profiler.record('return_response.s3_object.stored')
        presigned_url = self.storage_service.get_presigned_url(object_key)
        profiler.record('return_response.s3_presigned_url.ready')
        headers = {'Content-Type': 'application/json', 'Location': presigned_url}
        profiler.record('return_response.exit')

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

        _sum = raw_response['aggregations']['by_type']
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


class ProjectSummaryResponse(BaseSummaryResponse):
    """
    Build summary field for each project in projects endpoint
    """

    @classmethod
    def get_bucket_terms(cls, project_id, project_buckets, agg_key):
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

    @classmethod
    def get_bucket_value(cls, project_id, project_buckets, agg_key):
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
            if project_bucket['key'] == project_id:
                return project_bucket[agg_key]['value']
        return -1

    @classmethod
    def get_cell_count(cls, hit):
        """
        Iterate through specimens to get overall and per organ cell count. Expects specimens to already be grouped
        and aggregated by organ.
        """
        organ_cell_count = defaultdict(int)
        for specimen in hit['_source']['contents']['specimens']:
            assert len(specimen['organ']) == 1
            try:  # We should use .get() here but ElasticsearchDSL's AttrDict doesn't expose it
                cellcount = specimen['total_estimated_cells']
            except KeyError:
                pass
            else:
                organ_cell_count[specimen['organ'][0]] += cellcount
        total_cell_count = sum(organ_cell_count.values())
        organ_cell_count = [{'key': k, 'value': v} for k, v in organ_cell_count.items()]
        return total_cell_count, organ_cell_count

    def __init__(self, project_id, raw_response):
        super().__init__(raw_response)

        for hit in raw_response['hits']['hits']:
            if hit['_id'] == project_id:
                total_cell_count, organ_cell_count = self.get_cell_count(hit)
                break
        else:
            assert False

        project_aggregates = self.aggregates['_project_agg']

        self.apiResponse = ProjectSummaryRepresentation(
            donorCount=self.get_bucket_value(project_id, project_aggregates, 'donor_count'),
            totalCellCount=total_cell_count,
            organSummaries=[OrganCellCountSummary.create_object_from_simple_count(count)
                            for count in organ_cell_count],
            genusSpecies=self.get_bucket_terms(project_id, project_aggregates, 'species'),
            libraryConstructionApproach=self.get_bucket_terms(project_id, project_aggregates,
                                                              'libraryConstructionApproach'),
            disease=self.get_bucket_terms(project_id, project_aggregates, 'disease')
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
                "sha1": _file.get("sha1"),
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
                "storageMethod": specimen.get("storage_method", None),
                "source": specimen.get("_source", None),
                "totalCells": specimen.get("total_estimated_cells", None)
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
            'files': self.make_files(entry)
        } if self.entity_type == 'files' else {
            'fileTypeSummaries': [FileTypeSummary.for_aggregate(aggregate_file).to_json()
                                  for aggregate_file in entry["contents"]["files"]]
        }
        return HitEntry(processes=self.make_processes(entry),
                        entryId=entry["entity_id"],
                        projects=self.make_projects(entry),
                        specimens=self.make_specimens(entry),
                        bundles=self.make_bundles(entry),
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
