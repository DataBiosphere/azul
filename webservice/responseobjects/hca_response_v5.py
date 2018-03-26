#!/usr/bin/python
import abc
from utilities import json_pp
from flask_excel import make_response_from_array
import logging
import jmespath
from jsonobject import JsonObject, StringProperty, FloatProperty, \
    IntegerProperty, ListProperty, ObjectProperty, BooleanProperty

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


class ProcessObject(JsonObject):
    """
    Class defining the Process object in the HitEntry object
    """
    processId = StringProperty()
    pairedEnds = BooleanProperty()
    libraryConstructionApproach = StringProperty()
    instrumentManufacturerModel = StringProperty()
    dissociationMethod = StringProperty()


class ProtocolObject(JsonObject):
    """
    Class defining the Protocol object in the HitEntry object
    """
    protocolId = StringProperty()


class BiomaterialObject(JsonObject):
    """
    Class defining a Biomaterial Object in the HitEntry Object
    """
    biomaterialId = StringProperty()
    biomaterialNcbiTaxonIds = ListProperty(IntegerProperty)
    biomaterialGenusSpecies = ListProperty(StringProperty)
    biomaterialOrgan = StringProperty()
    biomaterialOrganPart = StringProperty()
    biomaterialDisease = ListProperty(StringProperty)
    biologicalSex = StringProperty()
    organismAge = StringProperty()
    organismAgeUnit = StringProperty()


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
    page = IntegerProperty()
    sort = StringProperty()
    order = StringProperty(choices=['asc', 'desc'])


class HitEntry(JsonObject):
    """
    Class defining a hit entry in the Api response
    """
    _id = StringProperty(name='id')
    objectID = StringProperty()
    processes = ListProperty(ProcessObject)
    protocols = ListProperty(ProtocolObject)
    bundleUuid = StringProperty()
    bundleVersion = StringProperty()
    bundleType = StringProperty()
    fileCopies = ListProperty(FileCopyObj)
    projectShortname = StringProperty()
    projectContributorsEmail = ListProperty(StringProperty)
    biomaterials = ListProperty(BiomaterialObject)


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
    biomaterialCount = IntegerProperty()
    projectCount = IntegerProperty()
    organCounts = IntegerProperty()


class DonorAutoCompleteEntry(JsonObject):
    """
    Class defining the Donor Autocomplete Entry
    Out of commission until we begin dealing with
    more indexes
    """
    _id = StringProperty(name='id')
    projectId = StringProperty()
    sampleIds = ListProperty(StringProperty)
    specimenIds = ListProperty(StringProperty)
    submittedId = StringProperty()
    submittedSampleIds = ListProperty(StringProperty)
    submittedSpecimenIds = ListProperty(StringProperty)
    _type = StringProperty(name='type', default='donor')


class FileDonorAutoCompleteEntry(JsonObject):
    """
    Class defining the Donor Autocomplete Entry
    """
    _id = StringProperty(name='id')
    _type = StringProperty(name='type', default='donor')


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

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError(
            'users must define return_response to use this base class')


class ManifestResponse(AbstractResponse):
    """
    Class for the Manifest response. Based on the AbstractionResponse class
    """
    def return_response(self):
        return self.apiResponse

    def __init__(self, raw_response, manifest_entries, mapping):
        """
        The constructor takes the raw response from ElasticSearch and creates
        a tsv file based on the columns from the manifest_entries
        :param raw_response: The raw response from ElasticSearch
        :param mapping: The mapping between the columns to values within ES
        :param manifest_entries: The columns that will be present in the tsv
        """
        # Get a list of the hits in the raw response
        hits = [x['_source'] for x in raw_response['hits']['hits']]

        def handle_entry(mapping, entry, column):
            """
            Local method for handling entries in the ES response
            """
            if entry[mapping[column]] is not None:
                _entry = entry[mapping[column]]
                if isinstance(_entry, list):
                    return _entry[0]
                else:
                    return _entry
            else:
                return ''
        # Create the body of the entries in the manifest
        mapped_manifest = [[handle_entry(mapping, entry, column)
                            for column in manifest_entries]
                           for entry in hits]
        # Prepend the header as the first entry on the manifest
        mapped_manifest.insert(0, [column for column in manifest_entries])
        self.apiResponse = make_response_from_array(
            mapped_manifest, 'tsv', file_name='manifest')


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
            print e
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
            biomaterialCount=self.agg_contents(
                aggregates, 'biomaterialCount', agg_form='value'),
            projectCount=self.agg_contents(
                aggregates, 'projectCode', agg_form='value'),
            totalFileSize=self.agg_contents(
                aggregates, 'total_size', agg_form='value'),
            organCounts=self.agg_contents(
                aggregates, 'organsCount', agg_form='value')
        )


class KeywordSearchResponse(AbstractResponse, EntryFetcher):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """

    def return_response(self):
        return self.apiResponse

    def make_processes(self, entry):
        processes = []
        for es_process in entry['processes']:
            api_process = ProcessObject(
                processId=jmespath.search(
                    "content.process_core.process_id", es_process),
                pairedEnds=jmespath.search(
                    "content.paired_ends", es_process),
                libraryConstructionApproach=jmespath.search(
                    "content.library_construction_approach",
                    es_process),
                instrumentManufacturerModel=jmespath.search(
                    "content.instrument_manufacturer_model.text", es_process
                ),
                dissociationMethod=jmespath.search(
                    "content.dissociation_method", es_process
                )
            )
            processes.append(api_process)
        return processes

    def make_protocols(self, entry):
        protocols = []
        for es_protocol in entry['protocols']:
            api_protocol = ProtocolObject(
                protocolId=jmespath.search(
                    "content.protocol_core.protocol_id", es_protocol)
            )
            protocols.append(api_protocol)
        return protocols

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

    def make_biomaterials(self, entry):
        biomaterials = []
        for es_biomaterial in entry['biomaterials']:
            api_biomaterial = BiomaterialObject(
                biomaterialId=jmespath.search(
                    "content.biomaterial_core.biomaterial_id", es_biomaterial
                ),
                biomaterialNcbiTaxonIds=jmespath.search(
                    "content.biomaterial_core.ncbi_taxon_id", es_biomaterial
                ),
                biomaterialGenusSpecies=jmespath.search(
                    "content.genus_species[*].text", es_biomaterial
                ),
                biomaterialOrgan=jmespath.search(
                    "content.organ.text", es_biomaterial
                ),
                biomaterialOrganPart=jmespath.search(
                    "content.organ_part.text", es_biomaterial
                ),
                biomaterialDisease=jmespath.search(
                    "content.disease[*].text", es_biomaterial
                ),
                biologicalSex=jmespath.search(
                    "content.biological_sex", es_biomaterial
                ),
                organismAge=jmespath.search(
                    "content.organism_age", es_biomaterial
                ),
                organismAgeUnit=jmespath.search(
                    "content.organims_age_unit", es_biomaterial
                )
            )
            biomaterials.append(api_biomaterial)
        return biomaterials

    def map_entries(self, entry):
        """
        Returns a HitEntry Object. Creates a single HitEntry object.
        :param entry: A dictionary corresponding to a single hit from
        ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """
        mapped_entry = HitEntry(
            processes=self.make_processes(entry),
            protocols=self.make_protocols(entry),
            bundleType=jmespath.search("bundles[0].type", entry),
            bundleUuid=jmespath.search("bundles[0].uuid", entry),
            bundleVersion=jmespath.search("bundles[0].version", entry),
            fileCopies=self.handle_list(self.make_file_copy(entry)),
            _id=jmespath.search("es_uuid", entry),
            objectID=jmespath.search("es_uuid", entry),
            projectShortname=jmespath.search(
                "project.content.project_core.project_shortname", entry),
            projectContributorsEmail=jmespath.search(
                "project.content.contributors[*].email", entry),
            biomaterials=self.make_biomaterials(entry)
        )
        return mapped_entry

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
        elif _type == 'file-donor' or _type == 'donor':
            # Create a file-donor representation
            # TODO: Need to work on the donor exclusive representation.
            mapped_entry = FileDonorAutoCompleteEntry(
                _id=self.fetch_entry_value(mapping, entry, 'id'),
                _type='donor'
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
