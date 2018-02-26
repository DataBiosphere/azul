#!/usr/bin/python
import abc
from utilities import json_pp
from flask_excel import make_response_from_array
import logging
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


class AnalysisObj(JsonObject):
    """
    Class defining an AnalysisObj in the HitEntry object
    """
    analysisId = StringProperty()
    analysisComputationalMethod = StringProperty()


class AssayObject(JsonObject):
    """
    Class defining the assay object in the HitEntry object
    """
    assayId = StringProperty()
    singleCellHandling = StringProperty()
    seqInstrumentPlatform = StringProperty()
    pairedEnds = BooleanProperty()
    rnaLibraryConstruction = StringProperty()
    rnaSpikeIn = StringProperty()


class SampleObj(JsonObject):
    """
    Class defining a Donor Object in the HitEntry Object
    """
    sampleId = StringProperty()
    sampleBodyPart = StringProperty()
    sampleSpecies = StringProperty()
    sampleNcbiTaxonIds = IntegerProperty()


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
    assay = ObjectProperty(AssayObject)
    projectId = StringProperty()
    projectContributorsEmail = ListProperty(StringProperty)
    analysis = ObjectProperty(AnalysisObj)
    bundleUuid = StringProperty()
    bundleVersion = StringProperty()
    bundleType = StringProperty()
    fileCopies = ListProperty(FileCopyObj)
    samples = ListProperty(SampleObj)


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
    donorCount = IntegerProperty()
    projectCount = IntegerProperty()
    primarySiteCount = IntegerProperty()


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
        # Create the body of the entries in the manifest
        mapped_manifest = [[entry[mapping[column]] if entry[mapping[column]] is
                            not None else '' for column in manifest_entries]
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
            donorCount=self.agg_contents(
                aggregates, 'sample', agg_form='value'),
            projectCount=self.agg_contents(
                aggregates, 'projectCode'),
            totalFileSize=self.agg_contents(
                aggregates, 'total_size', agg_form='value'),
            primarySiteCount=self.agg_contents(
                aggregates, 'sampleBodyPart')
        )


class KeywordSearchResponse(AbstractResponse, EntryFetcher):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """

    def return_response(self):
        return self.apiResponse

    def make_assay_object(self, mapping, entry):
        return AssayObject(
            assayId=self.fetch_entry_value(mapping, entry, 'assayId'),
            singleCellHandling=self.fetch_entry_value(
                mapping, entry, 'singleCellHandling'),
            seqInstrumentPlatform=self.fetch_entry_value(
                mapping, entry, 'seqInstrumentPlatform'),
            pairedEnds=self.fetch_entry_value(
                mapping, entry, 'pairedEnds'),
            rnaLibraryConstruction=self.fetch_entry_value(
                mapping, entry, 'rnaLibraryConstruction'),
            rnaSpikeIn=self.fetch_entry_value(mapping, entry, 'rnaSpikeIn')
        )

    def make_analysis_method(self, mapping, entry):
        """
        Returns an AnalysisObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns an AnalysisObj
        """
        return AnalysisObj(
            analysisId=self.fetch_entry_value(mapping, entry, 'analysisId'),
            analysisComputationalMethod=self.fetch_entry_value(
                mapping, entry, 'analysisComputationalMethod')
        )

    def make_file_copy(self, mapping, entry):
        """
        Returns a FileCopyObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a FileCopyObj
        """
        _entry = entry['files']
        return FileCopyObj(
            fileUuid=self.fetch_entry_value(mapping, _entry, 'fileUuid'),
            fileVersion=self.fetch_entry_value(mapping, _entry, 'fileVersion'),
            fileSha1=self.fetch_entry_value(mapping, _entry, 'fileSha1'),
            fileSize=self.fetch_entry_value(mapping, _entry, 'fileSize'),
            fileFormat=self.fetch_entry_value(mapping, _entry, 'fileFormat'),
            fileName=self.fetch_entry_value(mapping, _entry, 'fileName')
        )

    def make_samples(self, mapping, entry):
        array = []
        for sample in entry['samples']:
            sample_obj = SampleObj(
                sampleId=self.fetch_entry_value(mapping, sample, 'sampleId'),
                sampleBodyPart=self.fetch_entry_value(
                    mapping, sample, 'sampleBodyPart'),
                sampleSpecies=self.fetch_entry_value(
                    mapping, sample, 'sampleSpecies'),
                sampleNcbiTaxonIds=self.fetch_entry_value(
                    mapping, sample, 'sampleNcbiTaxonIds')
            )
            array.append(sample_obj)
        return array

    def map_entries(self, mapping, entry):
        """
        Returns a HitEntry Object. Takes the mapping and maps the appropriate
        fields from entry to the corresponding entry in the mapping
        :param mapping: Takes in a Json object with the mapping to the
        corresponding field in the entry object
        :param entry: A 1 dimensional dictionary corresponding to a single
        hit from ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """
        mapped_entry = HitEntry(
            _id=self.fetch_entry_value(mapping, entry, 'id'),
            objectID=self.fetch_entry_value(mapping, entry, 'objectID'),
            assay=self.make_assay_object(mapping['assay'], entry),
            projectId=self.fetch_entry_value(mapping, entry, 'projectId'),
            projectContributorsEmail=self.fetch_entry_value(
                mapping, entry, 'projectContributorsEmail'),
            analysis=self.make_analysis_method(mapping['analysis'], entry),
            bundleUuid=self.fetch_entry_value(
                mapping, entry['bundles'][0], 'bundleUuid'),
            bundleVersion=self.fetch_entry_value(
                mapping, entry['bundles'][0], 'bundleVersion'),
            bundleType=self.fetch_entry_value(
                mapping, entry['bundles'][0], 'bundleType'),
            fileCopies=self.handle_list(
                self.make_file_copy(mapping['fileCopies'][0], entry)),
            samples=self.make_samples(mapping['samples'][0], entry)
        )
        return mapped_entry

    def __init__(self, mapping, hits):
        """
        Constructs the object and initializes the apiResponse attribute
        :param mapping: A JSON with the mapping for the field
        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.KeywordSearchResponse')
        # TODO: This is actually wrong. The Response from a single fileId call
        # isn't under hits. It is actually not wrapped under anything
        super(KeywordSearchResponse, self).__init__()
        self.logger.info('Mapping the entries')
        self.logger.debug('Mapping config: \n{}'.format(json_pp(mapping)))
        class_entries = {'hits': [
            self.map_entries(mapping, x) for x in hits], 'pagination': None}
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
            total=contents['doc_count'],
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

    def __init__(self, mapping, hits, pagination, facets):
        """
        Constructs the object and initializes the apiResponse attribute
        :param mapping: A JSON with the mapping for the field
        :param hits: A list of hits from ElasticSearch
        """
        # Setup the logger
        self.logger = logging.getLogger(
            'dashboardService.api_response.FileSearchResponse')
        # This should initialize the self.apiResponse attribute of the object
        KeywordSearchResponse.__init__(self, mapping, hits)
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
