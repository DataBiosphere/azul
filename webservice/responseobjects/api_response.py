#!/usr/bin/python
import abc
from jsonobject import *


class FileCopyObj(JsonObject):
    """
    Class defining a FileCopy Object in the HitEntry Object
    """
    repoDataBundleId = StringProperty()
    repoDataSetIds = ListProperty(StringProperty)
    repoCode = StringProperty()
    repoOrg = StringProperty()
    repoName = StringProperty()
    repoType = StringProperty()
    repoCountry = StringProperty()
    repoBaseUrl = StringProperty()
    repoDataPath = StringProperty()
    repoMetadataPath = StringProperty()
    fileName = StringProperty()
    fileFormat = StringProperty()
    fileSize = IntegerProperty()
    fileMd5sum = StringProperty()
    lastModified = StringProperty()  # DateTimeProperty Int given the ICGC format uses an int and not DateTimeProperty


class DataCategorizationObj(JsonObject):
    """
    Class defining the data categorization in the HitEntry object
    """
    dataType = StringProperty()
    experimentalStrategy = StringProperty()


class AnalysisObj(JsonObject):
    """
    Class defining an AnalysisObj in the HitEntry object
    """
    analysisType = StringProperty()
    software = StringProperty()


class ReferenceGenomeObj(JsonObject):
    """
    Class defining a reference genome object in the HitEntry object
    """
    genomeBuild = StringProperty()
    referenceName = StringProperty()
    downloadUrl = StringProperty()


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
    type = StringProperty()


class OtherObj(JsonObject):
    """
    Class defining OtherObj in the HitEntry object
    """
    redwoodDonorUUID = ListProperty(StringProperty, name='RedwoodDonorUUID')


class DonorObj(JsonObject):
    """
    Class defining a Donor Object in the HitEntry Object
    """
    donorId = StringProperty()
    primarySite = StringProperty()
    projectCode = StringProperty()
    study = StringProperty()
    sampleId = ListProperty(StringProperty)
    specimenType = ListProperty(StringProperty)
    submittedDonorId = StringProperty()
    submittedSampleId = ListProperty(StringProperty)
    submittedSpecimenId = ListProperty(StringProperty)
    otherIdentifiers = ObjectProperty(OtherObj)


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
    access = StringProperty()
    centerName = StringProperty(name='center_name')
    study = ListProperty(StringProperty)
    program = StringProperty()
    dataCategorization = ObjectProperty(DataCategorizationObj)
    fileCopies = ListProperty(FileCopyObj)
    donors = ListProperty(DonorObj)
    analysisMethod = ObjectProperty(AnalysisObj)
    referenceGenome = ObjectProperty(ReferenceGenomeObj)


class ApiResponse(JsonObject):
    """
    Class defining an API response
    """
    hits = ListProperty(HitEntry)
    pagination = ObjectProperty(PaginationObj, exclude_if_none=True, default=None)
    termFacets = DictProperty(FacetObj, exclude_if_none=True)


class SummaryRepresentation(JsonObject):
    """
    Class defining the Summary Response
    """
    fileCount = IntegerProperty()
    totalFileSize = IntegerProperty()
    donorCount = IntegerProperty()
    projectCount = IntegerProperty()
    primarySiteCount = IntegerProperty()


class AbstractResponse(object):
    """
    Abstract class to be used for each /files API response.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError('users must define return_response to use this base class')


class SummaryResponse(AbstractResponse):
    """
    Class for the summary response. Based on the AbstractResponse class
    """
    def return_response(self):
        return self.apiResponse

    @staticmethod
    def agg_contents(aggs_dict, agg_name, agg_form="buckets"):
        """
        Helper function for parsing aggregate dictionary and returning the contents of the aggregation
        :param aggs_dict: ES dictionary response containing the aggregates
        :param agg_name: Name of aggregate to inspect
        :param agg_form: Part of the aggregate to return.
        :return: Returns the agg_form within the aggregate agg_name
        """
        # Return the specified content of the aggregate. Otherwise return an empty string
        # return aggs_dict[agg_name][agg_form] if agg_name in aggs_dict else ""
        try:
            contents = aggs_dict[agg_name][agg_form]
        except Exception as e:
            print e
            # If for whatever reason it can't do it, just assign contents as an empty string
            contents = ""
        return contents


    def __init__(self, raw_response):
        # Separate the raw_response into hits and aggregates
        hits = raw_response['hits']
        aggregates = raw_response['aggregations']
        # Create a SummaryRepresentation object
        self.apiResponse = SummaryRepresentation(
            fileCount=hits['total'],
            donorCount=len(self.agg_contents(aggregates, 'donor')),
            projectCount=len(self.agg_contents(aggregates, 'projectCode')),
            totalFileSize=self.agg_contents(aggregates, 'projectCode', agg_form='value'),
            primarySiteCount=len(self.agg_contents(aggregates, 'submitterDonorPrimarySite'))
        )


class KeywordSearchResponse(AbstractResponse):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """
    @staticmethod
    def handle_list(value):
        return [value] if value is not None else []

    @staticmethod
    def fetch_entry_value(mapping, entry, key):
        """
        Helper method for getting the value of key on the mapping
        :param mapping: Mapping in question. Values should be at the root level
        :param entry: Dictionary where the contents are to be looking for in
        :param key: Key to be used to get the right value
        :return: Returns entry[mapping[key]] if present. Other
        """
        m = mapping[key]
        if m is not None:
            if isinstance(m, list):
                return entry[m[0]] if m[0] is not None else None
            else:
                return entry[m] if m in entry else None
        else:
            return None

    def return_response(self):
        return self.apiResponse

    def make_data_categorization(self, mapping, entry):
        """
        Returns a DataCategorizationObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a DataCategorizationObj
        """
        return DataCategorizationObj(
            dataType=self.fetch_entry_value(mapping, entry, 'dataType'),
            experimentalStrategy=self.fetch_entry_value(mapping, entry, 'experimentalStrategy')
        )

    def make_analysis_method(self, mapping, entry):
        """
        Returns an AnalysisObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns an AnalysisObj
        """
        return AnalysisObj(
            analysisType=self.fetch_entry_value(mapping, entry, 'analysisType'),
            software=self.fetch_entry_value(mapping, entry, 'software')
        )

    def make_reference_genome(self, mapping, entry):
        """
        Returns a ReferenceGenomeObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a ReferenceGenomeObj
        """
        return ReferenceGenomeObj(
            genomeBuild=self.fetch_entry_value(mapping, entry, 'genomeBuild'),
            referenceName=self.fetch_entry_value(mapping, entry, 'referenceName'),
            downloadUrl=self.fetch_entry_value(mapping, entry, 'downloadUrl')
        )

    def make_other_obj(self, mapping, entry):
        """
        Returns an OtherObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns an OtherObj
        """
        return OtherObj(
            redwoodDonorUUID=[self.fetch_entry_value(mapping, entry, 'RedwoodDonorUUID')]
        )

    def make_file_copy(self, mapping, entry):
        """
        Returns a FileCopyObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a FileCopyObj
        """
        return FileCopyObj(
            repoDataBundleId=self.fetch_entry_value(mapping, entry, 'repoDataBundleId'),
            repoDataSetIds=self.handle_list(self.fetch_entry_value(mapping, entry, 'repoDataSetIds')),
            repoCode=self.fetch_entry_value(mapping, entry, 'repoCode'),
            repoOrg=self.fetch_entry_value(mapping, entry, 'repoOrg'),
            repoName=self.fetch_entry_value(mapping, entry, 'repoName'),
            repoType=self.fetch_entry_value(mapping, entry, 'repoType'),
            repoCountry=self.fetch_entry_value(mapping, entry, 'repoCountry'),
            repoBaseUrl=self.fetch_entry_value(mapping, entry, 'repoBaseUrl'),
            repoDataPath=self.fetch_entry_value(mapping, entry, 'repoDataPath'),
            repoMetadataPath=self.fetch_entry_value(mapping, entry, 'repoMetadataPath'),
            fileName=self.fetch_entry_value(mapping, entry, 'fileName'),
            fileFormat=self.fetch_entry_value(mapping, entry, 'fileFormat'),
            fileSize=self.fetch_entry_value(mapping, entry, 'fileSize'),
            fileMd5sum=self.fetch_entry_value(mapping, entry, 'fileMd5sum'),
            lastModified=self.fetch_entry_value(mapping, entry, 'lastModified')
        )

    def make_donor(self, mapping, entry):
        """
        Returns a DonorObj based on the mapping entry params
        :param mapping: The mapping for the object
        :param entry: The entry in ElasticSearch containing the results.
        :return: Returns a DonorObj
        """
        return DonorObj(
            donorId=self.fetch_entry_value(mapping, entry, 'donorId'),
            primarySite=self.fetch_entry_value(mapping, entry, 'primarySite'),
            projectCode=self.fetch_entry_value(mapping, entry, 'projectCode'),
            study=self.fetch_entry_value(mapping, entry, 'study'),
            sampleId=[self.fetch_entry_value(mapping, entry, 'sampleId')],
            specimenType=[self.fetch_entry_value(mapping, entry, 'specimenType')],
            submittedDonorId=self.fetch_entry_value(mapping, entry, 'submittedDonorId'),
            submittedSampleId=[self.fetch_entry_value(mapping, entry, 'submittedSampleId')],
            submittedSpecimenId=[self.fetch_entry_value(mapping, entry, 'submittedSpecimenId')],
            otherIdentifiers=self.make_other_obj(mapping['otherIdentifiers'], entry)
        )

    def map_entries(self, mapping, entry):
        """
        Returns a HitEntry Object. Takes the mapping and maps the appropriate fields from entry to
        the corresponding entry in the mapping
        :param mapping: Takes in a Json object with the mapping to the corresponding field in the entry object
        :param entry: A 1 dimensional dictionary corresponding to a single hit from ElasticSearch
        :return: A HitEntry Object with the appropriate fields mapped
        """
        mapped_entry = HitEntry(
            _id=self.fetch_entry_value(mapping, entry, 'id'),
            objectID=self.fetch_entry_value(mapping, entry, 'objectID'),
            access=self.fetch_entry_value(mapping, entry, 'access'),
            study=[self.fetch_entry_value(mapping, entry, 'study')],
            dataCategorization=self.make_data_categorization(mapping['dataCategorization'], entry),
            fileCopies=[self.make_file_copy(mapping['fileCopies'][0], entry)],
            centerName=self.fetch_entry_value(mapping, entry, 'center_name'),
            program=self.fetch_entry_value(mapping, entry, 'program'),
            donors=[self.make_donor(mapping['donors'][0], entry)],
            analysisMethod=self.make_analysis_method(mapping['analysisMethod'], entry),
            referenceGenome=self.make_reference_genome(mapping['referenceGenome'], entry)
        )
        return mapped_entry

    def __init__(self, mapping, hits):
        """
        Constructs the object and initializes the apiResponse attribute
        :param mapping: A JSON with the mapping for the field
        :param hits: A list of hits from ElasticSearch
        """
        # TODO: This is actually wrong. The Response from a single fileId call isn't under hits. It is actually not
        # wrapped under anything
        class_entries = {'hits': [self.map_entries(mapping, x) for x in hits], 'pagination': None}
        self.apiResponse = ApiResponse(**class_entries)


class FileSearchResponse(KeywordSearchResponse):
    """
    Class for the file search response. Inherits from KeywordSearchResponse
    """
    @staticmethod
    def create_facet(contents):
        """
        This function creates a FacetObj. It takes in the contents of a particular aggregate from ElasticSearch
        with the format
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
        term_list = [TermObj(**{"term": term['key'], "count":term['doc_count']})
                     for term in contents['myTerms']['buckets']]
        facet = FacetObj(
            terms=term_list,
            total=contents['doc_count'],
            type='terms'  # This has to change once we on-board more types of contents.
        )
        return facet

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
        # This should initialize the self.apiResponse attribute of the object
        KeywordSearchResponse.__init__(self, mapping, hits)
        # Add the paging via **kwargs of dictionary 'pagination'
        self.apiResponse.pagination = PaginationObj(**pagination)
        # Add the facets
        self.apiResponse.termFacets = self.add_facets(facets)
