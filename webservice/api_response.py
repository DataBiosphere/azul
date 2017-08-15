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
    fileMd5Sum = StringProperty()
    lastModified = IntegerProperty()  # DateTimeProperty Int given the ICGC format uses an int and not DateTimeProperty


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
    tcgaSampleBarcode = ListProperty(StringProperty)
    tcgaAliquotBarcode = ListProperty(StringProperty)


class DonorObj(JsonObject):
    """
    Class defining a Donor Object in the HitEntry Object
    """
    donorId = StringProperty()
    primarySite = StringProperty()
    projectCode = StringProperty()
    study = StringProperty()
    sampleId = ListProperty(StringProperty)
    specimenId = ListProperty(StringProperty)
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
    sort = StringProperty(choices=['asc', 'desc'])


class HitEntry(JsonObject):
    """
    Class defining a hit entry in the Api response
    """
    _id = StringProperty()
    objectId = StringProperty()
    access = StringProperty()
    centerName = StringProperty()
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
    pagination = ObjectProperty(PaginationObj, exclude_if_none=True)
    termFacets = DictProperty(FacetObj, exclude_if_none=True)


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
        pass


class KeywordSearchResponse(AbstractResponse):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    """
    def return_response(self):
        pass

    def fetch_entry_value(self, mapping, entry, key):
        if mapping[key]:
            return entry[mapping[key]]
        else:
            return None

    def make_data_categorization(self, mapping, entry):
        return DataCategorizationObj(
            dataType=self.fetch_entry_value(mapping, entry, 'dataType'),
            experimentalStrategy=self.fetch_entry_value(mapping, entry, 'experimentalStrategy')
        )

    def make_file_copy(self, mapping, entry):
        return FileCopyObj(
            repoDataBundleId=self.fetch_entry_value(mapping, entry, 'repoDataBundleId'),
            repoDataSetIds=[self.fetch_entry_value(mapping, entry, 'repoDataSetIds')],
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
            fileMd5Sum=self.fetch_entry_value(mapping, entry, 'fileMd5Sum'),
            lastModified=self.fetch_entry_value(mapping, entry, 'lastModified')
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
            objectId=self.fetch_entry_value(mapping, entry, 'objectId'),
            access=self.fetch_entry_value(mapping, entry, 'access'),
            study=[self.fetch_entry_value(mapping, entry, 'study')],
            dataCategorization=self.make_data_categorization(mapping, entry),
            fileCopies=[self.make_file_copy(mapping, entry)],

        )
        return mapped_entry

    def __init__(self, mapping, hits):
        """
        Constructs the object and initializes the apiResponse attribute
        :param mapping: A JSON with the mapping for the field
        :param hits: A list of hits from ElasticSearch
        """
        class_entries = {'hits': [self.map_entries(mapping, x) for x in hits]}
        self.apiResponse = ApiResponse(**class_entries)


class FileSearchResponse(KeywordSearchResponse):
    """
    Class for the file search response. Inherits from KeywordSearchResponse
    """
    def add_facets(self):
        pass

    def add_paging(self):
        pass

