#!/usr/bin/python
import abc
from jsonobject import *


class FileCopyObj(JsonObject):
    repoDataBundleId = StringProperty()
    repoDataSetIds = StringProperty()
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
    dataType = StringProperty()
    experimentalStrategy = StringProperty()


class AnalysisObj(JsonObject):
    analysisType = StringProperty()
    software = StringProperty()


class ReferenceGenomeObj(JsonObject):
    genomeBuild = StringProperty()
    referenceName = StringProperty()
    downloadUrl = StringProperty()


class TermObj(JsonObject):
    count = IntegerProperty()
    term = StringProperty()


class FacetObj(JsonObject):
    terms = ListProperty(TermObj)
    total = IntegerProperty()
    type = StringProperty()


class OtherObj(JsonObject):
    tcgaSampleBarcode = ListProperty(StringProperty)
    tcgaAliquotBarcode = ListProperty(StringProperty)


class DonorObj(JsonObject):
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
    count = IntegerProperty()
    total = IntegerProperty()
    size = IntegerProperty()
    _from = IntegerProperty(name='from')
    page = IntegerProperty()
    sort = StringProperty(choices=['asc', 'desc'])


class HitEntry(JsonObject):
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
    hits = ListProperty(HitEntry)
    pagination = ObjectProperty(PaginationObj, exclude_if_none=True)
    termFacets = DictProperty(FacetObj, exclude_if_none=True)


class AbstractResponse(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def return_response(self):
        raise NotImplementedError('users must define return_response to use this base class')


class SummaryResponse(AbstractResponse):
    """
    Class for the summary response
    """
    def return_response(self):
        pass


class KeywordSearchResponse(AbstractResponse):
    """
    Class for the keyword search response
    """
    def return_response(self):
        pass

    def map_entries(self):
        pass


class FileSearchResponse(KeywordSearchResponse):
    """
    Class for the file search response.
    """
    def add_facets(self):
        pass

    def add_paging(self):
        pass
