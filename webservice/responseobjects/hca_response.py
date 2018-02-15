#!/usr/bin/python
import abc
from flask_excel import make_response_from_array
from jsonobject import JsonObject, StringProperty, FloatProperty, \
    IntegerProperty, ListProperty, ObjectProperty, DictProperty


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


class SampleObj(JsonObject):
    """
    Class defining a Donor Object in the HitEntry Object
    """
    sampleId = StringProperty()
    sampleBodyPart = StringProperty()
    sampleSpecies = StringProperty()
    sampleNcbiTaxonIds = StringProperty()


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
    centerName = StringProperty(name='center_name')
    study = ListProperty(StringProperty)
    program = StringProperty()
    dataCategorization = ObjectProperty(DataCategorizationObj)
    fileCopies = ListProperty(FileCopyObj)
    donors = ListProperty(DonorObj)
    analysisMethod = ObjectProperty(AnalysisObj)
    referenceGenome = ObjectProperty(ReferenceGenomeObj)

    _id = StringProperty(name='id')
    objectID = StringProperty()
    assayId = StringProperty()
    


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
    totalFileSize = FloatProperty()
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


class ManifestResponse(AbstractResponse):
    """
    Class for the Manifest response. Based on the AbstractionResponse class
    """
    def return_response(self):
        return self.apiResponse

    def __init__(self, raw_response, manifest_entries, mapping):
        """
        The constructor takes the raw response from ElasticSearch and creates a tsv file based on
        the columns from the manifest_entries
        :param raw_response: The raw response from ElasticSearch
        :param mapping: The mapping between the columns to values within ElasticSearch
        :param manifest_entries: The columns that will be present in the tsv
        """
        # Get a list of the hits in the raw response
        hits = [x['_source'] for x in raw_response['hits']['hits']]
        # Create the body of the entries in the manifest
        mapped_manifest = [[entry[mapping[column]] if entry[mapping[column]] is not None else ''
                            for column in manifest_entries] for entry in hits]
        # Prepend the header as the first entry on the manifest
        mapped_manifest.insert(0, [column for column in manifest_entries])
        self.apiResponse = make_response_from_array(mapped_manifest, 'tsv', file_name='manifest')


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
            if agg_form == "buckets":
                contents = len(contents)
        except Exception as e:
            print e
            # If for whatever reason it can't do it, just return a negative number
            contents = -1
        return contents

    def __init__(self, raw_response):
        # Separate the raw_response into hits and aggregates
        hits = raw_response['hits']
        aggregates = raw_response['aggregations']
        # Create a SummaryRepresentation object
        self.apiResponse = SummaryRepresentation(
            fileCount=hits['total'],
            donorCount=self.agg_contents(aggregates, 'donor', agg_form='value'),
            projectCount=self.agg_contents(aggregates, 'projectCode'),
            totalFileSize=self.agg_contents(aggregates, 'total_size', agg_form='value'),
            primarySiteCount=self.agg_contents(aggregates, 'submitterDonorPrimarySite')
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
            redwoodDonorUUID=self.handle_list(self.fetch_entry_value(mapping, entry, 'RedwoodDonorUUID'))
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
            lastModified=self.fetch_entry_value(mapping, entry, 'lastModified'),
            # Additional Stuff
            ** {
                "fileVersion": self.fetch_entry_value(mapping, entry, 'fileVersion')
            }
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
            sampleId=self.handle_list(self.fetch_entry_value(mapping, entry, 'sampleId')),
            specimenType=self.handle_list(self.fetch_entry_value(mapping, entry, 'specimenType')),
            submittedDonorId=self.fetch_entry_value(mapping, entry, 'submittedDonorId'),
            submittedSampleId=self.handle_list(self.fetch_entry_value(mapping, entry, 'submittedSampleId')),
            submittedSpecimenId=self.handle_list(self.fetch_entry_value(mapping, entry, 'submittedSpecimenId')),
            otherIdentifiers=self.make_other_obj(mapping['otherIdentifiers'], entry),
            # Additional Stuff
            ** {
                "living": self.fetch_entry_value(mapping, entry, 'living'),
                "speciesOntology": self.fetch_entry_value(mapping, entry, 'speciesOntology'),
                "speciesText": self.fetch_entry_value(mapping, entry, 'speciesText'),
                "cultureType": self.fetch_entry_value(mapping, entry, 'cultureType'),
                "cellCycle": self.fetch_entry_value(mapping, entry, 'cellCycle'),
                "bodyPart": self.fetch_entry_value(mapping, entry, 'bodyPart'),
                "organ": self.fetch_entry_value(mapping, entry, 'organ'),
                "sampleName": self.fetch_entry_value(mapping, entry, 'sampleName'),

            }
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
            study=self.handle_list(self.fetch_entry_value(mapping, entry, 'study')),
            dataCategorization=self.make_data_categorization(mapping['dataCategorization'], entry),
            fileCopies=self.handle_list(self.make_file_copy(mapping['fileCopies'][0], entry)),
            centerName=self.fetch_entry_value(mapping, entry, 'center_name'),
            program=self.fetch_entry_value(mapping, entry, 'program'),
            donors=self.handle_list(self.make_donor(mapping['donors'][0], entry)),
            analysisMethod=self.make_analysis_method(mapping['analysisMethod'], entry),
            referenceGenome=self.make_reference_genome(mapping['referenceGenome'], entry),
            # Additional entries
            ** {
                "computationalMethod": self.fetch_entry_value(mapping, entry, 'computationalMethod'),
                "seq": {
                    "instrumentPlatform": self.fetch_entry_value(mapping['seq'], entry, 'instrumentPlatform'),
                    "libraryConstruction": self.fetch_entry_value(mapping['seq'], entry, 'libraryConstruction'),
                    "pairedEnds": self.fetch_entry_value(mapping['seq'], entry, 'pairedEnds')
                },
                "rna": {
                    "libraryConstruction": self.fetch_entry_value(mapping['rna'], entry, 'libraryConstruction'),
                    "spikeIn": self.fetch_entry_value(mapping['rna'], entry, 'spikeIn'),
                },
                "singleCell": {
                    "cellHandling": self.fetch_entry_value(mapping['singleCell'], entry, 'cellHandling')
                },
                "submitterInstitution": self.fetch_entry_value(mapping, entry, 'submitterInstitution'),
                "submitterName": self.fetch_entry_value(mapping, entry, 'submitterName'),
                "submitterCountry": self.fetch_entry_value(mapping, entry, 'submitterCountry'),
            }
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
