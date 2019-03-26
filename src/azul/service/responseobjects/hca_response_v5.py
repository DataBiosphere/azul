#!/usr/bin/python
import abc
from collections import OrderedDict, defaultdict
import os
import csv
from tempfile import TemporaryDirectory
from io import TextIOWrapper, StringIO
from itertools import chain
import logging
from typing import MutableSet, List

from uuid import uuid4

from chalice import Response
from jsonobject.api import JsonObject
from jsonobject.properties import (FloatProperty,
                                   IntegerProperty,
                                   DefaultProperty,
                                   ListProperty,
                                   ObjectProperty,
                                   StringProperty)
from bdbag import bdbag_api
from shutil import copy
from more_itertools import one
from azul import config
from azul import drs
from azul.service.responseobjects.storage_service import (MultipartUploadHandler,
                                                          StorageService,
                                                          AWS_S3_DEFAULT_MINIMUM_PART_SIZE)
from azul.service.responseobjects.buffer import FlushableBuffer
from azul.service.responseobjects.utilities import json_pp
from azul.json_freeze import freeze, thaw
from azul.strings import to_camel_case
from azul.transformer import SetAccumulator
from azul.types import JSON

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
    search_after = DefaultProperty()
    search_after_uid = StringProperty()
    search_before = DefaultProperty()
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

    def _extract_fields(self, entities: List[JSON], column_mapping: JSON):
        def validate(s: str) -> str:
            assert '||' not in s
            return s

        cell_values = []
        for field_name in column_mapping.values():
            cell_value = []
            for entity in entities:
                try:
                    field_value = entity[field_name]
                except KeyError:
                    pass
                else:
                    if isinstance(field_value, list):
                        cell_value += [validate(str(v)) for v in field_value if v is not None]
                    else:
                        cell_value.append(validate(str(field_value)))
            cell_values.append(" || ".join(sorted(set(cell_value))))

        return cell_values

    def _push_content(self) -> str:
        """
        Push the content to S3 with multipart upload

        :return: S3 object key
        """
        object_key = f'manifests/{uuid4()}.tsv'
        content_type = 'text/tab-separated-values'

        with MultipartUploadHandler(object_key, content_type) as multipart_upload:
            with FlushableBuffer(AWS_S3_DEFAULT_MINIMUM_PART_SIZE, multipart_upload.push) as buffer:
                buffer_wrapper = TextIOWrapper(buffer, encoding="utf-8", write_through=True)
                writer = csv.writer(buffer_wrapper, dialect='excel-tab')
                writer.writerow(self.ordered_column_names)
                for hit in self.es_search.scan():
                    self._iterate_hit_tsv(hit, writer)

        return object_key

    def _push_content_single_part(self) -> str:
        """
        Push the content to S3 in a single object put

        :return: S3 object key
        """
        if self.format == 'tsv':
            data = self._construct_tsv_content().encode()
            content_type = 'text/tab-separated-values'
            object_key = f'manifests/{uuid4()}.tsv'
            parameters = dict(object_key=object_key,
                              data=data,
                              content_type=content_type)
            return self.storage_service.put(**parameters)
        elif self.format == 'bdbag':
            file_name = self._construct_bdbag()
            try:
                object_key = f'manifests/{uuid4()}.zip'
                return self.storage_service.upload(file_name, object_key)
            finally:
                os.remove(file_name)
        else:
            assert False

    def _construct_tsv_content(self):
        es_search = self.es_search

        output = StringIO()
        writer = csv.writer(output, dialect='excel-tab')
        writer.writerow(self.ordered_column_names)
        for hit in es_search.scan():
            self._iterate_hit_tsv(hit, writer)

        return output.getvalue()

    def _construct_bdbag(self) -> str:
        """
        Create and return a file object of the sample data.
        """
        es_search = self.es_search
        sample_file_object = StringIO()
        sample_writer = csv.writer(sample_file_object, dialect='excel-tab')
        # Add fieldnames.
        sample_writer.writerow(list(chain(self.manifest_entries['contents.specimens'].keys(),
                                          self.manifest_entries['contents.cell_suspensions'].keys(),
                                          self.manifest_entries['bundles'].keys(),
                                          self.manifest_entries['contents.files'].keys(),
                                          ['file_url'], ['dos_url'])))
        participants = set()
        for hit in es_search.scan():
            self._iterate_hit_bdbag(hit, participants, sample_writer)

        participant_file_object = StringIO()
        participant_writer = csv.writer(participant_file_object, dialect='excel-tab')
        participant_writer.writerow(['entity:participant_id'])
        participant_writer.writerows(zip(participants))
        return self._create_zipped_bdbag(participant_file_object, sample_file_object)

    @staticmethod
    def _create_zipped_bdbag(participant_file_object: StringIO, sample_file_object: StringIO) -> str:
        """
        Create write participant and sample data files to disk, and create and return zipped BDBag.
        """

        with TemporaryDirectory() as bag_path, TemporaryDirectory() as tsv_file_dir:
            # Discard return value since we don't use it, bag is updated below
            bdbag_api.make_bag(bag_path)
            data_path = os.path.join(bag_path, 'data')

            # Write participant and sample data to their respective files.
            tsvs = [('participant.tsv', participant_file_object),
                    ('sample.tsv', sample_file_object)]
            for tsv_filename, file_object in tsvs:
                with open(os.path.join(tsv_file_dir, tsv_filename), 'w') as f:
                    f.write(file_object.getvalue())
                copy(os.path.join(tsv_file_dir, tsv_filename), data_path)

            assert sorted(tsv[0] for tsv in tsvs) == sorted(os.listdir(data_path))
            bag = bdbag_api.make_bag(bag_path, update=True)  # update TSV checksums
            assert bdbag_api.is_bag(bag_path)
            bdbag_api.validate_bag(bag_path)
            assert bdbag_api.check_payload_consistency(bag)

            return bdbag_api.archive_bag(bag_path, 'zip')

    def _iterate_hit_tsv(self, es_search_hit, writer):
        hit_dict = es_search_hit.to_dict()

        inner_entity_fields = []
        for doc_path, column_mapping in self.manifest_entries.items():
            doc_path = doc_path.split('.')
            assert doc_path
            entities = hit_dict
            for key in doc_path[:-1]:
                entities = entities.get(key, {})
            entities = entities.get(doc_path[-1], [])

            inner_entity_fields += self._extract_fields(entities, column_mapping)

        writer.writerow(inner_entity_fields)

    def _iterate_hit_bdbag(self, es_search_hit, participants: MutableSet[str], sample_writer):
        hit_dict = es_search_hit.to_dict()
        # Some files are not associated with specimens or cell suspensions (e.g., PDFs, JPEGs) - skip them.
        if 'specimens' in hit_dict['contents'].keys() and 'cell_suspensions' in hit_dict['contents'].keys():
            specimen = one(hit_dict['contents']['specimens'])
            cell_suspension = one(hit_dict['contents']['cell_suspensions'])
            file = one(hit_dict['contents']['files'])

            specimen_fields = self._translate(specimen, 'contents.specimens')
            cell_suspension_fields = self._translate(cell_suspension, 'contents.cell_suspensions')
            file_fields = self._translate(file, 'contents.files')

            # Construct URL for file.
            file_uuid = file['uuid']
            file_version = file['version']
            replica = 'gcp'
            endpoint = f'files/{file_uuid}?version={file_version}&replica={replica}'
            dss_url = config.dss_endpoint + '/' + endpoint
            drs_url = drs.object_url(file_uuid, file_version)

            for bundle in hit_dict['bundles']:
                sample_writer.writerow(chain(specimen_fields[0],
                                             specimen_fields[1],
                                             cell_suspension_fields[0],
                                             self._translate(bundle, 'bundles'),
                                             file_fields,
                                             [dss_url],
                                             [drs_url]))
                participant_id = specimen_fields[1][0]
                participants.add(participant_id)

    def return_response(self):
        if config.disable_multipart_manifests or self.format == 'bdbag':
            object_key = self._push_content_single_part()
        else:
            object_key = self._push_content()
        file_name = 'hca-manifest-' + object_key.rsplit('/', )[-1]
        presigned_url = self.storage_service.get_presigned_url(object_key, file_name=file_name)
        headers = {'Content-Type': 'application/json', 'Location': presigned_url}

        return Response(body='', headers=headers, status_code=302)

    def __init__(self, es_search, manifest_entries, mapping, format):
        """
        The constructor takes the raw response from ElasticSearch and creates
        a csv file based on the columns from the manifest_entries
        :param es_search: The Elasticsearch DSL Search object
        :param mapping: The mapping between the columns to values within ES
        :param manifest_entries: The columns that will be present in the tsv
        """
        self.es_search = es_search
        self.manifest_entries = OrderedDict(manifest_entries)
        self.mapping = mapping
        self.storage_service = StorageService()
        self.format = format

        sources = list(self.manifest_entries.keys())
        self.ordered_column_names = [field_name for source in sources for field_name in self.manifest_entries[source]]


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
        for protocol in es_hit_contents['protocols']:
            if 'library_construction_approach' in protocol:
                library_accumulator.accumulate(protocol['library_construction_approach'])

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

    def make_protocols(self, entry):
        protocols = []
        for protocol in entry["contents"]["protocols"]:
            translated_process = {
                "libraryConstructionApproach": protocol.get("library_construction_approach", []),
                "instrumentManufacturerModel": protocol.get("instrument_manufacturer_model", [])
            }
            protocols.append(translated_process)
        return protocols

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
                translated_project['arrayExpressAccessions'] = project.get('array_express_accessions', [])
                translated_project['geoSeriesAccessions'] = project.get('geo_series_accessions', [])
                translated_project['insdcProjectAccessions'] = project.get('insdc_project_accessions', [])
                translated_project['insdcStudyAccessions'] = project.get('insdc_study_accessions', [])
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
        return HitEntry(protocols=self.make_protocols(entry),
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
