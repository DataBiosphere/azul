import abc
from collections import ChainMap, OrderedDict, defaultdict
from copy import deepcopy
import csv
from datetime import datetime, timedelta, timezone
import email.utils
from io import StringIO, TextIOWrapper
from itertools import chain
import logging
import os
from tempfile import TemporaryDirectory, mkstemp
from typing import IO, List, MutableMapping, Mapping, Any
from uuid import uuid4

from bdbag import bdbag_api
from chalice import Response
from jsonobject.api import JsonObject
from jsonobject.properties import (DefaultProperty,
                                   FloatProperty,
                                   IntegerProperty,
                                   ListProperty,
                                   ObjectProperty,
                                   StringProperty)
from more_itertools import one
from werkzeug.http import parse_dict_header

from azul import config, drs
from azul.json_freeze import freeze, thaw
from azul.service.responseobjects.buffer import FlushableBuffer
from azul.service.responseobjects.storage_service import (AWS_S3_DEFAULT_MINIMUM_PART_SIZE,
                                                          MultipartUploadHandler,
                                                          StorageService)
from azul.service.responseobjects.utilities import json_pp
from azul.strings import to_camel_case
from azul.transformer import Document
from azul.types import JSON, MutableJSON, AnyMutableJSON

logger = logging.getLogger(__name__)


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
        self.fileType = aggregate_file['file_format']
        assert isinstance(self.fileType, str)
        assert len(self.fileType)
        return self


class OrganCellCountSummary(JsonObject):
    organType = ListProperty()  # List could have strings and/or None (eg. ['Brain', 'Skin', None])
    countOfDocsWithOrganType = IntegerProperty()
    totalCellCountByOrgan = FloatProperty()

    @classmethod
    def for_bucket(cls, bucket):
        key = Document.translate_field(bucket['key'],
                                       path=('contents', 'cell_suspensions', 'organ'),
                                       forward=False)
        self = cls()
        self.organType = [key]
        self.countOfDocsWithOrganType = bucket['doc_count']
        self.totalCellCountByOrgan = bucket['cell_count']['value']
        return self


class OrganType:

    @classmethod
    def for_bucket(cls, bucket):
        # Un-translate values that had been translated with translate_fields() to handle Nones in Elasticsearch
        # The path for the translation directly relates to the field used in transform_summary() for the aggregation
        return Document.translate_field(bucket['key'],
                                        path=('contents', 'samples', 'effective_organ'),
                                        forward=False)


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
    projectCount = IntegerProperty()
    specimenCount = IntegerProperty()
    fileCount = IntegerProperty()
    totalFileSize = FloatProperty()
    donorCount = IntegerProperty()
    labCount = IntegerProperty()
    totalCellCount = FloatProperty()
    organTypes = ListProperty(StringProperty(required=False))
    fileTypeSummaries = ListProperty(FileTypeSummary)
    cellCountSummaries = ListProperty(OrganCellCountSummary)


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

    def __init__(self, es_search, manifest_entries, mapping, format_, object_key=None):
        """
        The constructor takes the raw response from ElasticSearch and creates
        a csv file based on the columns from the manifest_entries
        :param es_search: The Elasticsearch DSL Search object
        :param manifest_entries: The columns that will be present in the tsv
        :param mapping: The mapping between the columns to values within ES
        :param object_key: A UUID string to use as the manifest's object key
        """
        self.es_search = es_search
        self.manifest_entries = OrderedDict(manifest_entries)
        self.mapping = mapping
        self.storage_service = StorageService()
        self.format = format_
        self.object_key = object_key if object_key is not None else uuid4()

        sources = list(self.manifest_entries.keys())
        self.ordered_column_names = [field_name for source in sources for field_name in self.manifest_entries[source]]

    column_joiner = ' || '

    def _extract_fields(self, entities: List[JSON], column_mapping: JSON, row: MutableMapping[str, str]):
        stripped_joiner = self.column_joiner.strip()

        def validate(s: str) -> str:
            assert stripped_joiner not in s
            return s

        for column_name, field_name in column_mapping.items():
            assert column_name not in row, f'Column mapping defines {column_name} twice'
            column_value = []
            for entity in entities:
                try:
                    field_value = entity[field_name]
                except KeyError:
                    pass
                else:
                    if isinstance(field_value, list):
                        column_value += [validate(str(v)) for v in field_value if v is not None]
                    else:
                        column_value.append(validate(str(field_value)))
            column_value = self.column_joiner.join(sorted(set(column_value)))
            row[column_name] = column_value

    _date_diff_margin = 10  # seconds

    @classmethod
    def _get_seconds_until_expire(cls, head_response: Mapping[str, Any]) -> float:
        """
        Get the number of seconds before a cached manifest is past its expiration.

        :param head_response: A storage service object header dict
        :return: time to expiration in seconds
        """
        # example Expiration: 'expiry-date="Fri, 21 Dec 2012 00:00:00 GMT", rule-id="Rule for testfile.txt"'
        now = datetime.now(timezone.utc)
        expiration = parse_dict_header(head_response['Expiration'])
        expiry_datetime = email.utils.parsedate_to_datetime(expiration['expiry-date'])
        expiry_seconds = (expiry_datetime - now).total_seconds()
        # Verify that 'Expiration' matches value calculated from 'LastModified'
        last_modified = head_response['LastModified']
        expected_expiry_date: datetime = last_modified + timedelta(days=config.manifest_expiration)
        expected_expiry_seconds = (expected_expiry_date - now).total_seconds()
        if abs(expiry_seconds - expected_expiry_seconds) > cls._date_diff_margin:
            logger.error('The actual object expiration (%s) does not match expected value (%s)',
                         expiration, expected_expiry_date)
        else:
            logger.debug('Manifest object expires in %s seconds, on %s', expiry_seconds, expiry_datetime)
        return expiry_seconds

    def _can_use_cached_manifest(self, object_key: str) -> bool:
        """
        Check if the manifest was previously created, still exists in the bucket and won't be expiring soon.

        :param object_key: S3 object key (eg. 'manifests/e0fabf97-7abb-5111-af97-810f1e736c71.tsv'
        """
        try:
            response = self.storage_service.head(object_key)
        except self.storage_service.client.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                logger.info('Cached manifest not found: %s', object_key)
                return False
            else:
                raise e
        else:
            seconds_until_expire = self._get_seconds_until_expire(response)
            if seconds_until_expire > config.manifest_expiration_margin:
                return True
            else:
                logger.info('Cached manifest about to expire: %s', object_key)
                return False

    def _push_content(self) -> str:
        """
        Push the content to S3 with multipart upload

        :return: S3 object key
        """
        object_key = f'manifests/{self.object_key}.tsv'
        content_type = 'text/tab-separated-values'

        if self.format == 'full' and self._can_use_cached_manifest(object_key):
            return object_key

        with MultipartUploadHandler(object_key, content_type) as multipart_upload:
            with FlushableBuffer(AWS_S3_DEFAULT_MINIMUM_PART_SIZE, multipart_upload.push) as buffer:
                text_buffer = TextIOWrapper(buffer, encoding="utf-8", write_through=True)
                if self.format == 'tsv':
                    self._write_tsv(text_buffer)
                elif self.format == 'full':
                    self._write_full(text_buffer)
                else:
                    raise NotImplementedError(f'Multipart upload not implemented for `{self.format}`')

        return object_key

    def _push_content_single_part(self) -> str:
        """
        Push the content to S3 in a single object put

        :return: S3 object key
        """
        if self.format == 'tsv':
            output = StringIO()
            self._write_tsv(output)
            return self.storage_service.put(object_key=f'manifests/{self.object_key}.tsv',
                                            data=output.getvalue().encode(),
                                            content_type='text/tab-separated-values')
        elif self.format == 'bdbag':
            bdbag_path = self._create_bdbag_archive()
            try:
                object_key = f'manifests/{self.object_key}.zip'
                return self.storage_service.upload(bdbag_path, object_key)
            finally:
                os.remove(bdbag_path)
        elif self.format == 'full':
            object_key = f'metadata/{self.object_key}.tsv'
            if self._can_use_cached_manifest(object_key):
                return object_key
            else:
                output = StringIO()
                self._write_full(output)
                return self.storage_service.put(object_key=object_key,
                                                data=output.getvalue().encode(),
                                                content_type='text/tab-separated-values')
        else:
            assert False

    def _write_tsv(self, output: IO[str]) -> None:
        writer = csv.DictWriter(output, self.ordered_column_names, dialect='excel-tab')
        writer.writeheader()
        for hit in self.es_search.scan():
            doc = Document.translate_fields(hit.to_dict(), forward=False)
            assert isinstance(doc, dict)
            for bundle in list(doc['bundles']):  # iterate over copy …
                doc['bundles'] = [bundle]        # … to facilitate this in-place modifaction
                row = {}
                for doc_path, column_mapping in self.manifest_entries.items():
                    entities = self._get_entities(doc_path, doc)
                    self._extract_fields(entities, column_mapping, row)
                writer.writerow(row)

    def _write_full(self, output: IO[str]) -> None:
        sources = list(self.manifest_entries['contents'].keys())
        writer = csv.DictWriter(output, sources, dialect='excel-tab')
        writer.writeheader()
        for hit in self.es_search.scan():
            doc = hit['contents'].to_dict()
            for metadata in list(doc['metadata']):
                row = dict.fromkeys(sources)
                row.update(metadata)
                writer.writerow(row)

    def _create_bdbag_archive(self) -> str:
        with TemporaryDirectory() as temp_path:
            bag_path = os.path.join(temp_path, 'manifest')
            os.makedirs(bag_path)
            bdbag_api.make_bag(bag_path)
            with open(os.path.join(bag_path, 'data', 'participants.tsv'), 'w') as samples_tsv:
                self._write_bdbag_samples_tsv(samples_tsv)
            bag = bdbag_api.make_bag(bag_path, update=True)  # update TSV checksums
            assert bdbag_api.is_bag(bag_path)
            bdbag_api.validate_bag(bag_path)
            assert bdbag_api.check_payload_consistency(bag)
            temp, temp_path = mkstemp()
            os.close(temp)
            archive_path = bdbag_api.archive_bag(bag_path, 'zip')
            # Moves the bdbag archive out of the temporary directory. This prevents
            # the archive from being deleted when the temporary directory self-destructs.
            os.rename(archive_path, temp_path)
            return temp_path

    column_path_separator = '__'

    def _write_bdbag_samples_tsv(self, bundle_tsv: IO[str]) -> None:
        """
        Write the BDBag as a local temporary file and return the path to that file.
        """
        other_column_mappings = deepcopy(self.manifest_entries)
        bundle_column_mapping = other_column_mappings.pop('bundles')
        file_column_mapping = other_column_mappings.pop('contents.files')

        bundles = defaultdict(lambda: defaultdict(list))

        # For each outer file entity_type in the response …
        for hit in self.es_search.scan():
            doc = Document.translate_fields(hit.to_dict(), forward=False)

            # Extract fields from inner entities other than bundles or files
            other_cells = {}
            for doc_path, column_mapping in other_column_mappings.items():
                entities = self._get_entities(doc_path, doc)
                self._extract_fields(entities, column_mapping, other_cells)

            # Extract fields from the sole inner file entity_type
            file = one(doc['contents']['files'])
            file_cells = dict(file_url=self._dss_url(file),
                              dos_url=self._drs_url(file))
            self._extract_fields([file], file_column_mapping, file_cells)

            # Determine the column qualifier. The qualifier will be used to
            # prefix the names of file-specific columns in the TSV
            qualifier = file['file_format']
            if qualifier in ('fastq.gz', 'fastq'):
                qualifier = f"fastq_{file['read_index']}"

            # For each bundle containing the current file …
            bundle: JSON
            for bundle in doc['bundles']:
                bundle_fqid = bundle['uuid'], bundle['version'].replace('.', '_')

                bundle_cells = {'entity:participant_id': '_'.join(bundle_fqid)}
                self._extract_fields([bundle], bundle_column_mapping, bundle_cells)

                # Register the three extracted sets of fields as a group for this bundle and qualifier
                group = {
                    'file': file_cells,
                    'bundle': bundle_cells,
                    'other': other_cells
                }
                bundles[bundle_fqid][qualifier].append(group)

        # Return a complete column name by adding a qualifier and optionally a
        # numeric index. The index is necessary to distinguish between more than
        # one file per file format
        def qualify(qualifier, column_name, index=None):
            if index is not None:
                qualifier = f"{qualifier}_{index}"
            return f"{self.column_path_separator}{qualifier}{self.column_path_separator}{column_name}"

        num_groups_per_qualifier = defaultdict(int)

        # Track the max number of groups for each qualifier in any bundle
        for bundle in bundles.values():
            for qualifier, groups in bundle.items():
                # Sort the groups by reversed file name. This essentially sorts
                # by file extension and any other more general suffixes
                # preceding the extension. It ensure that `patient1_qc.bam` and
                # `patient2_qc.bam` always end up in qualifier `bam[0]` while
                # `patient1_metric.bam` and `patient2_metric.bam` end up in
                # qualifier `bam[1]`.
                groups.sort(key=lambda group: group['file']['file_name'][::-1])
                if len(groups) > num_groups_per_qualifier[qualifier]:
                    num_groups_per_qualifier[qualifier] = len(groups)

        # Compute the column names in deterministic order, bundle_columns first
        # followed by other columns
        column_names = dict.fromkeys(chain(
            ['entity:participant_id'],
            bundle_column_mapping.keys(),
            *(column_mapping.keys() for column_mapping in other_column_mappings.values())))

        # Add file columns for each qualifier and group
        for qualifier, num_groups in sorted(num_groups_per_qualifier.items()):
            for index in range(num_groups):
                for column_name in chain(file_column_mapping.keys(), ('dos_url', 'file_url')):
                    index = None if num_groups == 1 else index
                    column_names[qualify(qualifier, column_name, index=index)] = None

        # Write the TSV header
        bundle_tsv_writer = csv.DictWriter(bundle_tsv, column_names, dialect='excel-tab')
        bundle_tsv_writer.writeheader()

        # Write the actual rows of the TSV
        for bundle in bundles.values():
            row = {}
            for qualifier, groups in bundle.items():
                group: JSON
                for i, group in enumerate(groups):
                    for entity, cells in group.items():
                        if entity == 'bundle':
                            # The bundle-specific cells should be consistent accross all files in a bundle
                            if row:
                                row.update(cells)
                            else:
                                assert cells.items() <= row.items()
                        elif entity == 'other':
                            # Cells from other entities need to be concatenated. Note that for fields that differ
                            # between the files in a bundle this algorithm retains the values but loses the
                            # association between each individual value and the respective file.
                            for column_name, cell_value in cells.items():
                                row.setdefault(column_name, set()).update(cell_value.split(self.column_joiner))
                        elif entity == 'file':
                            # Since file-specfic cells are placed into qualified columns, no concatenation is necessary
                            index = None if num_groups_per_qualifier[qualifier] == 1 else i
                            row.update((qualify(qualifier, column_name, index=index), cell)
                                       for column_name, cell in cells.items())
                        else:
                            assert False
            # Join concatenated values using the joiner
            row = {k: self.column_joiner.join(sorted(v)) if isinstance(v, set) else v for k, v in row.items()}
            bundle_tsv_writer.writerow(row)

    def _get_entities(self, path: str, doc: JSON) -> List[JSON]:
        """
        Given a document and a dotted path into that document, return the list of entities designated by that path.
        """
        path = path.split('.')
        assert path
        d = doc
        for key in path[:-1]:
            d = d.get(key, {})
        entities = d.get(path[-1], [])
        return entities

    def _drs_url(self, file):
        file_uuid = file['uuid']
        file_version = file['version']
        drs_url = drs.object_url(file_uuid, file_version)
        return drs_url

    def _dss_url(self, file):
        file_uuid = file['uuid']
        file_version = file['version']
        replica = 'gcp'
        path = f'files/{file_uuid}?version={file_version}&replica={replica}'
        dss_url = config.dss_endpoint + '/' + path
        return dss_url

    def return_response(self):
        if config.disable_multipart_manifests or self.format == 'bdbag':
            object_key = self._push_content_single_part()
            file_name = None
        else:
            object_key = self._push_content()
            file_name = 'hca-manifest-' + object_key.rsplit('/', )[-1]
        presigned_url = self.storage_service.get_presigned_url(object_key, file_name=file_name)
        headers = {'Content-Type': 'application/json', 'Location': presigned_url}

        return Response(body='', headers=headers, status_code=302)


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
        contents = aggs_dict[agg_name][agg_form]
        if agg_form == "buckets":
            contents = len(contents)
        return contents

    def __init__(self, raw_response):
        # Separate the raw_response into hits and aggregates
        self.hits = raw_response['hits']
        self.aggregates = raw_response['aggregations']
        self.apiResponse = None


class SummaryResponse(BaseSummaryResponse):
    """
    Build response for the summary endpoint
    """

    def __init__(self, raw_response):
        super().__init__(raw_response)

        _file_types = raw_response['aggregations']['fileFormat']["myTerms"]
        _cell_counts = raw_response['aggregations']['group_by_organ']
        _organ_types = raw_response['aggregations']['organTypes']

        self.apiResponse = SummaryRepresentation(
            projectCount=self.agg_contents(self.aggregates, 'projectCount', agg_form='value'),
            specimenCount=self.agg_contents(self.aggregates, 'specimenCount', agg_form='value'),
            fileCount=self.agg_contents(self.aggregates, 'fileCount', agg_form='value'),
            totalFileSize=self.agg_contents(self.aggregates, 'total_size', agg_form='value'),
            donorCount=self.agg_contents(self.aggregates, 'donorCount', agg_form='value'),
            labCount=self.agg_contents(self.aggregates, 'labCount', agg_form='value'),
            totalCellCount=self.agg_contents(self.aggregates, 'total_cell_count', agg_form='value'),
            organTypes=list(map(OrganType.for_bucket, _organ_types['buckets'])),
            fileTypeSummaries=list(map(FileTypeSummary.for_bucket, _file_types['buckets'])),
            cellCountSummaries=list(map(OrganCellCountSummary.for_bucket, _cell_counts['buckets'])))


class KeywordSearchResponse(AbstractResponse, EntryFetcher):
    """
    Class for the keyword search response. Based on the AbstractResponse class
    Not to be confused with the 'keywords' endpoint
    """

    def _merge(self, dict_1: MutableJSON, dict_2: MutableJSON, identifier):
        merged_dict: MutableMapping[str, List[AnyMutableJSON]] = defaultdict(list)
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
                    merged_dict[key] = list(map(thaw, set(map(freeze, cleaned_list))))
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
                "instrumentManufacturerModel": protocol.get("instrument_manufacturer_model", []),
                "pairedEnd": protocol.get("paired_end", []),
                "workflow": protocol.get("workflow", []),
                "assayType": protocol.get("assay_type", []),
            }
            protocols.append(translated_process)
        return protocols

    def make_projects(self, entry):
        projects = []
        for project in entry["contents"]["projects"]:
            translated_project = {
                "projectTitle": project.get("project_title"),
                "projectShortname": project["project_short_name"],
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

    def make_specimen(self, specimen):
        return {
            "id": specimen["biomaterial_id"],
            "organ": specimen.get("organ", None),
            "organPart": specimen.get("organ_part", None),
            "disease": specimen.get("disease", None),
            "preservationMethod": specimen.get("preservation_method", None),
            "source": specimen.get("_source", None)
        }

    def make_specimens(self, entry):
        return [self.make_specimen(specimen) for specimen in entry["contents"]["specimens"]]

    def make_cell_suspension(self, cell_suspension):
        return {
            "organ": cell_suspension.get("organ", None),
            "organPart": cell_suspension.get("organ_part", None),
            "selectedCellType": cell_suspension.get("selected_cell_type", None),
            "totalCells": cell_suspension.get("total_estimated_cells", None)
        }

    def make_cell_suspensions(self, entry):
        return [self.make_cell_suspension(cs) for cs in entry["contents"]["cell_suspensions"]]

    def make_cell_line(self, cell_line):
        return {
            "id": cell_line["biomaterial_id"],
            "cellLineType": cell_line.get("cell_line_type", None),
            "modelOrgan": cell_line.get("model_organ", None),
        }

    def make_cell_lines(self, entry):
        return [self.make_cell_line(cell_line) for cell_line in entry["contents"]["cell_lines"]]

    def make_donor(self, donor):
        return {
            "id": donor["biomaterial_id"],
            "genusSpecies": donor.get("genus_species", None),
            "organismAge": donor.get("organism_age", None),
            "organismAgeUnit": donor.get("organism_age_unit", None),
            "organismAgeRange": donor.get("organism_age_range", None),
            "biologicalSex": donor.get("biological_sex", None),
            "disease": donor.get("disease", None)
        }

    def make_donors(self, entry):
        return [self.make_donor(donor) for donor in entry["contents"]["donors"]]

    def make_organoid(self, organoid):
        return {
            "id": organoid["biomaterial_id"],
            "modelOrgan": organoid.get("model_organ", None),
            "modelOrganPart": organoid.get("model_organ_part", None)
        }

    def make_organoids(self, entry):
        return [self.make_organoid(organoid) for organoid in entry["contents"]["organoids"]]

    def make_sample(self, sample):
        lookup = {
            'cell_lines': ('cellLines', self.make_cell_line),
            'organoids': ('organoids', self.make_organoid),
            'specimens': ('specimens', self.make_specimen),
        }
        entity_type = sample['entity_type']
        if isinstance(entity_type, list):
            entity_type, make_functions = map(list, zip(*map(lookup.get, entity_type)))
            entity_dicts = (make_function(sample) for make_function in make_functions)
            entity_dict = ChainMap(*entity_dicts)
        else:
            entity_type, make_function = lookup[entity_type]
            entity_dict = make_function(sample)
        return {
            'sampleEntityType': entity_type,
            'effectiveOrgan': sample['effective_organ'],
            **entity_dict
        }

    def make_samples(self, entry):
        return [self.make_sample(sample) for sample in entry["contents"]["samples"]]

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
        } if self.entity_type in ('files', 'bundles') else {
            'fileTypeSummaries': [FileTypeSummary.for_aggregate(aggregate_file).to_json()
                                  for aggregate_file in entry["contents"]["files"]]
        }
        return HitEntry(protocols=self.make_protocols(entry),
                        entryId=entry["entity_id"],
                        projects=self.make_projects(entry),
                        samples=self.make_samples(entry),
                        specimens=self.make_specimens(entry),
                        cellLines=self.make_cell_lines(entry),
                        donorOrganisms=self.make_donors(entry),
                        organoids=self.make_organoids(entry),
                        cellSuspensions=self.make_cell_suspensions(entry),
                        **kwargs)

    def __init__(self, hits, entity_type):
        """
        Constructs the object and initializes the apiResponse attribute

        :param hits: A list of hits from ElasticSearch
        """
        self.entity_type = entity_type
        # TODO: This is actually wrong. The Response from a single fileId call
        # isn't under hits. It is actually not wrapped under anything
        super(KeywordSearchResponse, self).__init__()
        logger.info('Creating the entries in ApiResponse')
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
            elif _term['key'] is None:
                return None
            elif isinstance(_term['key'], bool):
                return str(_term['key']).lower()
            else:
                return str(_term['key'])

        term_list = []
        for term in contents['myTerms']['buckets']:
            term_object_params = {'term': choose_entry(term), 'count': term['doc_count']}
            if 'myProjectIds' in term:
                term_object_params['projectId'] = [bucket['key'] for bucket in term['myProjectIds']['buckets']]
            term_list.append(TermObj(**term_object_params))

        untagged_count = contents['untagged']['doc_count']

        # Add the untagged_count to the existing termObj for a None value, or add a new one
        if untagged_count > 0:
            for term_obj in term_list:
                if term_obj.term is None:
                    term_obj.count += untagged_count
                    untagged_count = 0
                    break
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Entry to be mapped: \n%s', json_pp(entry))
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
        # Overriding the __init__ method of the parent class
        EntryFetcher.__init__(self)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Mapping: \n%s', json_pp(mapping))
        class_entries = {'hits': [self.map_entries(
            mapping, x, _type) for x in hits], 'pagination': None}
        self.apiResponse = AutoCompleteRepresentation(**class_entries)
        # Add the paging via **kwargs of dictionary 'pagination'
        self.apiResponse.pagination = PaginationObj(**pagination)
