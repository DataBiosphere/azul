from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    defaultdict,
)
from collections.abc import (
    Mapping,
)
from copy import (
    deepcopy,
)
import csv
from datetime import (
    datetime,
)
from io import (
    BytesIO,
)
from itertools import (
    combinations,
    starmap,
)
import json
import os
from pathlib import (
    Path,
)
from typing import (
    Optional,
    cast,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    patch,
)
import unittest.result
from uuid import (
    UUID,
)

import attrs
from chalice import (
    ForbiddenError,
)
import fastavro
from furl import (
    furl,
)
from more_itertools import (
    chunked,
    one,
)
import requests
from requests import (
    Response,
)

from azul import (
    config,
    iif,
)
from azul.filters import (
    Filters,
    FiltersJSON,
)
from azul.http import (
    parse_header,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.lib import (
    R,
    cache,
)
from azul.lib.collections import (
    adict,
    compose_keys,
    none_safe_tuple_key,
)
from azul.lib.json import (
    copy_json,
    json_hash,
)
from azul.lib.strings import (
    single_quote as sq,
)
from azul.lib.types import (
    JSON,
    JSONs,
    MutableCompositeJSON,
    MutableJSON,
    MutableJSONs,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins import (
    ManifestFormat,
    MetadataPlugin,
)
from azul.plugins.metadata.hca import (
    FileTransformer,
)
from azul.plugins.repository.dss import (
    DSSBundle,
    DSSBundleFQID,
    DSSSourceRef,
)
from azul.service import (
    avro_pfb,
    manifest_service,
)
from azul.service.manifest_controller import (
    ManifestController,
)
from azul.service.manifest_service import (
    CachedManifestNotFound,
    Manifest,
    ManifestGenerator,
    ManifestKey,
    ManifestPartition,
    ManifestService,
    PagedManifestGenerator,
    SignedManifestKey,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.source import (
    Prefix,
    SimpleSourceSpec,
)
from azul_test_case import (
    patch_config,
)
from indexer import (
    AnvilCannedBundleTestCase,
    CannedFileTestCase,
    DCP1CannedBundleTestCase,
)
from service import (
    DocumentCloningTestCase,
    MirrorTestCase,
    StorageServiceTestCase,
    WebServiceTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class CannedManifestTestCase(CannedFileTestCase):
    """
    Support for tests that deal with canned manifests
    """

    def _canned_manifest_path(self, *path: str) -> Path:
        return self._data_path('service', 'manifest', *path)

    def _load_canned_manifest(self, *path: str) -> MutableCompositeJSON:
        with open(self._canned_manifest_path(*path)) as f:
            manifest = json.load(f)
        assert isinstance(manifest, (dict, list)), type(manifest)
        return manifest

    def _load_canned_pfb(self, *path: str) -> tuple[MutableJSON, MutableJSONs]:
        schema = self._load_canned_manifest(*path, 'pfb_schema.json')
        assert isinstance(schema, dict), type(schema)
        entities = self._load_canned_manifest(*path, 'pfb_entities.json')
        assert isinstance(entities, list), type(entities)
        return schema, entities

    def _assert_pfb_schema(self, schema):
        fastavro.parse_schema(schema)
        # Parsing successfully proves our schema is valid
        with self.assertRaises(KeyError):
            fastavro.parse_schema({'this': 'is not', 'an': 'avro schema'})

        actual = json.dumps(schema, indent=4, sort_keys=True)
        expected = self._canned_manifest_path('terra', 'pfb_schema.json')
        self._assert_or_create_json_can(expected, actual)

    def _assert_or_create_json_can(self, expected: Path, actual: str):
        if expected.exists():
            with open(expected, 'r') as f:
                expected = json.load(f)
            self.assertEqual(expected, json.loads(actual))
        else:
            with open(expected, 'w') as f:
                f.write(actual)

    def _assert_jsonl(self, expected: list[JSON], actual: Response):
        """
        Assert that the body of the given response is the expected JSON array,
        disregarding any row ordering differences.

        :param expected: a list of JSON objects.

        :param actual: an HTTP response containing JSON objects separated by
                       newlines
        """
        manifest = [
            json.loads(row)
            for row in actual.content.decode().splitlines()
        ]

        def sort_key(row: JSON) -> bytes:
            return json_hash(row).digest()

        manifest.sort(key=sort_key)
        expected.sort(key=sort_key)
        # The canned manifests are saved as a JSON array instead of JSON Lines
        # so that changes to the files are easier to read
        self.assertEqual(expected, manifest)

    def _assert_pfb(self,
                    expected_schema: JSON,
                    expected_entities: JSONs,
                    actual: Response):
        """
        Assert that the body of the given response contains a valid PFB manifest
        matching the expected schema and content, disregarding differences in
        the ordering of the PFB entities.

        :param expected_schema: a PFB schema.

        :param expected_entities: a list of PFB entities.

        :param actual: an HTTP response containing a PFB manifest.
        """
        manifest = fastavro.reader(BytesIO(actual.content))
        schema = manifest.writer_schema
        # The ordering of the entities in the manifest depends on the order of
        # the replica documents in the index. We haven't figured out how to
        # ensure that this ordering is reliably deterministic, so we sort to
        # make the test insensitive to it.
        # FIXME: Document order of replicas is nondeterministic
        #        https://github.com/DataBiosphere/azul/issues/6442
        sort_key = compose_keys(none_safe_tuple_key(),
                                # This is necessary to stabilize the ordering of
                                # DUOS replicas, which have the same id as the
                                # main dataset replica.
                                lambda entity: (entity['id'], entity['object'].get('datarepo_row_id')))
        expected_entities = sorted(expected_entities, key=sort_key)
        entities = sorted(manifest, key=sort_key)
        self.assertEqual(expected_schema, schema)
        self.assertEqual(expected_entities, entities)


class ManifestTestCase(WebServiceTestCase,
                       StorageServiceTestCase,
                       CannedManifestTestCase,
                       MirrorTestCase,
                       metaclass=ABCMeta):

    def setUp(self):
        super().setUp()
        self.addPatch(patch.object(PagedManifestGenerator, 'page_size', 1))
        self.addPatch(patch.dict(os.environ,
                                 azul_git_commit='9347432ab0da43c73409ac7fd3edfe29cf3ae678',
                                 azul_git_dirty='0'))
        self._setup_indices()

    def tearDown(self):
        self._teardown_indices()
        super().tearDown()

    def _filters(self, filters: FiltersJSON) -> Filters:
        return Filters(explicit=filters, source_ids={self.source.ref.id})

    @property
    def _controller(self) -> ManifestController:
        controller = self._app.manifest_controller
        assert isinstance(controller, ManifestController)
        return controller

    @property
    def _metadata_plugin(self) -> MetadataPlugin:
        plugin = self._controller._metadata_plugin
        assert isinstance(plugin, MetadataPlugin)
        return plugin

    @property
    def _service(self):
        return ManifestService(file_url_func=self._controller._file_url)

    def _get_manifest(self,
                      format: ManifestFormat,
                      filters: FiltersJSON,
                      stream=False
                      ) -> Response:
        manifest, num_partitions = self._get_manifest_object(format, filters)
        self.assertEqual(1, num_partitions)
        url = furl(self._service.get_manifest_url(manifest))
        response = requests.get(str(url), stream=stream)
        # Moto doesn't support signed S3 URLs with Content-Disposition baked in,
        # so we'll retroactively inject it into the response header.
        content_disposition = url.args.get('response-content-disposition')
        if content_disposition is not None:
            response.headers['content-disposition'] = content_disposition
        return response

    def _get_manifest_object(self,
                             format: ManifestFormat,
                             filters: JSON
                             ) -> tuple[Manifest, int]:
        filters = self._filters(filters)
        partition = ManifestPartition.first()
        num_partitions = 1
        while True:
            partition = self._service.get_manifest(format=format,
                                                   catalog=self.catalog,
                                                   filters=filters,
                                                   partition=partition)
            if isinstance(partition, Manifest):
                return partition, num_partitions
            # Emulate controller serializing the partition between steps
            partition = ManifestPartition.from_json(partition.to_json())
            num_partitions += 1

    def _assert_tsv(self, expected: list[tuple[str, ...]], actual: Response):
        """
        Assert that the body of the given response is the expected TSV,
        disregarding any row ordering differences.

        :param expected: A transposed TSV, i.e. a list of columns. Each column
                         is a tuple, and the first element in each tuple is the
                         column header, or field name.

        :param actual: An HTTP response containing a TSV
        """
        expected = list(map(list, zip(*expected)))
        actual = actual.content.decode().splitlines()
        actual = list(csv.reader(actual, delimiter='\t'))
        actual[1:], expected[1:] = sorted(actual[1:]), sorted(expected[1:])
        self.assertEqual(expected, actual)

    def _assert_curl(self, expected_body: list[list[str]], actual: Response):
        expected_header = [
            '--http1.1', '',
            '--create-dirs', '',
            '--compressed', '',
            '--location', '',
            '--globoff', '',
            '--fail', '',
            '--fail-early', '',
            '--continue-at -', '',
            '--write-out "Downloading to: %{filename_effective}\\n\\n"', '',
        ]
        lines = actual.content.decode().splitlines()
        header_length = len(expected_header)
        header, body = lines[:header_length], lines[header_length:]
        self.assertEqual(expected_header, header)
        self.assertEqual(expected_body, sorted(chunked(body, 3)))

    def _file_url(self, file_id, version):
        return str(self.base_url.set(path='/repository/files/' + file_id,
                                     args=dict(catalog=self.catalog,
                                               version=version)))

    def _drs_uri(self, file_id, version=None):
        return str(furl(scheme='drs',
                        netloc=self._drs_domain,
                        path=file_id,
                        args=adict(version=version)))

    def _mirror_uri(self, digest: str) -> str:
        return f's3://{self.mirror_bucket}/file/{digest}.{self._digest_type()}'

    @abstractmethod
    def _digest_type(self) -> str:
        raise NotImplementedError

    @property
    def _drs_domain(self) -> str:
        return config.drs_domain or config.api_lambda_domain('service')


class DCP1ManifestTestCase(DCP1CannedBundleTestCase, ManifestTestCase):

    def _digest_type(self) -> str:
        return 'sha256'


class TestManifests(DCP1ManifestTestCase):

    def run(self,
            result: Optional[unittest.result.TestResult] = None
            ) -> Optional[unittest.result.TestResult]:
        # Disable caching of manifests to prevent false assertion positives
        with patch.object(ManifestService,
                          '_get_cached_manifest_file_name',
                          return_value=None):
            return super().run(result)

    _drs_domain_name = 'drs-test.lan'  # see canned PFB results

    def test_terra_pfb_manifest(self):
        # This test uses canned expectations. It might be difficult to manually
        # update the can after changes to the indexer. If that is the case,
        # delete the file and run this test. It will repopulate the file. Run
        # the test again; it should pass. Make sure you study the resulting diff
        # before committing to avoid canning a bug.
        self.maxDiff = None
        # This bundle contains zarrs which tests related_files (but is dated)
        zarr_bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                            version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(zarr_bundle_fqid)
        # This is a more up-to-date, modern bundle
        new_bundle_fqid = self.bundle_fqid(uuid='4da04038-adab-59a9-b6c4-3a61242cc972',
                                           version='2021-01-01T00:00:00.000000Z')
        new_bundle = self._add_ageless_donor(new_bundle_fqid)
        self._index_bundle(new_bundle, delete=False)
        shared_file_bundle = self._shared_file_bundle(new_bundle_fqid)
        self._index_bundle(shared_file_bundle, delete=False)

        # We write entities differently depending on debug so we test both cases
        for debug in (1, 0):
            with self.subTest(debug=debug):
                with patch.object(type(config), 'debug', debug):
                    response = self._get_manifest(ManifestFormat.terra_pfb, {})
                    self.assertEqual(200, response.status_code)
                    pfb_file = BytesIO(response.content)
                    reader = fastavro.reader(pfb_file)
                    schema = reader.writer_schema
                    self._assert_pfb_schema(schema)
                    records = list(reader)
                    expected = self._canned_manifest_path('terra', 'pfb_entities.json')
                    # 'default' is specified to handle the conversion of datetime values
                    actual = json.dumps(records, indent=4, sort_keys=True, default=str)
                    self._assert_or_create_json_can(expected, actual)

    def _shared_file_bundle(self, bundle):
        """
        Create a copy of an existing bundle with slight modifications in order
        to test PFB manifest generation with multiple inner-entities of the same
        type.
        """
        bundle = self._load_canned_bundle(bundle)
        new_specimen_id = '5275e5a0-6043-4ec9-86a1-6c1140cbeede'
        old_to_new = {
            # process
            '4da04038-adab-59a9-b6c4-3a61242cc972': '61af0068-1418-46e7-88ef-ab310e0ceaf8',
            # cell_suspension
            'd9eaaffe-4c93-5503-984f-762e8dfddce4': 'd6b3d2ab-5715-4486-a544-ac09fafac279',
            # specimen
            '224d3750-f1f7-5b04-bbce-e23f09eea7d7': new_specimen_id
        }
        metadata = self._replace_uuids(bundle.metadata, old_to_new)
        # Change organ to prevent cell_suspensions aggregating together
        metadata[f'specimen_from_organism/{new_specimen_id}']['organ'] = {
            'text': 'lung',
            'ontology': 'UBERON:0002048',
            'ontology_label': 'lung'
        }
        links = self._replace_uuids(bundle.links, old_to_new)
        return DSSBundle(fqid=self.bundle_fqid(uuid=old_to_new[bundle.uuid],
                                               version=bundle.version),
                         manifest=bundle.manifest,
                         metadata=metadata,
                         links=links)

    def _replace_uuids(self,
                       object_: JSON,
                       uuids: Mapping[str, str]
                       ) -> MutableJSON:
        object_str = json.dumps(object_)
        for old, new in uuids.items():
            assert old in object_str, old
            object_str = object_str.replace(old, new)
        return json.loads(object_str)

    def _add_ageless_donor(self, bundle):
        """
        We add a new donor which lacks "age" metadata to test PFB generation
        with both kinds of donors.
        """
        bundle = self._load_canned_bundle(bundle)
        # Since most of the metadata is duplicated (including biomaterial_id)
        # the donor_count will not increase.
        old_donor_id = '9173ee6a-f1b2-5762-9272-3433b5ef7530'
        duplicate_donor = deepcopy(bundle.metadata[f'donor_organism/{old_donor_id}'])
        del duplicate_donor['organism_age']
        del duplicate_donor['organism_age_unit']
        donor_id = '0895599c-f57d-4843-963e-11eab29f883b'
        duplicate_donor['provenance']['document_id'] = donor_id
        bundle.metadata[f'donor_organism/{donor_id}'] = duplicate_donor
        donor_link = one(ln for ln in bundle.links['links']
                         if one(ln['inputs'])['input_type'] == 'donor_organism')
        new_donor_reference = {
            'input_id': donor_id,
            'input_type': 'donor_organism'
        }
        donor_link['inputs'].append(new_donor_reference)
        return bundle

    def test_manifest_not_cached(self):
        """
        Assert that the patch to disable caching is effective.
        """
        for i in range(2):
            with self.subTest(i=i):
                manifest, num_partitions = self._get_manifest_object(ManifestFormat.compact, {})
                self.assertFalse(manifest.was_cached)
                self.assertEqual(1, num_partitions)

    def test_compact_manifest(self):
        expected = [
            ('source_id', self.source.ref.id, self.source.ref.id),
            ('source_spec', str(self.source.ref.spec), str(self.source.ref.spec)),
            ('bundle_uuid',
             'b81656cf-231b-47a3-9317-10f1e501a05c || f79257a7-dfc6-46d6-ae00-ba4b25313c10',
             'f79257a7-dfc6-46d6-ae00-ba4b25313c10'),
            ('bundle_version',
             '2000-01-01T01:00:00.000000Z || 2018-09-14T13:33:14.453337Z',
             '2018-09-14T13:33:14.453337Z'),
            ('file_document_id', '89e313db-4423-4d53-b17e-164949acfa8f', '6c946b6c-040e-45cc-9114-a8b1454c8d20'),
            ('file_type', 'supplementary_file', 'sequence_file'),
            ('file_name', 'SmartSeq2_RTPCR_protocol.pdf', '22028_5#300_1.fastq.gz'),
            ('file_format', 'pdf', 'fastq.gz'),
            ('read_index', '', 'read1'),
            ('file_size', '29230', '64718465'),
            ('file_uuid', '5f9b45af-9a26-4b16-a785-7f2d1053dd7c', 'f2b6c6f0-8d25-4aae-b255-1974cc110cfe'),
            ('file_version', '2018-09-14T12:33:47.012715Z', '2018-09-14T12:33:43.720332Z'),

            ('file_crc32c', 'b9364bfa', '980453cc'),
            ('file_sha256',
             '2f6866c4ede92123f90dd15fb180fac56e33309b8fd3f4f52f263ed2f8af2f16',
             '3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582'),

            ('file_content_type', 'application/pdf; dcp-type=data', 'application/gzip; dcp-type=data'),

            ('file_drs_uri',
             self._drs_uri('5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                           '2018-09-14T12:33:47.012715Z'),
             self._drs_uri('f2b6c6f0-8d25-4aae-b255-1974cc110cfe',
                           '2018-09-14T12:33:43.720332Z')),

            ('file_azul_url',
             self._file_url('5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                            '2018-09-14T12:33:47.012715Z'),
             self._file_url('f2b6c6f0-8d25-4aae-b255-1974cc110cfe',
                            '2018-09-14T12:33:43.720332Z')),

            ('file_mirror_uri',
             self._mirror_uri('2f6866c4ede92123f90dd15fb180fac56e33309b8fd3f4f52f263ed2f8af2f16'),
             self._mirror_uri('3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582')),

            ('cell_suspension.provenance.document_id',
             '',
             '0037c9eb-8038-432f-8d9d-13ee094e54ab || aaaaaaaa-8038-432f-8d9d-13ee094e54ab'),

            ('cell_suspension.biomaterial_core.biomaterial_id', '', '22028_5#300 || 22030_5#300'),
            ('cell_suspension.estimated_cell_count', '', '9001'),
            ('cell_suspension.selected_cell_type', '', 'CAFs'),
            ('sequencing_process.provenance.document_id', '', '72732ed3-7b71-47df-bcec-c765ef7ea758'),
            ('sequencing_protocol.instrument_manufacturer_model', '', 'Illumina HiSeq 2500'),
            ('sequencing_protocol.paired_end', '', 'True'),
            ('library_preparation_protocol.library_construction_approach', '', 'Smart-seq2'),
            ('library_preparation_protocol.nucleic_acid_source', '', 'single cell'),

            ('project.provenance.document_id',
             '67bc798b-a34a-4104-8cab-cad648471f69',
             '67bc798b-a34a-4104-8cab-cad648471f69'),

            ('project.contributors.institution',
             ' || '.join([
                 'DKFZ German Cancer Research Center',
                 'EMBL-EBI',
                 'University of Cambridge',
                 'University of Helsinki',
                 'Wellcome Trust Sanger Institute']),
             ' || '.join([
                 'DKFZ German Cancer Research Center',
                 'EMBL-EBI',
                 'University of Cambridge',
                 'University of Helsinki',
                 'Wellcome Trust Sanger Institute'])),

            ('project.contributors.laboratory',
             'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit || Sarah Teichmann',
             'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit || Sarah Teichmann'),

            ('project.project_core.project_short_name', 'Mouse Melanoma', 'Mouse Melanoma'),

            ('project.project_core.project_title',
             'Melanoma infiltration of stromal and immune cells',
             'Melanoma infiltration of stromal and immune cells'),

            ('project.estimated_cell_count', '', ''),

            ('specimen_from_organism.provenance.document_id',
             '',
             'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244 || b4e55fe1-7bab-44ba-a81d-3d8cb3873244'),

            ('specimen_from_organism.diseases', '', ''),
            ('specimen_from_organism.organ', '', 'brain || tumor'),
            ('specimen_from_organism.organ_part', '', ''),
            ('specimen_from_organism.preservation_storage.preservation_method', '', ''),
            ('donor_organism.sex', '', 'female'),
            ('donor_organism.biomaterial_core.biomaterial_id', '', '1209'),
            ('donor_organism.provenance.document_id', '', '89b50434-f831-4e15-a8c0-0d57e6baa94c'),
            ('donor_organism.genus_species', '', 'Mus musculus'),
            ('donor_organism.development_stage', '', 'adult'),
            ('donor_organism.diseases', '', 'subcutaneous melanoma'),
            ('donor_organism.organism_age', '', '6-12 week'),
            ('cell_line.provenance.document_id', '', ''),
            ('cell_line.biomaterial_core.biomaterial_id', '', ''),
            ('organoid.provenance.document_id', '', ''),
            ('organoid.biomaterial_core.biomaterial_id', '', ''),
            ('organoid.model_organ', '', ''),
            ('organoid.model_organ_part', '', ''),
            ('_entity_type', '', 'specimens'),

            ('sample.provenance.document_id',
             '',
             'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244 || b4e55fe1-7bab-44ba-a81d-3d8cb3873244'),

            ('sample.biomaterial_core.biomaterial_id', '', '1209_T || 1210_T'),

            ('sequencing_input.provenance.document_id',
             '',
             '0037c9eb-8038-432f-8d9d-13ee094e54ab || aaaaaaaa-8038-432f-8d9d-13ee094e54ab'),

            ('sequencing_input.biomaterial_core.biomaterial_id',
             '',
             '22028_5#300 || 22030_5#300'),

            ('sequencing_input_type', '', 'cell_suspension')
        ]
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        bundle = self._load_canned_bundle(bundle_fqid)
        self._index_bundle(bundle)

        # Duplicate one of the files into a minimal mock bundle to test
        # redundant file contributions from different bundles (for example due
        # to stitching)
        entity_ids = {
            '89e313db-4423-4d53-b17e-164949acfa8f',  # Supplementary file
            '67bc798b-a34a-4104-8cab-cad648471f69',  # Project
        }
        manifest = {
            ref: entry
            for ref, entry in bundle.manifest.items()
            if EntityReference.parse(ref).entity_id in entity_ids
        }
        metadata = {
            ref: copy_json(content)
            for ref, content in bundle.metadata.items()
            if EntityReference.parse(ref).entity_id in entity_ids
        }
        # This is an older bundle so there are no supplementary file links.
        # The existing links reference entities that weren't copied to the mock bundle.
        links = bundle.links
        links['links'].clear()
        new_bundle_fqid = self.bundle_fqid(uuid='b81656cf-231b-47a3-9317-10f1e501a05c',
                                           version='2000-01-01T01:00:00.000000Z')
        self._index_bundle(DSSBundle(fqid=new_bundle_fqid,
                                     manifest=manifest,
                                     metadata=metadata,
                                     links=links))

        special_fields = self._metadata_plugin.special_fields
        filters = {
            special_fields.file_uuid.name: {
                'is': [
                    '5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                    'f2b6c6f0-8d25-4aae-b255-1974cc110cfe'
                ]
            }
        }
        response = self._get_manifest(ManifestFormat.compact, filters)
        self.assertEqual(200, response.status_code)
        self._assert_tsv(expected, response)

    def test_manifest_zarr(self):
        """
        Test that when downloading a manifest with a zarr, all of the files are
        added into the manifest even if they are not listed in the service
        response.
        """
        self.maxDiff = None
        expected = [
            # Original file
            {
                'file_crc32c': '4e75003e',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zattrs',
                'file_uuid': 'c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d',
                'file_drs_uri': self._drs_uri('c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d',
                                              '2018-10-10T03:10:37.983672Z'),
                'file_azul_url': self._file_url('c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d',
                                                '2018-10-10T03:10:37.983672Z'),
                'specimen_from_organism.organ': 'brain'
            },
            # Related files from zarray store
            {
                'file_crc32c': '444a7707',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zgroup',
                'file_uuid': '54541cc5-9010-425b-9037-22e43948c97c',
                'file_drs_uri': self._drs_uri('54541cc5-9010-425b-9037-22e43948c97c',
                                              '2018-10-10T03:10:38.239541Z'),
                'file_azul_url': self._file_url('54541cc5-9010-425b-9037-22e43948c97c',
                                                '2018-10-10T03:10:38.239541Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '444a7707',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/.zgroup',
                'file_uuid': '66b8f976-6f1e-45b3-bd97-069658c3c847',
                'file_drs_uri': self._drs_uri('66b8f976-6f1e-45b3-bd97-069658c3c847',
                                              '2018-10-10T03:10:38.474167Z'),
                'file_azul_url': self._file_url('66b8f976-6f1e-45b3-bd97-069658c3c847',
                                                '2018-10-10T03:10:38.474167Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'c6ab0701',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/.zarray',
                'file_uuid': 'ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                'file_drs_uri': self._drs_uri('ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                                              '2018-10-10T03:10:38.714461Z'),
                'file_azul_url': self._file_url('ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                                                '2018-10-10T03:10:38.714461Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'cd2fd51f',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/0.0',
                'file_uuid': '0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                'file_drs_uri': self._drs_uri('0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                                              '2018-10-10T03:10:39.039270Z'),
                'file_azul_url': self._file_url('0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                                                '2018-10-10T03:10:39.039270Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'b89e6723',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/.zarray',
                'file_uuid': '136108ab-277e-47a4-acc3-1feed8fb2f25',
                'file_drs_uri': self._drs_uri('136108ab-277e-47a4-acc3-1feed8fb2f25',
                                              '2018-10-10T03:10:39.426609Z'),
                'file_azul_url': self._file_url('136108ab-277e-47a4-acc3-1feed8fb2f25',
                                                '2018-10-10T03:10:39.426609Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'caaefa77',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/0.0',
                'file_uuid': '0bef5419-739c-4a2c-aedb-43754d55d51c',
                'file_drs_uri': self._drs_uri('0bef5419-739c-4a2c-aedb-43754d55d51c',
                                              '2018-10-10T03:10:39.642846Z'),
                'file_azul_url': self._file_url('0bef5419-739c-4a2c-aedb-43754d55d51c',
                                                '2018-10-10T03:10:39.642846Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'f629ec34',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/.zarray',
                'file_uuid': '3a5f7299-1aa1-4060-9631-212c29b4d807',
                'file_drs_uri': self._drs_uri('3a5f7299-1aa1-4060-9631-212c29b4d807',
                                              '2018-10-10T03:10:39.899615Z'),
                'file_azul_url': self._file_url('3a5f7299-1aa1-4060-9631-212c29b4d807',
                                                '2018-10-10T03:10:39.899615Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '59d86b68',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/0.0',
                'file_uuid': 'a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                'file_drs_uri': self._drs_uri('a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                                              '2018-10-10T03:10:40.113268Z'),
                'file_azul_url': self._file_url('a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                                                '2018-10-10T03:10:40.113268Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/.zarray',
                'file_uuid': '68ba4711-1447-42ac-aa40-9c0e4cda1666',
                'file_drs_uri': self._drs_uri('68ba4711-1447-42ac-aa40-9c0e4cda1666',
                                              '2018-10-10T03:10:40.583439Z'),
                'file_azul_url': self._file_url('68ba4711-1447-42ac-aa40-9c0e4cda1666',
                                                '2018-10-10T03:10:40.583439Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '17a84191',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/0.0',
                'file_uuid': '27e66328-e337-4bcd-ba15-7893ecaf841f',
                'file_drs_uri': self._drs_uri('27e66328-e337-4bcd-ba15-7893ecaf841f',
                                              '2018-10-10T03:10:40.801631Z'),
                'file_azul_url': self._file_url('27e66328-e337-4bcd-ba15-7893ecaf841f',
                                                '2018-10-10T03:10:40.801631Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/.zarray',
                'file_uuid': '2ab1a516-ef36-41b6-a78f-513361658feb',
                'file_drs_uri': self._drs_uri('2ab1a516-ef36-41b6-a78f-513361658feb',
                                              '2018-10-10T03:10:40.958708Z'),
                'file_azul_url': self._file_url('2ab1a516-ef36-41b6-a78f-513361658feb',
                                                '2018-10-10T03:10:40.958708Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'bdc30523',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/0.0',
                'file_uuid': '351970aa-bc4c-405e-a274-be9e08e42e98',
                'file_drs_uri': self._drs_uri('351970aa-bc4c-405e-a274-be9e08e42e98',
                                              '2018-10-10T03:10:41.135992Z'),
                'file_azul_url': self._file_url('351970aa-bc4c-405e-a274-be9e08e42e98',
                                                '2018-10-10T03:10:41.135992Z'),
                'specimen_from_organism.organ': 'brain'
            }
        ]
        expected_keys = one(set(map(frozenset, map(dict.keys, expected))))
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(bundle_fqid)
        filters = {'fileFormat': {'is': ['matrix', 'mtx']}}
        url = self.base_url.set(path='/index/files',
                                args=dict(catalog=self.catalog,
                                          filters=json.dumps(filters)))
        response = requests.get(str(url))
        hits = response.json()['hits']
        self.assertEqual(len(hits), 1)

        format = ManifestFormat.compact
        with self.subTest(format=format):
            response = self._get_manifest(format, filters)
            self.assertEqual(200, response.status_code)
            # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
            lines = response.content.decode().splitlines()
            tsv_file = csv.DictReader(lines, delimiter='\t')
            rows = list(tsv_file)
            rows = [{k: v for k, v in row.items() if k in expected_keys} for row in rows]
            self.assertEqual(expected, rows)

        format = ManifestFormat.curl
        with self.subTest(format=format):
            response = self._get_manifest(format, filters)
            self.assertEqual(200, response.status_code)
            lines = response.content.decode().splitlines()
            file_prefix = 'output="587d74b4-1075-4bbf-b96a-4d1ede0481b2/'
            url = self.base_url.set(path='/repository/files')
            location_prefix = f'url="{str(url)}'
            curl_files = []
            urls = []
            related_urls = []
            for line in lines:
                if line.startswith(file_prefix):
                    self.assertTrue(line.endswith('"'))
                    file_name = line[len(file_prefix):-1]
                    curl_files.append(file_name)
                elif line.startswith(location_prefix):
                    self.assertTrue(line.endswith('"'))
                    url = furl(line[len(location_prefix):-1])
                    (related_urls if 'drsUri' in url.args else urls).append(url)
                else:
                    # The manifest contains a combination of line formats,
                    # we only validate `output` and `url` prefixed lines.
                    pass
            self.assertEqual(sorted([f['file_name'] for f in expected]),
                             sorted(curl_files))
            self.assertEqual(1, len(urls))
            self.assertEqual(len(expected) - 1, len(related_urls))
            expected_args = {'drsUri', 'fileName', 'requestIndex'}
            for url in related_urls:
                self.assertSetEqual(expected_args - set(url.args.keys()), set())

    def test_curl_manifest(self):
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        filters = {'fileFormat': {'is': ['pdf']}}
        response = self._get_manifest(ManifestFormat.curl, filters)
        self.assertEqual(200, response.status_code)
        base_url = str(self.base_url.set(path='/repository/files'))
        expected_body = [
            [
                f'url="{base_url}/0db87826-ea2d-422b-ba71-b15d0e4293ae'
                '?catalog=test&version=2018-09-14T12%3A33%3A47.221025Z"',
                'output="f79257a7-dfc6-46d6-ae00-ba4b25313c10/SmartSeq2_sequencing_protocol.pdf"',
                ''
            ],
            [
                f'url="{base_url}/156c15a3-3406-45d3-a25e-27179baf0c59'
                '?catalog=test&version=2018-09-14T12%3A33%3A46.866929Z"',
                'output="f79257a7-dfc6-46d6-ae00-ba4b25313c10/TissueDissociationProtocol.pdf"',
                ''
            ],
            [
                f'url="{base_url}/5f9b45af-9a26-4b16-a785-7f2d1053dd7c'
                '?catalog=test&version=2018-09-14T12%3A33%3A47.012715Z"',
                'output="f79257a7-dfc6-46d6-ae00-ba4b25313c10/SmartSeq2_RTPCR_protocol.pdf"',
                ''
            ],
        ]
        self._assert_curl(expected_body, response)

    def test_manifest_format_validation(self):
        url = self.base_url.set(path='/manifest/files',
                                args=dict(format='invalid-type'))
        response = requests.put(str(url))
        self.assertEqual(400, response.status_code, response.content)

    def test_manifest_filter_validation(self):
        url = self.base_url.set(path='/manifest/files',
                                args=dict(format='compact',
                                          filters=dict(fileFormat=['pdf'])))
        response = requests.put(str(url))
        self.assertEqual(400, response.status_code, response.content)

    @patch_config('enable_bundle_notifications', True)
    def test_content_disposition_header_with_notifications_enabled(self) -> None:
        self._test_content_disposition_header()

    @patch_config('enable_bundle_notifications', False)
    def test_content_disposition_header_with_notifications_disabled(self) -> None:
        self._test_content_disposition_header()

    def _test_content_disposition_header(self):
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        with patch.object(manifest_service, 'datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(1985, 10, 25, 1, 21)
            for format in [ManifestFormat.compact]:
                source_hash = '4bc67e84-4873-591f-b524-a5fe4ec215eb'
                cases = [
                    # For a single project, the content disposition file name should
                    # be the project name followed by the date and time
                    (
                        {'project': {'is': ['Single of human pancreas']}},
                        'Single of human pancreas 1985-10-25 01.21'
                        if config.enable_bundle_notifications else
                        'Single of human pancreas 1985-10-25 01.21'
                    ),
                    # In all other cases, the standard content disposition file name
                    # should be "hca-manifest-" followed by the manifest key,
                    # a pair of deterministically derived v5 UUIDs.
                    (
                        {'project': {'is': ['Single of human pancreas', 'Mouse Melanoma']}},
                        'hca-manifest-89bc9973-de91-5fc4-9c6a-8c1f547d45c6.' + source_hash
                        if config.enable_bundle_notifications else
                        'hca-manifest-e639622e-55b5-597e-907d-e28ceca3357e.' + source_hash
                    ),
                    (
                        {},
                        'hca-manifest-832a257c-5540-567b-bcb6-260d2e374508.' + source_hash
                        if config.enable_bundle_notifications else
                        'hca-manifest-57a7eb28-d918-5a62-9462-36b53b1ed111.' + source_hash
                    )
                ]
                for filters, expected_name in cases:
                    with self.subTest(filters=filters, format=format):
                        manifest, num_partitions = self._get_manifest_object(format, filters)
                        self.assertFalse(manifest.was_cached)
                        self.assertEqual(1, num_partitions)
                        url = furl(self._service.get_manifest_url(manifest))
                        expected_cd = f'attachment;filename="{expected_name}.tsv"'
                        actual_cd = url.args['response-content-disposition']
                        self.assertEqual(expected_cd, actual_cd)

    def test_verbatim_jsonl_manifest(self):
        response = self._get_manifest(ManifestFormat.verbatim_jsonl, {})
        self.assertEqual(200, response.status_code)
        path = ['verbatim', 'jsonl', 'hca', 'manifest.json']
        # FIXME: Some replicas are still missing for HCA
        #        https://github.com/DataBiosphere/azul/issues/6597
        expected = self._load_canned_manifest(*path)
        self._assert_jsonl(expected, response)

    def test_verbatim_pfb_manifest(self):
        response = self._get_manifest(ManifestFormat.verbatim_pfb, filters={})
        self.assertEqual(200, response.status_code)
        # FIXME: Some replicas are still missing for HCA
        #        https://github.com/DataBiosphere/azul/issues/6597
        canned_pfb = self._load_canned_pfb('verbatim', 'pfb', 'hca')
        expected_schema, expected_entities = canned_pfb
        self._assert_pfb(expected_schema, expected_entities, response)


class TestManifestCache(DCP1ManifestTestCase):

    @patch.object(StorageService, '_time_until_object_expires')
    def test_metadata_cache_expiration(self, _time_until_object_expires: MagicMock):
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)

        def test(expiration: int | None) -> list[str]:
            if expiration is not None:
                _time_until_object_expires.return_value = expiration
            filters = {'projectId': {'is': ['67bc798b-a34a-4104-8cab-cad648471f69']}}
            from azul.service.manifest_service import (
                log as service_log,
            )
            with self.assertLogs(logger=service_log, level='INFO') as logs:
                response = self._get_manifest(ManifestFormat.compact, filters)
                self.assertEqual(200, response.status_code)
            if expiration is None:
                _time_until_object_expires.assert_not_called()
            _time_until_object_expires.reset_mock()
            return logs.output

        # On the first request the cached manifest doesn't exist yet
        logs = test(expiration=None)
        self.assertTrue(any('Cached manifest not found' in message
                            for message in logs))

        # If the cached manifest has a long time till it expires then no log
        # message expected
        logs = test(expiration=3600)
        self.assertFalse(any('Cached manifest' in message
                             for message in logs))

        # If the cached manifest has a short time till it expires then a log
        # message is expected
        logs = test(expiration=30)
        self.assertTrue(any('Cached manifest is about to expire' in message
                            for message in logs))

    @patch.object(StorageService, '_time_until_object_expires')
    def test_compact_metadata_cache(self, _time_until_object_expires: MagicMock):
        self.maxDiff = None
        bundle_fqids = {
            '67bc798b-a34a-4104-8cab-cad648471f69':
                self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                 version='2018-09-14T13:33:14.453337Z'),
            '6615efae-fca8-4dd2-a223-9cfcf30fe94d':
                self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                 version='2018-09-14T13:33:14.453337Z')
        }
        for i, (project_id, bundle_fqid) in enumerate(bundle_fqids.items()):
            self._index_canned_bundle(bundle_fqid)
            file_names = defaultdict(list)
            for j in range(2):
                for filter_project_id in bundle_fqids.keys():
                    # We can only get a cache miss for the first of two the
                    # requests using the same filter (j==2). After indexing the
                    # first bundle, the first request for either filter will
                    # produce a miss. After indexing the second bundle, only the
                    # filter for the project of that second bundle will produce
                    # a miss. That's because indexing the second bundle won't
                    # affect the content hash of the manifest filtered by
                    # project of the first bundle. That manifest is empty.
                    cache_miss = j == 0 and (
                        i == 0
                        or i == 1 and project_id == filter_project_id
                    )
                    _time_until_object_expires.return_value = None if cache_miss else 3600
                    with self.subTest(bundle_fqid=bundle_fqid.uuid[0:8],
                                      cache_miss=cache_miss,
                                      filter_project_id=filter_project_id[0:8]):
                        filters = {'projectId': {'is': [filter_project_id]}}
                        response = self._get_manifest(ManifestFormat.compact, filters=filters)
                        self.assertEqual(200, response.status_code)
                        if cache_miss:
                            _time_until_object_expires.assert_not_called()
                        else:
                            _time_until_object_expires.assert_called_once()
                        _time_until_object_expires.reset_mock()
                        name = 'Content-Disposition'
                        value = response.headers[name]
                        value, params = parse_header(name, value)
                        self.assertEqual('attachment', value)
                        file_names[filter_project_id].append(params['filename'])
            with self.subTest(bundle_fqid=bundle_fqid.uuid[0:8]):
                self.assertEqual(file_names.keys(), bundle_fqids.keys())
                # The manifest for the current project should have a custom file
                # name instead of the generic one. The manifest for the other
                # project will have a generic name, if its empty because its
                # bundle hasn't been indexed yet.
                self.assertFalse(any(f.startswith('hca-') for f in file_names[project_id]))
                other_project_id = one(p for p in bundle_fqids.keys() if p != project_id)
                generic_names = (f.startswith('hca-') for f in file_names[other_project_id])
                if i == 0:
                    self.assertTrue(all(generic_names))
                else:
                    self.assertFalse(any(generic_names))
                self.assertEqual([2, 2], list(map(len, file_names.values())))
                self.assertEqual([1, 1], list(map(len, map(set, file_names.values()))))

    @patch_config('enable_bundle_notifications', True)
    def test_hash_validity_with_notifications_enabled(self) -> None:
        self._test_hash_validity()

    @patch_config('enable_bundle_notifications', False)
    def test_hash_validity_with_notifications_disabled(self) -> None:
        self._test_hash_validity()

    def _test_hash_validity(self):
        self.maxDiff = None
        bundles_by_project = {
            '67bc798b-a34a-4104-8cab-cad648471f69':
                self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                 version='2018-09-14T13:33:14.453337Z'),
            '6615efae-fca8-4dd2-a223-9cfcf30fe94d':
                self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                 version='2018-09-14T13:33:14.453337Z'),
            '091cf39b-01bc-42e5-9437-f419a66c8a45':
                self.bundle_fqid(uuid='cfab8304-dc9f-439e-af29-f8eb75b0729d',
                                 version='2019-07-18T21:28:20.595913Z'),
        }
        projects, bundles = zip(*bundles_by_project.items())
        self._index_canned_bundle(bundles[0])
        filters = self._filters(cast(FiltersJSON, {
            'projectId': {
                'is': [projects[0], projects[1]]
            }
        }))
        service = ManifestService(file_url_func=self._controller._file_url)

        def manifest_generator(format: ManifestFormat) -> ManifestGenerator:
            generator_cls = ManifestGenerator.cls_for_format(format)
            return generator_cls(service, self.catalog, filters)

        keys = [{}, {}]

        for format in ManifestFormat:
            with self.subTest('First bundle indexed', format=format):
                # A manifest for a filter matching files in the first bundle …
                generator = manifest_generator(format)
                manifest_key = generator.manifest_key()
                # … should remain cached …
                self.assertEqual(manifest_key, generator.manifest_key())
                keys[0][format] = manifest_key

        # … until a new bundle with files also matching the filter is indexed.
        self._index_canned_bundle(bundles[1])
        for format in ManifestFormat:
            with self.subTest('Second bundle indexed', format=format):
                generator = manifest_generator(format)
                manifest_key = generator.manifest_key()
                # The updated manifest is cached under a different key.
                self.assertNotEqual(keys[0][format], manifest_key)
                keys[1][format] = manifest_key

        # After indexing a bundle with files that don't match the filter, the
        # cached manifest remains valid.
        self._index_canned_bundle(bundles[2])
        for format in ManifestFormat:
            with self.subTest('Unrelated bundle indexed', format=format):
                generator = manifest_generator(format)
                manifest_key = generator.manifest_key()
                self.assertEqual(keys[1][format], manifest_key)

    @patch.object(StorageService, '_time_until_object_expires')
    def test_get_cached_manifest(self, _time_until_object_expires: MagicMock):
        format = ManifestFormat.curl
        filters = {}

        # Prime the cache
        manifest, _ = self._get_manifest_object(format=format, filters=filters)
        self.assertFalse(manifest.was_cached)
        manifest_key = manifest.manifest_key
        _time_until_object_expires.assert_not_called()

        # Simulate a valid cached manifest
        _time_until_object_expires.return_value = 3000
        filters = self._filters(filters)
        cached_manifest_1 = self._service.get_cached_manifest(format=format,
                                                              catalog=manifest_key.catalog,
                                                              filters=filters)
        self.assertTrue(cached_manifest_1.was_cached)
        _time_until_object_expires.assert_called_once()
        _time_until_object_expires.reset_mock()
        # The `was_cached` and `location` properties should be the only
        # differences. The `location` is a signed S3 URL that depends on
        # the current time. If both manifest where created in different
        # seconds, the signed URL is going to have a different expiration.
        manifest = attrs.evolve(manifest,
                                was_cached=True,
                                object_key=cached_manifest_1.object_key)
        self.assertEqual(manifest, cached_manifest_1)
        cached_manifest_2 = self._service.get_cached_manifest_with_key(manifest_key)
        cached_manifest_1 = attrs.evolve(cached_manifest_1,
                                         object_key=cached_manifest_2.object_key)
        self.assertEqual(cached_manifest_1, cached_manifest_2)
        _time_until_object_expires.assert_called_once()
        _time_until_object_expires.reset_mock()

        # Simulate an expired cached manifest
        _time_until_object_expires.return_value = 30
        with self.assertRaises(CachedManifestNotFound) as e:
            self._service.get_cached_manifest(format=format,
                                              catalog=manifest_key.catalog,
                                              filters=filters)
        self.assertEqual(manifest_key, e.exception.manifest_key)
        _time_until_object_expires.assert_called_once()
        _time_until_object_expires.reset_mock()
        with self.assertRaises(CachedManifestNotFound) as e:
            self._service.get_cached_manifest_with_key(manifest_key)
        self.assertEqual(manifest_key, e.exception.manifest_key)
        _time_until_object_expires.assert_called_once()
        _time_until_object_expires.reset_mock()


class TestManifestResponse(DCP1ManifestTestCase):

    @patch.dict(os.environ, AZUL_PRIVATE_API='0')
    @patch.object(ManifestService, 'get_cached_manifest')
    @patch.object(ManifestService, 'get_cached_manifest_with_key')
    @patch.object(ManifestService, 'sign_manifest_key')
    @patch.object(ManifestService, 'verify_manifest_key')
    @patch.object(ManifestService, 'get_manifest_url')
    def test_manifest(self,
                      get_manifest_url,
                      verify_manifest_key,
                      sign_manifest_key,
                      get_cached_manifest_with_key,
                      get_cached_manifest):
        """
        Verify the response from manifest endpoints for all manifest formats
        """

        def test(*, format: ManifestFormat, fetch: bool, url: Optional[furl] = None):
            object_url = furl('https://url.to.manifest?foo=bar')
            default_file_name = 'some_object_key.csv'
            manifest_key = ManifestKey(catalog=self.catalog,
                                       format=format,
                                       manifest_hash=UUID('d2b0ce3c-46f0-57fe-b9d4-2e38d8934fd4'),
                                       source_hash=UUID('77936747-5968-588e-809f-af842d6be9e0'))
            signed_manifest_key = SignedManifestKey(value=manifest_key, signature=b'123')
            sign_manifest_key.return_value = signed_manifest_key
            verify_manifest_key.return_value = manifest_key
            manifest = Manifest(object_key='key/of/manifest',
                                was_cached=False,
                                format=format,
                                manifest_key=manifest_key,
                                file_name=default_file_name)
            get_cached_manifest.return_value = manifest
            get_cached_manifest_with_key.return_value = manifest
            get_manifest_url.return_value = object_url
            args = dict(catalog=self.catalog,
                        format=format.value,
                        filters='{}')
            path = ['manifest', 'files']
            if fetch and format is ManifestFormat.curl:
                expected_url = self.base_url.set(path=[*path, signed_manifest_key.encode()])
                expected_url_for_bash = expected_url
            else:
                expected_url = object_url
                expected_url_for_bash = sq(str(expected_url))
            if format is ManifestFormat.curl:
                manifest_options = '--location --fail'
                file_options = '--retry 15 --retry-delay 10'
                expected = {
                    'cmd.exe': f'curl.exe {manifest_options} "{expected_url}"'
                               f' | curl.exe {file_options} --config -',
                    'bash': f'curl {manifest_options} {expected_url_for_bash}'
                            f' | curl {file_options} --config -'
                }
            else:
                file_name = manifest.file_name
                options = '--location --fail --output'
                expected = {
                    'cmd.exe': f'curl.exe {options} "{file_name}" "{expected_url}"',
                    'bash': f'curl {options} {file_name} {expected_url_for_bash}'
                }
            if url is None:
                method, request_url = 'PUT', self.base_url.set(path=path, args=args)
            else:
                assert not fetch
                method, request_url = 'GET', url
            if fetch:
                request_url.path.segments.insert(0, 'fetch')
                expected = {
                    'Status': 302,
                    'Location': str(expected_url),
                    'CommandLine': expected
                }
                response = requests.request('PUT', str(request_url))
                self.assertEqual(200, response.status_code)
                self.assertEqual(expected, response.json())
                self.assertEqual('application/json', response.headers['Content-Type'])
                if format is ManifestFormat.curl:
                    test(format=format, fetch=False, url=expected_url)
            else:
                response = requests.request(method, str(request_url), allow_redirects=False)
                expected = ''.join(
                    f'\nDownload the manifest in {shell} with `curl` using:\n\n{cmd}\n'
                    for shell, cmd in expected.items()
                )
                self.assertEqual(302, response.status_code)
                self.assertEqual(expected, response.text)
                self.assertEqual(object_url, furl(response.headers['location']))
                self.assertEqual('text/plain', response.headers['Content-Type'])

        for format in self._metadata_plugin.manifest_formats:
            for fetch in True, False:
                with self.subTest(format=format, fetch=fetch):
                    test(format=format, fetch=fetch)


class TestManifestPartitioning(DCP1ManifestTestCase, DocumentCloningTestCase):

    def setUp(self):
        super().setUp()
        self._setup_document_templates()
        self._add_docs(5000)

    def test(self):
        # This is the smallest valid S3 part size
        part_size = 5 * 1024 * 1024
        with patch.object(PagedManifestGenerator, 'part_size', part_size):
            manifest, num_partitions = self._get_manifest_object(ManifestFormat.compact,
                                                                 filters={})
        url = self._service.get_manifest_url(manifest)
        content = requests.get(url).content
        self.assertGreater(num_partitions, 1)
        self.assertGreater(len(content), (num_partitions - 1) * part_size)


class AnvilManifestTestCase(ManifestTestCase, AnvilCannedBundleTestCase):

    def _digest_type(self) -> str:
        return 'md5'

    @property
    def _drs_domain(self) -> str:
        return self.mock_tdr_service_url.netloc

    @classmethod
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.duos_bundle(),
            cls.supplementary_bundle(),
            cls.primary_bundle(),
            cls.replica_bundle()
        ]

    source_id_filters: FiltersJSON = {
        'source_id': {'is': ['6c87f0e1-509d-46a4-b845-7584df39263b']}
    }

    dataset_id_filters: FiltersJSON = {
        'datasets.dataset_id': {'is': ['52ee7665-7033-63f2-a8d9-ce8e32666739']}
    }

    dataset_title_filters: FiltersJSON = {
        'datasets.title': {'is': ['ANVIL_CMG_UWASH_DS_BDIS']}
    }

    neutral_file_filters: FiltersJSON = {
        'files.is_supplementary': {'is': [True, False]}
    }

    # Whether orphans ought to be present in verbatim manifests generated with
    # the given filters.
    expect_orphans_by_filters = [
        ({}, True),
        (source_id_filters, True),
        (dataset_title_filters, True),
        (dataset_id_filters, True),
        (neutral_file_filters, False),
        ({**neutral_file_filters, **dataset_title_filters}, False),
    ]

    def _test_verbatim_pfb_manifest(self, *, enable_relations: bool):
        with patch.object(type(config),
                          'enable_verbatim_relations',
                          new=PropertyMock(return_value=enable_relations)):
            for filters, expect_orphans in self.expect_orphans_by_filters:
                with self.subTest(filters=filters):
                    expect_relations = (
                        enable_relations
                        and expect_orphans
                        and not self.source.ref.prefix.common
                    )
                    expected_manifest = self._expected_pfb_manifest(expect_orphans, expect_relations)
                    expected_schema, expected_entities = expected_manifest
                    response = self._get_manifest(ManifestFormat.verbatim_pfb, filters)
                    self.assertEqual(200, response.status_code)
                    self._assert_pfb(expected_schema, expected_entities, response)

    @cache
    def _expected_pfb_manifest(self,
                               include_orphans: bool,
                               include_relations: bool
                               ) -> tuple[JSON, JSONs]:
        canned_pfb = self._load_canned_pfb('verbatim', 'pfb', 'anvil')
        pfb_schema, pfb_entities = canned_pfb
        if not include_relations:
            for entity in pfb_entities:
                entity['relations'].clear()
        if not include_orphans:
            # To avoid dangling references, relations are only populated when
            # including orphans
            assert not include_relations
            self.assertEqual('Entity', pfb_schema['name'])
            object_field_schema = one(
                field
                for field in pfb_schema['fields']
                if field['name'] == 'object'
            )
            # The `object` field is of a union type, so the schema's `type`
            # property is an array
            schemas = object_field_schema['type']
            # The first AVRO record is the *metadata entity* in PFB terms,
            # declaring higher level constraints that can't be expressed in
            # the AVRO schema
            metadata_entity = pfb_entities[0]
            self.assertEqual('Metadata', metadata_entity['name'])
            higher_schemas = metadata_entity['object']['nodes']
            for part in [schemas, higher_schemas, pfb_entities]:
                filtered = [e for e in part if e['name'] != 'non_schema_orphan_table']
                assert len(filtered) < len(part), 'Expected to filter orphan references'
                part[:] = filtered
        return pfb_schema, pfb_entities


class TestAnvilManifests(AnvilManifestTestCase):

    def test_compact_manifest(self):
        response = self._get_manifest(ManifestFormat.compact, filters={})
        self.assertEqual(200, response.status_code)
        # The `duos_id` field is absent from manifests since there is only one
        # DUOS bundle per dataset, and that bundle only contributes to outer
        # entities of the `datasets` type, not to entities of the other types,
        # such as files, which the manifest is generated from.
        expected = [
            (
                'bundles.bundle_uuid',
                '595c469e-604d-ab34-af39-f5b9f5d61818',
                '826dea02-e274-affe-aabc-eb3db63ad068',
                '826dea02-e274-affe-aabc-eb3db63ad068'
            ),
            (
                'bundles.bundle_version',
                '2022-06-01T00:00:00.000000Z',
                '2022-06-01T00:00:00.000000Z',
                '2022-06-01T00:00:00.000000Z'
            ),
            (
                'sources.source_id',
                '6c87f0e1-509d-46a4-b845-7584df39263b',
                '6c87f0e1-509d-46a4-b845-7584df39263b',
                '6c87f0e1-509d-46a4-b845-7584df39263b'
            ),
            (
                'sources.source_spec',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot'
            ),
            (
                'datasets.document_id',
                '2370f948-2783-4eb6-afea-e022897f4dcf',
                '2370f948-2783-4eb6-afea-e022897f4dcf',
                '2370f948-2783-4eb6-afea-e022897f4dcf'
            ),
            (
                'datasets.source_datarepo_row_ids',
                'workspace_attributes:7a22b629-9d81-4e4d-9297-f9e44ed760bc',
                'workspace_attributes:7a22b629-9d81-4e4d-9297-f9e44ed760bc',
                'workspace_attributes:7a22b629-9d81-4e4d-9297-f9e44ed760bc'
            ),
            (
                'datasets.dataset_id',
                '52ee7665-7033-63f2-a8d9-ce8e32666739',
                '52ee7665-7033-63f2-a8d9-ce8e32666739',
                '52ee7665-7033-63f2-a8d9-ce8e32666739'
            ),
            (
                'datasets.consent_group',
                'DS-BDIS',
                'DS-BDIS',
                'DS-BDIS'
            ),
            (
                'datasets.data_use_permission',
                'DS-BDIS',
                'DS-BDIS',
                'DS-BDIS'
            ),
            (
                'datasets.owner',
                'Debbie Nickerson',
                'Debbie Nickerson',
                'Debbie Nickerson'
            ),
            (
                'datasets.principal_investigator',
                '',
                '',
                ''
            ),
            (
                'datasets.registered_identifier',
                'phs000693',
                'phs000693',
                'phs000693'
            ),
            (
                'datasets.title',
                'ANVIL_CMG_UWASH_DS_BDIS',
                'ANVIL_CMG_UWASH_DS_BDIS',
                'ANVIL_CMG_UWASH_DS_BDIS'
            ),
            (
                'datasets.data_modality',
                '',
                '',
                ''
            ),
            (
                'donors.document_id',
                '',
                'bfd991f2-2797-4083-972a-da7c6d7f1b2e',
                'bfd991f2-2797-4083-972a-da7c6d7f1b2e'
            ),
            (
                'donors.source_datarepo_row_ids',
                '',
                'subject:c23887a0-20c1-44e4-a09e-1c5dfdc2d0ef',
                'subject:c23887a0-20c1-44e4-a09e-1c5dfdc2d0ef'
            ),
            (
                'donors.donor_id',
                '',
                '1e2bd7e5-f45e-a391-daea-7c060be76acd',
                '1e2bd7e5-f45e-a391-daea-7c060be76acd'
            ),
            (
                'donors.organism_type',
                '',
                'redacted-ACw+6ecI',
                'redacted-ACw+6ecI'
            ),
            (
                'donors.phenotypic_sex',
                '',
                'redacted-JfQ0b3xG',
                'redacted-JfQ0b3xG'
            ),
            (
                'donors.reported_ethnicity',
                '',
                'redacted-NSkwDycK',
                'redacted-NSkwDycK'
            ),
            (
                'donors.genetic_ancestry',
                '',
                '',
                ''
            ),
            (
                'diagnoses.document_id',
                '',
                '15d85d30-ad4a-4f50-87a8-a27f59dd1b5f || 939a4bd3-86ed-4a8a-81f4-fbe0ee673461',
                '15d85d30-ad4a-4f50-87a8-a27f59dd1b5f || 939a4bd3-86ed-4a8a-81f4-fbe0ee673461'
            ),
            (
                'diagnoses.source_datarepo_row_ids',
                '',
                'subject:c23887a0-20c1-44e4-a09e-1c5dfdc2d0ef',
                'subject:c23887a0-20c1-44e4-a09e-1c5dfdc2d0ef'
            ),
            (
                'diagnoses.diagnosis_id',
                '',
                '25ff8d32-18c9-fc3e-020a-5de20d35d906 || 5ebe9bc4-a1be-0ddf-7277-b1e88276d0f6',
                '25ff8d32-18c9-fc3e-020a-5de20d35d906 || 5ebe9bc4-a1be-0ddf-7277-b1e88276d0f6'
            ),
            (
                'diagnoses.disease',
                '',
                'redacted-A61iJlLx || redacted-g50ublm/',
                'redacted-A61iJlLx || redacted-g50ublm/'
            ),
            (
                'diagnoses.diagnosis_age_unit',
                '',
                '',
                ''
            ),
            (
                'diagnoses.diagnosis_age',
                '',
                "{'gte': None, 'lte': None}",
                "{'gte': None, 'lte': None}"
            ),
            (
                'diagnoses.onset_age_unit',
                '',
                '',
                ''
            ),
            (
                'diagnoses.onset_age',
                '',
                "{'gte': None, 'lte': None}",
                "{'gte': None, 'lte': None}"
            ),
            (
                'diagnoses.phenotype',
                '',
                'redacted-acSYHZUr',
                'redacted-acSYHZUr'
            ),
            (
                'diagnoses.phenopacket',
                '',
                '',
                ''
            ),
            (
                'biosamples.document_id',
                '',
                '826dea02-e274-4ffe-aabc-eb3db63ad068',
                '826dea02-e274-4ffe-aabc-eb3db63ad068'
            ),
            (
                'biosamples.source_datarepo_row_ids',
                '',
                'sample:98048c3b-2525-4090-94fd-477de31f2608',
                'sample:98048c3b-2525-4090-94fd-477de31f2608'
            ),
            (
                'biosamples.biosample_id',
                '',
                'f9d40cf6-37b8-22f3-ce35-0dc614d2452b',
                'f9d40cf6-37b8-22f3-ce35-0dc614d2452b'
            ),
            (
                'biosamples.anatomical_site',
                '',
                '',
                ''
            ),
            (
                'biosamples.apriori_cell_type',
                '',
                'bar || foo',
                'bar || foo'
            ),
            (
                'biosamples.biosample_type',
                '',
                '',
                ''
            ),
            (
                'biosamples.disease',
                '',
                '',
                ''
            ),
            (
                'biosamples.donor_age_at_collection_unit',
                '',
                '',
                ''
            ),
            (
                'biosamples.donor_age_at_collection',
                '',
                "{'gte': None, 'lte': None}",
                "{'gte': None, 'lte': None}"
            ),
            (
                'activities.document_id',
                '',
                '1509ef40-d1ba-440d-b298-16b7c173dcd4',
                '816e364e-1193-4e5b-a91a-14e4b009157c'
            ),
            (
                'activities.source_datarepo_row_ids',
                '',
                'sequencing:d4f6c0c4-1e11-438e-8218-cfea63b8b051',
                'sequencing:a6c663c7-6f26-4ed2-af9d-48e9c709a22b'
            ),
            (
                'activities.activity_id',
                '',
                '18b3be87-e26b-4376-0d8d-c1e370e90e07',
                'a60c5138-3749-f7cb-8714-52d389ad5231'
            ),
            (
                'activities.activity_type',
                '',
                'Sequencing',
                'Sequencing'
            ),
            (
                'activities.assay_type',
                '',
                '',
                ''
            ),
            (
                'activities.data_modality',
                '',
                '',
                ''
            ),
            (
                'activities.reference_assembly',
                '',
                '',
                ''
            ),
            (
                'files.document_id',
                '6b0f6c0f-5d80-4242-accb-840921351cd5',
                '15b76f9c-6b46-433f-851d-34e89f1b9ba6',
                '3b17377b-16b1-431c-9967-e5d01fc5923f'
            ),
            (
                'files.source_datarepo_row_ids',
                'file_inventory:04ff3af2-0543-4ea6-830a-d31b957fa2ee',
                'file_inventory:81d16471-97ac-48fe-99a0-73d9ec62c2c0',
                'file_inventory:9658d94a-511d-4b49-82c3-d0cb07e0cff2'
            ),
            (
                'files.file_id',
                '1fab11f5-7eab-4318-9a58-68d8d06e0715',
                '1e269f04-4347-4188-b060-1dcc69e71d67',
                '8b722e88-8103-49c1-b351-e64fa7c6ab37'
            ),
            (
                'files.data_modality',
                '',
                '',
                ''
            ),
            (
                'files.file_format',
                '.txt',
                '.vcf.gz',
                '.bam'
            ),
            (
                'files.file_size',
                '15079345',
                '213021639',
                '3306845592'
            ),
            (
                'files.file_md5sum',
                '4bf181ad18f3640418aa1deb7623d8cc',
                'beec606ee0aa299fdf913f4259316622',
                '7cd9fd7b54a8bf380e44e93706f1fa2d'
            ),
            (
                'files.reference_assembly',
                '',
                '',
                ''
            ),
            (
                'files.file_name',
                'CCDG_13607_B01_GRM_WGS_2019-02-19_chr15.recalibrated_variants.annotated.coding.txt',
                '307500.merged.matefixed.sorted.markeddups.recal.g.vcf.gz',
                '307500.merged.matefixed.sorted.markeddups.recal.bam'
            ),
            (
                'files.is_supplementary',
                'True',
                'False',
                'False'
            ),
            (
                'files.drs_uri',
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_1fab11f5-7eab-4318-9a58-68d8d06e0715'),
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_1e269f04-4347-4188-b060-1dcc69e71d67'),
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_8b722e88-8103-49c1-b351-e64fa7c6ab37')
            ),
            (
                'files.azul_url',
                self._file_url('6b0f6c0f-5d80-4242-accb-840921351cd5', self.version),
                self._file_url('15b76f9c-6b46-433f-851d-34e89f1b9ba6', self.version),
                self._file_url('3b17377b-16b1-431c-9967-e5d01fc5923f', self.version)
            ),
            (
                'files.azul_mirror_uri',
                self._mirror_uri('4bf181ad18f3640418aa1deb7623d8cc'),
                self._mirror_uri('beec606ee0aa299fdf913f4259316622'),
                self._mirror_uri('7cd9fd7b54a8bf380e44e93706f1fa2d'),
            )
        ]
        self._assert_tsv(expected, response)

    def test_curl_manifest(self):
        file_size_1 = 15079345
        file_size_2 = 213021639
        file_size_3 = 3306845592
        cases = [-1, file_size_1, file_size_2, file_size_3]
        for i, mirror_limit in enumerate(cases, start=1):
            with self.subTest(mirror_limit=mirror_limit):
                with self._patch_mirror_limit(self.catalog, mirror_limit):
                    response = self._get_manifest(ManifestFormat.curl,
                                                  # Redundant filter to avoid caching
                                                  filters={'source_id': {'is': [self.source.ref.id] * i}})
                self.assertEqual(200, response.status_code)
                base_url = str(self.base_url.set(path='/repository/files'))
                expected_body = [
                    *iif(file_size_2 <= mirror_limit, [[
                        f'url="{base_url}/15b76f9c-6b46-433f-851d-34e89f1b9ba6' +
                        '?catalog=test&version=2022-06-01T00%3A00%3A00.000000Z"',
                        'output="826dea02-e274-affe-aabc-eb3db63ad068/' +
                        '307500.merged.matefixed.sorted.markeddups.recal.g.vcf.gz"',
                        ''
                    ]]),
                    *iif(file_size_3 <= mirror_limit, [[
                        f'url="{base_url}/3b17377b-16b1-431c-9967-e5d01fc5923f' +
                        '?catalog=test&version=2022-06-01T00%3A00%3A00.000000Z"',
                        'output="826dea02-e274-affe-aabc-eb3db63ad068/' +
                        '307500.merged.matefixed.sorted.markeddups.recal.bam"',
                        ''
                    ]]),
                    *iif(file_size_1 <= mirror_limit, [[
                        f'url="{base_url}/6b0f6c0f-5d80-4242-accb-840921351cd5' +
                        '?catalog=test&version=2022-06-01T00%3A00%3A00.000000Z"',
                        'output="595c469e-604d-ab34-af39-f5b9f5d61818/' +
                        'CCDG_13607_B01_GRM_WGS_2019-02-19_chr15.recalibrated_variants.annotated.coding.txt"',
                        ''
                    ]])
                ]
                self._assert_curl(expected_body, response)

    def test_verbatim_jsonl_manifest(self):
        base_path = ['verbatim', 'jsonl', 'anvil']
        linked_rows = self._load_canned_manifest(*base_path, 'linked.json')
        all_rows = linked_rows + self._load_canned_manifest(*base_path, 'orphans.json')
        for filters, expect_orphans in self.expect_orphans_by_filters:
            with self.subTest(filters=filters):
                response = self._get_manifest(ManifestFormat.verbatim_jsonl, filters=filters)
                self.assertEqual(200, response.status_code)
                expected_rows = all_rows if expect_orphans else linked_rows
                self._assert_jsonl(expected_rows, response)

    def test_verbatim_pfb_manifest_with_relations(self):
        self._test_verbatim_pfb_manifest(enable_relations=True)

    # Due to manifest caching, these must be separate tests
    def test_verbatim_pfb_manifest_without_relations(self):
        self._test_verbatim_pfb_manifest(enable_relations=False)

    def _test_verbatim_pfb_manifest(self, *, enable_relations: bool):
        with patch.object(type(config),
                          'enable_verbatim_relations',
                          new=PropertyMock(return_value=enable_relations)):
            for filters, expect_orphans in self.expect_orphans_by_filters:
                with self.subTest(filters=filters):
                    expect_relations = enable_relations and expect_orphans
                    expected_manifest = self._expected_pfb_manifest(expect_orphans, expect_relations)
                    expected_schema, expected_entities = expected_manifest
                    response = self._get_manifest(ManifestFormat.verbatim_pfb, filters)
                    self.assertEqual(200, response.status_code)
                    self._assert_pfb(expected_schema, expected_entities, response)

    @cache
    def _expected_pfb_manifest(self,
                               include_orphans: bool,
                               include_relations: bool
                               ) -> tuple[JSON, JSONs]:
        canned_pfb = self._load_canned_pfb('verbatim', 'pfb', 'anvil')
        pfb_schema, pfb_entities = canned_pfb
        if not include_relations:
            for entity in pfb_entities:
                entity['relations'].clear()
        if not include_orphans:
            # To avoid dangling references, relations are only populated when
            # including orphans
            assert not include_relations
            self.assertEqual('Entity', pfb_schema['name'])
            object_field_schema = one(
                field
                for field in pfb_schema['fields']
                if field['name'] == 'object'
            )
            # The `object` field is of a union type, so the schema's `type`
            # property is an array
            schemas = object_field_schema['type']
            # The first AVRO record is the *metadata entity* in PFB terms,
            # declaring higher level constraints that can't be expressed in
            # the AVRO schema
            metadata_entity = pfb_entities[0]
            self.assertEqual('Metadata', metadata_entity['name'])
            higher_schemas = metadata_entity['object']['nodes']
            for part in [schemas, higher_schemas, pfb_entities]:
                filtered = [e for e in part if e['name'] != 'non_schema_orphan_table']
                assert len(filtered) < len(part), 'Expected to filter orphan references'
                part[:] = filtered
        return pfb_schema, pfb_entities


class TestAnvilManifestsWithCommonPrefix(AnvilManifestTestCase):
    source = AnvilManifestTestCase.source.with_prefix(Prefix.parse('abc/0'))

    def test(self):
        self._test_verbatim_pfb_manifest(enable_relations=True)


class TestVerbatimJSONLManifestPartitioningBySource(DCP1ManifestTestCase):
    """
    This test covers two important cases not covered by
    test_verbatim_jsonl_manifest: the interaction between implicit and explicit
    source filters, and partitioning across multiple sources.
    """

    sources_by_bundle_uuid = {
        '3ac62c33-93e1-56b4-b857-59497f5d942d':
            DSSSourceRef(id='706cc417-9ed1-4c09-8341-0df38e374423',
                         spec=SimpleSourceSpec.parse('eggs'),
                         prefix=Prefix.parse('/1')),
        '97f0cc83-f0ac-417a-8a29-221c77debde8':
            DSSSourceRef(id='d0024443-bddf-4d3e-b4c8-6a3a1b23e8cf',
                         spec=SimpleSourceSpec.parse('bacon'),
                         prefix=Prefix.parse('/2')),
        '4b03c1ce-9df1-5cd5-a8e4-48a2fe095081':
            DSSSourceRef(id='22213a35-5c8e-4bad-bcb9-d4b7740c7165',
                         spec=SimpleSourceSpec.parse('sausage'),
                         prefix=Prefix.parse('/3')),
    }

    @classmethod
    def bundle_fqid(cls, *, uuid: str, version: str) -> DSSBundleFQID:
        return DSSBundleFQID(uuid=uuid,
                             version=version,
                             source=cls.sources_by_bundle_uuid[uuid])

    @classmethod
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.bundle_fqid(uuid=uuid,
                            version='2022-06-01T00:00:00.000000Z')
            for uuid in cls.sources_by_bundle_uuid.keys()
        ]

    def _filters(self, filters: FiltersJSON) -> Filters:
        return Filters(explicit=filters,
                       source_ids={
                           source.id
                           for source in self.sources_by_bundle_uuid.values()
                       })

    def test_manifest_partitioning_by_source(self):
        # We can't assert the presence of every entity from the indexed bundles
        # because some HCA entities still lack replicas.
        #
        # FIXME: Some replicas are still missing for HCA
        #        https://github.com/DataBiosphere/azul/issues/6597
        #
        def replicas_exist_for(entity_type: EntityType) -> bool:
            return entity_type in (
                'project',
                'links',
                'donor_organism',
                'specimen_from_organism'
            ) or entity_type.endswith('_file')

        bundles_by_fqid = {
            fqid: self._load_canned_bundle(fqid)
            for fqid in self.bundles()
        }
        entity_ids_by_source_id: dict[str, set[EntityID]] = {
            bundle_fqid.source.id: {bundle_fqid.uuid} | {
                ref.entity_id
                for ref in map(EntityReference.parse, bundle.metadata)
                if replicas_exist_for(ref.entity_type)
            }
            for bundle_fqid, bundle in bundles_by_fqid.items()
        }

        # The manifest partitioning depends on the invariant that sources are
        # disjunctive. It's very easy to accidentally violate this invariant
        # while setting up this test, for example by choosing canned bundles
        # that came from the same source.
        #
        assert all(starmap(
            set.isdisjoint,
            combinations(entity_ids_by_source_id.values(), 2)
        ))

        for num_sources in range(1, len(entity_ids_by_source_id) + 1):
            for source_ids in combinations(entity_ids_by_source_id, r=num_sources):
                with self.subTest(sources=source_ids):
                    filters = {'sourceId': {'is': list(source_ids)}}
                    response = self._get_manifest(ManifestFormat.verbatim_jsonl, filters)
                    manifest_rows = list(map(json.loads, response.content.decode().splitlines()))

                    def entity_id(row: JSON) -> EntityID:
                        if row['type'] == 'links':
                            return one(
                                bundle_fqid.uuid
                                for bundle_fqid, bundle in bundles_by_fqid.items()
                                if row['value']['links'] == bundle.links['links']
                            )
                        else:
                            return row['value']['provenance']['document_id']

                    actual_entity_ids = {
                        entity_id(row)
                        for row in manifest_rows
                        if replicas_exist_for(row['type'])
                    }
                    expected_entity_ids = set.union(*(
                        entity_ids_by_source_id[source_id]
                        for source_id in source_ids
                    ))
                    self.assertEqual(expected_entity_ids, actual_entity_ids)

    def test_inaccessible_source(self):
        accessible_source = list(self.sources_by_bundle_uuid.values())[0].id
        inaccessible_source = 'cafebabe-5b46-40e9-81c5-aaa7ebadf00d'
        with self.assertRaises(ForbiddenError) as e:
            filters = {'sourceId': {'is': [accessible_source, inaccessible_source]}}
            self._get_manifest(ManifestFormat.verbatim_jsonl, filters)
        expected_args = (
            'Cannot filter by inaccessible sources',
            {inaccessible_source}
        )
        self.assertEqual(expected_args, e.exception.args)


class TestPFB(CannedManifestTestCase):
    """
    Tests of terra.pfb code that don't require an ES index.
    """

    def test_terra_pfb_schema(self):
        self.maxDiff = None
        field_types = FileTransformer.field_types()
        schema = avro_pfb.pfb_schema_from_field_types(field_types)
        self._assert_pfb_schema(schema)

    def test_pfb_metadata_object(self):
        links = avro_pfb.pfb_links_from_field_types(FileTransformer.field_types())
        metadata_entity = avro_pfb.pfb_metadata_entity(links)
        field_types = FileTransformer.field_types()
        schema = avro_pfb.pfb_schema_from_field_types(field_types)
        parsed_schema = fastavro.parse_schema(cast(dict, schema))
        fastavro.validate(metadata_entity, parsed_schema)

    def test_pfb_entity_id(self):
        # Terra limits ID's 254 chars
        avro_pfb.PFBEntity(id='a' * 254, name='foo', object={})
        with self.assertRaises(AssertionError) as e:
            avro_pfb.PFBEntity(id='a' * 255, name='foo', object={})
        self.assertTrue(R.caused(e.exception))
