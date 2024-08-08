from abc import (
    ABCMeta,
)
import cgi
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
    timedelta,
)
from io import (
    BytesIO,
)
import json
import os
from pathlib import (
    Path,
)
from tempfile import (
    TemporaryDirectory,
)
from typing import (
    Optional,
    cast,
)
from unittest.mock import (
    MagicMock,
    patch,
)
import unittest.result
from uuid import (
    UUID,
    uuid4,
)
from zipfile import (
    ZipFile,
)

import attrs
import fastavro
from furl import (
    furl,
)
from more_itertools import (
    chunked,
    first,
    one,
)
import requests
from requests import (
    Response,
)

from azul import (
    config,
)
from azul.collections import (
    adict,
    compose_keys,
    none_safe_tuple_key,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityReference,
)
from azul.json import (
    copy_json,
    json_hash,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins import (
    ManifestFormat,
)
from azul.plugins.repository.dss import (
    DSSBundle,
)
from azul.service import (
    Filters,
    FiltersJSON,
    manifest_service,
)
from azul.service.manifest_service import (
    BDBagManifestGenerator,
    Bundles,
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
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from indexer import (
    AnvilCannedBundleTestCase,
    DCP1CannedBundleTestCase,
)
from pfb_test_case import (
    PFBTestCase,
)
from service import (
    DocumentCloningTestCase,
    StorageServiceTestCase,
    WebServiceTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class ManifestTestCase(WebServiceTestCase,
                       StorageServiceTestCase,
                       metaclass=ABCMeta):

    def setUp(self):
        super().setUp()
        self.addPatch(patch.object(PagedManifestGenerator, 'page_size', 1))
        self._setup_indices()
        self._setup_git_commit()

    def tearDown(self):
        self._teardown_indices()
        self._teardown_git_commit()
        super().tearDown()

    def _filters(self, filters: FiltersJSON) -> Filters:
        return Filters(explicit=filters, source_ids={self.source.id})

    def _setup_git_commit(self):
        """
        Set git variables required to derive the manifest object key
        """
        assert 'azul_git_commit' not in os.environ
        assert 'azul_git_dirty' not in os.environ
        os.environ['azul_git_commit'] = '9347432ab0da43c73409ac7fd3edfe29cf3ae678'
        os.environ['azul_git_dirty'] = 'False'

    def _teardown_git_commit(self):
        os.environ.pop('azul_git_commit')
        os.environ.pop('azul_git_dirty')

    @property
    def _service(self):
        return ManifestService(self.storage_service, self.app_module.app.file_url)

    def _get_manifest(self,
                      format: ManifestFormat,
                      filters: FiltersJSON,
                      stream=False
                      ) -> Response:
        manifest, num_partitions = self._get_manifest_object(format, filters)
        self.assertEqual(1, num_partitions)
        response = requests.get(manifest.location, stream=stream)
        # Moto doesn't support signed S3 URLs with Content-Disposition baked in,
        # so we'll retroactively inject it into the response header.
        location = furl(manifest.location)
        content_disposition = location.args.get('response-content-disposition')
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

    def _file_url(self, file_id, version):
        return str(self.base_url.set(path='/repository/files/' + file_id,
                                     args=dict(catalog=self.catalog,
                                               version=version)))

    def _drs_uri(self, file_id, version=None):
        return str(furl(scheme='drs',
                        netloc=self._drs_domain,
                        path=file_id,
                        args=adict(version=version)))

    @property
    def _drs_domain(self) -> str:
        return config.drs_domain or config.api_lambda_domain('service')


class DCP1ManifestTestCase(ManifestTestCase, DCP1CannedBundleTestCase):
    pass


class TestManifests(DCP1ManifestTestCase, PFBTestCase):

    def run(self,
            result: Optional[unittest.result.TestResult] = None
            ) -> Optional[unittest.result.TestResult]:
        # Disable caching of manifests to prevent false assertion positives
        with patch.object(ManifestService,
                          '_get_cached_manifest_file_name',
                          return_value=None):
            return super().run(result)

    _drs_domain_name = 'drs-test.lan'  # see canned PFB results

    def test_pfb_manifest(self):
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

        def to_json(records):
            # 'default' is specified to handle the conversion of datetime values
            return json.dumps(records, indent=4, sort_keys=True, default=str)

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
                    results_file = Path(__file__).parent / 'data' / 'pfb_manifest.results.json'
                    if results_file.exists():
                        with open(results_file, 'r') as f:
                            expected_records = json.load(f)
                        self.assertEqual(expected_records, json.loads(to_json(records)))
                    else:
                        with open(results_file, 'w') as f:
                            f.write(to_json(records))

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
                         manifest=cast(MutableJSONs, bundle.manifest),
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
            ('source_id', self.source.id, self.source.id),
            ('source_spec', str(self.source.spec), str(self.source.spec)),
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

            ('file_url',
             self._file_url('5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                            '2018-09-14T12:33:47.012715Z'),
             self._file_url('f2b6c6f0-8d25-4aae-b255-1974cc110cfe',
                            '2018-09-14T12:33:43.720332Z')),

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
            '89e313db-4423-4d53-b17e-164949acfa8f',  # Supplementary file (metadata)
            '5f9b45af-9a26-4b16-a785-7f2d1053dd7c',  # Supplementary file (data)
            '67bc798b-a34a-4104-8cab-cad648471f69',  # Project
        }
        manifest = [
            entry
            for entry in bundle.manifest
            if entry['uuid'] in entity_ids
        ]
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

        filters = {
            'fileId': {
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
                'file_url': self._file_url('c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d',
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
                'file_url': self._file_url('54541cc5-9010-425b-9037-22e43948c97c',
                                           '2018-10-10T03:10:38.239541Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '444a7707',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/.zgroup',
                'file_uuid': '66b8f976-6f1e-45b3-bd97-069658c3c847',
                'file_drs_uri': self._drs_uri('66b8f976-6f1e-45b3-bd97-069658c3c847',
                                              '2018-10-10T03:10:38.474167Z'),
                'file_url': self._file_url('66b8f976-6f1e-45b3-bd97-069658c3c847',
                                           '2018-10-10T03:10:38.474167Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'c6ab0701',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/.zarray',
                'file_uuid': 'ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                'file_drs_uri': self._drs_uri('ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                                              '2018-10-10T03:10:38.714461Z'),
                'file_url': self._file_url('ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                                           '2018-10-10T03:10:38.714461Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'cd2fd51f',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/0.0',
                'file_uuid': '0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                'file_drs_uri': self._drs_uri('0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                                              '2018-10-10T03:10:39.039270Z'),
                'file_url': self._file_url('0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                                           '2018-10-10T03:10:39.039270Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'b89e6723',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/.zarray',
                'file_uuid': '136108ab-277e-47a4-acc3-1feed8fb2f25',
                'file_drs_uri': self._drs_uri('136108ab-277e-47a4-acc3-1feed8fb2f25',
                                              '2018-10-10T03:10:39.426609Z'),
                'file_url': self._file_url('136108ab-277e-47a4-acc3-1feed8fb2f25',
                                           '2018-10-10T03:10:39.426609Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'caaefa77',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/0.0',
                'file_uuid': '0bef5419-739c-4a2c-aedb-43754d55d51c',
                'file_drs_uri': self._drs_uri('0bef5419-739c-4a2c-aedb-43754d55d51c',
                                              '2018-10-10T03:10:39.642846Z'),
                'file_url': self._file_url('0bef5419-739c-4a2c-aedb-43754d55d51c',
                                           '2018-10-10T03:10:39.642846Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'f629ec34',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/.zarray',
                'file_uuid': '3a5f7299-1aa1-4060-9631-212c29b4d807',
                'file_drs_uri': self._drs_uri('3a5f7299-1aa1-4060-9631-212c29b4d807',
                                              '2018-10-10T03:10:39.899615Z'),
                'file_url': self._file_url('3a5f7299-1aa1-4060-9631-212c29b4d807',
                                           '2018-10-10T03:10:39.899615Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '59d86b68',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/0.0',
                'file_uuid': 'a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                'file_drs_uri': self._drs_uri('a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                                              '2018-10-10T03:10:40.113268Z'),
                'file_url': self._file_url('a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                                           '2018-10-10T03:10:40.113268Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/.zarray',
                'file_uuid': '68ba4711-1447-42ac-aa40-9c0e4cda1666',
                'file_drs_uri': self._drs_uri('68ba4711-1447-42ac-aa40-9c0e4cda1666',
                                              '2018-10-10T03:10:40.583439Z'),
                'file_url': self._file_url('68ba4711-1447-42ac-aa40-9c0e4cda1666',
                                           '2018-10-10T03:10:40.583439Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '17a84191',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/0.0',
                'file_uuid': '27e66328-e337-4bcd-ba15-7893ecaf841f',
                'file_drs_uri': self._drs_uri('27e66328-e337-4bcd-ba15-7893ecaf841f',
                                              '2018-10-10T03:10:40.801631Z'),
                'file_url': self._file_url('27e66328-e337-4bcd-ba15-7893ecaf841f',
                                           '2018-10-10T03:10:40.801631Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/.zarray',
                'file_uuid': '2ab1a516-ef36-41b6-a78f-513361658feb',
                'file_drs_uri': self._drs_uri('2ab1a516-ef36-41b6-a78f-513361658feb',
                                              '2018-10-10T03:10:40.958708Z'),
                'file_url': self._file_url('2ab1a516-ef36-41b6-a78f-513361658feb',
                                           '2018-10-10T03:10:40.958708Z'),
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'bdc30523',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/0.0',
                'file_uuid': '351970aa-bc4c-405e-a274-be9e08e42e98',
                'file_drs_uri': self._drs_uri('351970aa-bc4c-405e-a274-be9e08e42e98',
                                              '2018-10-10T03:10:41.135992Z'),
                'file_url': self._file_url('351970aa-bc4c-405e-a274-be9e08e42e98',
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

    def test_terra_bdbag_manifest(self):
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)

        bam_b0_0_uuid, bam_b0_0_version = '51c9ad31-5888-47eb-9e0c-02f042373c4e', '2018-10-10T03:10:35.284782Z'
        bam_b0_1_uuid, bam_b0_1_version = 'b1c167da-0825-4c63-9cbc-2aada1ab367c', '2018-10-10T03:10:35.971561Z'
        fastq_b0_r1_uuid, fastq_b0_r1_version = 'c005f647-b3fb-45a8-857a-8f5e6a878ccf', '2018-10-10T02:38:11.612423Z'
        fastq_b0_r2_uuid, fastq_b0_r2_version = 'b764ce7d-3938-4451-b68c-678feebc8f2a', '2018-10-10T02:38:11.851483Z'
        fastq_b1_r1_uuid, fastq_b1_r1_version = '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb', '2018-11-02T11:33:44.698028Z'
        fastq_b1_r2_uuid, fastq_b1_r2_version = '74897eb7-0701-4e4f-9e6b-8b9521b2816b', '2018-11-02T11:33:44.450442Z'
        expected_rows = [
            {
                'entity:participant_id': '587d74b4-1075-4bbf-b96a-4d1ede0481b2.2018-09-14T133314.453337Z',
                'bundle_uuid': '587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                'bundle_version': '2018-09-14T13:33:14.453337Z',
                'source_id': self.source.id,
                'source_spec': str(self.source.spec),
                'cell_suspension__provenance__document_id': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0',
                'cell_suspension__biomaterial_core__biomaterial_id': 'Q4_DEMO-cellsus_SAMN02797092',
                'cell_suspension__estimated_cell_count': '',
                'cell_suspension__selected_cell_type': '',
                'sequencing_process__provenance__document_id': '5afa951e-1591-4bad-a4f8-2e13cbdb760c',
                'sequencing_protocol__instrument_manufacturer_model': 'Illumina HiSeq 2500',
                'sequencing_protocol__paired_end': 'True',
                'library_preparation_protocol__library_construction_approach': 'Smart-seq2',
                'library_preparation_protocol__nucleic_acid_source': 'single cell',
                'project__provenance__document_id': '6615efae-fca8-4dd2-a223-9cfcf30fe94d',
                'project__contributors__institution': 'Fake Institution',
                'project__contributors__laboratory': '',
                'project__project_core__project_short_name': 'integration/Smart-seq2/2018-10-10T02:23:36Z',
                'project__project_core__project_title': 'Q4_DEMO-Single cell RNA-seq of primary human glioblastomas',
                'project__estimated_cell_count': '',
                'specimen_from_organism__provenance__document_id': 'b5894cf5-ecdc-4ea6-a0b9-5335ab678c7a',
                'specimen_from_organism__diseases': 'glioblastoma',
                'specimen_from_organism__organ': 'brain',
                'specimen_from_organism__organ_part': 'temporal lobe',
                'specimen_from_organism__preservation_storage__preservation_method': '',
                'donor_organism__sex': 'unknown',
                'donor_organism__biomaterial_core__biomaterial_id': 'Q4_DEMO-donor_MGH30',
                'donor_organism__provenance__document_id': '242e38d2-c975-47ee-800a-6645b47e92d2',
                'donor_organism__genus_species': 'Homo sapiens',
                'donor_organism__development_stage': 'adult',
                'donor_organism__diseases': '',
                'donor_organism__organism_age': '',
                'cell_line__provenance__document_id': '',
                'cell_line__biomaterial_core__biomaterial_id': '',
                'organoid__provenance__document_id': '',
                'organoid__biomaterial_core__biomaterial_id': '',
                'organoid__model_organ': '',
                'organoid__model_organ_part': '',
                '_entity_type': 'specimens',
                'sample__provenance__document_id': 'b5894cf5-ecdc-4ea6-a0b9-5335ab678c7a',
                'sample__biomaterial_core__biomaterial_id': 'Q4_DEMO-sample_SAMN02797092',
                'sequencing_input__provenance__document_id': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0',
                'sequencing_input__biomaterial_core__biomaterial_id': 'Q4_DEMO-cellsus_SAMN02797092',
                'sequencing_input_type': 'cell_suspension',
                '__bam_0__file_document_id': 'a5acdc07-18bf-4c06-b212-2b36e52173ef',
                '__bam_0__file_type': 'analysis_file',
                '__bam_0__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_qc.bam',
                '__bam_0__file_format': 'bam',
                '__bam_0__read_index': '',
                '__bam_0__file_size': '550597',
                '__bam_0__file_uuid': bam_b0_0_uuid,
                '__bam_0__file_version': bam_b0_0_version,
                '__bam_0__file_crc32c': '700bd519',
                '__bam_0__file_sha256': 'e3cd90d79f520c0806dddb1ca0c5a11fbe26ac0c0be983ba5098d6769f78294c',
                '__bam_0__file_content_type': 'application/gzip; dcp-type=data',
                '__bam_0__file_drs_uri': self._drs_uri(bam_b0_0_uuid, bam_b0_0_version),
                '__bam_0__file_url': self._file_url(bam_b0_0_uuid, bam_b0_0_version),
                '__bam_1__file_document_id': '14d63962-7cd3-43fc-a4d6-dc8f761c9ebd',
                '__bam_1__file_type': 'analysis_file',
                '__bam_1__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_rsem.bam',
                '__bam_1__file_format': 'bam',
                '__bam_1__read_index': '',
                '__bam_1__file_size': '3752733',
                '__bam_1__file_uuid': bam_b0_1_uuid,
                '__bam_1__file_version': bam_b0_1_version,
                '__bam_1__file_crc32c': '3d94b063',
                '__bam_1__file_sha256': 'f25053412d65429cefc0157c0d18ae12d4bf4c4113a6af7a1820b62246c075a4',
                '__bam_1__file_content_type': 'application/gzip; dcp-type=data',
                '__bam_1__file_drs_uri': self._drs_uri(bam_b0_1_uuid, bam_b0_1_version),
                '__bam_1__file_url': self._file_url(bam_b0_1_uuid, bam_b0_1_version),
                '__fastq_read1__file_document_id': '5f0cdf49-aabe-40f4-8af3-033115805bb0',
                '__fastq_read1__file_type': 'sequence_file',
                '__fastq_read1__file_name': 'R1.fastq.gz',
                '__fastq_read1__file_format': 'fastq.gz',
                '__fastq_read1__read_index': 'read1',
                '__fastq_read1__file_size': '125191',
                '__fastq_read1__file_uuid': fastq_b0_r1_uuid,
                '__fastq_read1__file_version': fastq_b0_r1_version,
                '__fastq_read1__file_crc32c': '4ef74578',
                '__fastq_read1__file_sha256': 'fe6d4fdfea2ff1df97500dcfe7085ac3abfb760026bff75a34c20fb97a4b2b29',
                '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read1__file_url': self._file_url(fastq_b0_r1_uuid, fastq_b0_r1_version),
                '__fastq_read1__file_drs_uri': self._drs_uri(fastq_b0_r1_uuid, fastq_b0_r1_version),
                '__fastq_read2__file_document_id': '74c8c730-139e-40a5-b77e-f46088fa4d95',
                '__fastq_read2__file_type': 'sequence_file',
                '__fastq_read2__file_name': 'R2.fastq.gz',
                '__fastq_read2__file_format': 'fastq.gz',
                '__fastq_read2__read_index': 'read2',
                '__fastq_read2__file_size': '130024',
                '__fastq_read2__file_uuid': fastq_b0_r2_uuid,
                '__fastq_read2__file_version': fastq_b0_r2_version,
                '__fastq_read2__file_crc32c': '69987b3e',
                '__fastq_read2__file_sha256': 'c305bee37b3c3735585e11306272b6ab085f04cd22ea8703957b4503488cfeba',
                '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read2__file_url': self._file_url(fastq_b0_r2_uuid, fastq_b0_r2_version),
                '__fastq_read2__file_drs_uri': self._drs_uri(fastq_b0_r2_uuid, fastq_b0_r2_version),
            },
            {
                'entity:participant_id': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d.2018-11-02T113344.698028Z',
                'bundle_uuid': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                'bundle_version': '2018-11-02T11:33:44.698028Z',
                'source_id': self.source.id,
                'source_spec': str(self.source.spec),
                'cell_suspension__provenance__document_id': '412898c5-5b9b-4907-b07c-e9b89666e204',
                'cell_suspension__biomaterial_core__biomaterial_id': 'GSM2172585 1',
                'cell_suspension__estimated_cell_count': '1',
                'cell_suspension__selected_cell_type': '',
                'sequencing_process__provenance__document_id': '771ddaf6-3a4f-4314-97fe-6294ff8e25a4',
                'sequencing_protocol__instrument_manufacturer_model': 'Illumina NextSeq 500',
                'sequencing_protocol__paired_end': 'True',
                'library_preparation_protocol__library_construction_approach': 'Smart-seq2',
                'library_preparation_protocol__nucleic_acid_source': 'single cell',
                'project__provenance__document_id': 'e8642221-4c2c-4fd7-b926-a68bce363c88',
                'project__contributors__institution': 'Farmers Trucks || University',
                'project__contributors__laboratory': 'John Dear',
                'project__project_core__project_short_name': 'Single of human pancreas',
                'project__project_core__project_title': 'Single cell transcriptome patterns.',
                'project__estimated_cell_count': '',
                'specimen_from_organism__provenance__document_id': 'a21dc760-a500-4236-bcff-da34a0e873d2',
                'specimen_from_organism__diseases': 'normal',
                'specimen_from_organism__organ': 'pancreas',
                'specimen_from_organism__organ_part': 'islet of Langerhans',
                'specimen_from_organism__preservation_storage__preservation_method': '',
                'donor_organism__sex': 'female',
                'donor_organism__biomaterial_core__biomaterial_id': 'DID_scRSq06',
                'donor_organism__provenance__document_id': '7b07b9d0-cc0e-4098-9f64-f4a569f7d746',
                'donor_organism__genus_species': 'Australopithecus',
                'donor_organism__development_stage': '',
                'donor_organism__diseases': 'normal',
                'donor_organism__organism_age': '38 year',
                'cell_line__provenance__document_id': '',
                'cell_line__biomaterial_core__biomaterial_id': '',
                'organoid__provenance__document_id': '',
                'organoid__biomaterial_core__biomaterial_id': '',
                'organoid__model_organ': '',
                'organoid__model_organ_part': '',
                '_entity_type': 'specimens',
                'sample__provenance__document_id': 'a21dc760-a500-4236-bcff-da34a0e873d2',
                'sample__biomaterial_core__biomaterial_id': 'DID_scRSq06_pancreas',
                'sequencing_input__provenance__document_id': '412898c5-5b9b-4907-b07c-e9b89666e204',
                'sequencing_input__biomaterial_core__biomaterial_id': 'GSM2172585 1',
                'sequencing_input_type': 'cell_suspension',
                '__bam_0__file_document_id': '',
                '__bam_0__file_type': '',
                '__bam_0__file_name': '',
                '__bam_0__file_format': '',
                '__bam_0__read_index': '',
                '__bam_0__file_size': '',
                '__bam_0__file_uuid': '',
                '__bam_0__file_version': '',
                '__bam_0__file_crc32c': '',
                '__bam_0__file_sha256': '',
                '__bam_0__file_content_type': '',
                '__bam_0__file_drs_uri': '',
                '__bam_0__file_url': '',
                '__bam_1__file_document_id': '',
                '__bam_1__file_type': '',
                '__bam_1__file_name': '',
                '__bam_1__file_format': '',
                '__bam_1__read_index': '',
                '__bam_1__file_size': '',
                '__bam_1__file_uuid': '',
                '__bam_1__file_version': '',
                '__bam_1__file_crc32c': '',
                '__bam_1__file_sha256': '',
                '__bam_1__file_content_type': '',
                '__bam_1__file_drs_uri': '',
                '__bam_1__file_url': '',
                '__fastq_read1__file_document_id': '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb',
                '__fastq_read1__file_type': 'sequence_file',
                '__fastq_read1__file_name': 'SRR3562915_1.fastq.gz',
                '__fastq_read1__file_format': 'fastq.gz',
                '__fastq_read1__read_index': 'read1',
                '__fastq_read1__file_size': '195142097',
                '__fastq_read1__file_uuid': fastq_b1_r1_uuid,
                '__fastq_read1__file_version': fastq_b1_r1_version,
                '__fastq_read1__file_crc32c': '1d998e49',
                '__fastq_read1__file_sha256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
                '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read1__file_drs_uri': self._drs_uri(fastq_b1_r1_uuid, fastq_b1_r1_version),
                '__fastq_read1__file_url': self._file_url(fastq_b1_r1_uuid, fastq_b1_r1_version),
                '__fastq_read2__file_document_id': '70d1af4a-82c8-478a-8960-e9028b3616ca',
                '__fastq_read2__file_type': 'sequence_file',
                '__fastq_read2__file_name': 'SRR3562915_2.fastq.gz',
                '__fastq_read2__file_format': 'fastq.gz',
                '__fastq_read2__read_index': 'read2',
                '__fastq_read2__file_size': '190330156',
                '__fastq_read2__file_uuid': fastq_b1_r2_uuid,
                '__fastq_read2__file_version': fastq_b1_r2_version,
                '__fastq_read2__file_crc32c': '54bb9c82',
                '__fastq_read2__file_sha256': '465a230aa127376fa641f8b8f8cad3f08fef37c8aafc67be454f0f0e4e63d68d',
                '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read2__file_drs_uri': self._drs_uri(fastq_b1_r2_uuid, fastq_b1_r2_version),
                '__fastq_read2__file_url': self._file_url(fastq_b1_r2_uuid, fastq_b1_r2_version),
            }
        ]
        filters = {'fileFormat': {'is': ['bam', 'fastq.gz', 'fastq']}}
        rows, fieldnames = self._extract_bdbag_response(filters)

        # The order in which the rows appear in the TSV is ultimately
        # driven by the order in which the documents are coming back
        # from the `files` index in Elasticsearch. To get a consistent
        # ordering of the ES response, we could apply a sort but doing
        # so slows down the scroll API which we use for manifests,
        # because manifest responses need exhaust the index. Instead,
        # we do comparison here that's insensitive of the row ordering.
        # We'll assert the column ordering independently below.

        def sort_rows(rows: list[dict[str, str]]) -> list[list[tuple[str, str]]]:
            return sorted(map(sorted, map(dict.items, rows)))

        self.assertEqual(sort_rows(expected_rows), sort_rows(rows))
        self.assertEqual([
            'entity:participant_id',
            'bundle_uuid',
            'bundle_version',
            'source_id',
            'source_spec',
            'cell_suspension__provenance__document_id',
            'cell_suspension__biomaterial_core__biomaterial_id',
            'cell_suspension__estimated_cell_count',
            'cell_suspension__selected_cell_type',
            'sequencing_process__provenance__document_id',
            'sequencing_protocol__instrument_manufacturer_model',
            'sequencing_protocol__paired_end',
            'library_preparation_protocol__library_construction_approach',
            'library_preparation_protocol__nucleic_acid_source',
            'project__provenance__document_id',
            'project__contributors__institution',
            'project__contributors__laboratory',
            'project__project_core__project_short_name',
            'project__project_core__project_title',
            'project__estimated_cell_count',
            'specimen_from_organism__provenance__document_id',
            'specimen_from_organism__diseases',
            'specimen_from_organism__organ',
            'specimen_from_organism__organ_part',
            'specimen_from_organism__preservation_storage__preservation_method',
            'donor_organism__sex',
            'donor_organism__biomaterial_core__biomaterial_id',
            'donor_organism__provenance__document_id',
            'donor_organism__genus_species',
            'donor_organism__development_stage',
            'donor_organism__diseases',
            'donor_organism__organism_age',
            'cell_line__provenance__document_id',
            'cell_line__biomaterial_core__biomaterial_id',
            'organoid__provenance__document_id',
            'organoid__biomaterial_core__biomaterial_id',
            'organoid__model_organ',
            'organoid__model_organ_part',
            '_entity_type',
            'sample__provenance__document_id',
            'sample__biomaterial_core__biomaterial_id',
            'sequencing_input__provenance__document_id',
            'sequencing_input__biomaterial_core__biomaterial_id',
            'sequencing_input_type',
            '__bam_0__file_document_id',
            '__bam_0__file_type',
            '__bam_0__file_name',
            '__bam_0__file_format',
            '__bam_0__read_index',
            '__bam_0__file_size',
            '__bam_0__file_uuid',
            '__bam_0__file_version',
            '__bam_0__file_crc32c',
            '__bam_0__file_sha256',
            '__bam_0__file_content_type',
            '__bam_0__file_drs_uri',
            '__bam_0__file_url',
            '__bam_1__file_document_id',
            '__bam_1__file_type',
            '__bam_1__file_name',
            '__bam_1__file_format',
            '__bam_1__read_index',
            '__bam_1__file_size',
            '__bam_1__file_uuid',
            '__bam_1__file_version',
            '__bam_1__file_crc32c',
            '__bam_1__file_sha256',
            '__bam_1__file_content_type',
            '__bam_1__file_drs_uri',
            '__bam_1__file_url',
            '__fastq_read1__file_document_id',
            '__fastq_read1__file_type',
            '__fastq_read1__file_name',
            '__fastq_read1__file_format',
            '__fastq_read1__read_index',
            '__fastq_read1__file_size',
            '__fastq_read1__file_uuid',
            '__fastq_read1__file_version',
            '__fastq_read1__file_crc32c',
            '__fastq_read1__file_sha256',
            '__fastq_read1__file_content_type',
            '__fastq_read1__file_drs_uri',
            '__fastq_read1__file_url',
            '__fastq_read2__file_document_id',
            '__fastq_read2__file_type',
            '__fastq_read2__file_name',
            '__fastq_read2__file_format',
            '__fastq_read2__read_index',
            '__fastq_read2__file_size',
            '__fastq_read2__file_uuid',
            '__fastq_read2__file_version',
            '__fastq_read2__file_crc32c',
            '__fastq_read2__file_sha256',
            '__fastq_read2__file_content_type',
            '__fastq_read2__file_drs_uri',
            '__fastq_read2__file_url',
        ], fieldnames)

    def _extract_bdbag_response(self,
                                filters: FiltersJSON
                                ) -> tuple[list[dict[str, str]], list[str]]:
        with TemporaryDirectory() as zip_dir:
            response = self._get_manifest(ManifestFormat.terra_bdbag, filters, stream=True)
            self.assertEqual(200, response.status_code)
            with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
                self.assertTrue(all('manifest' == first(name.split('/')) for name in zip_fh.namelist()))
                zip_fname = os.path.dirname(first(zip_fh.namelist()))
            with open(os.path.join(zip_dir, zip_fname, 'data', 'participants.tsv'), 'r') as fh:
                reader = csv.DictReader(fh, delimiter='\t')
                return list(reader), list(reader.fieldnames)

    def test_bdbag_manifest_remove_redundant_entries(self):
        """
        Test BDBagManifestGenerator._remove_redundant_entries() directly with a
        large set of sample data
        """
        now = datetime.utcnow()

        def v(i):
            return (now + timedelta(seconds=i)).strftime('%Y-%m-%dT%H%M%S.000000Z')

        def u():
            return str(uuid4())

        bundles: Bundles = {}
        # Create sample data that can be passed to
        # BDBagManifestGenerator._remove_redundant_entries()
        # Each entry is given a timestamp 1 second later than the previous entry
        # to have variety in the entries
        num_of_entries = 100_000
        for i in range(num_of_entries):
            bundle_fqid = u(), v(i)
            bundles[bundle_fqid] = {
                'bam': [
                    {'file': {'file_uuid': u()}},
                    {'file': {'file_uuid': u()}},
                    {'file': {'file_uuid': u()}}
                ]
            }
        fqids = {}
        keys = list(bundles.keys())
        # Add an entry with the same set of files as another entry though with a
        # later timestamp
        bundle_fqid = u(), v(num_of_entries + 1)
        # An arbitrary entry in bundles
        bundles[bundle_fqid] = deepcopy(bundles[keys[100]])
        # With same set of files [1] will be removed (earlier timestamp)
        fqids['equal'] = bundle_fqid, keys[100]
        # Add an entry with a subset of files compared to another entry
        bundle_fqid = u(), v(num_of_entries + 2)
        # An arbitrary entry in bundles
        bundles[bundle_fqid] = deepcopy(bundles[keys[200]])
        del bundles[bundle_fqid]['bam'][2]
        # [0] will be removed as it has a subset of files that [1] has
        fqids['subset'] = bundle_fqid, keys[200]
        # Add an entry with a superset of files compared to another entry
        bundle_fqid = u(), v(num_of_entries + 3)
        # An arbitrary entry in bundles
        bundles[bundle_fqid] = deepcopy(bundles[keys[300]])
        bundles[bundle_fqid]['bam'].append({'file': {'file_uuid': u()}})
        # [0] has a superset of files that [0] has so [0] wil be removed
        fqids['superset'] = bundle_fqid, keys[300]

        # the generated entries plus 3 redundant entries
        self.assertEqual(len(bundles), num_of_entries + 3)
        self.assertIn(fqids['equal'][0], bundles)
        self.assertIn(fqids['equal'][1], bundles)
        self.assertIn(fqids['subset'][0], bundles)
        self.assertIn(fqids['subset'][1], bundles)
        self.assertIn(fqids['superset'][0], bundles)
        self.assertIn(fqids['superset'][1], bundles)

        BDBagManifestGenerator._remove_redundant_entries(bundles)

        # 3 redundant entries removed
        self.assertEqual(len(bundles), num_of_entries)
        # Removed for a duplicate file set with an earlier timestamp
        self.assertNotIn(fqids['equal'][1], bundles)
        self.assertIn(fqids['equal'][0], bundles)
        # Removed for having a subset of files as another entry
        self.assertNotIn(fqids['subset'][0], bundles)
        self.assertIn(fqids['subset'][1], bundles)
        self.assertIn(fqids['superset'][0], bundles)
        # Removed for having a subset of files as another entry
        self.assertNotIn(fqids['superset'][1], bundles)

    def test_bdbag_manifest_for_redundant_entries(self):
        """
        Test that redundant bundles are removed from the terra.bdbag manifest response
        """
        self.maxDiff = None
        # Primary bundle cfab8304 has the files:
        # - d879f732-d8d4-4251-a2ca-a91a852a034b
        # - 1e14d503-31b1-4db6-82ba-f8d83bd85b9b
        # and derived analysis bundle f0731ab4 has the same files and more.
        for bundle in (
            self.bundle_fqid(uuid='cfab8304-dc9f-439e-af29-f8eb75b0729d',
                             version='2019-07-18T21:28:20.595913Z'),
            self.bundle_fqid(uuid='f0731ab4-6b80-4eed-97c9-4984de81a47c',
                             version='2019-07-23T06:21:20.663434Z')
        ):
            self._index_canned_bundle(bundle)

        # In both subtests below we expect the primary bundle to be omitted from
        # the results as it is redundant compared to the analysis bundle.
        expected_bundle_uuids = {
            # An analysis bundle
            'f0731ab4-6b80-4eed-97c9-4984de81a47c',
            # A bundle with no shared files (added by WebServiceTestCase)
            'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'
        }
        for filters in [
            # With a filter limiting to fastq both the primary and analysis
            # bundles will have the same set of files when compared.
            {'fileFormat': {'is': ['fastq.gz']}},
            # With no filter the primary bundle will contain a subset of
            # files when compared to its analysis bundle.
            {}
        ]:
            rows, fieldnames = self._extract_bdbag_response(filters)
            bundle_uuids = {row['bundle_uuid'] for row in rows}
            self.assertEqual(bundle_uuids, expected_bundle_uuids)

    def test_curl_manifest(self):
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        filters = {'fileFormat': {'is': ['pdf']}}
        response = self._get_manifest(ManifestFormat.curl, filters)
        self.assertEqual(200, response.status_code)
        lines = response.content.decode().splitlines()
        expected_header = [
            '--create-dirs',
            '',
            '--compressed',
            '',
            '--location',
            '',
            '--globoff',
            '',
            '--fail',
            '',
            '--write-out "Downloading to: %{filename_effective}\\n\\n"',
            '',
        ]
        header_length = len(expected_header)
        header, body = lines[:header_length], lines[header_length:]
        self.assertEqual(expected_header, header)
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
        self.assertEqual(expected_body, sorted(chunked(body, 3)))

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

    def test_manifest_content_disposition_header(self):
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        with patch.object(manifest_service, 'datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(1985, 10, 25, 1, 21)
            for format in [ManifestFormat.compact]:
                for filters, expected_name in [
                    # For a single project, the content disposition file name should
                    # be the project name followed by the date and time
                    (
                        {'project': {'is': ['Single of human pancreas']}},
                        'Single of human pancreas 1985-10-25 01.21'
                    ),
                    # In all other cases, the standard content disposition file name
                    # should be "hca-manifest-" followed by the manifest key,
                    # a pair of deterministically derived v5 UUIDs.
                    (
                        {'project': {'is': ['Single of human pancreas', 'Mouse Melanoma']}},
                        'hca-manifest-20d97863-d8cf-54f3-8575-0f9593d3d7ef.4bc67e84-4873-591f-b524-a5fe4ec215eb'
                    ),
                    (
                        {},
                        'hca-manifest-c3cf398e-1927-5aae-ba2a-81d8d1800b2d.4bc67e84-4873-591f-b524-a5fe4ec215eb'
                    )
                ]:
                    with self.subTest(filters=filters, format=format):
                        manifest, num_partitions = self._get_manifest_object(format, filters)
                        self.assertFalse(manifest.was_cached)
                        self.assertEqual(1, num_partitions)
                        query = furl(manifest.location).query
                        expected_cd = f'attachment;filename="{expected_name}.tsv"'
                        actual_cd = query.params['response-content-disposition']
                        self.assertEqual(expected_cd, actual_cd)

    def test_verbatim_jsonl_manifest(self):
        expected = []
        for bundle in self.bundles():
            bundle = self._load_canned_bundle(bundle)
            expected.append({
                'type': 'links',
                'value': bundle.links
            })
            for ref in [
                'cell_suspension/412898c5-5b9b-4907-b07c-e9b89666e204',
                'project/e8642221-4c2c-4fd7-b926-a68bce363c88',
                'sequence_file/70d1af4a-82c8-478a-8960-e9028b3616ca',
                'sequence_file/0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb',
                'specimen_from_organism/a21dc760-a500-4236-bcff-da34a0e873d2'
            ]:
                expected.append({
                    'type': EntityReference.parse(ref).entity_type,
                    'value': bundle.metadata[ref],
                })

        response = self._get_manifest(ManifestFormat.verbatim_jsonl, {})
        self.assertEqual(200, response.status_code)
        self._assert_jsonl(expected, response)

    def test_verbatim_pfb_manifest(self):
        response = self._get_manifest(ManifestFormat.verbatim_pfb, filters={})
        self.assertEqual(200, response.status_code)
        with open(self._data_path('service') / 'verbatim/hca/pfb_schema.json') as f:
            expected_schema = json.load(f)
        with open(self._data_path('service') / 'verbatim/hca/pfb_entities.json') as f:
            expected_entities = json.load(f)
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
                        header = response.headers['Content-Disposition']
                        value, params = cgi.parse_header(header)
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

    def test_hash_validity(self):
        self.maxDiff = None
        bundle_uuid = 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'
        version1 = '2018-11-02T11:33:44.698028Z'
        version2 = '2018-11-04T11:33:44.698028Z'
        assert (version1 != version2)
        original_fqid = self.bundle_fqid(uuid=bundle_uuid, version=version1)
        self._index_canned_bundle(original_fqid)
        filters = self._filters({'project': {'is': ['Single of human pancreas']}})
        old_keys = {}
        service = ManifestService(self.storage_service, self.app_module.app.file_url)

        def manifest_generator(format: ManifestFormat) -> ManifestGenerator:
            generator_cls = ManifestGenerator.cls_for_format(format)
            return generator_cls(service, self.catalog, filters)

        for format in ManifestFormat:
            with self.subTest('indexing new bundle', format=format):
                # When a new bundle is indexed and its compact manifest cached,
                # a matching manifest key is generated ...
                generator = manifest_generator(format)
                old_bundle_key = generator.manifest_key()
                # and should remain valid ...
                self.assertEqual(old_bundle_key, generator.manifest_key())
                old_keys[format] = old_bundle_key

        # ... until a new bundle belonging to the same project is indexed, at
        # which point a manifest request will generate a different key ...
        update_fqid = self.bundle_fqid(uuid=bundle_uuid, version=version2)
        self._index_canned_bundle(update_fqid)
        new_keys = {}
        for format in ManifestFormat:
            with self.subTest('indexing second bundle', format=format):
                generator = manifest_generator(format)
                new_bundle_key = generator.manifest_key()
                # ... invalidating the cached object previously used for the same filter.
                self.assertNotEqual(old_keys[format], new_bundle_key)
                new_keys[format] = new_bundle_key

        # Updates or additions, unrelated to that project do not affect object
        # key generation
        other_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                      version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(other_fqid)
        for format in ManifestFormat:
            with self.subTest('indexing unrelated bundle', format=format):
                generator = manifest_generator(format)
                latest_bundle_key = generator.manifest_key()
                self.assertEqual(latest_bundle_key, new_keys[format])

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
                                location=cached_manifest_1.location)
        self.assertEqual(manifest, cached_manifest_1)
        cached_manifest_2 = self._service.get_cached_manifest_with_key(manifest_key)
        cached_manifest_1 = attrs.evolve(cached_manifest_1,
                                         location=cached_manifest_2.location)
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
    def test_manifest(self,
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
            manifest = Manifest(location=str(object_url),
                                was_cached=False,
                                format=format,
                                manifest_key=manifest_key,
                                file_name=default_file_name)
            get_cached_manifest.return_value = manifest
            get_cached_manifest_with_key.return_value = manifest
            args = dict(catalog=self.catalog,
                        format=format.value,
                        filters='{}')
            path = ['manifest', 'files']
            if fetch and format is ManifestFormat.curl:
                expected_url = self.base_url.set(path=[*path, signed_manifest_key.encode()])
                expected_url_for_bash = expected_url
            else:
                expected_url = object_url
                expected_url_for_bash = f"'{expected_url}'"
            if format is ManifestFormat.curl:
                manifest_options = '--location --fail'
                file_options = '--fail-early --continue-at - --retry 15 --retry-delay 10'
                expected = {
                    'cmd.exe': f'curl.exe {manifest_options} "{expected_url}"'
                               f' | curl.exe {file_options} --config -',
                    'bash': f'curl {manifest_options} {expected_url_for_bash}'
                            f' | curl {file_options} --config -'
                }
            else:
                if format is ManifestFormat.terra_bdbag:
                    file_name = default_file_name
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

        for format in self.app_module.app.metadata_plugin.manifest_formats:
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
        content = requests.get(manifest.location).content
        self.assertGreater(num_partitions, 1)
        self.assertGreater(len(content), (num_partitions - 1) * part_size)


class AnvilManifestTestCase(ManifestTestCase, AnvilCannedBundleTestCase):

    @property
    def _drs_domain(self) -> str:
        return self.mock_tdr_service_url.netloc


class TestAnvilManifests(AnvilManifestTestCase):

    @classmethod
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.bundle_fqid(uuid='2370f948-2783-aeb6-afea-e022897f4dcf',
                            version=cls.version),
            cls.bundle_fqid(uuid='6b0f6c0f-5d80-a242-accb-840921351cd5',
                            version=cls.version),
            cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068',
                            version=cls.version)
        ]

    def test_compact_manifest(self):
        response = self._get_manifest(ManifestFormat.compact, filters={})
        self.assertEqual(200, response.status_code)
        expected = [
            (
                'bundle_uuid',
                '6b0f6c0f-5d80-a242-accb-840921351cd5',
                '826dea02-e274-affe-aabc-eb3db63ad068',
                '826dea02-e274-affe-aabc-eb3db63ad068'
            ),
            (
                'bundle_version',
                '2022-06-01T00:00:00.000000Z',
                '2022-06-01T00:00:00.000000Z',
                '2022-06-01T00:00:00.000000Z'
            ),
            (
                'source_id',
                '6c87f0e1-509d-46a4-b845-7584df39263b',
                '6c87f0e1-509d-46a4-b845-7584df39263b',
                '6c87f0e1-509d-46a4-b845-7584df39263b'
            ),
            (
                'source_spec',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot:/2',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot:/2',
                'tdr:bigquery:gcp:test_anvil_project:anvil_snapshot:/2'
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
                '',
                ''
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
                'activities.activity_table',
                '',
                'sequencingactivity',
                'sequencingactivity'
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
                'S/GBrRjzZAQYqh3rdiPYzA==',
                'vuxgbuCqKZ/fkT9CWTFmIg==',
                'fNn9e1SovzgOROk3BvH6LQ=='
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
                'files.crc32',
                '',
                '',
                ''
            ),
            (
                'files.sha256',
                '',
                '',
                ''
            ),
            (
                'files.drs_uri',
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_1fab11f5-7eab-4318-9a58-68d8d06e0715'),
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_1e269f04-4347-4188-b060-1dcc69e71d67'),
                self._drs_uri('v1_6c87f0e1-509d-46a4-b845-7584df39263b_8b722e88-8103-49c1-b351-e64fa7c6ab37')
            ),
            (
                'files.file_url',
                self._file_url('6b0f6c0f-5d80-4242-accb-840921351cd5', self.version),
                self._file_url('15b76f9c-6b46-433f-851d-34e89f1b9ba6', self.version),
                self._file_url('3b17377b-16b1-431c-9967-e5d01fc5923f', self.version)
            )
        ]
        self._assert_tsv(expected, response)

    def test_verbatim_jsonl_manifest(self):
        response = self._get_manifest(ManifestFormat.verbatim_jsonl, filters={})
        self.assertEqual(200, response.status_code)
        expected = {
            # Consolidate entities with the same replica (i.e. datasets)
            json_hash(entity).digest(): {
                'type': 'anvil_' + entity_ref.entity_type,
                'value': entity,
            }
            for bundle in self.bundles()
            for entity_ref, entity in self._load_canned_bundle(bundle).entities.items()
        }.values()
        self._assert_jsonl(list(expected), response)

    def test_verbatim_pfb_manifest(self):
        response = self._get_manifest(ManifestFormat.verbatim_pfb, filters={})
        self.assertEqual(200, response.status_code)
        with open(self._data_path('service') / 'verbatim/anvil/pfb_schema.json') as f:
            expected_schema = json.load(f)
        with open(self._data_path('service') / 'verbatim/anvil/pfb_entities.json') as f:
            expected_entities = json.load(f)
        self._assert_pfb(expected_schema, expected_entities, response)
