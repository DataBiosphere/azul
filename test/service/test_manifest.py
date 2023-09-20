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
    timezone,
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
from unittest import (
    mock,
)
from unittest.mock import (
    patch,
)
import unittest.result
from urllib.parse import (
    parse_qs,
    urlparse,
)
import uuid
from zipfile import (
    ZipFile,
)

import fastavro
from furl import (
    furl,
)
from more_itertools import (
    chunked,
    first,
    one,
)
from moto import (
    mock_s3,
    mock_sts,
)
import requests
from requests import (
    Response,
)

from azul import (
    config,
)
from azul.json import (
    copy_json,
)
from azul.json_freeze import (
    freeze,
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
    Manifest,
    ManifestGenerator,
    ManifestPartition,
    ManifestService,
    PagedManifestGenerator,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from azul_test_case import (
    AzulUnitTestCase,
    DCP1TestCase,
)
from service import (
    DocumentCloningTestCase,
    StorageServiceTestMixin,
    WebServiceTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


@mock_s3
class ManifestTestCase(DCP1TestCase, WebServiceTestCase, StorageServiceTestMixin):

    def setUp(self):
        super().setUp()
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

    def _get_manifest(self, format_: ManifestFormat, filters: FiltersJSON, stream=False) -> Response:
        manifest, num_partitions = self._get_manifest_object(format_, filters)
        self.assertEqual(1, num_partitions)
        return requests.get(manifest.location, stream=stream)

    def _get_manifest_object(self, format_: ManifestFormat, filters: JSON) -> tuple[Manifest, int]:
        service = ManifestService(self.storage_service, self.app_module.app.file_url)
        filters = self._filters(filters)
        partition = ManifestPartition.first()
        num_partitions = 1
        while True:
            partition = service.get_manifest(format_=format_,
                                             catalog=self.catalog,
                                             filters=filters,
                                             partition=partition,
                                             authentication=None)
            if isinstance(partition, Manifest):
                return partition, num_partitions
            # Emulate controller serializing the partition between steps
            partition = ManifestPartition.from_json(partition.to_json())
            num_partitions += 1


def manifest_test(test):
    """
    A decorator for test methods that test manifest functionality
    """

    @mock_sts
    @mock_s3
    def wrapper(self, *args, **kwargs):
        self.storage_service.create_bucket()
        with mock.patch.object(PagedManifestGenerator, 'page_size', 1):
            return test(self, *args, **kwargs)

    return wrapper


class TestManifestEndpoints(ManifestTestCase):
    _drs_domain_name = 'drs-test.lan'

    def run(self, result: Optional[unittest.result.TestResult] = None) -> Optional[unittest.result.TestResult]:
        # Disable caching of manifests to prevent false assertion positives
        with mock.patch.object(ManifestService,
                               '_get_cached_manifest_file_name',
                               return_value=None):
            return super().run(result)

    @manifest_test
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
                with mock.patch.object(type(config), 'debug', debug):
                    response = self._get_manifest(ManifestFormat.terra_pfb, {})
                    self.assertEqual(200, response.status_code)
                    pfb_file = BytesIO(response.content)
                    reader = fastavro.reader(pfb_file)
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
        old_to_new = {
            # process
            '4da04038-adab-59a9-b6c4-3a61242cc972': '61af0068-1418-46e7-88ef-ab310e0ceaf8',
            # cell_suspension
            'd9eaaffe-4c93-5503-984f-762e8dfddce4': 'd6b3d2ab-5715-4486-a544-ac09fafac279',
            # specimen
            '224d3750-f1f7-5b04-bbce-e23f09eea7d7': '5275e5a0-6043-4ec9-86a1-6c1140cbeede',
        }
        manifest = self._replace_uuids(bundle.manifest, old_to_new)
        metadata_files = self._replace_uuids(bundle.metadata_files, old_to_new)
        # Change organ to prevent cell_suspensions aggregating together
        metadata_files['specimen_from_organism_0.json']['organ'] = {
            "text": "lung",
            "ontology": "UBERON:0002048",
            "ontology_label": "lung"
        }
        assert isinstance(manifest, list)
        return DSSBundle(fqid=self.bundle_fqid(uuid=old_to_new[bundle.uuid],
                                               version=bundle.version),
                         manifest=cast(MutableJSONs, manifest),
                         metadata_files=metadata_files)

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
        duplicate_donor = deepcopy(bundle.metadata_files['donor_organism_0.json'])
        del duplicate_donor['organism_age']
        del duplicate_donor['organism_age_unit']
        donor_id = '0895599c-f57d-4843-963e-11eab29f883b'
        duplicate_donor['provenance']['document_id'] = donor_id
        bundle.metadata_files['donor_organism_1.json'] = duplicate_donor
        donor_link = one(ln for ln in bundle.metadata_files['links.json']['links']
                         if one(ln['inputs'])['input_type'] == 'donor_organism')
        new_donor_reference = {
            'input_id': donor_id,
            'input_type': 'donor_organism'
        }
        donor_link['inputs'].append(new_donor_reference)
        return bundle

    @manifest_test
    def test_manifest_not_cached(self):
        """
        Assert that the patch to disable caching is effective.
        """
        for i in range(2):
            with self.subTest(i=i):
                manifest, num_partitions = self._get_manifest_object(ManifestFormat.compact, {})
                self.assertFalse(manifest.was_cached)
                self.assertEqual(1, num_partitions)

    @manifest_test
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
             f'drs://{self.drs_domain}/5f9b45af-9a26-4b16-a785-7f2d1053dd7c?version=2018-09-14T12%3A33%3A47.012715Z',
             f'drs://{self.drs_domain}/f2b6c6f0-8d25-4aae-b255-1974cc110cfe?version=2018-09-14T12%3A33%3A43.720332Z'),

            ('file_url',
             str(self.base_url.set(path='/repository/files/5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                                   args=dict(catalog=self.catalog,
                                             version='2018-09-14T12:33:47.012715Z'))),
             str(self.base_url.set(path='/repository/files/f2b6c6f0-8d25-4aae-b255-1974cc110cfe',
                                   args=dict(catalog=self.catalog,
                                             version='2018-09-14T12:33:43.720332Z')))),

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
        files_names = {
            'supplementary_file_1.json',
            'SmartSeq2_RTPCR_protocol.pdf',
            'links.json',
            'project_0.json'
        }
        manifest = [
            entry
            for entry in bundle.manifest
            if entry['name'] in files_names
        ]
        metadata_files = {
            file_name: copy_json(content)
            for file_name, content in bundle.metadata_files.items()
            if file_name in files_names
        }
        # This is an older bundle so there are no supplementary file links.
        # The existing links reference entities that weren't copied to the mock bundle.
        metadata_files['links.json']['links'].clear()
        self._index_bundle(DSSBundle(fqid=self.bundle_fqid(uuid='b81656cf-231b-47a3-9317-10f1e501a05c',
                                                           version='2000-01-01T01:00:00.000000Z'),
                                     manifest=manifest,
                                     metadata_files=metadata_files))

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

    @property
    def drs_domain(self):
        return config.drs_domain or config.api_lambda_domain('service')

    def _assert_tsv(self, expected, actual):
        expected_field_names, *expected_rows = map(list, zip(*expected))
        # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
        lines = actual.content.decode('utf-8').splitlines()
        tsv_file = csv.reader(lines, delimiter='\t')
        field_names = next(tsv_file)
        rows = list(tsv_file)
        self.assertEqual(expected_field_names, field_names)
        self.assertEqual(sorted(freeze(expected_rows)), sorted(freeze(rows)))

    @manifest_test
    def test_manifest_zarr(self):
        """
        Test that when downloading a manifest with a zarr, all of the files are added into the manifest even
        if they are not listed in the service response.
        """
        self.maxDiff = None
        expected = [
            # Original file
            {
                'file_crc32c': '4e75003e',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zattrs',
                'file_uuid': 'c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d',
                'file_drs_uri': f'drs://{self.drs_domain}/c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d'
                                f'?version=2018-10-10T03%3A10%3A37.983672Z',
                'file_url': f'{self.base_url}/repository/files/c1c4a2bc-b5fb-4083-af64-f5dec70d7f9d'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A37.983672Z',
                'specimen_from_organism.organ': 'brain'
            },
            # Related files from zarray store
            {
                'file_crc32c': '444a7707',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zgroup',
                'file_uuid': '54541cc5-9010-425b-9037-22e43948c97c',
                'file_drs_uri': f'drs://{self.drs_domain}/54541cc5-9010-425b-9037-22e43948c97c'
                                f'?version=2018-10-10T03%3A10%3A38.239541Z',
                'file_url': f'{self.base_url}/repository/files/54541cc5-9010-425b-9037-22e43948c97c'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A38.239541Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '444a7707',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/.zgroup',
                'file_uuid': '66b8f976-6f1e-45b3-bd97-069658c3c847',
                'file_drs_uri': f'drs://{self.drs_domain}/66b8f976-6f1e-45b3-bd97-069658c3c847'
                                f'?version=2018-10-10T03%3A10%3A38.474167Z',
                'file_url': f'{self.base_url}/repository/files/66b8f976-6f1e-45b3-bd97-069658c3c847'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A38.474167Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'c6ab0701',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/.zarray',
                'file_uuid': 'ac05d7fb-d6b9-4ab1-8c04-6211450dbb62',
                'file_drs_uri': f'drs://{self.drs_domain}/ac05d7fb-d6b9-4ab1-8c04-6211450dbb62'
                                f'?version=2018-10-10T03%3A10%3A38.714461Z',
                'file_url': f'{self.base_url}/repository/files/ac05d7fb-d6b9-4ab1-8c04-6211450dbb62'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A38.714461Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'cd2fd51f',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/cell_id/0.0',
                'file_uuid': '0c518a52-f315-4ea2-beed-1c9d8f2d802b',
                'file_drs_uri': f'drs://{self.drs_domain}/0c518a52-f315-4ea2-beed-1c9d8f2d802b'
                                f'?version=2018-10-10T03%3A10%3A39.039270Z',
                'file_url': f'{self.base_url}/repository/files/0c518a52-f315-4ea2-beed-1c9d8f2d802b'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A39.039270Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'b89e6723',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/.zarray',
                'file_uuid': '136108ab-277e-47a4-acc3-1feed8fb2f25',
                'file_drs_uri': f'drs://{self.drs_domain}/136108ab-277e-47a4-acc3-1feed8fb2f25'
                                f'?version=2018-10-10T03%3A10%3A39.426609Z',
                'file_url': f'{self.base_url}/repository/files/136108ab-277e-47a4-acc3-1feed8fb2f25'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A39.426609Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'caaefa77',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/expression/0.0',
                'file_uuid': '0bef5419-739c-4a2c-aedb-43754d55d51c',
                'file_drs_uri': f'drs://{self.drs_domain}/0bef5419-739c-4a2c-aedb-43754d55d51c'
                                f'?version=2018-10-10T03%3A10%3A39.642846Z',
                'file_url': f'{self.base_url}/repository/files/0bef5419-739c-4a2c-aedb-43754d55d51c'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A39.642846Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'f629ec34',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/.zarray',
                'file_uuid': '3a5f7299-1aa1-4060-9631-212c29b4d807',
                'file_drs_uri': f'drs://{self.drs_domain}/3a5f7299-1aa1-4060-9631-212c29b4d807'
                                f'?version=2018-10-10T03%3A10%3A39.899615Z',
                'file_url': f'{self.base_url}/repository/files/3a5f7299-1aa1-4060-9631-212c29b4d807'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A39.899615Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '59d86b68',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/gene_id/0.0',
                'file_uuid': 'a8f0dc39-6019-4fc7-899d-4e34a48d03e5',
                'file_drs_uri': f'drs://{self.drs_domain}/a8f0dc39-6019-4fc7-899d-4e34a48d03e5'
                                f'?version=2018-10-10T03%3A10%3A40.113268Z',
                'file_url': f'{self.base_url}/repository/files/a8f0dc39-6019-4fc7-899d-4e34a48d03e5'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A40.113268Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/.zarray',
                'file_uuid': '68ba4711-1447-42ac-aa40-9c0e4cda1666',
                'file_drs_uri': f'drs://{self.drs_domain}/68ba4711-1447-42ac-aa40-9c0e4cda1666'
                                f'?version=2018-10-10T03%3A10%3A40.583439Z',
                'file_url': f'{self.base_url}/repository/files/68ba4711-1447-42ac-aa40-9c0e4cda1666'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A40.583439Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '17a84191',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_metric/0.0',
                'file_uuid': '27e66328-e337-4bcd-ba15-7893ecaf841f',
                'file_drs_uri': f'drs://{self.drs_domain}/27e66328-e337-4bcd-ba15-7893ecaf841f'
                                f'?version=2018-10-10T03%3A10%3A40.801631Z',
                'file_url': f'{self.base_url}/repository/files/27e66328-e337-4bcd-ba15-7893ecaf841f'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A40.801631Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': '25d193cf',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/.zarray',
                'file_uuid': '2ab1a516-ef36-41b6-a78f-513361658feb',
                'file_drs_uri': f'drs://{self.drs_domain}/2ab1a516-ef36-41b6-a78f-513361658feb'
                                f'?version=2018-10-10T03%3A10%3A40.958708Z',
                'file_url': f'{self.base_url}/repository/files/2ab1a516-ef36-41b6-a78f-513361658feb'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A40.958708Z',
                'specimen_from_organism.organ': 'brain'
            },
            {
                'file_crc32c': 'bdc30523',
                'file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/expression_matrix/qc_values/0.0',
                'file_uuid': '351970aa-bc4c-405e-a274-be9e08e42e98',
                'file_drs_uri': f'drs://{self.drs_domain}/351970aa-bc4c-405e-a274-be9e08e42e98'
                                f'?version=2018-10-10T03%3A10%3A41.135992Z',
                'file_url': f'{self.base_url}/repository/files/351970aa-bc4c-405e-a274-be9e08e42e98'
                            f'?catalog=test&version=2018-10-10T03%3A10%3A41.135992Z',
                'specimen_from_organism.organ': 'brain'
            }
        ]
        expected_keys = one(set(map(frozenset, map(dict.keys, expected))))
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(bundle_fqid)
        filters = {"fileFormat": {"is": ["matrix", "mtx"]}}
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

    @manifest_test
    def test_terra_bdbag_manifest(self):
        """
        moto will mock the requests.get call so we can't hit localhost; add_passthru lets us hit
        the server (see GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270)
        """
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        domain = self.drs_domain

        bam_b0_0_uuid, bam_b0_0_version = "51c9ad31-5888-47eb-9e0c-02f042373c4e", "2018-10-10T03:10:35.284782Z"
        bam_b0_1_uuid, bam_b0_1_version = "b1c167da-0825-4c63-9cbc-2aada1ab367c", "2018-10-10T03:10:35.971561Z"
        fastq_b0_r1_uuid, fastq_b0_r1_version = "c005f647-b3fb-45a8-857a-8f5e6a878ccf", "2018-10-10T02:38:11.612423Z"
        fastq_b0_r2_uuid, fastq_b0_r2_version = "b764ce7d-3938-4451-b68c-678feebc8f2a", "2018-10-10T02:38:11.851483Z"
        fastq_b1_r1_uuid, fastq_b1_r1_version = "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb", "2018-11-02T11:33:44.698028Z"
        fastq_b1_r2_uuid, fastq_b1_r2_version = "74897eb7-0701-4e4f-9e6b-8b9521b2816b", "2018-11-02T11:33:44.450442Z"
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
                '__bam_0__file_drs_uri': str(furl(url=f'drs://{domain}/{bam_b0_0_uuid}',
                                                  args={'version': bam_b0_0_version})),
                '__bam_0__file_url': str(furl(url=f'{self.base_url}/repository/files/{bam_b0_0_uuid}',
                                              args={'catalog': 'test', 'version': bam_b0_0_version})),
                '__bam_1__file_document_id': '14d63962-7cd3-43fc-a4d6-dc8f761c9ebd',
                '__bam_1__file_type': 'analysis_file',
                '__bam_1__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_rsem.bam',
                '__bam_1__file_format': 'bam',
                '__bam_1__read_index': '',
                '__bam_1__file_size': '3752733',
                '__bam_1__file_uuid': f'{bam_b0_1_uuid}',
                '__bam_1__file_version': bam_b0_1_version,
                '__bam_1__file_crc32c': '3d94b063',
                '__bam_1__file_sha256': 'f25053412d65429cefc0157c0d18ae12d4bf4c4113a6af7a1820b62246c075a4',
                '__bam_1__file_content_type': 'application/gzip; dcp-type=data',
                '__bam_1__file_drs_uri': str(furl(url=f'drs://{domain}/{bam_b0_1_uuid}',
                                                  args={'version': bam_b0_1_version})),
                '__bam_1__file_url': str(furl(url=f'{self.base_url}/repository/files/{bam_b0_1_uuid}',
                                              args={'catalog': 'test', 'version': bam_b0_1_version})),
                '__fastq_read1__file_document_id': '5f0cdf49-aabe-40f4-8af3-033115805bb0',
                '__fastq_read1__file_type': 'sequence_file',
                '__fastq_read1__file_name': 'R1.fastq.gz',
                '__fastq_read1__file_format': 'fastq.gz',
                '__fastq_read1__read_index': 'read1',
                '__fastq_read1__file_size': '125191',
                '__fastq_read1__file_uuid': fastq_b0_r1_uuid,
                '__fastq_read1__file_version': f'{fastq_b0_r1_version}',
                '__fastq_read1__file_crc32c': '4ef74578',
                '__fastq_read1__file_sha256': 'fe6d4fdfea2ff1df97500dcfe7085ac3abfb760026bff75a34c20fb97a4b2b29',
                '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read1__file_url': str(furl(url=f'{self.base_url}/repository/files/{fastq_b0_r1_uuid}',
                                                    args={'catalog': 'test', 'version': fastq_b0_r1_version})),
                '__fastq_read1__file_drs_uri': str(furl(url=f'drs://{domain}/{fastq_b0_r1_uuid}',
                                                        args={'version': fastq_b0_r1_version})),
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
                '__fastq_read2__file_url': str(furl(url=f'{self.base_url}/repository/files/{fastq_b0_r2_uuid}',
                                                    args={'catalog': 'test', 'version': fastq_b0_r2_version})),
                '__fastq_read2__file_drs_uri': str(furl(url=f'drs://{domain}/{fastq_b0_r2_uuid}',
                                                        args={'version': fastq_b0_r2_version})),
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
                '__fastq_read1__file_drs_uri': str(furl(url=f'drs://{domain}/{fastq_b1_r1_uuid}',
                                                        args={'version': fastq_b1_r1_version})),
                '__fastq_read1__file_url': str(furl(url=f'{self.base_url}/repository/files/{fastq_b1_r1_uuid}',
                                                    args={'catalog': 'test', 'version': fastq_b1_r1_version})),
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
                '__fastq_read2__file_drs_uri': str(furl(url=f'drs://{domain}/{fastq_b1_r2_uuid}',
                                                        args={'version': fastq_b1_r2_version})),
                '__fastq_read2__file_url': str(furl(url=f'{self.base_url}/repository/files/{fastq_b1_r2_uuid}',
                                                    args={'catalog': 'test', 'version': fastq_b1_r2_version})),
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

    def _extract_bdbag_response(self, filters: FiltersJSON) -> tuple[list[dict[str, str]], list[str]]:
        with TemporaryDirectory() as zip_dir:
            response = self._get_manifest(ManifestFormat.terra_bdbag, filters, stream=True)
            self.assertEqual(200, response.status_code)
            with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
                self.assertTrue(all(['manifest' == first(name.split('/')) for name in zip_fh.namelist()]))
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
            return str(uuid.uuid4())

        bundles: Bundles = {}
        # Create sample data that can be passed to BDBagManifestGenerator._remove_redundant_entries()
        # Each entry is given a timestamp 1 second later than the previous entry to have variety in the entries
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
        # Add an entry with the same set of files as another entry though with a later timestamp
        bundle_fqid = u(), v(num_of_entries + 1)
        bundles[bundle_fqid] = deepcopy(bundles[keys[100]])  # An arbitrary entry in bundles
        fqids['equal'] = bundle_fqid, keys[100]  # With same set of files [1] will be removed (earlier timestamp)
        # Add an entry with a subset of files compared to another entry
        bundle_fqid = u(), v(num_of_entries + 2)
        bundles[bundle_fqid] = deepcopy(bundles[keys[200]])  # An arbitrary entry in bundles
        del bundles[bundle_fqid]['bam'][2]
        fqids['subset'] = bundle_fqid, keys[200]  # [0] will be removed as it has a subset of files that [1] has
        # Add an entry with a superset of files compared to another entry
        bundle_fqid = u(), v(num_of_entries + 3)
        bundles[bundle_fqid] = deepcopy(bundles[keys[300]])  # An arbitrary entry in bundles
        bundles[bundle_fqid]['bam'].append({'file': {'file_uuid': u()}})
        fqids['superset'] = bundle_fqid, keys[300]  # [0] has a superset of files that [0] has so [0] wil be removed

        self.assertEqual(len(bundles), num_of_entries + 3)  # the generated entries plus 3 redundant entries
        self.assertIn(fqids['equal'][0], bundles)
        self.assertIn(fqids['equal'][1], bundles)
        self.assertIn(fqids['subset'][0], bundles)
        self.assertIn(fqids['subset'][1], bundles)
        self.assertIn(fqids['superset'][0], bundles)
        self.assertIn(fqids['superset'][1], bundles)

        BDBagManifestGenerator._remove_redundant_entries(bundles)

        self.assertEqual(len(bundles), num_of_entries)  # 3 redundant entries removed
        self.assertNotIn(fqids['equal'][1], bundles)  # Removed for a duplicate file set with an earlier timestamp
        self.assertIn(fqids['equal'][0], bundles)
        self.assertNotIn(fqids['subset'][0], bundles)  # Removed for having a subset of files as another entry
        self.assertIn(fqids['subset'][1], bundles)
        self.assertIn(fqids['superset'][0], bundles)
        self.assertNotIn(fqids['superset'][1], bundles)  # Removed for having a subset of files as another entry

    @manifest_test
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
            'f0731ab4-6b80-4eed-97c9-4984de81a47c',  # analysis bundle
            'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'  # bundle with no shared files (added by WebServiceTestCase)
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

    @manifest_test
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
            '--fail-early',
            '',
            '--continue-at -',
            '',
            '--retry 2',
            '',
            '--retry-delay 10',
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
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.content)

    def test_manifest_filter_validation(self):
        url = self.base_url.set(path='/manifest/files',
                                args=dict(format='compact',
                                          filters=dict(fileFormat=['pdf'])))
        response = requests.get(str(url))
        self.assertEqual(400, response.status_code, response.content)

    @manifest_test
    def test_manifest_content_disposition_header(self):
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)
        with mock.patch.object(manifest_service, 'datetime') as mock_response:
            mock_response.now.return_value = datetime(1985, 10, 25, 1, 21)
            for format_ in [ManifestFormat.compact]:
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
                    with self.subTest(filters=filters, format_=format_):
                        manifest, num_partitions = self._get_manifest_object(format_, filters)
                        self.assertFalse(manifest.was_cached)
                        self.assertEqual(1, num_partitions)
                        query = urlparse(manifest.location).query
                        expected_cd = f'attachment;filename="{expected_name}.tsv"'
                        actual_cd = one(parse_qs(query).get('response-content-disposition'))
                        self.assertEqual(expected_cd, actual_cd)


class TestManifestCache(ManifestTestCase):

    @manifest_test
    @mock.patch('azul.service.manifest_service.ManifestService._get_seconds_until_expire')
    def test_metadata_cache_expiration(self, get_seconds):
        self.maxDiff = None
        bundle_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                       version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(bundle_fqid)

        # moto will mock the requests.get call so we can't hit localhost; add_passthru lets us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        def log_messages_from_manifest_request(seconds_until_expire: int) -> list[str]:
            get_seconds.return_value = seconds_until_expire
            filters = {'projectId': {'is': ['67bc798b-a34a-4104-8cab-cad648471f69']}}
            from azul.service.manifest_service import (
                logger as logger_,
            )
            with self.assertLogs(logger=logger_, level='INFO') as logs:
                response = self._get_manifest(ManifestFormat.compact, filters)
                self.assertEqual(200, response.status_code)
                logger_.info('Dummy log message so assertLogs() does not fail if no other error log is generated')
                return logs.output

        # On the first request the cached manifest doesn't exist yet
        logs_output = log_messages_from_manifest_request(seconds_until_expire=30)
        self.assertTrue(any('Cached manifest not found' in message for message in logs_output))

        # If the cached manifest has a long time till it expires then no log message expected
        logs_output = log_messages_from_manifest_request(seconds_until_expire=3600)
        self.assertFalse(any('Cached manifest' in message for message in logs_output))

        # If the cached manifest has a short time till it expires then a log message is expected
        logs_output = log_messages_from_manifest_request(seconds_until_expire=30)
        self.assertTrue(any('Cached manifest is about to expire' in message for message in logs_output))

    @manifest_test
    @mock.patch('azul.service.manifest_service.ManifestService._get_seconds_until_expire')
    def test_compact_metadata_cache(self, get_seconds):
        get_seconds.return_value = 3600
        self.maxDiff = None
        for bundle_fqid in [
            self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                             version='2018-09-14T13:33:14.453337Z'),
            self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                             version='2018-09-14T13:33:14.453337Z')
        ]:
            self._index_canned_bundle(bundle_fqid)

            project_ids = [
                '67bc798b-a34a-4104-8cab-cad648471f69',
                '6615efae-fca8-4dd2-a223-9cfcf30fe94d'
            ]
            file_names = defaultdict(list)

            # Run the generation of manifests twice to verify generated file names are the same when re-run
            for project_id in project_ids * 2:
                response = self._get_manifest(ManifestFormat.compact,
                                              filters={'projectId': {'is': [project_id]}})
                self.assertEqual(200, response.status_code)
                file_name = urlparse(response.url).path
                file_names[project_id].append(file_name)

            self.assertEqual(file_names.keys(), set(project_ids))
            self.assertEqual([2, 2], list(map(len, file_names.values())))
            self.assertEqual([1, 1], list(map(len, map(set, file_names.values()))))

    @manifest_test
    def test_hash_validity(self):
        self.maxDiff = None
        bundle_uuid = 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'
        original_fqid = self.bundle_fqid(uuid=bundle_uuid,
                                         version='2018-11-02T11:33:44.698028Z')
        self._index_canned_bundle(original_fqid)
        filters = self._filters({'project': {'is': ['Single of human pancreas']}})
        old_object_keys = {}
        service = ManifestService(self.storage_service, self.app_module.app.file_url)
        for format_ in ManifestFormat:
            with self.subTest('indexing new bundle', format_=format_):
                # When a new bundle is indexed and its compact manifest cached,
                # a matching object_key is generated ...
                generator = ManifestGenerator.for_format(format_=format_,
                                                         service=service,
                                                         catalog=self.catalog,
                                                         filters=filters,
                                                         authentication=None)

                old_bundle_object_key = generator.compute_object_key()
                # and should remain valid ...
                self.assertEqual(old_bundle_object_key, generator.compute_object_key())
                old_object_keys[format_] = old_bundle_object_key

        # ... until a new bundle belonging to the same project is indexed, at which point a manifest request
        # will generate a different object_key ...
        update_fqid = self.bundle_fqid(uuid=bundle_uuid,
                                       version='2018-11-04T11:33:44.698028Z')
        self._index_canned_bundle(update_fqid)
        new_object_keys = {}
        for format_ in ManifestFormat:
            with self.subTest('indexing second bundle', format_=format_):
                generator = ManifestGenerator.for_format(format_=format_,
                                                         service=service,
                                                         catalog=self.catalog,
                                                         filters=filters,
                                                         authentication=None)
                new_bundle_object_key = generator.compute_object_key()
                # ... invalidating the cached object previously used for the same filter.
                self.assertNotEqual(old_object_keys[format_], new_bundle_object_key)
                new_object_keys[format_] = new_bundle_object_key

        # Updates or additions, unrelated to that project do not affect object key generation
        other_fqid = self.bundle_fqid(uuid='f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                                      version='2018-09-14T13:33:14.453337Z')
        self._index_canned_bundle(other_fqid)
        for format_ in ManifestFormat:
            with self.subTest('indexing unrelated bundle', format_=format_):
                generator = ManifestGenerator.for_format(format_=format_,
                                                         service=service,
                                                         catalog=self.catalog,
                                                         filters=filters,
                                                         authentication=None)
                latest_bundle_object_key = generator.compute_object_key()
                self.assertEqual(latest_bundle_object_key, new_object_keys[format_])


class TestManifestResponse(ManifestTestCase):

    @mock.patch.dict(os.environ, AZUL_PRIVATE_API='0')
    @mock.patch('azul.service.manifest_service.ManifestService.get_cached_manifest')
    def test_manifest(self, get_cached_manifest):
        """
        Verify the response from manifest endpoints for all manifest formats
        """
        for format_ in self.app_module.app.metadata_plugin.manifest_formats:
            for fetch in True, False:
                with self.subTest(format=format_, fetch=fetch):
                    object_url = 'https://url.to.manifest?foo=bar'
                    default_file_name = 'some_object_key'
                    object_key = f'manifests/{default_file_name}'
                    manifest = Manifest(location=object_url,
                                        was_cached=False,
                                        format_=format_,
                                        catalog=self.catalog,
                                        filters=self._filters({}),
                                        object_key=object_key,
                                        file_name=default_file_name)
                    get_cached_manifest.return_value = None, manifest
                    args = dict(catalog=self.catalog,
                                format=format_.value,
                                filters='{}')
                    request_url = self.base_url.set(path='/manifest/files', args=args)
                    redirect_url = self.base_url.set(path='/manifest/files',
                                                     args=dict(args, objectKey=object_key))
                    expect_redirect = fetch and format_ is ManifestFormat.curl
                    expected_url = redirect_url if expect_redirect else object_url
                    if format_ is ManifestFormat.curl:
                        expected = {
                            'cmd.exe': f'curl.exe --location --fail "{expected_url}" | curl.exe --config -',
                            'bash': f"curl --location --fail '{expected_url}' | curl --config -"
                        }
                    else:
                        if format_ is ManifestFormat.terra_bdbag:
                            file_name = default_file_name
                        else:
                            file_name = manifest.file_name
                        options = '--location --fail --output'
                        expected = {
                            'cmd.exe': f'curl.exe {options} "{file_name}" "{expected_url}"',
                            'bash': f"curl {options} {file_name} '{expected_url}'"
                        }
                    if fetch:
                        request_url.path.segments.insert(0, 'fetch')
                        response = requests.get(str(request_url)).json()
                        expected = {
                            'Status': 302,
                            'Location': str(expected_url),
                            'CommandLine': expected
                        }
                        self.assertEqual(expected, response)
                    else:
                        response = requests.get(str(request_url), allow_redirects=False)
                        expected = ''.join(
                            f'\nDownload the manifest in {shell} with `curl` using:\n\n{cmd}\n'
                            for shell, cmd in expected.items()
                        )
                        self.assertEqual(expected, response.text)
                        self.assertEqual(302, response.status_code)
                        self.assertEqual(object_url, response.headers['location'])


class TestManifestExpiration(AzulUnitTestCase):

    def test_get_seconds_until_expire(self):
        """
        Verify a header with valid Expiration and LastModified values returns
        the correct expiration value.
        """
        test_data = [(1, False), (0, False), (-1, True)]
        for object_age, expect_error in test_data:
            with self.subTest(object_age=object_age, expect_error=expect_error):
                with mock.patch.object(manifest_service, 'datetime') as mock_datetime:
                    now = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                    mock_datetime.now.return_value = now
                    with self.assertLogs(logger=manifest_service.logger, level='DEBUG') as logs:
                        headers = {
                            'Expiration': 'expiry-date="Wed, 01 Jan 2020 00:00:00 UTC", rule-id="Test Rule"',
                            'LastModified': now - timedelta(days=float(config.manifest_expiration),
                                                            seconds=object_age)
                        }
                        self.assertEqual(0, ManifestService._get_seconds_until_expire(headers))
                    self.assertIs(expect_error, any('does not match' in log for log in logs.output))


class TestManifestPartitioning(ManifestTestCase, DocumentCloningTestCase):

    def setUp(self):
        super().setUp()
        self._setup_document_templates()
        self._add_docs(5000)

    @manifest_test
    def test(self):
        # This is the smallest valid S3 part size
        part_size = 5 * 1024 * 1024
        with patch.object(PagedManifestGenerator, 'part_size', part_size):
            manifest, num_partitions = self._get_manifest_object(ManifestFormat.compact,
                                                                 filters={})
        content = requests.get(manifest.location).content
        self.assertGreater(num_partitions, 1)
        self.assertGreater(len(content), (num_partitions - 1) * part_size)
