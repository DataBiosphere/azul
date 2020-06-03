from collections import defaultdict
from copy import deepcopy
import csv
from datetime import (
    datetime,
    timedelta,
    timezone,
)
from io import BytesIO
import json
import logging
import os
from tempfile import TemporaryDirectory
from typing import (
    List,
    Optional,
    Tuple,
)
from unittest import mock
import unittest.result
from urllib.parse import (
    parse_qs,
    urlparse,
)
import uuid
from zipfile import ZipFile

from more_itertools import (
    first,
    one,
)
from moto import (
    mock_s3,
    mock_sts,
)
import requests

from azul import config
from azul.indexer import BundleFQID
from azul.json_freeze import freeze
from azul.logging import configure_test_logging
from azul.service import (
    manifest_service,
)
from azul.service.manifest_service import (
    BDBagManifestGenerator,
    Bundles,
    ManifestFormat,
    ManifestService,
)
from azul.service.storage_service import StorageService
from azul.types import JSON
from azul_test_case import AzulTestCase
from retorts import ResponsesHelper
from service import WebServiceTestCase

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(logger)


class ManifestTestCase(WebServiceTestCase):

    def setUp(self):
        super().setUp()
        self._setup_indices()
        self._setup_git_commit()

    def tearDown(self):
        self._teardown_indices()
        self._teardown_git_commit()
        super().tearDown()

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

    def _get_manifest(self, format_: ManifestFormat, filters: JSON, stream=False):
        url, was_cached = self._get_manifest_url(format_, filters)
        return requests.get(url, stream=stream)

    def _get_manifest_url(self, format_: ManifestFormat, filters: JSON) -> Tuple[str, bool]:
        service = ManifestService(StorageService())
        return service.get_manifest(format_, filters)


class TestManifestEndpoints(ManifestTestCase):

    def run(self, result: Optional[unittest.result.TestResult] = None) -> Optional[unittest.result.TestResult]:
        # Disable caching of manifests to prevent false assertion positives
        with mock.patch('azul.service.manifest_service.ManifestService._can_use_cached_manifest') as m:
            m.return_value = False
            return super().run(result)

    @mock_sts
    @mock_s3
    def test_manifest_not_cached(self):
        """
        Assert that the patch to disable caching is effective.
        """
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            for i in range(2):
                with self.subTest(i=i):
                    url, was_cached = self._get_manifest_url(ManifestFormat.compact, {})
                    self.assertFalse(was_cached)

    @mock_sts
    @mock_s3
    def test_manifest(self):
        expected = [
            ('bundle_uuid', 'f79257a7-dfc6-46d6-ae00-ba4b25313c10', 'f79257a7-dfc6-46d6-ae00-ba4b25313c10'),
            ('bundle_version', '2018-09-14T133314.453337Z', '2018-09-14T133314.453337Z'),
            ('file_name', 'SmartSeq2_RTPCR_protocol.pdf', '22028_5#300_1.fastq.gz'),
            ('file_format', 'pdf', 'fastq.gz'),
            ('read_index', '', 'read1'),
            ('file_size', '29230', '64718465'),
            ('file_uuid', '5f9b45af-9a26-4b16-a785-7f2d1053dd7c', 'f2b6c6f0-8d25-4aae-b255-1974cc110cfe'),
            ('file_version', '2018-09-14T123347.012715Z', '2018-09-14T123343.720332Z'),
            ('file_sha256',
             '2f6866c4ede92123f90dd15fb180fac56e33309b8fd3f4f52f263ed2f8af2f16',
             '3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582'),
            ('file_content_type', 'application/pdf; dcp-type=data', 'application/gzip; dcp-type=data'),
            ('cell_suspension.provenance.document_id',
             '',
             '0037c9eb-8038-432f-8d9d-13ee094e54ab || aaaaaaaa-8038-432f-8d9d-13ee094e54ab'),
            ('cell_suspension.estimated_cell_count', '', '9001'),
            ('cell_suspension.selected_cell_type', '', 'CAFs'),
            ('sequencing_process.provenance.document_id', '', '72732ed3-7b71-47df-bcec-c765ef7ea758'),
            ('sequencing_protocol.instrument_manufacturer_model', '', 'Illumina HiSeq 2500'),
            ('sequencing_protocol.paired_end', '', 'True'),
            ('library_preparation_protocol.library_construction_approach', '', 'Smart-seq2'),
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
            ('donor_organism.diseases', '', 'subcutaneous melanoma'),
            ('donor_organism.organism_age', '', '6-12'),
            ('donor_organism.organism_age_unit', '', 'week'),
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
        ]
        self.maxDiff = None
        bundle_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            for single_part in False, True:
                with self.subTest(is_single_part=single_part):
                    with mock.patch.object(type(config), 'disable_multipart_manifests', single_part):
                        response = self._get_manifest(ManifestFormat.compact, {})
                        self.assertEqual(200, response.status_code, 'Unable to download manifest')
                        self._assert_tsv(expected, response)

    def _assert_tsv(self, expected, actual):
        expected_field_names, *expected_rows = map(list, zip(*expected))
        # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
        lines = actual.content.decode('utf-8').splitlines()
        tsv_file = csv.reader(lines, delimiter='\t')
        actual_field_names = next(tsv_file)
        rows = freeze(list(tsv_file))
        self.assertEqual(expected_field_names, actual_field_names)
        for row in expected_rows:
            self.assertIn(freeze(row), rows)
        self.assertTrue(all(len(row) == len(expected_rows[0]) for row in rows))

    @mock_sts
    @mock_s3
    def test_manifest_zarr(self):
        """
        Test that when downloading a manifest with a zarr, all of the files are added into the manifest even
        if they are not listed in the service response.
        """
        bundle_fqid = BundleFQID('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z')
        self._index_canned_bundle(bundle_fqid)
        filters = {"fileFormat": {"is": ["matrix", "mtx"]}}
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            response = requests.get(self.base_url + '/index/files',
                                    params=dict(filters=json.dumps(filters)))
            hits = response.json()['hits']
            self.assertEqual(len(hits), 1)
            for single_part in False, True:
                with self.subTest(is_single_part=single_part):
                    with mock.patch.object(type(config), 'disable_multipart_manifests', single_part):
                        response = self._get_manifest(ManifestFormat.compact, filters)
                        self.assertEqual(200, response.status_code, 'Unable to download manifest')
                        # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
                        lines = response.content.decode('utf-8').splitlines()
                        tsv_file = csv.DictReader(lines, delimiter='\t')
                        rows = list(tsv_file)
                        self.assertEqual(len(rows), 13)  # 12 related file, one original
                        self.assertEqual(len(rows), len({row['file_uuid'] for row in rows}), 'Rows are not unique')

    @mock_sts
    @mock_s3
    def test_terra_bdbag_manifest(self):
        """
        moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit
        the server (see GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270)
        """
        self.maxDiff = None
        bundle_fqid = BundleFQID('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        domain = config.drs_domain or config.api_lambda_domain('service')
        dss = config.dss_endpoint

        bam_b0_0_uuid, bam_b0_0_version = "51c9ad31-5888-47eb-9e0c-02f042373c4e", "2018-10-10T031035.284782Z"
        bam_b0_1_uuid, bam_b0_1_version = "b1c167da-0825-4c63-9cbc-2aada1ab367c", "2018-10-10T031035.971561Z"
        fastq_b0_r1_uuid, fastq_b0_r1_version = "c005f647-b3fb-45a8-857a-8f5e6a878ccf", "2018-10-10T023811.612423Z"
        fastq_b0_r2_uuid, fastq_b0_r2_version = "b764ce7d-3938-4451-b68c-678feebc8f2a", "2018-10-10T023811.851483Z"
        fastq_b1_r1_uuid, fastq_b1_r1_version = "7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb", "2018-11-02T113344.698028Z"
        fastq_b1_r2_uuid, fastq_b1_r2_version = "74897eb7-0701-4e4f-9e6b-8b9521b2816b", "2018-11-02T113344.450442Z"
        expected_rows = [
            {
                'entity:participant_id': '587d74b4-1075-4bbf-b96a-4d1ede0481b2.2018-09-14T133314.453337Z',
                'bundle_uuid': '587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                'bundle_version': '2018-09-14T133314.453337Z',
                'cell_suspension__provenance__document_id': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0',
                'cell_suspension__estimated_cell_count': '0',
                'cell_suspension__selected_cell_type': '',
                'sequencing_process__provenance__document_id': '5afa951e-1591-4bad-a4f8-2e13cbdb760c',
                'sequencing_protocol__instrument_manufacturer_model': 'Illumina HiSeq 2500',
                'sequencing_protocol__paired_end': 'True',
                'library_preparation_protocol__library_construction_approach': 'Smart-seq2',
                'project__provenance__document_id': '6615efae-fca8-4dd2-a223-9cfcf30fe94d',
                'project__contributors__institution': 'Fake Institution',
                'project__contributors__laboratory': '',
                'project__project_core__project_short_name': 'integration/Smart-seq2/2018-10-10T02:23:36Z',
                'project__project_core__project_title': 'Q4_DEMO-Single cell RNA-seq of primary human glioblastomas',
                'specimen_from_organism__provenance__document_id': 'b5894cf5-ecdc-4ea6-a0b9-5335ab678c7a',
                'specimen_from_organism__diseases': 'glioblastoma',
                'specimen_from_organism__organ': 'brain',
                'specimen_from_organism__organ_part': 'temporal lobe',
                'specimen_from_organism__preservation_storage__preservation_method': '',
                'donor_organism__sex': 'unknown',
                'donor_organism__biomaterial_core__biomaterial_id': 'Q4_DEMO-donor_MGH30',
                'donor_organism__provenance__document_id': '242e38d2-c975-47ee-800a-6645b47e92d2',
                'donor_organism__genus_species': 'Homo sapiens',
                'donor_organism__diseases': '',
                'donor_organism__organism_age': '',
                'donor_organism__organism_age_unit': '',
                'cell_line__provenance__document_id': '',
                'cell_line__biomaterial_core__biomaterial_id': '',
                'organoid__provenance__document_id': '',
                'organoid__biomaterial_core__biomaterial_id': '',
                'organoid__model_organ': '',
                'organoid__model_organ_part': '',
                '_entity_type': 'specimens',
                'sample__provenance__document_id': 'b5894cf5-ecdc-4ea6-a0b9-5335ab678c7a',
                'sample__biomaterial_core__biomaterial_id': 'Q4_DEMO-sample_SAMN02797092',
                '__bam_0__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_qc.bam',
                '__bam_0__file_format': 'bam',
                '__bam_0__read_index': '',
                '__bam_0__file_size': '550597',
                '__bam_0__file_uuid': bam_b0_0_uuid,
                '__bam_0__file_version': bam_b0_0_version,
                '__bam_0__file_sha256': 'e3cd90d79f520c0806dddb1ca0c5a11fbe26ac0c0be983ba5098d6769f78294c',
                '__bam_0__file_content_type': 'application/gzip; dcp-type=data',
                '__bam_0__drs_url': f'drs://{domain}/{bam_b0_0_uuid}?version={bam_b0_0_version}',
                '__bam_0__file_url': f'{dss}/files/{bam_b0_0_uuid}?version={bam_b0_0_version}&replica=gcp',
                '__bam_1__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_rsem.bam',
                '__bam_1__file_format': 'bam',
                '__bam_1__read_index': '',
                '__bam_1__file_size': '3752733',
                '__bam_1__file_uuid': f'{bam_b0_1_uuid}',
                '__bam_1__file_version': bam_b0_1_version,
                '__bam_1__file_sha256': 'f25053412d65429cefc0157c0d18ae12d4bf4c4113a6af7a1820b62246c075a4',
                '__bam_1__file_content_type': 'application/gzip; dcp-type=data',
                '__bam_1__drs_url': f'drs://{domain}/{bam_b0_1_uuid}?version={bam_b0_1_version}',
                '__bam_1__file_url': f'{dss}/files/{bam_b0_1_uuid}?version={bam_b0_1_version}&replica=gcp',
                '__fastq_read1__file_name': 'R1.fastq.gz',
                '__fastq_read1__file_format': 'fastq.gz',
                '__fastq_read1__read_index': 'read1',
                '__fastq_read1__file_size': '125191',
                '__fastq_read1__file_uuid': fastq_b0_r1_uuid,
                '__fastq_read1__file_version': f'{fastq_b0_r1_version}',
                '__fastq_read1__file_sha256': 'fe6d4fdfea2ff1df97500dcfe7085ac3abfb760026bff75a34c20fb97a4b2b29',
                '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read1__file_url': f'{dss}/files/{fastq_b0_r1_uuid}?version={fastq_b0_r1_version}&replica=gcp',
                '__fastq_read1__drs_url': f'drs://{domain}/{fastq_b0_r1_uuid}?version={fastq_b0_r1_version}',
                '__fastq_read2__file_name': 'R2.fastq.gz',
                '__fastq_read2__file_format': 'fastq.gz',
                '__fastq_read2__read_index': 'read2',
                '__fastq_read2__file_size': '130024',
                '__fastq_read2__file_uuid': fastq_b0_r2_uuid,
                '__fastq_read2__file_version': fastq_b0_r2_version,
                '__fastq_read2__file_sha256': 'c305bee37b3c3735585e11306272b6ab085f04cd22ea8703957b4503488cfeba',
                '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read2__file_url': f'{dss}/files/{fastq_b0_r2_uuid}?version={fastq_b0_r2_version}&replica=gcp',
                '__fastq_read2__drs_url': f'drs://{domain}/{fastq_b0_r2_uuid}?version={fastq_b0_r2_version}',
            },
            {
                'entity:participant_id': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d.2018-11-02T113344.698028Z',
                'bundle_uuid': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                'bundle_version': '2018-11-02T113344.698028Z',
                'cell_suspension__provenance__document_id': '412898c5-5b9b-4907-b07c-e9b89666e204',
                'cell_suspension__estimated_cell_count': '1',
                'cell_suspension__selected_cell_type': '',
                'sequencing_process__provenance__document_id': '771ddaf6-3a4f-4314-97fe-6294ff8e25a4',
                'sequencing_protocol__instrument_manufacturer_model': 'Illumina NextSeq 500',
                'sequencing_protocol__paired_end': 'True',
                'library_preparation_protocol__library_construction_approach': 'Smart-seq2',
                'project__provenance__document_id': 'e8642221-4c2c-4fd7-b926-a68bce363c88',
                'project__contributors__institution': 'Farmers Trucks || University',
                'project__contributors__laboratory': 'John Dear',
                'project__project_core__project_short_name': 'Single of human pancreas',
                'project__project_core__project_title': 'Single cell transcriptome patterns.',
                'specimen_from_organism__provenance__document_id': 'a21dc760-a500-4236-bcff-da34a0e873d2',
                'specimen_from_organism__diseases': 'normal',
                'specimen_from_organism__organ': 'pancreas',
                'specimen_from_organism__organ_part': 'islet of Langerhans',
                'specimen_from_organism__preservation_storage__preservation_method': '',
                'donor_organism__sex': 'female',
                'donor_organism__biomaterial_core__biomaterial_id': 'DID_scRSq06',
                'donor_organism__provenance__document_id': '7b07b9d0-cc0e-4098-9f64-f4a569f7d746',
                'donor_organism__genus_species': 'Australopithecus',
                'donor_organism__diseases': 'normal',
                'donor_organism__organism_age': '38',
                'donor_organism__organism_age_unit': 'year',
                'cell_line__provenance__document_id': '',
                'cell_line__biomaterial_core__biomaterial_id': '',
                'organoid__provenance__document_id': '',
                'organoid__biomaterial_core__biomaterial_id': '',
                'organoid__model_organ': '',
                'organoid__model_organ_part': '',
                '_entity_type': 'specimens',
                'sample__provenance__document_id': 'a21dc760-a500-4236-bcff-da34a0e873d2',
                'sample__biomaterial_core__biomaterial_id': 'DID_scRSq06_pancreas',
                '__bam_0__file_name': '',
                '__bam_0__file_format': '',
                '__bam_0__read_index': '',
                '__bam_0__file_size': '',
                '__bam_0__file_uuid': '',
                '__bam_0__file_version': '',
                '__bam_0__file_sha256': '',
                '__bam_0__file_content_type': '',
                '__bam_0__drs_url': '',
                '__bam_0__file_url': '',
                '__bam_1__file_name': '',
                '__bam_1__file_format': '',
                '__bam_1__read_index': '',
                '__bam_1__file_size': '',
                '__bam_1__file_uuid': '',
                '__bam_1__file_version': '',
                '__bam_1__file_sha256': '',
                '__bam_1__file_content_type': '',
                '__bam_1__drs_url': '',
                '__bam_1__file_url': '',
                '__fastq_read1__file_name': 'SRR3562915_1.fastq.gz',
                '__fastq_read1__file_format': 'fastq.gz',
                '__fastq_read1__read_index': 'read1',
                '__fastq_read1__file_size': '195142097',
                '__fastq_read1__file_uuid': fastq_b1_r1_uuid,
                '__fastq_read1__file_version': fastq_b1_r1_version,
                '__fastq_read1__file_sha256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
                '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read1__drs_url': f'drs://{domain}/{fastq_b1_r1_uuid}?version={fastq_b1_r1_version}',
                '__fastq_read1__file_url': f'{dss}/files/{fastq_b1_r1_uuid}?version={fastq_b1_r1_version}&replica=gcp',
                '__fastq_read2__file_name': 'SRR3562915_2.fastq.gz',
                '__fastq_read2__file_format': 'fastq.gz',
                '__fastq_read2__read_index': 'read2',
                '__fastq_read2__file_size': '190330156',
                '__fastq_read2__file_uuid': fastq_b1_r2_uuid,
                '__fastq_read2__file_version': fastq_b1_r2_version,
                '__fastq_read2__file_sha256': '465a230aa127376fa641f8b8f8cad3f08fef37c8aafc67be454f0f0e4e63d68d',
                '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                '__fastq_read2__drs_url': f'drs://{domain}/{fastq_b1_r2_uuid}?version={fastq_b1_r2_version}',
                '__fastq_read2__file_url': f'{dss}/files/{fastq_b1_r2_uuid}?version={fastq_b1_r2_version}&replica=gcp',
            }
        ]
        with ResponsesHelper() as helper, TemporaryDirectory() as zip_dir:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            filters = {'fileFormat': {'is': ['bam', 'fastq.gz', 'fastq']}}
            response = self._get_manifest(ManifestFormat.terra_bdbag, filters, stream=True)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
                self.assertTrue(all(['manifest' == first(name.split('/')) for name in zip_fh.namelist()]))
                zip_fname = os.path.dirname(first(zip_fh.namelist()))
            with open(os.path.join(zip_dir, zip_fname, 'data', 'participants.tsv'), 'r') as fh:
                reader = csv.DictReader(fh, delimiter='\t')
                # The order in which the rows appear in the TSV is ultimately
                # driven by the order in which the documents are coming back
                # from the `files` index in Elasticsearch. To get a consistent
                # ordering of the ES response, we could apply a sort but doing
                # so slows down the scroll API which we use for manifests,
                # because manifest responses need exhaust the index. Instead,
                # we do comparison here that's insensitive of the row ordering.
                # We'll assert the column ordering independently below.
                self.assertEqual(set(map(freeze, expected_rows)), set(map(freeze, reader)))
                self.assertEqual([
                    'entity:participant_id',
                    'bundle_uuid',
                    'bundle_version',
                    'cell_suspension__provenance__document_id',
                    'cell_suspension__estimated_cell_count',
                    'cell_suspension__selected_cell_type',
                    'sequencing_process__provenance__document_id',
                    'sequencing_protocol__instrument_manufacturer_model',
                    'sequencing_protocol__paired_end',
                    'library_preparation_protocol__library_construction_approach',
                    'project__provenance__document_id',
                    'project__contributors__institution',
                    'project__contributors__laboratory',
                    'project__project_core__project_short_name',
                    'project__project_core__project_title',
                    'specimen_from_organism__provenance__document_id',
                    'specimen_from_organism__diseases',
                    'specimen_from_organism__organ',
                    'specimen_from_organism__organ_part',
                    'specimen_from_organism__preservation_storage__preservation_method',
                    'donor_organism__sex',
                    'donor_organism__biomaterial_core__biomaterial_id',
                    'donor_organism__provenance__document_id',
                    'donor_organism__genus_species',
                    'donor_organism__diseases',
                    'donor_organism__organism_age',
                    'donor_organism__organism_age_unit',
                    'cell_line__provenance__document_id',
                    'cell_line__biomaterial_core__biomaterial_id',
                    'organoid__provenance__document_id',
                    'organoid__biomaterial_core__biomaterial_id',
                    'organoid__model_organ',
                    'organoid__model_organ_part',
                    '_entity_type',
                    'sample__provenance__document_id',
                    'sample__biomaterial_core__biomaterial_id',
                    '__bam_0__file_name',
                    '__bam_0__file_format',
                    '__bam_0__read_index',
                    '__bam_0__file_size',
                    '__bam_0__file_uuid',
                    '__bam_0__file_version',
                    '__bam_0__file_sha256',
                    '__bam_0__file_content_type',
                    '__bam_0__drs_url',
                    '__bam_0__file_url',
                    '__bam_1__file_name',
                    '__bam_1__file_format',
                    '__bam_1__read_index',
                    '__bam_1__file_size',
                    '__bam_1__file_uuid',
                    '__bam_1__file_version',
                    '__bam_1__file_sha256',
                    '__bam_1__file_content_type',
                    '__bam_1__drs_url',
                    '__bam_1__file_url',
                    '__fastq_read1__file_name',
                    '__fastq_read1__file_format',
                    '__fastq_read1__read_index',
                    '__fastq_read1__file_size',
                    '__fastq_read1__file_uuid',
                    '__fastq_read1__file_version',
                    '__fastq_read1__file_sha256',
                    '__fastq_read1__file_content_type',
                    '__fastq_read1__drs_url',
                    '__fastq_read1__file_url',
                    '__fastq_read2__file_name',
                    '__fastq_read2__file_format',
                    '__fastq_read2__read_index',
                    '__fastq_read2__file_size',
                    '__fastq_read2__file_uuid',
                    '__fastq_read2__file_version',
                    '__fastq_read2__file_sha256',
                    '__fastq_read2__file_content_type',
                    '__fastq_read2__drs_url',
                    '__fastq_read2__file_url',
                ], reader.fieldnames)

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

    @mock_sts
    @mock_s3
    def test_bdbag_manifest_for_redundant_entries(self):
        """
        Test that redundant bundles are removed from the terra.bdbag manifest response
        """
        self.maxDiff = None
        # Primary bundle cfab8304 has the files:
        # - d879f732-d8d4-4251-a2ca-a91a852a034b
        # - 1e14d503-31b1-4db6-82ba-f8d83bd85b9b
        # and derived analysis bundle f0731ab4 has the same files and more.
        self._index_canned_bundle(BundleFQID('cfab8304-dc9f-439e-af29-f8eb75b0729d', '2019-07-18T212820.595913Z'))
        self._index_canned_bundle(BundleFQID('f0731ab4-6b80-4eed-97c9-4984de81a47c', '2019-07-23T062120.663434Z'))

        # In both subtests below we expect the primary bundle to be omitted from
        # the results as it is redundant compared to the analysis bundle.
        expected_bundle_uuids = {
            'f0731ab4-6b80-4eed-97c9-4984de81a47c',  # analysis bundle
            'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'  # bundle with no shared files (added by WebServiceTestCase)
        }
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper, TemporaryDirectory() as zip_dir:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            for filters in [
                # With a filter limiting to fastq both the primary and analysis
                # bundles will have the same set of files when compared.
                {'fileFormat': {'is': ['fastq.gz']}},
                # With no filter the primary bundle will contain a subset of
                # files when compared to its analysis bundle.
                {}
            ]:
                with self.subTest(filters=filters):
                    response = self._get_manifest(ManifestFormat.terra_bdbag, filters=filters, stream=True)
                    self.assertEqual(200, response.status_code, 'Unable to download manifest')
                    with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                        zip_fh.extractall(zip_dir)
                        self.assertTrue(all(['manifest' == first(name.split('/')) for name in zip_fh.namelist()]))
                        zip_fname = os.path.dirname(first(zip_fh.namelist()))
                    with open(os.path.join(zip_dir, zip_fname, 'data', 'participants.tsv'), 'r') as fh:
                        reader = csv.DictReader(fh, delimiter='\t')
                        bundle_uuids = {row['bundle_uuid'] for row in reader}
                        self.assertEqual(bundle_uuids, expected_bundle_uuids)

    @mock_sts
    @mock_s3
    def test_full_metadata(self):
        self.maxDiff = None
        bundle_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            response = self._get_manifest(ManifestFormat.full, {})
            self.assertEqual(200, response.status_code, 'Unable to download manifest')

            expected = [
                ('bundle_uuid',
                 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                 'f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                 'f79257a7-dfc6-46d6-ae00-ba4b25313c10'),

                ('bundle_version',
                 '2018-11-02T113344.698028Z',
                 '2018-11-02T113344.698028Z',
                 '2018-09-14T133314.453337Z',
                 '2018-09-14T133314.453337Z'),

                ('cell_suspension.biomaterial_core.biomaterial_description',
                 'Single cell from human pancreas',
                 'Single cell from human pancreas',
                 '',
                 ''),

                ('cell_suspension.biomaterial_core.insdc_biomaterial', 'SRS1459312', 'SRS1459312', '', ''),
                ('cell_suspension.biomaterial_core.ncbi_taxon_id', '9606', '9606', '10090||10091', '10090||10091'),

                ('cell_suspension.biomaterial_core.supplementary_files',
                 '',
                 '',
                 'FACS_sorting_markers.pdf',
                 'FACS_sorting_markers.pdf'),

                ('cell_suspension.genus_species.ontology',
                 'NCBITaxon:9606',
                 'NCBITaxon:9606',
                 'NCBITaxon:10090',
                 'NCBITaxon:10090'),

                ('cell_suspension.genus_species.ontology_label', 'Homo sapiens', 'Homo sapiens', '', ''),
                ('cell_suspension.genus_species.text', 'Homo sapiens', 'Homo sapiens', 'Mus musculus', 'Mus musculus'),
                ('cell_suspension.plate_based_sequencing.plate_id', '', '', '827', '827'),
                ('cell_suspension.plate_based_sequencing.well_id', '', '', 'G06', 'G06'),

                ('cell_suspension.provenance.document_id',
                 '412898c5-5b9b-4907-b07c-e9b89666e204',
                 '412898c5-5b9b-4907-b07c-e9b89666e204',
                 '0037c9eb-8038-432f-8d9d-13ee094e54ab||aaaaaaaa-8038-432f-8d9d-13ee094e54ab',
                 '0037c9eb-8038-432f-8d9d-13ee094e54ab||aaaaaaaa-8038-432f-8d9d-13ee094e54ab'),

                ('cell_suspension.selected_cell_type.text', '', '', 'CAFs', 'CAFs'),
                ('cell_suspension.total_estimated_cells', '1', '1', '1||9000', '1||9000'),

                ('dissociation_protocol.dissociation_method.ontology',
                 'EFO:0009108',
                 'EFO:0009108',
                 'EFO:0009129',
                 'EFO:0009129'),

                ('dissociation_protocol.dissociation_method.ontology_label',
                 'fluorescence-activated cell sorting',
                 'fluorescence-activated cell sorting',
                 '',
                 ''),

                ('dissociation_protocol.dissociation_method.text',
                 'fluorescence-activated cell sorting',
                 'fluorescence-activated cell sorting',
                 'mechanical dissociation',
                 'mechanical dissociation'),

                ('dissociation_protocol.protocol_core.document',
                 '',
                 '',
                 'TissueDissociationProtocol.pdf',
                 'TissueDissociationProtocol.pdf'),

                ('dissociation_protocol.protocol_core.protocol_name',
                 '',
                 '',
                 'Extracting cells from lymph nodes',
                 'Extracting cells from lymph nodes'),

                ('dissociation_protocol.protocol_core.publication_doi',
                 'https://doi.org/10.1101/108043',
                 'https://doi.org/10.1101/108043',
                 '',
                 ''),

                ('dissociation_protocol.provenance.document_id',
                 '31e708d3-79df-49b8-a3df-b1d694963468',
                 '31e708d3-79df-49b8-a3df-b1d694963468',
                 '40056e47-131d-4c6e-a884-a927bfccf8ce',
                 '40056e47-131d-4c6e-a884-a927bfccf8ce'),

                ('donor_organism.biomaterial_core.biomaterial_name', '', '', 'Mouse_day8_rep12', 'Mouse_day8_rep12'),
                ('donor_organism.biomaterial_core.ncbi_taxon_id', '9606', '9606', '10090', '10090'),
                ('donor_organism.death.cause_of_death', 'stroke', 'stroke', '', ''),
                ('donor_organism.diseases.ontology', 'PATO:0000461', 'PATO:0000461', 'MONDO:0005105', 'MONDO:0005105'),
                ('donor_organism.diseases.ontology_label', 'normal', 'normal', '', ''),
                ('donor_organism.diseases.text', 'normal', 'normal', 'subcutaneous melanoma', 'subcutaneous melanoma'),

                ('donor_organism.genus_species.ontology',
                 'NCBITaxon:9606',
                 'NCBITaxon:9606',
                 'NCBITaxon:10090',
                 'NCBITaxon:10090'),

                ('donor_organism.genus_species.ontology_label', 'Australopithecus', 'Australopithecus', '', ''),

                ('donor_organism.genus_species.text',
                 'Australopithecus',
                 'Australopithecus',
                 'Mus musculus',
                 'Mus musculus'),

                ('donor_organism.human_specific.body_mass_index', '29.5', '29.5', '', ''),
                ('donor_organism.human_specific.ethnicity.ontology', 'hancestro:0005', 'hancestro:0005', '', ''),
                ('donor_organism.human_specific.ethnicity.ontology_label', 'European', 'European', '', ''),
                ('donor_organism.human_specific.ethnicity.text', 'European', 'European', '', ''),
                ('donor_organism.is_living', 'no', 'no', 'no', 'no'),
                ('donor_organism.mouse_specific.strain.ontology', '', '', 'EFO:0004472', 'EFO:0004472'),
                ('donor_organism.mouse_specific.strain.text', '', '', 'C57BL/6', 'C57BL/6'),
                ('donor_organism.organism_age', '38', '38', '6-12', '6-12'),
                ('donor_organism.organism_age_unit.ontology', 'UO:0000036', 'UO:0000036', 'UO:0000034', 'UO:0000034'),
                ('donor_organism.organism_age_unit.ontology_label', 'year', 'year', '', ''),
                ('donor_organism.organism_age_unit.text', 'year', 'year', 'week', 'week'),

                ('donor_organism.provenance.document_id',
                 '7b07b9d0-cc0e-4098-9f64-f4a569f7d746',
                 '7b07b9d0-cc0e-4098-9f64-f4a569f7d746',
                 '89b50434-f831-4e15-a8c0-0d57e6baa94c',
                 '89b50434-f831-4e15-a8c0-0d57e6baa94c'),

                ('donor_organism.sex', 'female', 'female', 'female', 'female'),

                ('enrichment_protocol.enrichment_method.ontology',
                 'EFO:0009108',
                 'EFO:0009108',
                 'EFO:0009108',
                 'EFO:0009108'),

                ('enrichment_protocol.enrichment_method.ontology_label',
                 'fluorescence-activated cell sorting',
                 'fluorescence-activated cell sorting',
                 '',
                 ''),

                ('enrichment_protocol.enrichment_method.text',
                 'FACS',
                 'FACS',
                 'fluorescence-activated cell sorting',
                 'fluorescence-activated cell sorting'),

                ('enrichment_protocol.markers',
                 'HPx1+ HPi2+ CD133/1+ CD133/2+',
                 'HPx1+ HPi2+ CD133/1+ CD133/2+',
                 'CD45- GFP+ CD31-',
                 'CD45- GFP+ CD31-'),

                ('enrichment_protocol.protocol_core.protocol_name',
                 '',
                 '',
                 'FACS sorting cells by surface markers',
                 'FACS sorting cells by surface markers'),

                ('enrichment_protocol.protocol_core.publication_doi',
                 'https://doi.org/10.1101/108043',
                 'https://doi.org/10.1101/108043',
                 '',
                 ''),

                ('enrichment_protocol.provenance.document_id',
                 '5bd4ba68-4c0e-4d22-840d-afc025e7badc',
                 '5bd4ba68-4c0e-4d22-840d-afc025e7badc',
                 'd3287615-b97a-4984-a8cf-30a1c30e4773',
                 'd3287615-b97a-4984-a8cf-30a1c30e4773'),

                ('file_format',
                 'fastq.gz',
                 'fastq.gz',
                 'fastq.gz',
                 'fastq.gz'),

                ('file_name',
                 'SRR3562915_2.fastq.gz',
                 'SRR3562915_1.fastq.gz',
                 '22028_5#300_1.fastq.gz',
                 '22028_5#300_2.fastq.gz'),

                ('file_sha256',
                 '465a230aa127376fa641f8b8f8cad3f08fef37c8aafc67be454f0f0e4e63d68d',
                 '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
                 '3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582',
                 'cda141411815a9e8e4c3145f6b855a295352fd18f7db449d3797d8de38fb052a'),

                ('file_size',
                 '190330156',
                 '195142097',
                 '64718465',
                 '65008198'),

                ('file_uuid',
                 '74897eb7-0701-4e4f-9e6b-8b9521b2816b',
                 '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                 'f2b6c6f0-8d25-4aae-b255-1974cc110cfe',
                 'f6608ce9-a570-4d5d-bd1f-407454958424'),

                ('file_version',
                 '2018-11-02T113344.450442Z',
                 '2018-11-02T113344.698028Z',
                 '2018-09-14T123343.720332Z',
                 '2018-09-14T123345.304412Z'),

                ('library_preparation_protocol.end_bias', 'full length', 'full length', 'full length', 'full length'),
                ('library_preparation_protocol.input_nucleic_acid_molecule.ontology',
                 'OBI:0000869',
                 'OBI:0000869',
                 'OBI:0000869',
                 'OBI:0000869'),

                ('library_preparation_protocol.input_nucleic_acid_molecule.text',
                 'polyA RNA',
                 'polyA RNA',
                 'polyA RNA',
                 'polyA RNA'),

                ('library_preparation_protocol.library_construction_approach.ontology',
                 'EFO:0008931',
                 'EFO:0008931',
                 'EFO:0008931',
                 'EFO:0008931'),

                ('library_preparation_protocol.library_construction_approach.ontology_label',
                 'Smart-seq2',
                 'Smart-seq2',
                 '',
                 ''),

                ('library_preparation_protocol.library_construction_approach.text',
                 'Smart-seq2',
                 'Smart-seq2',
                 'Smart-seq2',
                 'Smart-seq2'),

                ('library_preparation_protocol.library_construction_kit.manufacturer', 'Illumina', 'Illumina', '', ''),

                ('library_preparation_protocol.library_construction_kit.retail_name',
                 'Nextera XT kit',
                 'Nextera XT kit',
                 '',
                 ''),

                ('library_preparation_protocol.nucleic_acid_source',
                 'single cell',
                 'single cell',
                 'single cell',
                 'single cell'),

                ('library_preparation_protocol.primer', 'poly-dT', 'poly-dT', 'poly-dT', 'poly-dT'),
                ('library_preparation_protocol.protocol_core.document',
                 '',
                 '',
                 'SmartSeq2_RTPCR_protocol.pdf',
                 'SmartSeq2_RTPCR_protocol.pdf'),

                ('library_preparation_protocol.protocol_core.protocol_name',
                 '',
                 '',
                 'Make/amplify cDNA for each cell',
                 'Make/amplify cDNA for each cell'),

                ('library_preparation_protocol.provenance.document_id',
                 '9c32cf70-3ed7-4720-badc-5ee71e8a38af',
                 '9c32cf70-3ed7-4720-badc-5ee71e8a38af',
                 '0076f0aa-14c6-4cb9-93f8-97229787be21',
                 '0076f0aa-14c6-4cb9-93f8-97229787be21'),

                ('library_preparation_protocol.strand', 'unstranded', 'unstranded', 'unstranded', 'unstranded'),
                ('library_preparation_protocol.umi_barcode.barcode_length', '', '', '16', '16'),
                ('library_preparation_protocol.umi_barcode.barcode_offset', '', '', '0', '0'),
                ('library_preparation_protocol.umi_barcode.barcode_read', '', '', 'Read 1', 'Read 1'),

                ('process.provenance.document_id',
                 '||'.join([
                     '4674255d-5ecd-4860-9b8d-beae98772cd9',
                     '4c28e079-59af-4bd3-8c8b-763ea0beba98',
                     '771ddaf6-3a4f-4314-97fe-6294ff8e25a4']),
                 '||'.join([
                     '4674255d-5ecd-4860-9b8d-beae98772cd9',
                     '4c28e079-59af-4bd3-8c8b-763ea0beba98',
                     '771ddaf6-3a4f-4314-97fe-6294ff8e25a4']),
                 '||'.join([
                     '6d77eef9-96cf-410e-8bbc-a83430267b61',
                     '72732ed3-7b71-47df-bcec-c765ef7ea758',
                     'c0f05fdb-8375-4c39-adba-24a63c004b9d']),
                 '||'.join([
                     '6d77eef9-96cf-410e-8bbc-a83430267b61',
                     '72732ed3-7b71-47df-bcec-c765ef7ea758',
                     'c0f05fdb-8375-4c39-adba-24a63c004b9d'])),

                ('project.geo_series', 'GSE81547', 'GSE81547', '', ''),
                ('project.insdc_project', 'SRP075496', 'SRP075496', '', ''),

                ('project.project_core.project_short_name',
                 'Single of human pancreas',
                 'Single of human pancreas',
                 'Mouse Melanoma',
                 'Mouse Melanoma'),

                ('project.project_core.project_title',
                 'Single cell transcriptome patterns.',
                 'Single cell transcriptome patterns.',
                 'Melanoma infiltration of stromal and immune cells',
                 'Melanoma infiltration of stromal and immune cells'),

                ('project.provenance.document_id',
                 'e8642221-4c2c-4fd7-b926-a68bce363c88',
                 'e8642221-4c2c-4fd7-b926-a68bce363c88',
                 '67bc798b-a34a-4104-8cab-cad648471f69',
                 '67bc798b-a34a-4104-8cab-cad648471f69'),

                ('project.supplementary_links',
                 'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results',
                 'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-81547/Results',
                 '',
                 ''),

                ('sequence_file.insdc_run', 'SRR3562915', 'SRR3562915', '', ''),
                ('sequence_file.lane_index', '', '', '5', '5'),

                ('sequence_file.provenance.document_id',
                 '70d1af4a-82c8-478a-8960-e9028b3616ca',
                 '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb',
                 '6c946b6c-040e-45cc-9114-a8b1454c8d20',
                 'c86e42d7-854a-479b-a627-6be1b49c980c'),

                ('sequence_file.read_index', 'read2', 'read1', 'read1', 'read2'),
                ('sequence_file.read_length', '75', '75', '', ''),

                ('sequencing_protocol.instrument_manufacturer_model.ontology',
                 'EFO:0008566',
                 'EFO:0008566',
                 'EFO:0008567',
                 'EFO:0008567'),

                ('sequencing_protocol.instrument_manufacturer_model.ontology_label',
                 'Illumina NextSeq 500',
                 'Illumina NextSeq 500',
                 '',
                 ''),

                ('sequencing_protocol.instrument_manufacturer_model.text',
                 'Illumina NextSeq 500',
                 'Illumina NextSeq 500',
                 'Illumina HiSeq 2500',
                 'Illumina HiSeq 2500'),

                ('sequencing_protocol.paired_end', 'True', 'True', 'True', 'True'),
                ('sequencing_protocol.protocol_core.document',
                 '',
                 '',
                 'SmartSeq2_sequencing_protocol.pdf',
                 'SmartSeq2_sequencing_protocol.pdf'),
                ('sequencing_protocol.protocol_core.protocol_name',
                 '',
                 '',
                 'Sequencing SmartSeq2 cells',
                 'Sequencing SmartSeq2 cells'),

                ('sequencing_protocol.provenance.document_id',
                 '61e629ed-0135-4492-ac8a-5c4ab3ccca8a',
                 '61e629ed-0135-4492-ac8a-5c4ab3ccca8a',
                 '362d9c34-f5c0-4906-955b-61ba0aac58cc',
                 '362d9c34-f5c0-4906-955b-61ba0aac58cc'),

                ('sequencing_protocol.sequencing_approach.ontology',
                 'EFO:0008896',
                 'EFO:0008896',
                 'EFO:0008931',
                 'EFO:0008931'),

                ('sequencing_protocol.sequencing_approach.ontology_label', 'RNA-Seq', 'RNA-Seq', '', ''),
                ('sequencing_protocol.sequencing_approach.text', 'RNA-Seq', 'RNA-Seq', 'Smart-seq2', 'Smart-seq2'),

                ('specimen_from_organism.biomaterial_core.biomaterial_name',
                 '',
                 '',
                 'Mouse_day10_T_rep12||Mouse_day8_T_rep12',
                 'Mouse_day10_T_rep12||Mouse_day8_T_rep12'),

                ('specimen_from_organism.biomaterial_core.ncbi_taxon_id',
                 '9606',
                 '9606',
                 '10090||10091',
                 '10090||10091'),

                ('specimen_from_organism.diseases.ontology', 'PATO:0000461', 'PATO:0000461', '', ''),
                ('specimen_from_organism.diseases.ontology_label', 'normal', 'normal', '', ''),
                ('specimen_from_organism.diseases.text', 'normal', 'normal', '', ''),

                ('specimen_from_organism.genus_species.ontology',
                 'NCBITaxon:9606',
                 'NCBITaxon:9606',
                 'NCBITaxon:10090||NCBITaxon:10091',
                 'NCBITaxon:10090||NCBITaxon:10091'),

                ('specimen_from_organism.genus_species.ontology_label', 'Australopithecus', 'Australopithecus', '', ''),

                ('specimen_from_organism.genus_species.text',
                 'Australopithecus',
                 'Australopithecus',
                 'Mus musculus||heart',
                 'Mus musculus||heart'),

                ('specimen_from_organism.organ.ontology', 'UBERON:0001264', 'UBERON:0001264', '', ''),
                ('specimen_from_organism.organ.ontology_label', 'pancreas', 'pancreas', '', ''),
                ('specimen_from_organism.organ.text', 'pancreas', 'pancreas', 'brain||tumor', 'brain||tumor'),
                ('specimen_from_organism.organ_part.ontology', 'UBERON:0000006', 'UBERON:0000006', '', ''),
                ('specimen_from_organism.organ_part.ontology_label', 'islet of Langerhans', 'islet of Langerhans', '',
                 ''),
                ('specimen_from_organism.organ_part.text', 'islet of Langerhans', 'islet of Langerhans', '', ''),

                ('specimen_from_organism.provenance.document_id',
                 'a21dc760-a500-4236-bcff-da34a0e873d2',
                 'a21dc760-a500-4236-bcff-da34a0e873d2',
                 'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244||b4e55fe1-7bab-44ba-a81d-3d8cb3873244',
                 'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244||b4e55fe1-7bab-44ba-a81d-3d8cb3873244')
            ]
            self._assert_tsv(expected, response)

    @mock_sts
    @mock_s3
    def test_full_metadata_missing_fields(self):
        self.maxDiff = None
        bundle_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            filters = {'project': {'is': ['Single of human pancreas']}}
            response = self._get_manifest(ManifestFormat.full, filters)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
            lines = response.content.decode('utf-8').splitlines()
            tsv_file1 = csv.reader(lines, delimiter='\t')
            fieldnames1 = set(next(tsv_file1))

            filters = {'project': {'is': ['Mouse Melanoma']}}
            response = self._get_manifest(ManifestFormat.full, filters)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            # Cannot use response.iter_lines() because of https://github.com/psf/requests/issues/3980
            lines = response.content.decode('utf-8').splitlines()
            tsv_file2 = csv.reader(lines, delimiter='\t')
            fieldnames2 = set(next(tsv_file2))

            intersection = fieldnames1 & fieldnames2
            symmetric_diff = fieldnames1 ^ fieldnames2

            self.assertGreater(len(intersection), 0)
            self.assertGreater(len(symmetric_diff), 0)

    def test_manifest_format_validation(self):
        url = self.base_url + '/manifest/files?format=invalid-type'
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.content)

    def test_manifest_filter_validation(self):
        url = self.base_url + '/manifest/files?format=compact&filters={"fileFormat":["pdf"]}'
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.content)

    @mock_sts
    @mock_s3
    def test_manifest_content_disposition_header(self):
        bundle_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        with mock.patch.object(manifest_service, 'datetime') as mock_response:
            mock_response.now.return_value = datetime(1985, 10, 25, 1, 21)
            storage_service = StorageService()
            storage_service.create_bucket()
            for single_part in True, False:
                for filters, expected_name in [
                    # For a single project, the content disposition file name should
                    # be the project name followed by the date and time
                    (
                        {'project': {'is': ['Single of human pancreas']}},
                        'Single of human pancreas 1985-10-25 01.21'
                    ),
                    # In all other cases, the standard content disposition file name
                    # should be "hca-manifest-" followed by the manifest key, a
                    # v5 UUID deterministically derived from the filter and
                    (
                        {'project': {'is': ['Single of human pancreas', 'Mouse Melanoma']}},
                        'hca-manifest-' + (
                            '4ad2e5aa-dcbd-5fed-ac3e-17bcd82c8c0f' if single_part
                            else '525cebc4-b96a-5f75-b329-8268fe947788'
                        ),
                    ),
                    (
                        {},
                        'hca-manifest-' + (
                            '4a5cd62a-8c55-5ddf-94e5-86c51f7603cd' if single_part
                            else '585ac408-338e-5e3e-a856-e4df962509b6'
                        ),
                    )
                ]:
                    with self.subTest(filters=filters, single_part=single_part):
                        with mock.patch.object(type(config), 'disable_multipart_manifests', single_part):
                            assert config.disable_multipart_manifests is single_part
                            url, was_cached = self._get_manifest_url(ManifestFormat.full, filters)
                            self.assertFalse(was_cached)
                            query = urlparse(url).query
                            expected_cd = f'attachment;filename="{expected_name}.tsv"'
                            actual_cd = one(parse_qs(query).get('response-content-disposition'))
                            self.assertEqual(actual_cd, expected_cd)


class TestManifestCache(ManifestTestCase):

    @mock_sts
    @mock_s3
    @mock.patch('azul.service.manifest_service.ManifestService._get_seconds_until_expire')
    def test_metadata_cache_expiration(self, get_seconds):
        self.maxDiff = None
        bundle_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(bundle_fqid)
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            def log_messages_from_manifest_request(seconds_until_expire: int) -> List[str]:
                get_seconds.return_value = seconds_until_expire
                filters = {'projectId': {'is': ['67bc798b-a34a-4104-8cab-cad648471f69']}}
                from azul.service.manifest_service import logger as logger_
                with self.assertLogs(logger=logger_, level='INFO') as logs:
                    response = self._get_manifest(ManifestFormat.full, filters)
                    self.assertEqual(200, response.status_code, 'Unable to download manifest')
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
            self.assertTrue(any('Cached manifest about to expire' in message for message in logs_output))

    @mock_sts
    @mock_s3
    @mock.patch('azul.service.manifest_service.ManifestService._get_seconds_until_expire')
    def test_full_metadata_cache(self, get_seconds):
        get_seconds.return_value = 3600
        self.maxDiff = None
        for bundle_fqid in [
            BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z'),
            BundleFQID('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-09-14T133314.453337Z')
        ]:
            self._index_canned_bundle(bundle_fqid)
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            for single_part in False, True:
                with self.subTest(is_single_part=single_part):
                    with mock.patch.object(type(config), 'disable_multipart_manifests', single_part):
                        project_ids = ['67bc798b-a34a-4104-8cab-cad648471f69', '6615efae-fca8-4dd2-a223-9cfcf30fe94d']
                        file_names = defaultdict(list)

                        # Run the generation of manifests twice to verify generated file names are the same when re-run
                        for project_id in project_ids * 2:
                            response = self._get_manifest(ManifestFormat.full, {'projectId': {'is': [project_id]}})
                            self.assertEqual(200, response.status_code, 'Unable to download manifest')
                            file_name = urlparse(response.url).path
                            file_names[project_id].append(file_name)

                        self.assertEqual(file_names.keys(), set(project_ids))
                        self.assertEqual([2, 2], list(map(len, file_names.values())))
                        self.assertEqual([1, 1], list(map(len, map(set, file_names.values()))))

    @mock_sts
    @mock_s3
    def test_hash_validity(self):
        self.maxDiff = None
        bundle_uuid = 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'
        original_fqid = BundleFQID(bundle_uuid, '2018-11-02T113344.698028Z')
        self._index_canned_bundle(original_fqid)
        filters = {'project': {'is': ['Single of human pancreas']}}
        old_object_keys = {}
        service = ManifestService(StorageService())
        for format_ in ManifestFormat:
            with self.subTest(msg='indexing new bundle', format_=format_):
                # When a new bundle is indexed and its full manifest cached,
                # a matching object_key is generated ...
                generator = manifest_service.ManifestGenerator.for_format(format_, service, filters)
                old_bundle_object_key = service._derive_manifest_key(format_, filters,
                                                                     generator.manifest_content_hash)
                # and should remain valid ...
                new_generator = manifest_service.ManifestGenerator.for_format(format_, service, filters)
                self.assertEqual(old_bundle_object_key,
                                 service._derive_manifest_key(format_, filters,
                                                              new_generator.manifest_content_hash))
                old_object_keys[format_] = old_bundle_object_key

        # ... until a new bundle belonging to the same project is indexed, at which point a manifest request
        # will generate a different object_key ...
        update_fqid = BundleFQID(bundle_uuid, '2018-11-04T113344.698028Z')
        self._index_canned_bundle(update_fqid)
        new_object_keys = {}
        for format_ in ManifestFormat:
            with self.subTest(msg='indexing second bundle', format_=format_):
                generator = manifest_service.ManifestGenerator.for_format(format_, service, filters)
                new_bundle_object_key = service._derive_manifest_key(format_, filters,
                                                                     generator.manifest_content_hash)
                # ... invalidating the cached object previously used for the same filter.
                self.assertNotEqual(old_object_keys[format_], new_bundle_object_key)
                new_object_keys[format_] = new_bundle_object_key

        # Updates or additions, unrelated to that project do not affect object key generation
        other_fqid = BundleFQID('f79257a7-dfc6-46d6-ae00-ba4b25313c10', '2018-09-14T133314.453337Z')
        self._index_canned_bundle(other_fqid)
        for format_ in ManifestFormat:
            with self.subTest(msg='indexing unrelated bundle', format_=format_):
                generator = manifest_service.ManifestGenerator.for_format(format_, service, filters)
                latest_bundle_object_key = service._derive_manifest_key(format_, filters,
                                                                        generator.manifest_content_hash)

                self.assertEqual(latest_bundle_object_key, new_object_keys[format_])


class TestManifestResponse(AzulTestCase):

    def test_get_seconds_until_expire(self):
        """
        Verify a header with valid Expiration and LastModified values returns the correct expiration value
        """
        margin = ManifestService._date_diff_margin
        for object_age, expect_error in [(0, False),
                                         (margin - 1, False),
                                         (margin, False),
                                         (margin + 1, True)]:
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
