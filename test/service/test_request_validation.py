import csv
import logging
import os
import sys

from tempfile import TemporaryDirectory
from unittest import mock
from io import BytesIO
from more_itertools import first

from moto import mock_s3, mock_sts
import requests

import azul.changelog
from azul import config
from azul.json_freeze import freeze
from azul.service import service_config
from azul.service.responseobjects.storage_service import StorageService
from retorts import ResponsesHelper
from service import WebServiceTestCase
from zipfile import ZipFile

log = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class FacetNameValidationTest(WebServiceTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    filter_facet_message = {"Code": "BadRequestError",
                            "Message": "BadRequestError: Unable to filter by undefined facet bad-facet."}
    sort_facet_message = {"Code": "BadRequestError",
                          "Message": "BadRequestError: Unable to sort by undefined facet bad-facet."}
    service_config_dir = os.path.dirname(service_config.__file__)

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                for dirty in True, False:
                    with self.subTest(is_repo_dirty=dirty):
                        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
                            url = self.base_url + "/version"
                            response = requests.get(url)
                            response.raise_for_status()
                            expected_json = {
                                'commit': commit,
                                'dirty': dirty
                            }
                            self.assertEqual(response.json()['git'], expected_json)

    def test_bad_single_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?from=1&size=1&filters={'file':{'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?from=1&size=1" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?from=1&size=1" \
                              "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_sample(self):
        url = self.base_url + "/repository/samples?size=15&filters={}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?size=15" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=bad-facet&order=asc"

        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_valid_sort_facet_but_bad_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?size=15" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=organPart&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_sample(self):
        url = self.base_url + "/repository/samples?size=15" \
                              "&filters={'file':{'organPart':{'is':['fake-val2']}}}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?from=1&size=1" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?from=1&size=1" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?from=1&size=1" \
                              "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_file(self):
        url = self.base_url + "/repository/files?size=15&sort=bad-facet&order=asc" \
                              "&filters={}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?size=15" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?size=15&sort=bad-facet&order=asc" \
                              "&filters={'file':{'organ':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):
        url = self.base_url + "/repository/files?size=15&sort=organPart&order=asc" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_single_entity_error_responses(self):
        entity_types = ['files', 'projects']
        for uuid, expected_error_code in [('2b7959bb-acd1-4aa3-9557-345f9b3c6327', 404),
                                          ('-0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb-', 400),
                                          ('FOO', 400)]:
            for entity_type in entity_types:
                with self.subTest(entity_name=entity_type, error_code=expected_error_code, uuid=uuid):
                    url = self.base_url + f'/repository/{entity_type}/{uuid}'
                    response = requests.get(url)
                    self.assertEqual(expected_error_code, response.status_code)

    def test_file_order(self):
        url = self.base_url + '/repository/files/order'
        response = requests.get(url)
        self.assertEqual(200, response.status_code, response.json())

        order_config_filepath = '{}/order_config'.format(self.service_config_dir)
        with open(order_config_filepath, 'r') as order_settings_file:
            actual_field_order = [entity_field for entity_field in response.json()['order']]
            expected_field_order = [entity_field.strip() for entity_field in order_settings_file.readlines()]
            self.assertEqual(expected_field_order, actual_field_order, "Field order is not configured correctly")

    @mock_sts
    @mock_s3
    def test_manifest(self):
        self.maxDiff = None
        self._index_canned_bundle(("f79257a7-dfc6-46d6-ae00-ba4b25313c10", "2018-09-14T133314.453337Z"))
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            for single_part in False, True:
                with self.subTest(is_single_part=single_part):
                    with mock.patch.object(type(config), 'disable_multipart_manifests', single_part):
                        url = self.base_url + '/repository/files/export?filters={"file":{}}'
                        response = requests.get(url)
                        self.assertEqual(200, response.status_code, 'Unable to download manifest')

                        expected = [
                            ('bundle_uuid', 'f79257a7-dfc6-46d6-ae00-ba4b25313c10',
                             'f79257a7-dfc6-46d6-ae00-ba4b25313c10'),
                            ('bundle_version', '2018-09-14T133314.453337Z', '2018-09-14T133314.453337Z'),
                            ('file_name', 'SmartSeq2_RTPCR_protocol.pdf', '22028_5#300_1.fastq.gz'),
                            ('file_format', 'pdf', 'fastq.gz'),
                            ('read_index', '', 'read1'),
                            ('file_size', '29230', '64718465'),
                            ('file_uuid', '5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                             'f2b6c6f0-8d25-4aae-b255-1974cc110cfe'),
                            ('file_version', '2018-09-14T123347.012715Z', '2018-09-14T123343.720332Z'),
                            ('file_sha256', '2f6866c4ede92123f90dd15fb180fac56e33309b8fd3f4f52f263ed2f8af2f16',
                             '3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582'),
                            ('file_content_type', 'application/pdf; dcp-type=data', 'application/gzip; dcp-type=data'),
                            ('cell_suspension.provenance.document_id', '', '0037c9eb-8038-432f-8d9d-13ee094e54ab || aaaaaaaa-8038-432f-8d9d-13ee094e54ab'),
                            ('cell_suspension.estimated_cell_count', '', '9001'),
                            ('cell_suspension.selected_cell_type', '', 'CAFs'),
                            ('sequencing_protocol.instrument_manufacturer_model', '', 'Illumina HiSeq 2500'),
                            ('sequencing_protocol.paired_end', '', 'True'),
                            ('library_preparation_protocol.library_construction_approach', '', 'Smart-seq2'),
                            ('project.provenance.document_id', '67bc798b-a34a-4104-8cab-cad648471f69',
                             '67bc798b-a34a-4104-8cab-cad648471f69'),
                            ('project.contributors.institution',
                             'DKFZ German Cancer Research Center || EMBL-EBI || University of Cambridge'
                             ' || University of Helsinki || Wellcome Trust Sanger Institute',
                             'DKFZ German Cancer Research Center || EMBL-EBI || University of Cambridge'
                             ' || University of Helsinki || Wellcome Trust Sanger Institute'),
                            ('project.contributors.laboratory',
                             'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit'
                             ' || Sarah Teichmann',
                             'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit'
                             ' || Sarah Teichmann'),
                            ('project.project_core.project_short_name', 'Mouse Melanoma', 'Mouse Melanoma'),
                            ('project.project_core.project_title', 'Melanoma infiltration of stromal and immune cells',
                             'Melanoma infiltration of stromal and immune cells'),
                            ('specimen_from_organism.provenance.document_id', '', 'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244'
                             ' || b4e55fe1-7bab-44ba-a81d-3d8cb3873244'),
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
                            ('sample.provenance.document_id', '', 'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244'
                             ' || b4e55fe1-7bab-44ba-a81d-3d8cb3873244'),
                            ('sample.biomaterial_core.biomaterial_id', '', '1209_T || 1210_T'),
                        ]

                        expected_fieldnames, expected_pdf_row, expected_fastq_row = map(list, zip(*expected))
                        tsv_file = csv.reader(response.iter_lines(decode_unicode=True), delimiter='\t')
                        actual_fieldnames = next(tsv_file)
                        rows = freeze(list(tsv_file))
                        self.assertEqual(expected_fieldnames, actual_fieldnames)
                        self.assertIn(freeze(expected_pdf_row), rows)
                        self.assertIn(freeze(expected_fastq_row), rows)
                        self.assertTrue(all(len(row) == len(expected_pdf_row) for row in rows))

    @mock_sts
    @mock_s3
    def test_bdbag_manifest(self):
        """
        moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit
        the server (see GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270)
        """
        self.maxDiff = None
        self._index_canned_bundle(("587d74b4-1075-4bbf-b96a-4d1ede0481b2", "2018-09-14T133314.453337Z"))
        logging.getLogger('test_request_validation').warning('test_manifest is invoked')
        with ResponsesHelper() as helper, TemporaryDirectory() as zip_dir:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            url = self.base_url + '/repository/files/export?filters={"file":{"fileFormat":{"is":["bam", "fastq.gz", "fastq"]}}}&format=bdbag'
            response = requests.get(url, stream=True)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
                self.assertTrue(all(['manifest' == first(name.split('/')) for name in zip_fh.namelist()]))
                zip_fname = os.path.dirname(first(zip_fh.namelist()))
            service = config.api_lambda_domain('service')
            dss = config.dss_endpoint
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
                self.assertEqual({
                    freeze({
                        'entity:participant_id': '587d74b4-1075-4bbf-b96a-4d1ede0481b2_2018-09-14T133314_453337Z',
                        'bundle_uuid': '587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                        'bundle_version': '2018-09-14T133314.453337Z',
                        'cell_suspension__provenance__document_id': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0',
                        'cell_suspension__estimated_cell_count': '0',
                        'cell_suspension__selected_cell_type': '',
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
                        '__bam_0__file_uuid': '51c9ad31-5888-47eb-9e0c-02f042373c4e',
                        '__bam_0__file_version': '2018-10-10T031035.284782Z',
                        '__bam_0__file_sha256': 'e3cd90d79f520c0806dddb1ca0c5a11fbe26ac0c0be983ba5098d6769f78294c',
                        '__bam_0__file_content_type': 'application/gzip; dcp-type=data',
                        '__bam_0__dos_url': f'dos://{service}/51c9ad31-5888-47eb-9e0c-02f042373c4e?version=2018-10-10T031035.284782Z',
                        '__bam_0__file_url': f'{dss}/files/51c9ad31-5888-47eb-9e0c-02f042373c4e?version=2018-10-10T031035.284782Z&replica=gcp',
                        '__bam_1__file_name': '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0_rsem.bam',
                        '__bam_1__file_format': 'bam',
                        '__bam_1__read_index': '',
                        '__bam_1__file_size': '3752733',
                        '__bam_1__file_uuid': 'b1c167da-0825-4c63-9cbc-2aada1ab367c',
                        '__bam_1__file_version': '2018-10-10T031035.971561Z',
                        '__bam_1__file_sha256': 'f25053412d65429cefc0157c0d18ae12d4bf4c4113a6af7a1820b62246c075a4',
                        '__bam_1__file_content_type': 'application/gzip; dcp-type=data',
                        '__bam_1__dos_url': f'dos://{service}/b1c167da-0825-4c63-9cbc-2aada1ab367c?version=2018-10-10T031035.971561Z',
                        '__bam_1__file_url': f'{dss}/files/b1c167da-0825-4c63-9cbc-2aada1ab367c?version=2018-10-10T031035.971561Z&replica=gcp',
                        '__fastq_read1__file_name': 'R1.fastq.gz',
                        '__fastq_read1__file_format': 'fastq.gz',
                        '__fastq_read1__read_index': 'read1',
                        '__fastq_read1__file_size': '125191',
                        '__fastq_read1__file_uuid': 'c005f647-b3fb-45a8-857a-8f5e6a878ccf',
                        '__fastq_read1__file_version': '2018-10-10T023811.612423Z',
                        '__fastq_read1__file_sha256': 'fe6d4fdfea2ff1df97500dcfe7085ac3abfb760026bff75a34c20fb97a4b2b29',
                        '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                        '__fastq_read1__file_url': f'{dss}/files/c005f647-b3fb-45a8-857a-8f5e6a878ccf?version=2018-10-10T023811.612423Z&replica=gcp',
                        '__fastq_read1__dos_url': f'dos://{service}/c005f647-b3fb-45a8-857a-8f5e6a878ccf?version=2018-10-10T023811.612423Z',
                        '__fastq_read2__file_name': 'R2.fastq.gz',
                        '__fastq_read2__file_format': 'fastq.gz',
                        '__fastq_read2__read_index': 'read2',
                        '__fastq_read2__file_size': '130024',
                        '__fastq_read2__file_uuid': 'b764ce7d-3938-4451-b68c-678feebc8f2a',
                        '__fastq_read2__file_version': '2018-10-10T023811.851483Z',
                        '__fastq_read2__file_sha256': 'c305bee37b3c3735585e11306272b6ab085f04cd22ea8703957b4503488cfeba',
                        '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                        '__fastq_read2__file_url': f'{dss}/files/b764ce7d-3938-4451-b68c-678feebc8f2a?version=2018-10-10T023811.851483Z&replica=gcp',
                        '__fastq_read2__dos_url': f'dos://{service}/b764ce7d-3938-4451-b68c-678feebc8f2a?version=2018-10-10T023811.851483Z',
                    }),
                    freeze({
                        'entity:participant_id': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d_2018-11-02T113344_698028Z',
                        'bundle_uuid': 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                        'bundle_version': '2018-11-02T113344.698028Z',
                        'cell_suspension__provenance__document_id': '412898c5-5b9b-4907-b07c-e9b89666e204',
                        'cell_suspension__estimated_cell_count': '1',
                        'cell_suspension__selected_cell_type': '',
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
                        '__bam_0__dos_url': '',
                        '__bam_0__file_url': '',
                        '__bam_1__file_name': '',
                        '__bam_1__file_format': '',
                        '__bam_1__read_index': '',
                        '__bam_1__file_size': '',
                        '__bam_1__file_uuid': '',
                        '__bam_1__file_version': '',
                        '__bam_1__file_sha256': '',
                        '__bam_1__file_content_type': '',
                        '__bam_1__dos_url': '',
                        '__bam_1__file_url': '',
                        '__fastq_read1__file_name': 'SRR3562915_1.fastq.gz',
                        '__fastq_read1__file_format': 'fastq.gz',
                        '__fastq_read1__read_index': 'read1',
                        '__fastq_read1__file_size': '195142097',
                        '__fastq_read1__file_uuid': '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb',
                        '__fastq_read1__file_version': '2018-11-02T113344.698028Z',
                        '__fastq_read1__file_sha256': '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a',
                        '__fastq_read1__file_content_type': 'application/gzip; dcp-type=data',
                        '__fastq_read1__dos_url': f'dos://{service}/7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb?version=2018-11-02T113344.698028Z',
                        '__fastq_read1__file_url': f'{dss}/files/7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb?version=2018-11-02T113344.698028Z&replica=gcp',
                        '__fastq_read2__file_name': 'SRR3562915_2.fastq.gz',
                        '__fastq_read2__file_format': 'fastq.gz',
                        '__fastq_read2__read_index': 'read2',
                        '__fastq_read2__file_size': '190330156',
                        '__fastq_read2__file_uuid': '74897eb7-0701-4e4f-9e6b-8b9521b2816b',
                        '__fastq_read2__file_version': '2018-11-02T113344.450442Z',
                        '__fastq_read2__file_sha256': '465a230aa127376fa641f8b8f8cad3f08fef37c8aafc67be454f0f0e4e63d68d',
                        '__fastq_read2__file_content_type': 'application/gzip; dcp-type=data',
                        '__fastq_read2__dos_url': f'dos://{service}/74897eb7-0701-4e4f-9e6b-8b9521b2816b?version=2018-11-02T113344.450442Z',
                        '__fastq_read2__file_url': f'{dss}/files/74897eb7-0701-4e4f-9e6b-8b9521b2816b?version=2018-11-02T113344.450442Z&replica=gcp',
                    })
                }, set(freeze(row) for row in reader))
                self.assertEqual([
                    'entity:participant_id',
                    'bundle_uuid',
                    'bundle_version',
                    'cell_suspension__provenance__document_id',
                    'cell_suspension__estimated_cell_count',
                    'cell_suspension__selected_cell_type',
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
                    '__bam_0__dos_url',
                    '__bam_0__file_url',
                    '__bam_1__file_name',
                    '__bam_1__file_format',
                    '__bam_1__read_index',
                    '__bam_1__file_size',
                    '__bam_1__file_uuid',
                    '__bam_1__file_version',
                    '__bam_1__file_sha256',
                    '__bam_1__file_content_type',
                    '__bam_1__dos_url',
                    '__bam_1__file_url',
                    '__fastq_read1__file_name',
                    '__fastq_read1__file_format',
                    '__fastq_read1__read_index',
                    '__fastq_read1__file_size',
                    '__fastq_read1__file_uuid',
                    '__fastq_read1__file_version',
                    '__fastq_read1__file_sha256',
                    '__fastq_read1__file_content_type',
                    '__fastq_read1__dos_url',
                    '__fastq_read1__file_url',
                    '__fastq_read2__file_name',
                    '__fastq_read2__file_format',
                    '__fastq_read2__read_index',
                    '__fastq_read2__file_size',
                    '__fastq_read2__file_uuid',
                    '__fastq_read2__file_version',
                    '__fastq_read2__file_sha256',
                    '__fastq_read2__file_content_type',
                    '__fastq_read2__dos_url',
                    '__fastq_read2__file_url',
                ], reader.fieldnames)

    def test_manifest_format_validation(self):
        for manifest_endpoint in '/manifest/files', '/repository/files/export':
            with self.subTest(manifest_endpoint=manifest_endpoint):
                url = self.base_url + f'{manifest_endpoint}?format=invalid-type'
                response = requests.get(url)
                self.assertEqual(400, response.status_code, response.content)
