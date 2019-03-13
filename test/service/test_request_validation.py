import csv
import json
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
from azul.drs import dos_object_url
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

    def test_bad_single_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?from=1&size=1&filters={'file':{'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_multiple_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?from=1&size=1" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']},'bad-facet2':{'is':['fake-val2']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?from=1&size=1" \
                              "&filters={'file':{'organPart':{'is':['fake-val']},'bad-facet':{'is':['fake-val']}}}"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?size=15&filters={}&sort=bad-facet&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.sort_facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?size=15" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=bad-facet&order=asc"

        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.sort_facet_message, self.filter_facet_message])

    def test_valid_sort_facet_but_bad_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?size=15" \
                              "&filters={'file':{'bad-facet':{'is':['fake-val']}}}&sort=organPart&order=asc"
        response = requests.get(url)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.filter_facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_specimen(self):
        url = self.base_url + "/repository/specimens?size=15" \
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
                            ('bundle_version', '2018-09-14T133314.453337Z','2018-09-14T133314.453337Z'),
                            ('file_content_type', 'application/pdf; dcp-type=data', 'application/gzip; dcp-type=data'),
                            ('file_name', 'SmartSeq2_RTPCR_protocol.pdf', '22028_5#300_1.fastq.gz'),
                            ('file_sha256', '2f6866c4ede92123f90dd15fb180fac56e33309b8fd3f4f52f263ed2f8af2f16',
                                            '3125f2f86092798b85be93fbc66f4e733e9aec0929b558589c06929627115582'),
                            ('file_size', '29230', '64718465'),
                            ('file_uuid', '5f9b45af-9a26-4b16-a785-7f2d1053dd7c',
                                          'f2b6c6f0-8d25-4aae-b255-1974cc110cfe'),
                            ('file_version', '2018-09-14T123347.012715Z', '2018-09-14T123343.720332Z'),
                            ('file_indexed', 'False', 'False'),
                            ('file_format', 'pdf', 'fastq.gz'),
                            ('total_estimated_cells', '', '9001'),
                            ('instrument_manufacturer_model', '', 'Illumina HiSeq 2500'),
                            ('library_construction_approach', '', 'Smart-seq2'),
                            ('document_id', '67bc798b-a34a-4104-8cab-cad648471f69',
                             '67bc798b-a34a-4104-8cab-cad648471f69'),
                            ('institutions', 'DKFZ German Cancer Research Center || EMBL-EBI || University of Cambridge'
                                             ' || University of Helsinki || Wellcome Trust Sanger Institute',
                                             'DKFZ German Cancer Research Center || EMBL-EBI || University of Cambridge'
                                             ' || University of Helsinki || Wellcome Trust Sanger Institute'),
                            ('laboratory', 'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit'
                                           ' || Sarah Teichmann',
                                           'Human Cell Atlas Data Coordination Platform || MRC Cancer Unit'
                                           ' || Sarah Teichmann'),
                            ('project_shortname', 'Mouse Melanoma', 'Mouse Melanoma'),
                            ('project_title', 'Melanoma infiltration of stromal and immune cells',
                                              'Melanoma infiltration of stromal and immune cells'),
                            ('biological_sex', '', 'female'),
                            ('specimen_id', '', '1209_T || 1210_T'),
                            ('specimen_document_id', '', 'aaaaaaaa-7bab-44ba-a81d-3d8cb3873244'
                                                     ' || b4e55fe1-7bab-44ba-a81d-3d8cb3873244'),
                            ('disease', '', ''),
                            ('donor_biomaterial_id', '', '1209'),
                            ('donor_document_id', '', '89b50434-f831-4e15-a8c0-0d57e6baa94c'),
                            ('genus_species', '', 'Mus musculus'),
                            ('organ', '', 'brain || tumor'),
                            ('organ_part', '', ''),
                            ('organism_age', '', '6-12'),
                            ('organism_age_unit', '', 'week'),
                            ('preservation_method', '', '')
                        ]

                        expected_fieldnames, expected_pdf_row, expected_fastq_row = map(list, zip(*expected))
                        tsv_file = csv.reader(response.iter_lines(decode_unicode=True), delimiter='\t')
                        actual_fieldnames = next(tsv_file)
                        rows = freeze(list(tsv_file))

                        self.assertEqual(expected_fieldnames, actual_fieldnames,
                                         'Manifest headers are not configured correctly')
                        self.assertEqual(len(rows), 7, 'Wrong number of files were found.')
                        self.assertIn(freeze(expected_pdf_row), rows, 'Expected pdf contains invalid values.')
                        self.assertIn(freeze(expected_fastq_row), rows, 'Expected fastq contains invalid values.')
                        self.assertTrue(all(len(row) == len(expected_pdf_row) for row in rows),
                                        'Row sizes in manifest are not consistent.')

    @mock_sts
    @mock_s3
    def test_bdbag_manifest(self):
        """
        moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit
        the server (see GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270)
        """
        logging.getLogger('test_request_validation').warning('test_manifest is invoked')
        with ResponsesHelper() as helper, TemporaryDirectory() as zip_dir:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            url = self.base_url + '/repository/files/export?filters={"file":{}}&format=bdbag'
            response = requests.get(url, stream=True)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            with ZipFile(BytesIO(response.content), 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
                zip_fname = os.path.dirname(first(zip_fh.namelist()))
            with open(os.path.join(zip_dir, zip_fname, 'data', 'participant.tsv'), 'r') as fh:
                observed = list(csv.reader(fh, delimiter='\t'))
                expected = [['entity:participant_id'], ['7b07b9d0-cc0e-4098-9f64-f4a569f7d746']]
                self.assertEqual(expected, observed, 'participant.tsv contains incorrect data')

            expectations = [
                ('entity:sample_id', 'a21dc760-a500-4236-bcff-da34a0e873d2'),
                ('participant_id', '7b07b9d0-cc0e-4098-9f64-f4a569f7d746'),
                ('cell_suspension_id', '412898c5-5b9b-4907-b07c-e9b89666e204'),
                ('bundle_uuid', 'aaa96233-bf27-44c7-82df-b4dc15ad4d9d'),
                ('bundle_version', '2018-11-02T113344.698028Z'),
                ('file_content_type', 'application/gzip; dcp-type=data'),
                ('file_name', 'SRR3562915_1.fastq.gz'),
                ('file_sha256', '77337cb51b2e584b5ae1b99db6c163b988cbc5b894dda2f5d22424978c3bfc7a'),
                ('file_size', '195142097'),
                ('file_uuid', '7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'),
                ('file_version', '2018-11-02T113344.698028Z'),
                ('file_indexed', 'False'),
                ('file_url', config.dss_endpoint + '/files/7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb?'
                                                   'version=2018-11-02T113344.698028Z&replica=gcp'),
                ('dos_url', config.dss_endpoint + dos_object_url('7b07f99e-4a8a-4ad0-bd4f-db0d7a00c7bb'))
            ]
            expected_fieldnames, expected_row = map(list, zip(*expectations))
            with open(os.path.join(zip_dir, zip_fname, 'data', 'sample.tsv'), 'r') as fh:
                rows = iter(csv.reader(fh, delimiter='\t'))
                row = next(rows)
                self.assertEqual(expected_fieldnames, row)
                rows = freeze(list(rows))
                self.assertEqual(len(rows), 2)  # self.bundle has 2 files
                self.assertTrue(all(len(row) == len(expected_row) for row in rows))
                self.assertEqual(len(set(rows)), len(rows))
                self.assertIn(freeze(expected_row), rows)
