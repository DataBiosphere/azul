import csv
import json
import logging
import os
import sys

from tempfile import TemporaryDirectory
from unittest import mock

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
        # moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit the server
        # see this GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()

            url = self.base_url + '/repository/files/export?filters={"file":{}}'
            response = requests.get(url)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            tsv_file = csv.DictReader(response.iter_lines(decode_unicode=True), delimiter='\t')
            # 2 because self.bundle has 2 files
            self.assertEqual(len(list(tsv_file)), 2, 'Wrong number of files were found.')
            manifest_config = json.load(open('{}/request_config.json'.format(self.service_config_dir), 'r'))['manifest']
            expected_fieldnames = list(manifest_config['bundles'].keys()) + list(
                manifest_config['contents.files'].keys())
            self.assertEqual(expected_fieldnames, tsv_file.fieldnames, 'Manifest headers are not configured correctly')

    @mock_sts
    @mock_s3
    def test_bdbag_manifest(self):
        """
        As in test_manifest, moto will mock the requests.get call so we can't hit localhost; add_passthru let's us hit
        the server (see GitHub issue and comment: https://github.com/spulec/moto/issues/1026#issuecomment-380054270)
        """
        logging.getLogger('test_request_validation').warning('test_manifest is invoked')
        with ResponsesHelper() as helper, TemporaryDirectory() as zip_dir:
            helper.add_passthru(self.base_url)
            storage_service = StorageService()
            storage_service.create_bucket()
            url = self.base_url + '/repository/files/export?filters={"file":{}}&format=bdbag'
            response = requests.get(url)
            self.assertEqual(200, response.status_code, 'Unable to download manifest')
            with ZipFile(response.text, 'r') as zip_fh:
                zip_fh.extractall(zip_dir)
            zip_fname = os.path.basename(os.path.splitext(response.text)[0])
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
                                                   'version=2018-11-02T113344.698028Z&replica=gcp')
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
