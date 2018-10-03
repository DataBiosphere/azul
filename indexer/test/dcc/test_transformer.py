import os
import json
from pathlib import Path
from unittest import TestCase, main as unittest_main
from chalicelib.dcc.dcc_indexer import DCCJSONTransformer

DCC_TEST_FOLDER = os.path.dirname(os.path.abspath(__file__))
ROOT_FOLDER = Path(__file__).parents[2]

PROD_MAPPING_FILEPATH = os.path.join(ROOT_FOLDER, 'chalicelib', 'dcc', 'transformer_mapping.json')

TEST_FILES_FOLDER = os.path.join(DCC_TEST_FOLDER, 'test-files')

GENERIC_FILES_FOLDER = os.path.join(TEST_FILES_FOLDER, 'generic')

SIMPLE_MAPPING_FILEPATH = os.path.join(GENERIC_FILES_FOLDER, "simple_mapping.json")
SIMPLE_METADATA_FILEPATH = os.path.join(GENERIC_FILES_FOLDER, "simple_metadata.json")
SIMPLE_MANIFEST_FILEPATH = os.path.join(GENERIC_FILES_FOLDER, "simple_manifest.json")

FORMATTER_FFE_MAPPING_FILEPATH = os.path.join(GENERIC_FILES_FOLDER, "formatter_ffe_mapping.json")
FORMATTER_FFE_METADATA_FILEPATH = os.path.join(GENERIC_FILES_FOLDER, "formatter_ffe_metadata.json")

GEN3_FILES_FOLDER = os.path.join(TEST_FILES_FOLDER, 'gen3')

GEN3_METADATA_FILEPATH = os.path.join(GEN3_FILES_FOLDER, "example_metadata.json")
GEN3_MANIFEST_FILEPATH = os.path.join(GEN3_FILES_FOLDER, "example_manifest.json")


def get_json_from_file(filename: str) -> dict:
    with open(filename, 'r') as file:
        return json.load(file)


class TestDCCTransformer(TestCase):
    def setUp(self):
        pass

    def test_simple_transform(self):
        expected_index = 'test_index'
        expected_field_name = 'simple_index_field'
        expected_field_value = 'simple_value'

        metadata_json = get_json_from_file(SIMPLE_METADATA_FILEPATH)
        manifest_json = get_json_from_file(SIMPLE_MANIFEST_FILEPATH)

        transformer = DCCJSONTransformer(SIMPLE_MAPPING_FILEPATH)
        test_index_entries_json = transformer.transform(manifest_json, metadata_json)

        given_index_name, given_index_entry = next(iter(test_index_entries_json.items()))

        self.assertEqual(given_index_name, expected_index, "Incorrectly mapped source file.")
        self.assertIn(expected_field_name, given_index_entry.keys(), f"Incorrectly mapped dictionary keys.")
        self.assertEqual(given_index_entry[expected_field_name], expected_field_value,
                         f"Incorrectly mapped dictionary values.")

    def test_extract_file_formatter(self):
        expected_index = 'test_index'
        expected_field_name = 'formatter_ffe_index_field'
        expected_field_value = 'yay'

        metadata_json = get_json_from_file(FORMATTER_FFE_METADATA_FILEPATH)
        manifest_json = get_json_from_file(SIMPLE_MANIFEST_FILEPATH)

        transformer = DCCJSONTransformer(FORMATTER_FFE_MAPPING_FILEPATH)
        test_index_entries_json = transformer.transform(manifest_json, metadata_json)

        given_index_name, given_index_entry = next(iter(test_index_entries_json.items()))

        self.assertEqual(given_index_name, expected_index, "Incorrectly mapped source file.")
        self.assertIn(expected_field_name, given_index_entry.keys(), f"Incorrectly mapped dictionary keys.")
        self.assertEqual(given_index_entry[expected_field_name], expected_field_value,
                         f"Incorrectly mapped dictionary values.")

    def test_gen3_metadata(self):
        expected_index = 'fb_index'
        expected_results = [
            {
                "index_field": "analysis_type",
                "value": ""
            },
            {
                "index_field": "center_name",
                "value": "Baylor"
            },
            {
                "index_field": "project",
                "value": "topmed-public"
            },
            {
                "index_field": "lastModified",
                "value": "1822-07-20T000000.000000Z"
            },
            {
                "index_field": "program",
                "value": "TOPMed"
            },
            {
                "index_field": "redwoodDonorUUID",
                "value": ""
            },
            {
                "index_field": "fileSize",
                "value": 23438579833
            },
            {
                "index_field": "file_id",
                "value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            },
            {
                "index_field": "file_version",
                "value": "1822-07-20T000000.000000Z"
            },
            {
                "index_field": "fileMd5sum",
                "value": ""
            },
            {
                "index_field": "download_id",
                "value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            },
            {
                "index_field": "donor",
                "value": "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff"
            },
            {
                "index_field": "repoBaseUrl",
                "value": ""
            },
            {
                "index_field": "repoCode",
                "value": "DSS-AWS-Oregon"
            },
            {
                "index_field": "repoCountry",
                "value": "US"
            },
            {
                "index_field": "repoDataBundleId",
                "value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            },
            {
                "index_field": "repoName",
                "value": "DSS-AWS-Oregon"
            },
            {
                "index_field": "repoOrg",
                "value": "UCSC"
            },
            {
                "index_field": "repoType",
                "value": "HCA DSS"
            },
            {
                "index_field": "sampleId",
                "value": "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff"
            },
            {
                "index_field": "software",
                "value": ""
            },
            {
                "index_field": "specimen_type",
                "value": "Normal - solid tissue"
            },
            {
                "index_field": "study",
                "value": "topmed-public"
            },
            {
                "index_field": "submittedDonorId",
                "value": "NA12878"
            },
            {
                "index_field": "submittedSampleId",
                "value": "NWD119844"
            },
            {
                "index_field": "specimenUUID",
                "value": "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff"
            },
            {
                "index_field": "submittedSpecimenId",
                "value": "NWD119844"
            },
            {
                "index_field": "submitterDonorPrimarySite",
                "value": "B-lymphocyte"
            },
            {
                "index_field": "submitter_donor_id",
                "value": ""
            },
            {
                "index_field": "title",
                "value": "FKE6564321.recab.cram"
            },
            {
                "index_field": "file_type",
                "value": "cram"
            },
            {
                "index_field": "workflow",
                "value": ""
            },
            {
                "index_field": "urls",
                "value": ["gs://cgp-commons-multi-region-public/topmed_open_access"
                          "/99999999-8888-7777-6666-555555555555/NWD319341.recab.cram",
                          "s3://cgp-commons-public/topmed_open_access"
                          "/99999999-8888-7777-6666-555555555555/NWD319341.recab.cram"]
            },
            {
                "index_field": "workflowVersion",
                "value": ""
            },
            {
                "index_field": "metadataJson",
                "value": ""
            }
        ]

        metadata_json = get_json_from_file(GEN3_METADATA_FILEPATH)
        manifest_json = get_json_from_file(GEN3_MANIFEST_FILEPATH)

        transformer = DCCJSONTransformer(PROD_MAPPING_FILEPATH)
        test_index_entries_json = transformer.transform(manifest_json, metadata_json)

        given_index_name, given_index_entry = next(iter(test_index_entries_json.items()))

        self.assertEqual(given_index_name, expected_index, "Incorrectly mapped source file.")
        for result in expected_results:
            field = result['index_field']
            self.assertIn(field, given_index_entry.keys(), f"Incorrectly mapped index_field value.")
            self.assertEqual(given_index_entry[field], result['value'], f"Incorrectly mapped value for"
                                                                        f" field: {field}.")


if __name__ == '__main__':
    unittest_main()
