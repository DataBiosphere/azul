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
        actual_index = transformer.transform(manifest_json, metadata_json)

        given_index_name, given_index_entry = next(iter(actual_index.items()))

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
        actual_index = transformer.transform(manifest_json, metadata_json)

        given_index_name, given_index_entry = next(iter(actual_index.items()))

        self.assertEqual(given_index_name, expected_index, "Incorrectly mapped source file.")
        self.assertIn(expected_field_name, given_index_entry.keys(), f"Incorrectly mapped dictionary keys.")
        self.assertEqual(given_index_entry[expected_field_name], expected_field_value,
                         f"Incorrectly mapped dictionary values.")

    def test_gen3_metadata(self):
        expected_index_name = 'fb_index'
        expected_index = {
            "fb_index": {
                "program": "topmed",
                "project": "public",
                "study": "topmed-public",
                "donor": "NA12878",
                "submittedDonorId": "NA12878",
                "submitter_donor_id": "NA12878",
                "redwoodDonorUUID": "",
                "access": "",
                "age_range": "",
                "bmi": "",
                "gender": "female",
                "height": "",
                "race": "white",
                "ethnicity": "not hispanic or latino",
                "weight": "",
                "analysis_type": "",
                "center_name": "Baylor",
                "library_strategy": "WGS",
                "submittedSpecimenId": "NA12878_sample",
                "specimenUUID": "eadf3fbd-69dd-4adb-822b-382a9296860a",
                "specimen_type": "Normal - solid tissue",
                "submitterDonorPrimarySite": "B-lymphocyte",
                "biospecimen_anatomic_site": "",
                "biospecimen_anatomic_site_detail": "",
                "sampleId": "NWD119844",
                "submittedSampleId": "NWD119844",
                "experimentalStrategy": "",
                "analyte_isolation_method": "",
                "title": "FKE6564321.recab.cram",
                "file_type": "cram",
                "file_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "download_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "file_version": "1822-07-20T000000.000000Z",
                "lastModified": "1822-07-20T000000.000000Z",
                "urls": [
                    "gs://cgp-commons-multi-region-public/topmed_open_access/99999999-8888-7777-6666-555555555555/NWD319341.recab.cram",
                    "s3://cgp-commons-public/topmed_open_access/99999999-8888-7777-6666-555555555555/NWD319341.recab.cram"
                ],
                "aliases": [],
                "fileSize": 23438579833,
                "fileMd5sum": "",
                "repoName": "DSS-AWS-Oregon",
                "repoOrg": "UCSC",
                "repoType": "HCA DSS",
                "repoBaseUrl": "",
                "repoCode": "DSS-AWS-Oregon",
                "repoCountry": "US",
                "repoDataBundleId": "",
                "software": "",
                "workflow": "",
                "workflowVersion": "",
                "metadataJson": ""
            }
        }

        metadata_json = get_json_from_file(GEN3_METADATA_FILEPATH)
        manifest_json = get_json_from_file(GEN3_MANIFEST_FILEPATH)

        transformer = DCCJSONTransformer(PROD_MAPPING_FILEPATH)
        actual_result = transformer.transform(manifest_json, metadata_json)

        # with open("test_index_entries.json", "w") as fh:
        #     fh.write(json.dumps(actual_result, indent=4))

        actual_index_name, actual_index_entries = next(iter(actual_result.items()))

        self.assertEqual(expected_index_name, actual_index_name, "Incorrect index name.")
        expected_index_entries = expected_index.pop(expected_index_name)
        if expected_index_entries != actual_index_entries:
            for expected_key, expected_value in expected_index_entries.items():
                self.assertIn(expected_key, actual_index_entries.keys(),
                              f"Incorrectly mapped index_field: {expected_key}.")
                self.assertEqual(expected_value, actual_index_entries[expected_key],
                                 f"Incorrectly mapped value for field: {expected_key}.")
                # Ensure actual_index_entries does not contain any extra entries
                self.assertEqual(expected_index_entries.keys(), actual_index_entries.keys())


if __name__ == '__main__':
    unittest_main()
