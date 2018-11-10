"""
Suite for unit testing indexer.py
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import time
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch

from azul import config
from azul.json_freeze import freeze, sort_frozen
from indexer import IndexerTestCase

from azul.transformer import ElasticSearchDocument

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHCAIndexer(IndexerTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.old_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-29T152812.404846Z")
        cls.new_bundle = ("aee55415-d128-4b30-9644-e6b2742fa32b",
                          "2018-03-30T152812.404846Z")
        cls.specimens = [("9dec1bd6-ced8-448a-8e45-1fc7846d8995", "2018-03-29T154319.834528Z"),
                         ("56a338fe-7554-4b5d-96a2-7df127a7640b", "2018-03-29T153507.198365Z")]
        cls.analysis_bundle = ("d5e01f9d-615f-4153-8a56-f2317d7d9ce8",
                               "2018-09-06T185759.326912Z")

    def _get_es_results(self):
        es_results = []
        for entity_index in self.index_properties.index_names:
            results = self.es_client.search(index=entity_index,
                                            doc_type="doc",
                                            size=100)
            for result_dict in results["hits"]["hits"]:
                es_results.append(result_dict)
        return es_results

    def tearDown(self):
        for index_name in self.index_properties.index_names:
            self.es_client.indices.delete(index=index_name, ignore=[400, 404])

    def test_index_correctness(self):
        """
        Index a bundle and check that the index contains the correct attributes
        """
        self._mock_index(self.old_bundle)
        self.maxDiff = None

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            index_id = result_dict["_id"]
            expected_ids = set()
            path = os.path.join(data_prefix, f'aee55415-d128-4b30-9644-e6b2742fa32b.{entity_type}.results.json')
            with open(path, 'r') as fp:
                expected_dict = json.load(fp)
                self.assertGreater(len(expected_dict["hits"]["hits"]), 0)
                for expected_hit in expected_dict["hits"]["hits"]:
                    expected_ids.add(expected_hit["_id"])
                    if index_id == expected_hit["_id"]:
                        expected = sort_frozen(freeze(expected_hit["_source"]))
                        actual = sort_frozen(freeze(result_dict["_source"]))
                        self.assertEqual(expected, actual, entity_type)
            self.assertIn(index_id, expected_ids)

    def test_delete_correctness(self):
        """
        Delete a bundle and check that the index contains the appropriate flags
        """
        data_pack = self._get_data_files(self.new_bundle[0], self.new_bundle[1], updated=True)

        self._mock_delete(self.new_bundle, data_pack)

        # FIXME: there should be a test that removes a bundle contribution from an entity that has
        # contributions from other bundles (https://github.com/DataBiosphere/azul/issues/424)

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            result_doc = ElasticSearchDocument.from_index(result_dict)
            self.assertEqual(result_doc.bundles[0].uuid, self.new_bundle[0])
            self.assertEqual(result_doc.bundles[0].version, self.new_bundle[1])
            self.assertTrue(result_doc.bundles[0].deleted)

    def test_single_file_object_for_files_index(self):
        """
        Index an analysis bundle, which, unlike a primary bundle, has data files derived from other data
        files, and assert that the resulting `files` index document contains exactly one file entry.
        """
        self._mock_index(self.analysis_bundle)

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            result_uuid = result_dict["_source"]["bundles"][0]["uuid"]
            result_version = result_dict["_source"]["bundles"][0]["version"]
            result_contents = result_dict["_source"]["bundles"][0]["contents"]

            self.assertEqual(self.analysis_bundle[0], result_uuid)
            self.assertEqual(self.analysis_bundle[1], result_version)
            index_name = result_dict['_index']
            if index_name == config.es_index_name("files"):
                self.assertEqual(1, len(result_contents["files"]))
                self.assertGreater(len(result_contents["specimens"]), 0)
            elif index_name == config.es_index_name("specimens"):
                self.assertEqual(1, len(result_contents["specimens"]))
                self.assertGreater(len(result_contents["files"]), 0)
            elif index_name == config.es_index_name("projects"):
                self.assertGreater(len(result_contents["files"]), 0)
                self.assertGreater(len(result_contents["specimens"]), 0)
            else:
                self.fail(index_name)

    def test_update_with_newer_version(self):
        """
        Updating a bundle with a future version should overwrite the old version.
        """
        self._mock_index(self.old_bundle)

        old_results = self._get_es_results()
        self.assertGreater(len(old_results), 0)
        for result_dict in old_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            old_result_version = result_dict["_source"]["bundles"][0]["version"]
            old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

            self.assertEqual(self.old_bundle[1], old_result_version)
            self.assertEqual("Melanoma infiltration of stromal and immune cells",
                             old_result_contents["projects"][0]["project_title"])
            old_project = old_result_contents["projects"][0]
            self.assertEqual("Mouse Melanoma", old_project["project_shortname"])
            self.assertIn("Sarah Teichmann", old_project["laboratory"])
            self.assertIn("University of Helsinki",
                          [c.get('institution') for c in old_project["contributors"]])
            self.assertIn("Mus musculus", old_result_contents["specimens"][0]["genus_species"])

        self._mock_index(self.new_bundle, updated=True)
        new_results = self._get_es_results()

        for old_result_dict, new_result_dict in list(zip(old_results, new_results)):
            entity_type, aggregate = config.parse_es_index_name(new_result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
            old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

            new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
            new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

            self.assertNotEqual(old_result_version, new_result_version)
            old_project = old_result_contents["projects"][0]
            new_project = new_result_contents["projects"][0]
            self.assertNotEqual(old_project["project_title"], new_project["project_title"])
            self.assertEqual("Melanoma infiltration of stromal and immune cells 2",
                             new_project["project_title"])

            self.assertNotEqual(old_project["project_shortname"], new_project["project_shortname"])
            self.assertEqual("Aardvark Ailment", new_project["project_shortname"])

            self.assertNotEqual(old_project["laboratory"], new_project["laboratory"])
            self.assertNotIn("Sarah Teichmann", new_project["laboratory"])
            self.assertIn("John Denver", new_project["laboratory"])

            self.assertNotEqual(old_project["contributors"], new_project["contributors"])
            self.assertNotIn("University of Helsinki", [c.get('institution') for c in new_project["contributors"]])

            self.assertNotEqual(old_result_contents["specimens"][0]["genus_species"],
                                new_result_contents["specimens"][0]["genus_species"])

    def test_old_version_overwrite(self):
        """
        An attempt to overwrite a newer version of a bundle with an older version should fail.
        """
        self._mock_index(self.new_bundle, updated=True)

        old_results = self._get_es_results()
        self.assertGreater(len(old_results), 0)
        for result_dict in old_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict['_index'])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            old_result_version = result_dict["_source"]["bundles"][0]["version"]
            old_result_contents = result_dict["_source"]["bundles"][0]["contents"]

            self.assertEqual(self.new_bundle[1], old_result_version)
            self.assertEqual("Melanoma infiltration of stromal and immune cells 2",
                             old_result_contents["projects"][0]["project_title"])
            old_project = old_result_contents["projects"][0]
            self.assertEqual("Aardvark Ailment", old_project["project_shortname"])
            self.assertIn("John Denver", old_project["laboratory"])
            self.assertNotIn("University of Helsinki",
                             [c.get('institution') for c in old_project["contributors"]])
            self.assertIn("Lorem ipsum", old_result_contents["specimens"][0]["genus_species"])

        self._mock_index(self.old_bundle)
        new_results = self._get_es_results()
        for old_result_dict, new_result_dict in list(zip(old_results, new_results)):
            entity_type, aggregate = config.parse_es_index_name(new_result_dict['_index'])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            old_result_version = old_result_dict["_source"]["bundles"][0]["version"]
            old_result_contents = old_result_dict["_source"]["bundles"][0]["contents"]

            new_result_version = new_result_dict["_source"]["bundles"][0]["version"]
            new_result_contents = new_result_dict["_source"]["bundles"][0]["contents"]

            self.assertEqual(old_result_version, new_result_version)
            old_project = old_result_contents["projects"][0]
            new_project = new_result_contents["projects"][0]
            self.assertEqual(old_project["project_title"], new_project["project_title"])
            self.assertEqual(old_project["project_shortname"], new_project["project_shortname"])
            self.assertEqual(old_project["laboratory"], new_project["laboratory"])
            self.assertEqual(old_project["contributors"], new_project["contributors"])
            self.assertEqual(old_result_contents["specimens"][0]["genus_species"],
                             new_result_contents["specimens"][0]["genus_species"])

    def test_concurrent_specimen_submissions(self):
        """
        We submit two different bundles for the same specimen. What happens?
        """
        unmocked_mget = Elasticsearch.mget

        def mocked_mget(self, body):
            mget_return = unmocked_mget(self, body=body)
            # both threads sleep after reading to force conflict while writing
            time.sleep(0.5)
            return mget_return

        with patch.object(Elasticsearch, 'mget', new=mocked_mget):
            with self.assertLogs(level='WARNING') as cm:
                with ThreadPoolExecutor(max_workers=len(self.specimens)) as executor:
                    thread_results = executor.map(self._mock_index, self.specimens)
                    self.assertIsNotNone(thread_results)
                    self.assertTrue(all(r is None for r in thread_results))

                self.assertIsNotNone(cm.records)
                num_hits = sum(1 for log_msg in cm.output
                               if "There was a conflict with document" in log_msg
                               and ("azul_specimens" in log_msg or "azul_projects" in log_msg))
                # One conflict for the specimen and one for the project
                self.assertEqual(num_hits, 2)

        es_results = self._get_es_results()
        file_doc_ids = set()
        self.assertEqual(len(es_results), 12)
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict['_index'])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            self.assertEqual(result_dict["_id"], result_dict["_source"]["entity_id"])
            if entity_type == "files":
                # files assumes one bundle per result
                self.assertEqual(len(result_dict["_source"]["bundles"]), 1)
                result_contents = result_dict["_source"]["bundles"][0]["contents"]
                self.assertEqual(1, len(result_contents["files"]))
                file_doc_ids.add(result_contents["files"][0]["uuid"])
            elif entity_type in ('specimens', 'projects'):
                self.assertEqual(len(result_dict["_source"]["bundles"]), 2)
                for bundle in result_dict["_source"]["bundles"]:
                    result_contents = bundle["contents"]
                    self.assertEqual(2, len(result_contents["files"]))
            else:
                self.fail()

        self.assertEqual(len(file_doc_ids), 4)
        for spec_uuid, spec_version in self.specimens:
            _, _, spec_metadata = self._get_data_files(spec_uuid, spec_version)
            for file_dict in spec_metadata["file.json"]["files"]:
                self.assertIn(file_dict["hca_ingest"]["document_id"], file_doc_ids)

    def test_indexing_with_skipped_matrix_file(self):
        # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
        self._mock_index(('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z'))
        self.maxDiff = None

        es_results = self._get_es_results()
        file_names = set()
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate: continue  # FIXME (https://github.com/DataBiosphere/azul/issues/425)
            bundles = result_dict["_source"]['bundles']
            self.assertEqual(1, len(bundles))
            files = bundles[0]['contents']['files']
            for file in files:
                file_name = file['name']
                file_names.add(file_name)
        matrix_file_names = {file_name for file_name in file_names if '.zarr!' in file_name}
        self.assertEqual({'377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr!.zattrs'}, matrix_file_names)

    def test_plate_bundle(self):
        self._mock_index(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        self.maxDiff = None

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        documents_with_cell_suspension = 0
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate:
                contents = result_dict["_source"]['contents']
            else:
                bundles = result_dict["_source"]['bundles']
                self.assertEqual(1, len(bundles))
                contents = bundles[0]['contents']
            cell_suspensions = contents['cell_suspensions']
            if entity_type == 'files' and contents['files'][0]['file_format'] == 'pdf':
                # The PDF files in that bundle aren't linked to a specimen
                self.assertEqual(0, len(cell_suspensions))
            else:
                self.assertEqual(1 if aggregate else 384, len(cell_suspensions))
                # 384 wells in total, four of them empty, the rest with a single cell
                self.assertEqual(380, sum(cs['total_estimated_cells'] for cs in cell_suspensions))
                documents_with_cell_suspension += 1
        # Cell suspensions should be mentioned in one project, two files (one per fastq) and one
        # specimen. There should be one original one aggregate document for each of those.
        self.assertEqual(8, documents_with_cell_suspension)

    def test_well_bundles(self):
        self._mock_index(('3f8176ff-61a7-4504-a57c-fc70f38d5b13', '2018-10-24T234431.820615Z'))
        self._mock_index(('e2c3054e-9fba-4d7a-b85b-a2220d16da73', '2018-10-24T234303.157920Z'))
        self.maxDiff = None

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate:
                contents = result_dict["_source"]['contents']
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # Each bundle contributes a well with one cell. The data files in each bundle are derived from
                # the cell in that well. This is why each data file should only have a cell count of 1. Both
                # bundles refer to the same specimen and project, so the cell count for those should be 2.
                expected_cells = 1 if entity_type == 'files' else 2
                self.assertEqual(expected_cells, cell_suspensions[0]['total_estimated_cells'])

    def test_pooled_specimens(self):
        self._mock_index(('b7fc737e-9b7b-4800-8977-fe7c94e131df', '2018-09-12T121155.846604Z'))
        self.maxDiff = None

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            if aggregate:
                contents = result_dict["_source"]['contents']
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # This bundle contains three specimens which are pooled into the a single cell suspension with
                # 10000 cells. Until we introduced cell suspensions as an inner entity we used to associate cell
                # counts with specimen which would have inflated the total cell count to 30000 in this case.
                self.assertEqual(10000, cell_suspensions[0]['total_estimated_cells'])

    def test_project_contact_extraction(self):
        """
        Ensure all fields related to project contacts are properly extracted
        """
        self._mock_index(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        es_results = self._get_es_results()
        for index_results in es_results:
            entity_type, aggregate = config.parse_es_index_name(index_results['_index'])
            if not aggregate or entity_type != 'projects':
                continue
            contributor_values = defaultdict(set)
            contributors = index_results['_source']['contents']['projects'][0]['contributors']
            for contributor in contributors:
                for k, v in contributor.items():
                    contributor_values[k].add(v)
            self.assertEqual({'Matthew,,Green', 'Ido Amit', 'Assaf Weiner', 'Guy Ledergor', 'Eyal David'},
                             contributor_values['contact_name'])
            self.assertEqual({'assaf.weiner@weizmann.ac.il', 'guy.ledergor@weizmann.ac.il', 'hewgreen@ebi.ac.uk',
                              'eyald.david@weizmann.ac.il', 'ido.amit@weizmann.ac.il'},
                             contributor_values['email'])
            self.assertEqual({'EMBL-EBI European Bioinformatics Institute', 'The Weizmann Institute of Science'},
                             contributor_values['institution'])
            self.assertEqual({'Prof. Ido Amit', 'Human Cell Atlas Data Coordination Platform'},
                             contributor_values['laboratory'])
            self.assertEqual({False, True}, contributor_values['corresponding_contributor'])
            self.assertEqual({'Human Cell Atlas wrangler', None},
                             contributor_values['project_role'])

    def test_diseases_field(self):
        """
        Index a bundle with a specimen `diseases` value that is differs from its donor `diseases` value
        and assert that only the specimen's `diseases` value is in the indexed document.
        """
        self._mock_index(("3db604da-940e-49b1-9bcc-25699a55b295", "2018-11-02T184048.983513Z"))

        es_results = self._get_es_results()
        for index_results in es_results:
            entity_type, aggregate = config.parse_es_index_name(index_results["_index"])
            source = index_results['_source']
            contents = source['contents'] if aggregate else source['bundles'][0]['contents']
            diseases = contents['specimens'][0]['disease']
            self.assertEqual(1, len(diseases))
            self.assertEqual("atrophic vulva (specimen_from_organism)", diseases[0])


if __name__ == "__main__":
    unittest.main()
