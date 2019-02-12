from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import unittest
from unittest.mock import patch

from elasticsearch import Elasticsearch
from more_itertools import one

from azul import config
from azul.threads import Latch
from azul.transformer import Aggregate, Contribution
from indexer import IndexerTestCase

logger = logging.getLogger(__name__)


def setUpModule():
    logging.basicConfig(level=logging.INFO)


class TestHCAIndexer(IndexerTestCase):

    def _get_es_results(self):
        results = self.es_client.search(index=','.join(self.get_hca_indexer().index_names()),
                                        doc_type="doc",
                                        size=100)
        return results['hits']['hits']

    def tearDown(self):
        for index_name in self.get_hca_indexer().index_names():
            self.es_client.indices.delete(index=index_name, ignore=[400, 404])
        super().tearDown()

    old_bundle = ("aaa96233-bf27-44c7-82df-b4dc15ad4d9d", "2018-11-02T113344.698028Z")
    new_bundle = ("aaa96233-bf27-44c7-82df-b4dc15ad4d9d", "2018-11-04T113344.698028Z")

    def test_indexing(self):
        """
        Index a bundle and assert the index contents verbatim
        """
        self.maxDiff = None
        self._index_canned_bundle(self.old_bundle)
        expected_hits = self._load_canned_result(self.old_bundle)
        hits = self._get_es_results()
        self.assertElasticsearchResultsEqual(expected_hits, hits)

    def test_deletion(self):
        """
        Delete a bundle and check that the index contains the appropriate flags
        """
        manifest, metadata = self._load_canned_bundle(self.new_bundle)
        self._index_bundle(self.new_bundle, manifest, metadata)
        self._delete_bundle(self.new_bundle)

        # FIXME: there should be a test that removes a bundle contribution from an entity that has
        # contributions from other bundles (https://github.com/DataBiosphere/azul/issues/424)

        hits = self._get_es_results()
        self.assertEqual(len(hits), 8)
        num_aggregates, num_contribs = 0, 0
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            if aggregate:
                doc = Aggregate.from_index(hit)
                self.assertEqual(doc.contents, {})
                num_aggregates += 1
            else:
                doc = Contribution.from_index(hit)
                self.assertEqual((doc.bundle_uuid, doc.bundle_version), self.new_bundle)
                self.assertTrue(doc.bundle_deleted)
                num_contribs += 1
        self.assertEqual(num_aggregates, 4)
        self.assertEqual(num_contribs, 4)

    def test_derived_files(self):
        """
        Index an analysis bundle, which, unlike a primary bundle, has data files derived from other data
        files, and assert that the resulting `files` index document contains exactly one file entry.
        """
        analysis_bundle = 'd5e01f9d-615f-4153-8a56-f2317d7d9ce8', '2018-09-06T185759.326912Z'
        self._index_canned_bundle(analysis_bundle)
        hits = self._get_es_results()
        num_files = 33
        self.assertEqual(len(hits), (num_files + 1 + 1) * 2)
        num_contribs, num_aggregates = Counter(), Counter()
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            source = hit['_source']
            contents = source['contents']
            if aggregate:
                num_aggregates[entity_type] += 1
                bundle = one(source['bundles'])
                self.assertEqual(analysis_bundle, (bundle['uuid'], bundle['version']))
                if entity_type == 'files':
                    self.assertEqual(1, len(contents['files']))
                else:
                    self.assertEqual(num_files, sum(file['count'] for file in contents['files']))
            else:
                num_contribs[entity_type] += 1
                self.assertEqual(analysis_bundle, (source['bundle_uuid'], source['bundle_version']))
                self.assertEqual(1 if entity_type == 'files' else num_files, len(contents['files']))
            self.assertEqual(1, len(contents['specimens']))
            self.assertEqual(1, len(contents['projects']))
        num_expected = dict(files=num_files, specimens=1, projects=1)
        self.assertEqual(num_contribs, num_expected)
        self.assertEqual(num_aggregates, num_expected)

    def test_bundle_upgrade(self):
        """
        Updating a bundle with a future version should overwrite the old version.
        """
        self._index_canned_bundle(self.old_bundle)
        old_hits_by_id = self._assert_old_bundle()
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=4, old_hits_by_id=old_hits_by_id)

    def test_bundle_downgrade(self):
        """
        Indexing an old version of a bundle *after* a new version should not have an effect on aggregates.
        """
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=0)
        self._index_canned_bundle(self.old_bundle)
        self._assert_old_bundle(num_expected_new_contributions=4, ignore_aggregates=True)
        self._assert_new_bundle(num_expected_old_contributions=4)

    def _assert_old_bundle(self, num_expected_new_contributions=0, ignore_aggregates=False):
        num_actual_new_contributions = 0
        hits = self._get_es_results()
        self.assertEqual(4 + 4 + num_expected_new_contributions, len(hits))
        hits_by_id = {}
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            if aggregate and ignore_aggregates:
                continue
            source = hit['_source']
            hits_by_id[source['entity_id'], aggregate] = hit
            version = one(source['bundles'])['version'] if aggregate else source['bundle_version']
            if not aggregate and self.old_bundle[1] != version:
                self.assertLess(self.old_bundle[1], version)
                num_actual_new_contributions += 1
                continue
            contents = source['contents']
            project = one(contents['projects'])
            self.assertEqual('Single cell transcriptome patterns.', get(project['project_title']))
            self.assertEqual('Single of human pancreas', get(project['project_shortname']))
            self.assertIn('John Dear', get(project['laboratory']))
            if aggregate and entity_type != 'projects':
                self.assertIn('Farmers Trucks', project['institutions'])
            else:
                self.assertIn('Farmers Trucks', [c.get('institution') for c in project['contributors']])
            specimen = one(contents['specimens'])
            self.assertIn('Australopithecus', specimen['genus_species'])
        self.assertEqual(num_expected_new_contributions, num_actual_new_contributions)
        return hits_by_id

    def _assert_new_bundle(self, num_expected_old_contributions=0, old_hits_by_id=None):
        num_actual_old_contributions = 0
        hits = self._get_es_results()
        self.assertEqual(4 + 4 + num_expected_old_contributions, len(hits))
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            source = hit['_source']
            version = one(source['bundles'])['version'] if aggregate else source['bundle_version']
            contents = source['contents']
            project = one(contents['projects'])

            if not aggregate and version != self.new_bundle[1]:
                self.assertLess(version, self.new_bundle[1])
                num_actual_old_contributions += 1
                continue

            if old_hits_by_id is not None:
                old_hit = old_hits_by_id[source['entity_id'], aggregate]
                old_source = old_hit['_source']
                old_version = one(old_source['bundles'])['version'] if aggregate else old_source['bundle_version']
                self.assertLess(old_version, version)
                old_contents = old_source['contents']
                old_project = one(old_contents['projects'])
                self.assertNotEqual(old_project["project_title"], project["project_title"])
                self.assertNotEqual(old_project["project_shortname"], project["project_shortname"])
                self.assertNotEqual(old_project["laboratory"], project["laboratory"])
                if aggregate and entity_type != 'projects':
                    self.assertNotEqual(old_project["institutions"], project["institutions"])
                else:
                    self.assertNotEqual(old_project["contributors"], project["contributors"])
                self.assertNotEqual(old_contents["specimens"][0]["genus_species"],
                                    contents["specimens"][0]["genus_species"])

            self.assertEqual("Single cell transcriptome analysis of human pancreas reveals transcriptional "
                             "signatures of aging and somatic mutation patterns.",
                             get(project["project_title"]))
            self.assertEqual("Single cell transcriptome analysis of human pancreas",
                             get(project["project_shortname"]))
            self.assertNotIn("Sarah Teichmann", project["laboratory"])
            self.assertIn("Molecular Atlas", project["laboratory"])
            if aggregate and entity_type != 'projects':
                self.assertNotIn('Farmers Trucks', project['institutions'])
            else:
                self.assertNotIn('Farmers Trucks', [c.get('institution') for c in project['contributors']])

        self.assertEqual(num_expected_old_contributions, num_actual_old_contributions)

    def test_concurrent_specimen_submissions(self):
        """
        Index two bundles contributing to the same specimen and project, ensure that conflicts are detected and handled
        """
        bundles = [("9dec1bd6-ced8-448a-8e45-1fc7846d8995", "2018-03-29T154319.834528Z"),
                   ("56a338fe-7554-4b5d-96a2-7df127a7640b", "2018-03-29T153507.198365Z")]
        original_mget = Elasticsearch.mget
        latch = Latch(len(bundles))

        def mocked_mget(self, body):
            mget_return = original_mget(self, body=body)
            # all threads wait at the latch after reading to force conflict while writing
            latch.decrement(1)
            return mget_return

        with patch.object(Elasticsearch, 'mget', new=mocked_mget):
            with self.assertLogs(level='WARNING') as cm:
                with ThreadPoolExecutor(max_workers=len(bundles)) as executor:
                    thread_results = executor.map(self._index_canned_bundle, bundles)
                    self.assertIsNotNone(thread_results)
                    self.assertTrue(all(r is None for r in thread_results))

                self.assertIsNotNone(cm.records)
                num_hits = sum(1 for log_msg in cm.output
                               if "There was a conflict with document" in log_msg
                               and ("azul_specimens" in log_msg or "azul_projects" in log_msg))
                # One conflict for the specimen and one for the project
                self.assertEqual(num_hits, 2)

        hits = self._get_es_results()
        file_uuids = set()
        # One specimen, one project and two file contributions per bundle, eight contributions in total.
        # Both bundles share the specimen and the project, so two aggregates for those. None of the four files are
        # shared, so four aggregates for those. In total we should have 8 + 2 + 4 == 14 documents.
        self.assertEqual(14, len(hits))
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            contents = hit['_source']['contents']
            if aggregate:
                self.assertEqual(hit['_id'], hit['_source']['entity_id'])
            if entity_type == 'files':
                contents = hit['_source']['contents']
                self.assertEqual(1, len(contents['files']))
                if aggregate:
                    file_uuids.add(contents['files'][0]['uuid'])
            elif entity_type in ('specimens', 'projects'):
                if aggregate:
                    self.assertEqual(2, len(hit['_source']['bundles']))
                    # All four files are fastqs so the are grouped together
                    self.assertEqual(4, one(contents['files'])['count'])
                else:
                    self.assertEqual(2, len(contents['files']))
            else:
                self.fail()
        file_document_ids = set()
        self.assertEqual(4, len(file_uuids))
        for bundle_fqid in bundles:
            manifest, metadata = self._load_canned_bundle(bundle_fqid)
            for file in metadata['file.json']['files']:
                file_document_ids.add(file['hca_ingest']['document_id'])
        self.assertEqual(file_document_ids, file_uuids)

    def test_indexing_with_skipped_matrix_file(self):
        # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
        self._index_canned_bundle(('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z'))
        self.maxDiff = None
        hits = self._get_es_results()
        file_names, aggregate_file_names = set(), set()
        entities_with_matrix_files = set()
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            files = hit['_source']['contents']['files']
            if aggregate:
                if entity_type == 'files':
                    aggregate_file_names.add(one(files)['name'])
                else:
                    for file in files:
                        # FIXME: need for one() is odd, file_format is a group field
                        # https://github.com/DataBiosphere/azul/issues/612
                        if one(file['file_format']) == 'matrix':
                            self.assertEqual(1, file['count'])
                            entities_with_matrix_files.add(hit['_source']['entity_id'])
            else:
                for file in files:
                    file_name = file['name']
                    file_names.add(file_name)
        self.assertEqual(2, len(entities_with_matrix_files))  # a project and a specimen
        self.assertEqual(aggregate_file_names, file_names)
        matrix_file_names = {file_name for file_name in file_names if '.zarr!' in file_name}
        self.assertEqual({'377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr!.zattrs'}, matrix_file_names)

    def test_plate_bundle(self):
        self._index_canned_bundle(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        self.maxDiff = None

        es_results = self._get_es_results()
        self.assertGreater(len(es_results), 0)
        documents_with_cell_suspension = 0
        for result_dict in es_results:
            entity_type, aggregate = config.parse_es_index_name(result_dict["_index"])
            contents = result_dict['_source']['contents']
            if aggregate:
                bundles = result_dict['_source']['bundles']
                self.assertEqual(1, len(bundles))
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
        self._index_canned_bundle(('3f8176ff-61a7-4504-a57c-fc70f38d5b13', '2018-10-24T234431.820615Z'))
        self._index_canned_bundle(('e2c3054e-9fba-4d7a-b85b-a2220d16da73', '2018-10-24T234303.157920Z'))
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
        self._index_canned_bundle(('b7fc737e-9b7b-4800-8977-fe7c94e131df', '2018-09-12T121155.846604Z'))
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
        self._index_canned_bundle(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        es_results = self._get_es_results()
        for index_results in es_results:
            entity_type, aggregate = config.parse_es_index_name(index_results['_index'])
            if aggregate and entity_type == 'projects':
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
        self._index_canned_bundle(("3db604da-940e-49b1-9bcc-25699a55b295", "2018-11-02T184048.983513Z"))

        es_results = self._get_es_results()
        for index_results in es_results:
            source = index_results['_source']
            contents = source['contents']
            diseases = contents['specimens'][0]['disease']
            self.assertEqual(1, len(diseases))
            self.assertEqual("atrophic vulva (specimen_from_organism)", diseases[0])

    def test_organoid_priority(self):
        '''
        Index a bundle containing an Organoid and assert that the "organ" and "organ_part"
        values saved are the ones from the Organoid and not the SpecimenFromOrganism
        '''
        self._index_canned_bundle(('dcccb551-4766-4210-966c-f9ee25d19190', '2018-10-18T204655.866661Z'))
        es_results = self._get_es_results()
        inner_specimens, inner_cell_suspensions = 0, 0
        for index_results in es_results:

            contents = index_results['_source']['contents']
            entity_type, aggregate = config.parse_es_index_name(index_results['_index'])

            if entity_type != 'files' or one(contents['files'])['file_format'] != 'pdf':
                for cell_suspension in contents['cell_suspensions']:
                    inner_cell_suspensions += 1
                    self.assertEqual(['Brain'], cell_suspension['organ'])
                    self.assertEqual([None], cell_suspension['organ_part'])

            for specimen in contents['specimens']:
                inner_specimens += 1
                expect_list = aggregate and entity_type != 'specimens'
                self.assertEqual(['Brain'] if expect_list else 'Brain', specimen['organ'])
                self.assertEqual([None] if expect_list else None, specimen['organ_part'])

        projects = 1
        specimens = 4
        cell_suspensions = 1
        files = 16
        inner_specimens_in_contributions = (files + projects) * specimens + specimens * 1
        inner_specimens_in_aggregates = (files + specimens + projects) * 1
        inner_cell_suspensions_in_contributions = (files + specimens + projects) * cell_suspensions
        inner_cell_suspensions_in_aggregates = (files + specimens + projects) * 1

        self.assertEqual(inner_specimens_in_contributions + inner_specimens_in_aggregates,
                         inner_specimens)
        self.assertEqual(inner_cell_suspensions_in_contributions  + inner_cell_suspensions_in_aggregates,
                         inner_cell_suspensions)




def get(v):
    return one(v) if isinstance(v, list) else v


if __name__ == "__main__":
    unittest.main()
