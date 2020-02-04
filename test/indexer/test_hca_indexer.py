from collections import (
    Counter,
    defaultdict,
)
from concurrent.futures import ThreadPoolExecutor
import copy
from copy import deepcopy
import logging
import os
import re
from typing import (
    Mapping,
    NamedTuple,
    Tuple,
)
import unittest
from unittest.mock import patch
from uuid import uuid4

import boto3
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from more_itertools import one
from moto import (
    mock_sqs,
    mock_sts,
)
import requests
from requests_http_signature import HTTPSignatureAuth

from app_test_case import LocalAppTestCase
from azul import (
    config,
    hmac,
)
import azul.indexer
from azul.indexer import IndexWriter
from azul.logging import configure_test_logging
from azul.plugin import Plugin
from azul.project.hca.metadata_generator import MetadataGenerator
from azul.threads import Latch
from azul.transformer import (
    Aggregate,
    Contribution,
)
from azul.types import (
    JSONs,
)
from indexer import IndexerTestCase
from retorts import ResponsesHelper

logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(logger)


class TestHCAIndexer(IndexerTestCase):

    def _get_all_hits(self):
        hits = scan(client=self.es_client,
                    index=','.join(self.get_hca_indexer().index_names()),
                    doc_type="doc")
        return list(hits)

    def tearDown(self):
        self._delete_indices()
        super().tearDown()

    def _delete_indices(self):
        for index_name in self.get_hca_indexer().index_names():
            self.es_client.indices.delete(index=index_name, ignore=[400, 404])

    old_bundle = ("aaa96233-bf27-44c7-82df-b4dc15ad4d9d", "2018-11-02T113344.698028Z")
    new_bundle = ("aaa96233-bf27-44c7-82df-b4dc15ad4d9d", "2018-11-04T113344.698028Z")

    def test_indexing(self):
        """
        Index a bundle and assert the index contents verbatim
        """
        self.maxDiff = None
        self._index_canned_bundle(self.old_bundle)
        expected_hits = self._load_canned_result(self.old_bundle)
        hits = self._get_all_hits()
        self.assertElasticsearchResultsEqual(expected_hits, hits)

    def test_deletion(self):
        """
        Delete a bundle and check that the index contains the appropriate flags
        """

        class BundleAndSize(NamedTuple):
            bundle: Tuple[str, str]
            size: int

        # Ensure that we have a bundle whose documents are written individually and another one that's written in bulk
        small_bundle = BundleAndSize(bundle=self.new_bundle,
                                     size=6)
        large_bundle = BundleAndSize(bundle=("2a87dc5c-0c3c-4d91-a348-5d784ab48b92", "2018-03-29T103945.437487Z"),
                                     size=258)
        self.assertTrue(small_bundle.size < IndexWriter.bulk_threshold < large_bundle.size)

        field_types = Plugin.load().field_types()

        for bundle, size in small_bundle, large_bundle:
            with self.subTest(size=size):
                manifest, metadata = self._load_canned_bundle(bundle)
                try:
                    self._index_bundle(self.new_bundle, manifest, metadata)

                    hits = self._get_all_hits()
                    self.assertEqual(len(hits), size * 2)
                    num_aggregates, num_contribs = 0, 0
                    for hit in hits:
                        entity_type, aggregate = config.parse_es_index_name(hit["_index"])
                        if aggregate:
                            doc = Aggregate.from_index(field_types, hit)
                            self.assertNotEqual(doc.contents, {})
                            num_aggregates += 1
                        else:
                            doc = Contribution.from_index(field_types, hit)
                            self.assertEqual((doc.bundle_uuid, doc.bundle_version), self.new_bundle)
                            self.assertFalse(doc.bundle_deleted)
                            num_contribs += 1
                    self.assertEqual(num_aggregates, size)
                    self.assertEqual(num_contribs, size)

                    self._index_bundle(self.new_bundle, manifest, metadata, delete=True)

                    hits = self._get_all_hits()
                    # Twice the size because deletions create new contribution
                    self.assertEqual(len(hits), 2 * size)
                    docs_by_entity_id = defaultdict(list)
                    for hit in hits:
                        doc = Contribution.from_index(field_types, hit)
                        docs_by_entity_id[doc.entity.entity_id].append(doc)
                        entity_type, aggregate = config.parse_es_index_name(hit["_index"])
                        # Since there is only one bundle and it was deleted, nothing should be aggregated
                        self.assertFalse(aggregate)
                        self.assertEqual((doc.bundle_uuid, doc.bundle_version), self.new_bundle)

                    for pair in docs_by_entity_id.values():
                        self.assertEqual(list(sorted(doc.bundle_deleted for doc in pair)), [False, True])
                finally:
                    self._delete_indices()

    def test_duplicate_notification(self):
        manifest, metadata = self._load_canned_bundle(self.new_bundle)
        tallies = dict(self._write_contributions(self.new_bundle, manifest, metadata))

        with self.assertLogs(logger=azul.indexer.log, level='WARNING') as logs:
            # Writing again simulates a duplicate notification being processed
            tallies.update(self._write_contributions(self.new_bundle, manifest, metadata))
        message_re = re.compile(r'^WARNING:azul\.indexer:Writing document .* requires update\. Possible causes include '
                                r'duplicate notifications or reindexing without clearing the index\.$')
        for message in logs.output:
            self.assertRegex(message, message_re)
        # Tallies should not be inflated despite indexing document twice
        self.get_hca_indexer().aggregate(tallies)
        self._assert_new_bundle()

    def test_zero_tallies(self):
        """
        Since duplicate notifications are subtracted back out of tally counts, it's possible to receive a tally with
        zero notifications. Test that a tally with count 0 still triggers aggregation.
        """
        manifest, metadata = self._load_canned_bundle(self.new_bundle)
        tallies = dict(self._write_contributions(self.new_bundle, manifest, metadata))
        for tally in tallies:
            tallies[tally] = 0
        # Aggregating should not be a non-op even though tally counts are all zero
        with self.assertLogs(elasticsearch.client.logger, level='INFO') as logs:
            self.get_hca_indexer().aggregate(tallies)
        doc_ids = {
            '70d1af4a-82c8-478a-8960-e9028b3616ca',
            'a21dc760-a500-4236-bcff-da34a0e873d2',
            'e8642221-4c2c-4fd7-b926-a68bce363c88',
            '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb',
            'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
        }
        for doc_id in doc_ids:
            message_re = re.compile(fr'^INFO:elasticsearch:PUT .*_aggregate_.*/doc/{doc_id}.* \[status:201 .*\]$')
            self.assertTrue(any(message_re.fullmatch(message) for message in logs.output))

    def test_deletion_before_addition(self):
        self._index_canned_bundle(self.new_bundle, delete=True)
        self._assert_index_counts(just_deletion=True)
        self._index_canned_bundle(self.new_bundle)
        self._assert_index_counts(just_deletion=False)

    def _assert_index_counts(self, just_deletion):
        # Five entities (two files, one project, one sample and one bundle)
        num_expected_addition_contributions = 0 if just_deletion else 6
        num_expected_deletion_contributions = 6
        num_expected_aggregates = 0
        hits = self._get_all_hits()
        actual_addition_contributions = [h for h in hits if not h['_source']['bundle_deleted']]
        actual_deletion_contributions = [h for h in hits if h['_source']['bundle_deleted']]

        def is_aggregate(h):
            _, aggregate_ = config.parse_es_index_name(h['_index'])
            return aggregate_

        actual_aggregates = [h for h in hits if is_aggregate(h)]

        self.assertEqual(len(actual_addition_contributions), num_expected_addition_contributions)
        self.assertEqual(len(actual_deletion_contributions), num_expected_deletion_contributions)
        self.assertEqual(len(actual_aggregates), num_expected_aggregates)
        self.assertEqual(num_expected_addition_contributions
                         + num_expected_deletion_contributions
                         + num_expected_aggregates,
                         len(hits))

    def test_bundle_delete_downgrade(self):
        """
        Delete an updated version of a bundle, and ensure that the index reverts to the previous bundle.
        """
        self._index_canned_bundle(self.old_bundle)
        old_hits_by_id = self._assert_old_bundle()
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=6, old_hits_by_id=old_hits_by_id)
        self._index_canned_bundle(self.new_bundle, delete=True)
        self._assert_old_bundle(num_expected_new_deleted_contributions=6)

    def test_multi_entity_contributing_bundles(self):
        """
        Delete a bundle which shares entities with another bundle and ensure shared entities
        are not deleted. Only entity associated with deleted bundle should be marked as deleted.
        """
        bundle_fqid = ("8543d32f-4c01-48d5-a79f-1c5439659da3", "2018-03-29T143828.884167Z")
        patched_bundle_fqid = ("9654e431-4c01-48d5-a79f-1c5439659da3", "2018-03-29T153828.884167Z")
        manifest, metadata = self._load_canned_bundle(bundle_fqid)
        old_file_uuid, patched_manifest, patched_metadata = self._patch_bundle(manifest, metadata)
        self._index_bundle(bundle_fqid, manifest, metadata)
        self._index_bundle(patched_bundle_fqid, patched_manifest, patched_metadata)

        hits_before = self._get_all_hits()
        num_docs_by_index_before = self._num_docs_by_index(hits_before)

        self._index_canned_bundle(bundle_fqid, delete=True)

        hits_after = self._get_all_hits()
        num_docs_by_index_after = self._num_docs_by_index(hits_after)

        for entity_type, aggregate in num_docs_by_index_after.keys():
            # Both bundles reference two files. They both share one file
            # and exclusively own another one. Deleting one of the bundles removes the file owned exclusively by
            # that bundle, as well as the bundle itself.
            if aggregate:
                difference = 1 if entity_type in ('files', 'bundles') else 0
                self.assertEqual(num_docs_by_index_after[entity_type, aggregate],
                                 num_docs_by_index_before[entity_type, aggregate] - difference)
            elif entity_type in ('bundles', 'samples', 'projects', 'cell_suspensions'):
                # Count one extra deletion contribution
                self.assertEqual(num_docs_by_index_after[entity_type, aggregate],
                                 num_docs_by_index_before[entity_type, aggregate] + 1)
            else:
                # Count two extra deletion contributions for the two files
                self.assertEqual(entity_type, 'files')
                self.assertEqual(num_docs_by_index_after[entity_type, aggregate],
                                 num_docs_by_index_before[entity_type, aggregate] + 2)

        deleted_document_id = Contribution.make_document_id(old_file_uuid, *bundle_fqid, bundle_deleted=True)
        hits = [hit['_source'] for hit in hits_after if hit['_id'] == deleted_document_id]
        self.assertTrue(one(hits)['bundle_deleted'])

    def _patch_bundle(self, manifest, metadata):
        new_file_uuid = str(uuid4())
        manifest = copy.deepcopy(manifest)
        file_name = '21935_7#154_2.fastq.gz'
        for file in manifest:
            if file['name'] == file_name:
                old_file_uuid = file['uuid']
                file['uuid'] = new_file_uuid
                break
        else:
            assert False, f"Unable to find file name {file_name}"

        def _walkthrough(v):
            if isinstance(v, dict):
                return dict((k, _walkthrough(v)) for k, v in v.items())
            elif isinstance(v, list):
                return list(_walkthrough(i) for i in v)
            elif isinstance(v, (str, int, bool, float)):
                return new_file_uuid if v == old_file_uuid else v
            else:
                assert False, f'Cannot handle values of type {type(v)}'

        metadata = _walkthrough(metadata)
        return old_file_uuid, manifest, metadata

    def _num_docs_by_index(self, hits) -> Mapping[Tuple[str, bool], int]:
        counter = Counter()
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            counter[entity_type, aggregate] += 1
        return counter

    def test_derived_files(self):
        """
        Index an analysis bundle, which, unlike a primary bundle, has data files derived from other data
        files, and assert that the resulting `files` index document contains exactly one file entry.
        """
        analysis_bundle = 'd5e01f9d-615f-4153-8a56-f2317d7d9ce8', '2018-09-06T185759.326912Z'
        self._index_canned_bundle(analysis_bundle)
        hits = self._get_all_hits()
        num_files = 33
        self.assertEqual(len(hits), (num_files + 1 + 1 + 1 + 1) * 2)
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
                elif entity_type == 'bundles':
                    self.assertEqual(num_files, len(contents['files']))
                else:
                    self.assertEqual(num_files, sum(file['count'] for file in contents['files']))
            else:
                num_contribs[entity_type] += 1
                self.assertEqual(analysis_bundle, (source['bundle_uuid'], source['bundle_version']))
                self.assertEqual(1 if entity_type == 'files' else num_files, len(contents['files']))
            self.assertEqual(1, len(contents['specimens']))
            self.assertEqual(1, len(contents['projects']))
        num_expected = dict(files=num_files, samples=1, cell_suspensions=1, projects=1, bundles=1)
        self.assertEqual(num_contribs, num_expected)
        self.assertEqual(num_aggregates, num_expected)

    def test_bundle_upgrade(self):
        """
        Updating a bundle with a future version should overwrite the old version.
        """
        self._index_canned_bundle(self.old_bundle)
        old_hits_by_id = self._assert_old_bundle()
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=6, old_hits_by_id=old_hits_by_id)

    def test_bundle_downgrade(self):
        """
        Indexing an old version of a bundle *after* a new version should not have an effect on aggregates.
        """
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=0)
        self._index_canned_bundle(self.old_bundle)
        self._assert_old_bundle(num_expected_new_contributions=6, ignore_aggregates=True)
        self._assert_new_bundle(num_expected_old_contributions=6)

    def _assert_old_bundle(self,
                           num_expected_new_contributions=0,
                           num_expected_new_deleted_contributions=0,
                           ignore_aggregates=False):
        """
        Assert that the old bundle is still indexed correctly

        :param num_expected_new_contributions: Contributions from the new bundle without a corresponding deletion
        contribution
        :param num_expected_new_deleted_contributions: Contributions from the new bundle WITH a corresponding deletion
        contribution
        :param ignore_aggregates: Don't consider aggregates when counting docs in index
        """
        num_actual_new_contributions = 0
        num_actual_new_deleted_contributions = 0
        hits = self._get_all_hits()
        # Six entities (two files, one project, one cell suspension, one sample, and one bundle)
        # One contribution and one aggregate per entity
        # Two times number of deleted contributions since deletes don't remove a contribution, but add a new one
        self.assertEqual(6 + 6 + num_expected_new_contributions + num_expected_new_deleted_contributions * 2, len(hits))
        hits_by_id = {}
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            if aggregate and ignore_aggregates:
                continue
            source = hit['_source']
            hits_by_id[source['entity_id'], aggregate] = hit
            version = one(source['bundles'])['version'] if aggregate else source['bundle_version']
            if aggregate or self.old_bundle[1] == version:
                contents = source['contents']
                project = one(contents['projects'])
                self.assertEqual('Single cell transcriptome patterns.', get(project['project_title']))
                self.assertEqual('Single of human pancreas', get(project['project_short_name']))
                self.assertIn('John Dear', get(project['laboratory']))
                if aggregate and entity_type != 'projects':
                    self.assertIn('Farmers Trucks', project['institutions'])
                else:
                    self.assertIn('Farmers Trucks', [c.get('institution') for c in project['contributors']])
                donor = one(contents['donors'])
                self.assertIn('Australopithecus', donor['genus_species'])
                if not aggregate:
                    self.assertFalse(source['bundle_deleted'])
            else:
                if source['bundle_deleted']:
                    num_actual_new_deleted_contributions += 1
                else:
                    self.assertLess(self.old_bundle[1], version)
                    num_actual_new_contributions += 1
        # We count the deleted contributions here too since they should have a corresponding addition contribution
        self.assertEqual(num_expected_new_contributions + num_expected_new_deleted_contributions,
                         num_actual_new_contributions)
        self.assertEqual(num_expected_new_deleted_contributions, num_actual_new_deleted_contributions)
        return hits_by_id

    def _assert_new_bundle(self, num_expected_old_contributions=0, old_hits_by_id=None):
        num_actual_old_contributions = 0
        hits = self._get_all_hits()
        # Six entities (two files, one project, one cell suspension, one sample and one bundle)
        # One contribution and one aggregate per entity
        self.assertEqual(6 + 6 + num_expected_old_contributions, len(hits))
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
                self.assertNotEqual(old_project["project_short_name"], project["project_short_name"])
                self.assertNotEqual(old_project["laboratory"], project["laboratory"])
                if aggregate and entity_type != 'projects':
                    self.assertNotEqual(old_project["institutions"], project["institutions"])
                else:
                    self.assertNotEqual(old_project["contributors"], project["contributors"])
                self.assertNotEqual(old_contents["donors"][0]["genus_species"],
                                    contents["donors"][0]["genus_species"])

            self.assertEqual("Single cell transcriptome analysis of human pancreas reveals transcriptional "
                             "signatures of aging and somatic mutation patterns.",
                             get(project["project_title"]))
            self.assertEqual("Single cell transcriptome analysis of human pancreas",
                             get(project["project_short_name"]))
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

        def mocked_mget(self, body, _source_include):
            mget_return = original_mget(self, body=body, _source_include=_source_include)
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
                               and ("azul_samples" in log_msg or "azul_projects" in log_msg))
                # One conflict for the specimen and one for the project
                self.assertEqual(num_hits, 2)

        hits = self._get_all_hits()
        file_uuids = set()
        # Two bundles each with 1 sample, 1 cell suspension, 1 project, 1 bundle and 2 files
        # Both bundles share the same sample and the project, so they get aggregated only once:
        # 2 samples + 2 projects + 2 cell suspension + 2 bundles + 4 files +
        # 1 samples agg + 1 projects agg + 2 cell suspension agg + 2 bundle agg + 4 file agg = 22 hits
        self.assertEqual(22, len(hits))
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
            elif entity_type in ('samples', 'projects'):
                if aggregate:
                    self.assertEqual(2, len(hit['_source']['bundles']))
                    # All four files are fastqs so the are grouped together
                    self.assertEqual(4, one(contents['files'])['count'])
                else:
                    self.assertEqual(2, len(contents['files']))
            elif entity_type == 'bundles':
                if aggregate:
                    self.assertEqual(1, len(hit['_source']['bundles']))
                    self.assertEqual(2, len(contents['files']))
                else:
                    self.assertEqual(2, len(contents['files']))
            elif entity_type == 'cell_suspensions':
                if aggregate:
                    self.assertEqual(1, len(hit['_source']['bundles']))
                    self.assertEqual(1, len(contents['files']))
                else:
                    self.assertEqual(2, len(contents['files']))
            else:
                self.fail()
        file_document_ids = set()
        self.assertEqual(4, len(file_uuids))
        for bundle_fqid in bundles:
            manifest, metadata = self._load_canned_bundle(bundle_fqid)
            files: JSONs = metadata['file.json']['files']
            for file in files:
                file_document_ids.add(file['hca_ingest']['document_id'])
        self.assertEqual(file_document_ids, file_uuids)

    def test_indexing_matrix_related_files(self):
        self._index_canned_bundle(('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z'))
        self.maxDiff = None
        hits = self._get_all_hits()
        zarrs = []
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            if entity_type == 'files':
                file = one(hit['_source']['contents']['files'])
                if len(file['related_files']) > 0:
                    self.assertEqual(file['file_format'], 'matrix')
                    zarrs.append(hit)
                elif file['file_format'] == 'matrix':
                    # Matrix of Loom or CSV format possibly
                    self.assertNotIn('.zarr', file['name'])
            elif not aggregate:
                for file in hit['_source']['contents']['files']:
                    self.assertEqual(file['related_files'], [])

        self.assertEqual(len(zarrs), 2)  # One contribution, one aggregate
        for zarr_file in zarrs:
            zarr_file = one(zarr_file['_source']['contents']['files'])
            related_files = zarr_file['related_files']
            self.assertNotIn(zarr_file['name'], {f['name'] for f in related_files})
            self.assertEqual(len(related_files), 12)

    def test_indexing_with_skipped_matrix_file(self):
        # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
        self._index_canned_bundle(('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z'))
        self.maxDiff = None
        hits = self._get_all_hits()
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
                        if entity_type == 'bundles':
                            if file['file_format'] == 'matrix':
                                entities_with_matrix_files.add(hit['_source']['entity_id'])
                        else:
                            if file['file_format'] == 'matrix':
                                self.assertEqual(1, file['count'])
                                entities_with_matrix_files.add(hit['_source']['entity_id'])
            else:
                for file in files:
                    file_name = file['name']
                    file_names.add(file_name)
        self.assertEqual(4, len(entities_with_matrix_files))  # a project, a specimen, a cell suspension and a bundle
        self.assertEqual(aggregate_file_names, file_names)
        matrix_file_names = {file_name for file_name in file_names if '.zarr!' in file_name}
        self.assertEqual({'377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr!.zattrs'}, matrix_file_names)

    def test_plate_bundle(self):
        self._index_canned_bundle(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        counted_cell_count = 0
        expected_cell_count = 380  # 384 wells in total, four of them empty, the rest with a single cell
        documents_with_cell_suspension = 0
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            contents = hit['_source']['contents']
            cell_suspensions = contents['cell_suspensions']
            if entity_type == 'files' and contents['files'][0]['file_format'] == 'pdf':
                # The PDF files in that bundle aren't linked to a specimen
                self.assertEqual(0, len(cell_suspensions))
            else:
                if aggregate:
                    bundles = hit['_source']['bundles']
                    self.assertEqual(1, len(bundles))
                    self.assertEqual(one(contents['protocols'])['paired_end'], [True])
                else:
                    self.assertEqual({p.get('paired_end') for p in contents['protocols']}, {True, None})
                specimens = contents['specimens']
                for specimen in specimens:
                    self.assertEqual({'bone marrow', 'temporal lobe'}, set(specimen['organ_part']))
                for cell_suspension in cell_suspensions:
                    self.assertEqual({'bone marrow', 'temporal lobe'}, set(cell_suspension['organ_part']))
                    self.assertEqual({'Plasma cells'}, set(cell_suspension['selected_cell_type']))
                self.assertEqual(1 if entity_type == 'cell_suspensions' or aggregate else 384, len(cell_suspensions))
                if entity_type == 'cell_suspensions':
                    counted_cell_count += one(cell_suspensions)['total_estimated_cells']
                else:
                    self.assertEqual(expected_cell_count, sum(cs['total_estimated_cells'] for cs in cell_suspensions))
                documents_with_cell_suspension += 1
        self.assertEqual(expected_cell_count * 2, counted_cell_count)  # times 2 for original document and aggregate
        # Cell suspensions should be mentioned in 1 bundle, 1 project, 1 specimen, 384 cell suspensions, and 2 files
        # (one per fastq). There should be one original and one aggregate document for each of those. (389 * 2 = 778)
        self.assertEqual(778, documents_with_cell_suspension)

    def test_well_bundles(self):
        self._index_canned_bundle(('3f8176ff-61a7-4504-a57c-fc70f38d5b13', '2018-10-24T234431.820615Z'))
        self._index_canned_bundle(('e2c3054e-9fba-4d7a-b85b-a2220d16da73', '2018-10-24T234303.157920Z'))
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        for hit in hits:
            contents = hit["_source"]['contents']
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            if aggregate:
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # Each bundle contributes a well with one cell. The data files in each bundle are derived from
                # the cell in that well. This is why each data file and bundle should only have a cell count of 1.
                # Both bundles refer to the same specimen and project, so the cell count for those should be 2.
                expected_cells = 1 if entity_type in ('files', 'cell_suspensions', 'bundles') else 2
                self.assertEqual(expected_cells, cell_suspensions[0]['total_estimated_cells'])
                self.assertEqual(one(one(contents['protocols'])['workflow']), 'smartseq2_v2.1.0')
            else:
                self.assertEqual({p.get('workflow') for p in contents['protocols']}, {'smartseq2_v2.1.0', None})

    def test_pooled_specimens(self):
        """
        Index a bundle that combines 3 specimen_from_organism into 1 cell_suspension
        """
        self._index_canned_bundle(('b7fc737e-9b7b-4800-8977-fe7c94e131df', '2018-09-12T121155.846604Z'))
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            if aggregate:
                contents = hit["_source"]['contents']
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # This bundle contains three specimens which are pooled into the a single cell suspension with
                # 10000 cells. Until we introduced cell suspensions as an inner entity we used to associate cell
                # counts with specimen which would have inflated the total cell count to 30000 in this case.
                self.assertEqual(10000, cell_suspensions[0]['total_estimated_cells'])
                sample = one(contents['samples'])
                self.assertEqual(sample['organ'], sample['effective_organ'])
                if entity_type == 'samples':
                    self.assertTrue(sample['effective_organ'] in {'Brain 1', 'Brain 2', 'Brain 3'})
                else:
                    self.assertEqual(set(sample['effective_organ']), {'Brain 1', 'Brain 2', 'Brain 3'})

    def test_project_contact_extraction(self):
        """
        Ensure all fields related to project contacts are properly extracted
        """
        self._index_canned_bundle(('d0e17014-9a58-4763-9e66-59894efbdaa8', '2018-10-03T144137.044509Z'))
        hits = self._get_all_hits()
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            if aggregate and entity_type == 'projects':
                contributor_values = defaultdict(set)
                contributors = hit['_source']['contents']['projects'][0]['contributors']
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
                self.assertEqual({'Human Cell Atlas wrangler', config.null_keyword},
                                 contributor_values['project_role'])

    def test_diseases_field(self):
        """
        Index a bundle with a specimen `diseases` value that differs from the donor `diseases` value
        and assert that both values are represented in the indexed document.
        """
        self._index_canned_bundle(("3db604da-940e-49b1-9bcc-25699a55b295", "2018-11-02T184048.983513Z"))

        hits = self._get_all_hits()
        for hit in hits:
            source = hit['_source']
            contents = source['contents']
            specimen_diseases = contents['specimens'][0]['disease']
            donor_diseases = contents['donors'][0]['diseases']
            self.assertEqual(1, len(specimen_diseases))
            self.assertEqual("atrophic vulva (specimen_from_organism)", specimen_diseases[0])
            self.assertEqual(1, len(donor_diseases))
            self.assertEqual("atrophic vulva (donor_organism)", donor_diseases[0])

    def test_organoid_priority(self):
        """
        Index a bundle containing an Organoid and assert that the "organ" and "organ_part"
        values saved are the ones from the Organoid and not the SpecimenFromOrganism
        """
        self._index_canned_bundle(('dcccb551-4766-4210-966c-f9ee25d19190', '2018-10-18T204655.866661Z'))
        hits = self._get_all_hits()
        inner_specimens, inner_cell_suspensions = 0, 0
        for hit in hits:

            contents = hit['_source']['contents']
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])

            if entity_type != 'files' or one(contents['files'])['file_format'] != 'pdf':
                inner_cell_suspensions += len(contents['cell_suspensions'])

            for specimen in contents['specimens']:
                inner_specimens += 1
                expect_list = aggregate and entity_type != 'specimens'
                self.assertEqual(['skin of body'] if expect_list else 'skin of body', specimen['organ'])
                self.assertEqual(['skin epidermis'], specimen['organ_part'])

            for organoid in contents['organoids']:
                self.assertEqual(['Brain'] if aggregate else 'Brain', organoid['model_organ'])
                self.assertEqual([config.null_keyword] if aggregate else config.null_keyword,
                                 organoid['model_organ_part'])

        projects = 1
        bundles = 1
        specimens = 4
        cell_suspensions = 1
        files = 16
        all_entities = files + specimens + projects + bundles + cell_suspensions
        non_specimens = files + projects + bundles + cell_suspensions
        inner_specimens_in_contributions = non_specimens * specimens + specimens * 1
        inner_specimens_in_aggregates = all_entities * 1
        inner_cell_suspensions_in_contributions = all_entities * cell_suspensions
        inner_cell_suspensions_in_aggregates = all_entities * 1

        self.assertEqual(inner_specimens_in_contributions + inner_specimens_in_aggregates,
                         inner_specimens)
        self.assertEqual(inner_cell_suspensions_in_contributions + inner_cell_suspensions_in_aggregates,
                         inner_cell_suspensions)

    def test_accessions_fields(self):
        self._index_canned_bundle(('fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a', '2019-02-14T192438.034764Z'))
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            project = one(contents['projects'])
            self.assertEqual(['SRP000000'], project['insdc_project_accessions'])
            self.assertEqual(['GSE00000'], project['geo_series_accessions'])
            self.assertEqual(['E-AAAA-00'], project['array_express_accessions'])
            self.assertEqual(['PRJNA000000'], project['insdc_study_accessions'])

    def test_imaging_bundle(self):
        self._index_canned_bundle(('94f2ba52-30c8-4de0-a78e-f95a3f8deb9c', '2019-04-03T103426.471000Z'))
        hits = self._get_all_hits()
        sources = defaultdict(list)
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            sources[entity_type, aggregate].append(hit['_source'])
            # bundle has 240 imaging_protocol_0.json['target'] items, each with an assay_type of 'in situ sequencing'
            assay_type = ['in situ sequencing'] if aggregate else {'in situ sequencing': 240}
            self.assertEqual(one(hit['_source']['contents']['protocols'])['assay_type'], assay_type)
        for aggregate in True, False:
            with self.subTest(aggregate=aggregate):
                self.assertEqual(
                    {
                        'bundles': 1,
                        'files': 227,
                        'projects': 1,
                        'samples': 1,
                    },
                    {
                        entity_type: len(sources)
                        for (entity_type, _aggregate), sources in sources.items()
                        if _aggregate is aggregate
                    }
                )
                # This imaging bundle contains 6 data files in JSON format
                self.assertEqual(
                    Counter({'tiff': 221, 'json': 6}),
                    Counter(one(source['contents']['files'])['file_format']
                            for source in sources['files', aggregate])
                )

    def test_cell_line_sample(self):
        """
        Index a bundle with the following structure:
        donor -> specimen -> cell_line -> cell_line -> cell_suspension -> sequence_files
        and assert the singleton sample matches the first cell_line up from the sequence_files
        and assert cell_suspension inherits the organ value from the nearest ancestor cell_line
        """
        self._index_canned_bundle(('e0ae8cfa-2b51-4419-9cde-34df44c6458a', '2018-12-05T230917.591044Z'))
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            if entity_type == 'samples':
                sample = one(contents['samples'])
                sample_entity_type = sample['entity_type']
                if aggregate:
                    document_ids = one(contents[sample_entity_type])['document_id']
                else:
                    document_ids = [d['document_id'] for d in contents[sample_entity_type]]
                    entity = one([d for d in contents[sample_entity_type] if d['document_id'] == sample['document_id']])
                    self.assertEqual(sample['biomaterial_id'], entity['biomaterial_id'])
                self.assertTrue(sample['document_id'] in document_ids)
                self.assertEqual(one(contents['specimens'])['organ'], ['blood'] if aggregate else 'blood')
                self.assertEqual(one(contents['specimens'])['organ_part'], ['venous blood'])
                self.assertEqual(len(contents['cell_lines']), 1 if aggregate else 2)
                if aggregate:
                    cell_lines_model_organ = set(one(contents['cell_lines'])['model_organ'])
                else:
                    cell_lines_model_organ = {cl['model_organ'] for cl in contents['cell_lines']}
                self.assertEqual(cell_lines_model_organ, {'blood (parent_cell_line)', 'blood (child_cell_line)'})
                self.assertEqual(one(contents['cell_suspensions'])['organ'], ['blood (child_cell_line)'])
                self.assertEqual(one(contents['cell_suspensions'])['organ_part'], [config.null_keyword])

    def test_files_content_description(self):
        self._index_canned_bundle(('ffac201f-4b1c-4455-bd58-19c1a9e863b4', '2019-10-09T170735.528600Z'))
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, aggregate = config.parse_es_index_name(hit['_index'])
            if aggregate:
                # bundle aggregates keep individual files
                num_inner_files = 2 if entity_type == 'bundles' else 1
            else:
                # one inner file per file contribution
                num_inner_files = 1 if entity_type == 'files' else 2
            self.assertEqual(len(contents['files']), num_inner_files)
            for file in contents['files']:
                self.assertEqual(file['content_description'], ['RNA sequence'])

    def test_metadata_generator(self):
        index_bundle = ('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z')
        manifest, metadata_files = self._load_canned_bundle(index_bundle)
        generator = MetadataGenerator()
        uuid, version = index_bundle
        generator.add_bundle(uuid, version, manifest, metadata_files)
        metadata_rows = generator.dump()
        expected_metadata_contributions = 20
        self.assertEqual(expected_metadata_contributions, len(metadata_rows))
        for metadata_row in metadata_rows:
            self.assertEqual(uuid, metadata_row['bundle_uuid'])
            self.assertEqual(version, metadata_row['bundle_version'])
            if metadata_row['file_format'] == 'matrix':
                expected_file_name = '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/'
                self.assertEqual(expected_file_name, metadata_row['file_name'])
            else:
                self.assertIn(metadata_row['file_format'], {'txt', 'csv', 'fastq.gz', 'results', 'bam', 'bai'})

    def test_related_files_field_exclusion(self):
        self._index_canned_bundle(('587d74b4-1075-4bbf-b96a-4d1ede0481b2', '2018-10-10T022343.182000Z'))

        # Check that the dynamic mapping has the related_files field disabled
        index = config.es_index_name('files')
        mapping = self.es_client.indices.get_mapping(index=index)
        contents = mapping[index]['mappings']['doc']['properties']['contents']
        self.assertFalse(contents['properties']['files']['properties']['related_files']['enabled'])

        # Ensure that related_files exists
        hits = self._get_all_hits()
        for hit in hits:
            entity_type, aggregate = config.parse_es_index_name(hit["_index"])
            if aggregate and entity_type == 'files':
                file = one(hit['_source']['contents']['files'])
                self.assertIn('related_files', file)

        # … but that it can't be used for queries
        zattrs_file = "377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr!.zattrs"
        hits = self.es_client.search(index=index,
                                     body={
                                         "query": {
                                             "match": {
                                                 "contents.files.related_files.name": zattrs_file
                                             }
                                         }
                                     })
        self.assertEqual(0, hits["hits"]["total"])

    def test_metadata_field_exclusion(self):
        self._index_canned_bundle(self.old_bundle)
        bundles_index = config.es_index_name('bundles')

        # Ensure that a metadata row exists …
        hits = self._get_all_hits()
        bundles_hit = one(hit['_source'] for hit in hits if hit['_index'] == bundles_index)
        expected_metadata_hits = 2
        self.assertEqual(expected_metadata_hits, len(bundles_hit['contents']['metadata']))
        for metadata_row in bundles_hit['contents']['metadata']:
            self.assertEqual(self.old_bundle, (metadata_row['bundle_uuid'], metadata_row['bundle_version']))

        # … but that it can't be used for queries
        hits = self.es_client.search(index=bundles_index,
                                     body={
                                         "query": {
                                             "match": {
                                                 "contents.metadata.bundle_uuid": self.old_bundle[0]
                                             }
                                         }
                                     })
        self.assertEqual(0, hits["hits"]["total"])

        # Check that the dynamic mapping has the metadata field disabled
        mapping = self.es_client.indices.get_mapping(index=bundles_index)
        contents = mapping[bundles_index]['mappings']['doc']['properties']['contents']
        self.assertFalse(contents['properties']['metadata']['enabled'])

        # Ensure we can find the bundle UUID outside of `metadata`.
        hits = self.es_client.search(index=bundles_index,
                                     body={
                                         "query": {
                                             "match": {
                                                 "bundle_uuid": self.old_bundle[0]
                                             }
                                         }
                                     })
        self.assertEqual(1, hits["hits"]["total"])

    def test_variable_metadata_format_indexable(self):
        """
        Index different formats for a single field in metadata and ensure dynamic mapper in
        elasticsearch does not set a field type for incoming data.
        """
        multiple_cell_line_bundle = ('e0ae8cfa-2b51-4419-9cde-34df44c6458a', '2018-12-05T230917.591044Z')
        manifest_multiple_cell_lines, metadata_multiple_cell_lines = self._load_canned_bundle(multiple_cell_line_bundle)

        manifest_single_cell_line, metadata_single_cell_line = self._load_canned_bundle(self.new_bundle)

        # Patch bundles to obtain desired response from Elasticsearch dynamic type setter
        entity = 'cell_line_0.json'

        metadata_single_cell_line[entity] = deepcopy(metadata_multiple_cell_lines[entity])
        metadata_single_cell_line[entity]['date_established'] = '2014-10-24'
        metadata_single_cell_line[entity]['provenance'] = {
            'document_id': 'e7e0f358-a681-4412-8888-318234f90ca9',
            'submission_date': '2019-05-15T09:36:02.702Z',
            'update_date': '2019-05-15T09:36:11.640Z'
        }
        manifest_single_cell_line.append({
            'content-type': 'application/json; dcp-type=\'metadata/biomaterial\'',
            'crc32c': '44fe9793',
            'indexed': True,
            'name': 'cell_line_0.json',
            's3_etag': 'c3f292e01d299b4be3fd1f460842fc1d',
            'sha1': '6b13668c7446caa5ea1964e855faf196c7dc2bbb',
            'sha256': 'f5632ece248ccd7f9c0cfa2c08d41ee87ebcc280842404b22df366d08ad1e541',
            'size': 1791,
            'uuid': 'e7e0f358-a681-4412-8888-318234f90ca9',
            'version': '2018-11-04T103611.640000Z'
        })

        metadata_multiple_cell_lines[entity]['date_established'] = '2015-01-09'
        metadata_multiple_cell_lines['cell_line_1.json']['date_established'] = '2014-11-09'

        self._index_bundle(self.new_bundle, manifest_single_cell_line, metadata_single_cell_line)

        hits = self._get_all_hits()
        bundles_index = config.es_index_name('bundles')
        bundles_hit_1 = one(hit['_source'] for hit in hits if hit['_index'] == bundles_index)
        self.assertEqual('2014-10-24',
                         bundles_hit_1['contents']['metadata'][0]['cell_line.date_established'])

        self._index_bundle(multiple_cell_line_bundle, manifest_multiple_cell_lines, metadata_multiple_cell_lines)
        hits = self._get_all_hits()
        bundle_hits = [hit['_source'] for hit in hits if hit['_index'] == bundles_index]
        bundle_hits.remove(bundles_hit_1)
        bundles_hit_2 = one(bundle_hits)
        self.assertEqual('2014-11-09||2015-01-09',
                         bundles_hit_2['contents']['metadata'][0]['cell_line.date_established'])


class TestValidNotificationRequests(LocalAppTestCase):

    @classmethod
    def lambda_name(cls) -> str:
        return "indexer"

    @mock_sts
    @mock_sqs
    def test_succesful_notifications(self):
        self._create_mock_notify_queue()
        body = {
            'match': {
                'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                'bundle_version': '2018-03-28T13:55:26.044Z'
            }
        }
        for endpoint in ['/', '/delete']:
            with self.subTest(endpoint=endpoint):
                response = self._test(body, endpoint, valid_auth=True)
                self.assertEqual(202, response.status_code)
                self.assertEqual('', response.text)

    @mock_sts
    @mock_sqs
    def test_invalid_notifications(self):
        bodies = {
            "Missing body": {},
            "Missing bundle_uuid":
                {
                    'match': {
                        'bundle_version': '2018-03-28T13:55:26.044Z'
                    }
                },
            "bundle_uuid is None":
                {
                    'match': {
                        'bundle_uuid': None,
                        'bundle_version': '2018-03-28T13:55:26.044Z'
                    }
                },
            "Missing bundle_version":
                {
                    'match': {
                        'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd'
                    }
                },
            "bundle_version is None":
                {
                    'match': {
                        'bundle_uuid': 'bb2365b9-5a5b-436f-92e3-4fc6d86a9efd',
                        'bundle_version': None
                    }
                },
            'Malformed bundle_uuis value':
                {
                    'match': {
                        'bundle_uuid': f'}}{str(uuid4())}{{',
                        'bundle_version': "2019-12-31T00:00:00.000Z"
                    }
                },
            'Malformed bundle_version':
                {
                    'match': {
                        'bundle_uuid': str(uuid4()),
                        'bundle_version': ''
                    }
                }
        }
        for endpoint in ['/', '/delete']:
            with self.subTest(endpoint=endpoint):
                for test, body in bodies.items():
                    with self.subTest(test):
                        response = self._test(body, endpoint, valid_auth=True)
                        self.assertEqual(400, response.status_code)

    @mock_sts
    @mock_sqs
    def test_invalid_auth_for_notification_request(self):
        self._create_mock_notify_queue()
        body = {
            "match": {
                'bundle_uuid': str(uuid4()),
                'bundle_version': 'SomeBundleVersion'
            }
        }
        for endpoint in ['/', '/delete']:
            with self.subTest(endpoint=endpoint):
                response = self._test(body, endpoint=endpoint, valid_auth=False)
                self.assertEqual(401, response.status_code)

    @mock_sts
    @mock_sqs
    def test_index_test_mode(self):
        self._create_mock_notify_queue()
        notification = {
            "match": {
                "bundle_uuid": "bb2365b9-5a5b-436f-92e3-4fc6d86a9efd",
                "bundle_version": "2018-03-28T13:55:26.044Z"
            }
        }
        for endpoint in '/', '/delete':
            for test_mode in 0, 1:
                for test_name in None, "foo":
                    with self.subTest(test_mode=test_mode, endpoint=endpoint, test_name=test_name):
                        with patch.dict(os.environ, AZUL_TEST_MODE=str(test_mode)):
                            payload = {} if test_name is None else {'test_name': test_name}
                            with patch.dict(notification, **payload):
                                response = self._test(notification, endpoint=endpoint, valid_auth=True)
                                if test_mode == 1 and test_name is None:
                                    self.assertEqual(500, response.status_code)
                                    self.assertEqual(
                                        {
                                            'Code': 'ChaliceViewError',
                                            'Message': 'ChaliceViewError: The indexer is currently in test mode where '
                                                       'it only accepts specially instrumented notifications. Please '
                                                       'try again later'
                                        },
                                        response.json()
                                    )
                                elif test_mode == 0 and test_name is not None:
                                    self.assertEqual(400, response.status_code)
                                    self.assertEqual(
                                        {
                                            'Code': 'BadRequestError',
                                            'Message': 'BadRequestError: Cannot process test notifications outside of '
                                                       'test mode'
                                        },
                                        response.json()
                                    )
                                else:
                                    self.assertEqual(202, response.status_code)

    def _test(self, body, endpoint, valid_auth):
        with ResponsesHelper() as helper:
            helper.add_passthru(self.base_url)
            hmac_creds = {'key': b'good key', 'key_id': 'the id'}
            with patch('azul.deployment.aws.get_hmac_key_and_id', return_value=hmac_creds):
                with patch.dict(os.environ, AWS_DEFAULT_REGION='us-east-1'):
                    if valid_auth:
                        auth = hmac.prepare()
                    else:
                        auth = HTTPSignatureAuth(key=b'bad key', key_id='the id')
                    return requests.post(self.base_url + endpoint, json=body, auth=auth)

    @staticmethod
    def _create_mock_notify_queue():
        sqs = boto3.resource('sqs', region_name='us-east-1')
        sqs.create_queue(QueueName=config.notify_queue_name)


def get(v):
    return one(v) if isinstance(v, list) else v


if __name__ == "__main__":
    unittest.main()
