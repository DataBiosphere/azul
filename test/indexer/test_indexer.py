from bisect import (
    insort,
)
from collections import (
    Counter,
    defaultdict,
)
from collections.abc import (
    Mapping,
)
from concurrent.futures import (
    ThreadPoolExecutor,
)
import copy
from itertools import (
    chain,
)
import re
from typing import (
    Iterable,
    Optional,
    cast,
)
import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

import attr
import elasticsearch
from elasticsearch import (
    Elasticsearch,
)
from more_itertools import (
    one,
)

from azul import (
    RequirementError,
    cached_property,
    config,
)
from azul.collections import (
    NestedDict,
)
from azul.indexer import (
    Bundle,
    BundlePartition,
)
from azul.indexer.document import (
    CataloguedEntityReference,
    Contribution,
    ContributionCoordinates,
    DocumentType,
    EntityReference,
    EntityType,
    IndexName,
    null_bool,
    null_int,
    null_str,
)
from azul.indexer.index_service import (
    IndexExistsAndDiffersException,
    IndexService,
    IndexWriter,
    log as index_service_log,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins import (
    MetadataPlugin,
)
from azul.plugins.metadata.hca import (
    CellSuspensionTransformer,
)
from azul.plugins.repository.dss import (
    DSSBundle,
)
from azul.threads import (
    Latch,
)
from azul.types import (
    JSON,
    JSONs,
)
from azul_test_case import (
    AzulUnitTestCase,
    DCP1TestCase,
)
from indexer import (
    IndexerTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging(log)


class HCAIndexerTestCase(DCP1TestCase, IndexerTestCase):

    @cached_property
    def old_bundle(self):
        return self.bundle_fqid(uuid='aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                                version='2018-11-02T11:33:44.698028Z')

    @cached_property
    def new_bundle(self):
        return self.bundle_fqid(uuid='aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                                version='2018-11-04T11:33:44.698028Z')

    @cached_property
    def metadata_plugin(self) -> MetadataPlugin:
        return MetadataPlugin.load(self.catalog).create()

    def _assert_hit_counts(self,
                           hits: list[JSON],
                           num_contribs: int,
                           *,
                           num_aggs: Optional[int] = None,
                           ):
        """
        Verify that the indices contain the correct number of hits of each
        document type
        :param hits: Hits from ElasticSearch
        :param num_contribs: Expected number of contributions
        :param num_aggs: Expected number of aggregates. If unspecified, `num_contribs`
                         becomes the default.
        """
        if num_aggs is None:
            # By default, assume 1 aggregate per contribution.
            num_aggs = num_contribs
        expected = {
            DocumentType.contribution: num_contribs,
            DocumentType.aggregate: num_aggs,
        }
        actual = dict.fromkeys(expected.keys(), 0)
        actual |= Counter(self._parse_index_name(h)[1] for h in hits)
        self.assertDictEqual(expected, actual)


class TestHCAIndexer(HCAIndexerTestCase):

    def test_indexing(self):
        """
        Index a bundle and assert the index contents verbatim
        """
        self.maxDiff = None
        expected_hits = self._load_canned_result(self.old_bundle)
        for max_partition_size in [BundlePartition.max_partition_size, 1]:
            for page_size in (config.contribution_page_size, 1):
                with self.subTest(page_size=page_size, max_partition_size=max_partition_size):
                    with patch.object(BundlePartition, 'max_partition_size', new=max_partition_size):
                        with patch.object(type(config), 'contribution_page_size', new=page_size):
                            self.index_service.create_indices(self.catalog)
                            try:
                                self._index_canned_bundle(self.old_bundle)
                                hits = self._get_all_hits()
                                self.assertElasticEqual(expected_hits, hits)
                            finally:
                                self.index_service.delete_indices(self.catalog)

    def test_deletion(self):
        """
        Delete a bundle and check that the index contains the appropriate flags
        """
        # Ensure that we have a bundle whose documents are written individually
        # and another one that's written in bulk.
        bundle_sizes = {
            self.new_bundle: 6,
            self.bundle_fqid(uuid='2a87dc5c-0c3c-4d91-a348-5d784ab48b92',
                             version='2018-03-29T10:39:45.437487Z'): 258
        }
        self.assertTrue(min(bundle_sizes.values()) < IndexWriter.bulk_threshold < max(bundle_sizes.values()))

        field_types = self.index_service.catalogued_field_types()
        aggregate_cls = self.metadata_plugin.aggregate_class()
        for bundle_fqid, size in bundle_sizes.items():
            with self.subTest(size=size):
                bundle = self._load_canned_bundle(bundle_fqid)
                bundle = DSSBundle(fqid=bundle_fqid,
                                   manifest=bundle.manifest,
                                   metadata_files=bundle.metadata_files)
                self.index_service.create_indices(self.catalog)
                try:
                    self._index_bundle(bundle)
                    hits = self._get_all_hits()
                    self._assert_hit_counts(hits, size)
                    for hit in hits:
                        entity_type, doc_type = self._parse_index_name(hit)
                        if doc_type is DocumentType.aggregate:
                            doc = aggregate_cls.from_index(field_types, hit)
                            self.assertNotEqual(doc.contents, {})
                        elif doc_type is DocumentType.contribution:
                            doc = Contribution.from_index(field_types, hit)
                            self.assertEqual(bundle_fqid.upcast(), doc.coordinates.bundle)
                            self.assertFalse(doc.coordinates.deleted)
                        else:
                            assert False, doc_type

                    self._index_bundle(bundle, delete=True)

                    hits = self._get_all_hits()
                    # Twice the number of contributions because deletions create
                    # new documents instead of removing them. The aggregates are
                    # removed when the deletions cause their contents to become
                    # emtpy.
                    self._assert_hit_counts(hits, num_contribs=size * 2, num_aggs=0)
                    docs_by_entity: dict[EntityReference, list[Contribution]] = defaultdict(list)
                    for hit in hits:
                        entity_type, doc_type = self._parse_index_name(hit)
                        # Since there is only one bundle and it was deleted,
                        # nothing should be aggregated
                        self.assertEqual(doc_type, DocumentType.contribution)
                        doc = Contribution.from_index(field_types, hit)
                        docs_by_entity[doc.entity].append(doc)
                        self.assertEqual(bundle_fqid.upcast(), doc.coordinates.bundle)

                    for pair in docs_by_entity.values():
                        self.assertEqual(list(sorted(doc.coordinates.deleted for doc in pair)), [False, True])
                finally:
                    self.index_service.delete_indices(self.catalog)


class TestHCAIndexerWithIndexesSetUp(HCAIndexerTestCase):
    """
    Conveniently sets up (tears down) indices before (after) each test.
    """

    def setUp(self) -> None:
        super().setUp()
        self.index_service.create_indices(self.catalog)

    def tearDown(self):
        self.index_service.delete_indices(self.catalog)
        super().tearDown()

    translated_str_null = null_str.to_index(None)
    translated_int_null = null_int.to_index(None)
    translated_bool_null = null_bool.to_index(None)
    translated_bool_true = null_bool.to_index(True)
    translated_bool_false = null_bool.to_index(False)

    def _filter_hits(self,
                     hits: JSONs,
                     doc_type: Optional[DocumentType] = None,
                     entity_type: Optional[EntityType] = None,
                     ) -> Iterable[JSON]:
        for hit in hits:
            hit_entity_type, hit_doc_type = self._parse_index_name(hit)
            if entity_type in (None, hit_entity_type) and doc_type in (None, hit_doc_type):
                yield hit

    def test_duplicate_notification(self):
        # Contribute the bundle once
        bundle = self._load_canned_bundle(self.new_bundle)
        tallies_1 = self._write_contributions(bundle)

        # There should be one contribution per entity
        num_contributions = 6
        self.assertEqual([1] * num_contributions, list(tallies_1.values()))

        # Delete one of the contributions such that when contribute again, one
        # of the writes is NOT an overwrite. Since we pretend not having written
        # that contribution, we also need to remove its tally.
        tallies_1 = dict(tallies_1)
        entity, tally = tallies_1.popitem()
        coordinates = ContributionCoordinates(entity=entity,
                                              bundle=bundle.fqid.upcast(),
                                              deleted=False).with_catalog(self.catalog)
        self.es_client.delete(index=coordinates.index_name,
                              id=coordinates.document_id)

        # Contribute the bundle again, simulating a duplicate notification or
        # a retry of the original notification.
        with self.assertLogs(logger=index_service_log, level='WARNING') as logs:
            tallies_2 = self._write_contributions(bundle)

        # All entities except the one whose contribution we deleted should have
        # been overwrites and therefore have a tally of 0.
        expected_tallies_2 = {k: 0 for k in tallies_1.keys()}
        expected_tallies_2[entity] = 1
        self.assertEqual(expected_tallies_2, tallies_2)

        # All writes were logged as overwrites, except one.
        self.assertEqual(num_contributions - 1, len(logs.output))
        message_re = re.compile(r'^WARNING:azul\.indexer\.index_service:'
                                r'Document .* exists. '
                                r'Retrying with overwrite\.$')
        for message in logs.output:
            self.assertRegex(message, message_re)

        # Merge the tallies
        tallies = Counter()
        for k, v in chain(tallies_1.items(), tallies_2.items()):
            entity = CataloguedEntityReference.for_entity(entity=k, catalog=self.catalog)
            tallies[entity] += v
        self.assertEqual([1] * num_contributions, list(tallies.values()))

        # Aggregation should still work despite contributing same bundle twice
        self.index_service.aggregate(tallies)
        self._assert_new_bundle()

    def test_zero_tallies(self):
        """
        Since duplicate notifications are subtracted back out of tally counts,
        it's possible to receive a tally with zero notifications. Test that a
        tally with count 0 still triggers aggregation.
        """
        bundle = self._load_canned_bundle(self.new_bundle)
        tallies = dict(self._write_contributions(bundle))
        for tally in tallies:
            tallies[tally] = 0
        # Aggregating should not be a non-op even though tallies are all zero
        with self.assertLogs(elasticsearch.client.logger, level='INFO') as logs:
            self.index_service.aggregate(tallies)
        doc_ids = {
            '70d1af4a-82c8-478a-8960-e9028b3616ca',
            'a21dc760-a500-4236-bcff-da34a0e873d2',
            'e8642221-4c2c-4fd7-b926-a68bce363c88',
            '0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb',
            'aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
        }
        for doc_id in doc_ids:
            message_re = re.compile(fr'INFO:elasticsearch:'
                                    fr'Got 201 response after [^ ]+ from PUT to '
                                    fr'.*_aggregate/_doc/{doc_id}.*')
            self.assertTrue(any(message_re.fullmatch(message) for message in logs.output))

    def test_deletion_before_addition(self):
        self._index_canned_bundle(self.new_bundle, delete=True)
        self._assert_index_counts(just_deletion=True)
        self._index_canned_bundle(self.new_bundle)
        self._assert_index_counts(just_deletion=False)

    def _assert_index_counts(self, *, just_deletion: bool):
        # Five entities (two files, one project, one sample and one bundle)
        num_expected_addition_contributions = 0 if just_deletion else 6
        num_expected_deletion_contributions = 6
        num_expected_aggregates = 0
        hits = self._get_all_hits()
        actual_addition_contributions = [h for h in hits if not h['_source']['bundle_deleted']]
        actual_deletion_contributions = [h for h in hits if h['_source']['bundle_deleted']]

        self.assertEqual(len(actual_addition_contributions), num_expected_addition_contributions)
        self.assertEqual(len(actual_deletion_contributions), num_expected_deletion_contributions)
        self._assert_hit_counts(hits,
                                num_contribs=num_expected_addition_contributions + num_expected_deletion_contributions,
                                num_aggs=num_expected_aggregates)

    def test_bundle_delete_downgrade(self):
        """
        Delete an updated version of a bundle, and ensure that the index reverts
        to the previous bundle.
        """
        self._index_canned_bundle(self.old_bundle)
        old_hits_by_id = self._assert_old_bundle()
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=6, old_hits_by_id=old_hits_by_id)
        self._index_canned_bundle(self.new_bundle, delete=True)
        self._assert_old_bundle(num_expected_new_deleted_contributions=6)

    def test_multi_entity_contributing_bundles(self):
        """
        Delete a bundle which shares entities with another bundle and ensure
        shared entities are not deleted. Only entity associated with deleted
        bundle should be marked as deleted.
        """
        bundle_fqid = self.bundle_fqid(uuid='8543d32f-4c01-48d5-a79f-1c5439659da3',
                                       version='2018-03-29T14:38:28.884167Z')
        bundle = self._load_canned_bundle(bundle_fqid)
        self._index_bundle(bundle)
        patched_fqid = self.bundle_fqid(uuid='9654e431-4c01-48d5-a79f-1c5439659da3',
                                        version='2018-03-29T15:38:28.884167Z')
        patched_bundle = attr.evolve(bundle, fqid=patched_fqid)
        old_file_uuid = self._patch_bundle(patched_bundle)
        self._index_bundle(patched_bundle)

        hits_before = self._get_all_hits()
        num_docs_by_index_before = self._num_docs_by_index(hits_before)

        self._index_bundle(bundle, delete=True)

        hits_after = self._get_all_hits()
        num_docs_by_index_after = self._num_docs_by_index(hits_after)

        for entity_type, doc_type in num_docs_by_index_after.keys():
            # Both bundles reference two files. They both share one file and
            # exclusively own another one. Deleting one of the bundles removes
            # the file owned exclusively by that bundle, as well as the bundle itself.
            if doc_type is DocumentType.aggregate:
                difference = 1 if entity_type in ('files', 'bundles') else 0
                self.assertEqual(num_docs_by_index_after[entity_type, doc_type],
                                 num_docs_by_index_before[entity_type, doc_type] - difference)
            elif doc_type is DocumentType.contribution:
                if entity_type in ('bundles', 'samples', 'projects', 'cell_suspensions'):
                    # Count one extra deletion contribution
                    self.assertEqual(num_docs_by_index_after[entity_type, doc_type],
                                     num_docs_by_index_before[entity_type, doc_type] + 1)
                else:
                    # Count two extra deletion contributions for the two files
                    self.assertEqual(entity_type, 'files')
                    self.assertEqual(num_docs_by_index_after[entity_type, doc_type],
                                     num_docs_by_index_before[entity_type, doc_type] + 2)
            else:
                assert False, doc_type

        entity = CataloguedEntityReference(catalog=self.catalog,
                                           entity_id=old_file_uuid,
                                           entity_type='files')
        deletion = ContributionCoordinates(entity=entity,
                                           bundle=bundle_fqid.upcast(),
                                           deleted=True)
        index_name, document_id = deletion.index_name, deletion.document_id
        hits = [
            hit['_source']
            for hit in hits_after
            if hit['_id'] == document_id and hit['_index'] == index_name
        ]
        self.assertTrue(one(hits)['bundle_deleted'])

    def _patch_bundle(self, bundle: Bundle) -> str:
        new_file_uuid = str(uuid4())
        bundle.manifest = copy.deepcopy(bundle.manifest)
        file_name = '21935_7#154_2.fastq.gz'
        for file in bundle.manifest:
            if file['name'] == file_name:
                old_file_uuid = file['uuid']
                file['uuid'] = new_file_uuid
                break
        else:
            assert False, f'Unable to find file name {file_name}'

        def _walkthrough(v):
            if isinstance(v, dict):
                return dict((k, _walkthrough(v)) for k, v in v.items())
            elif isinstance(v, list):
                return list(_walkthrough(i) for i in v)
            elif isinstance(v, (str, int, bool, float)):
                return new_file_uuid if v == old_file_uuid else v
            else:
                assert False, f'Cannot handle values of type {type(v)}'

        bundle.metadata_files = _walkthrough(bundle.metadata_files)
        return old_file_uuid

    def _num_docs_by_index(self, hits) -> Mapping[tuple[str, DocumentType], int]:
        return Counter(map(self._parse_index_name, hits))

    def test_indexed_matrices(self):
        """
        Test indexing various types of DCP- and contributor-generated matrix
        bundles, including analysis and supplementary file CGMs.
        """
        bundles = [
            # A hacky CGM subgraph (project 8185730f)
            # 8 supplementary file CGMs each with a 'submitter_id'
            self.bundle_fqid(uuid='4b03c1ce-9df1-5cd5-a8e4-48a2fe095081',
                             version='2021-02-10T16:56:40.419579Z'),
            # A hacky DCP/1 matrix service subgraph (project 8185730f)
            # 3 supplementary file matrices each with a 'submitter_id'
            self.bundle_fqid(uuid='8338b891-f3fa-5e7b-885f-e4ee5689ee15',
                             version='2020-12-03T10:39:17.144517Z'),
            # A top-level DCP/2 analysis subgraph (project 8185730f)
            # 1 analysis file matrix with a 'submitter_id'
            self.bundle_fqid(uuid='00f48893-5e9d-52cd-b32d-af88edccabfa',
                             version='2020-02-03T10:30:00.000000Z'),
            # An organic CGM subgraph (project bd400331)
            # 2 analysis file CGMs each with a 'file_source'
            self.bundle_fqid(uuid='04836733-0449-4e57-be2e-6f3b8fbdfb12',
                             version='2021-05-10T23:25:12.412000Z')
        ]
        for bundle in bundles:
            self._index_canned_bundle(bundle)
        self.maxDiff = None
        hits = self._get_all_hits()

        expected_matrices = {
            '8185730f-4113-40d3-9cc3-929271784c2b': {
                'matrices': [
                    {
                        'file': [
                            {
                                # 3 supplementary files. The 'strata' value was provided in
                                # the supplementary_file metadata. Source from submitter_id.
                                'uuid': '538faa28-3235-5e4b-a998-5672e2d964e8',
                                'version': '2020-12-03T10:39:17.144517Z',
                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.csv.zip',
                                'size': 76742835,
                                'size_': 76742835,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '97a7a932',
                                'sha256': 'edb8e0139fece9702d89ae5fe7f761c41c291ef6a71129c6420857e025228a24',
                                'drs_uri': f'drs://{self._drs_domain_name}/538faa28-3235-5e4b-a998-5672e2d964e8'
                                           '?version=2020-12-03T10%3A39%3A17.144517Z',
                                'document_id': '538faa28-3235-5e4b-a998-5672e2d964e8',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'DCP/1 Matrix Service',
                                'strata': 'genusSpecies=Homo sapiens;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '6c142250-567c-5b63-bd4f-0d78499863f8',
                                'version': '2020-12-03T10:39:17.144517Z',
                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.mtx.zip',
                                'size': 124022765,
                                'size_': 124022765,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '7de6e00e',
                                'sha256': 'cb1467f4d23a2429b4928943b51652b32edb949099250d28cf400d13074f5440',
                                'drs_uri': f'drs://{self._drs_domain_name}/6c142250-567c-5b63-bd4f-0d78499863f8'
                                           '?version=2020-12-03T10%3A39%3A17.144517Z',
                                'document_id': '6c142250-567c-5b63-bd4f-0d78499863f8',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'DCP/1 Matrix Service',
                                'strata': 'genusSpecies=Homo sapiens;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                'version': '2020-12-03T10:39:17.144517Z',
                                'name': '8185730f-4113-40d3-9cc3-929271784c2b.homo_sapiens.loom',
                                'size': 154980798,
                                'size_': 154980798,
                                'content-type': 'application/vnd.loom; dcp-type=data',
                                'indexed': 0,
                                'crc32c': 'd675b7ea',
                                'sha256': '724b2c0ddf33c662b362179bc6ca90cd866b99b340d061463c35d27cfd5a23c5',
                                'drs_uri': f'drs://{self._drs_domain_name}/8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18'
                                           '?version=2020-12-03T10%3A39%3A17.144517Z',
                                'document_id': '8d2ba1c1-bc9f-5c2a-a74d-fe5e09bdfb18',
                                'file_type': 'supplementary_file',
                                'file_format': 'loom',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'DCP/1 Matrix Service',
                                'strata': 'genusSpecies=Homo sapiens;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                # An analysis file. The 'strata' value was gathered by walking
                                # the project graph from the file. Source from submitter_id.
                                'uuid': 'bd98f428-881e-501a-ac16-24f27a68ce2f',
                                'version': '2021-02-11T23:11:45.000000Z',
                                'name': 'wong-retina-human-eye-10XV2.loom',
                                'size': 255471211,
                                'size_': 255471211,
                                'content-type': 'application/vnd.loom; dcp-type=data',
                                'indexed': 0,
                                'crc32c': 'd1b06ce5',
                                'sha256': '6a6483c2e78da77017e912a4d350f141bda1ec7b269f20ca718b55145ee5c83c',
                                'drs_uri': f'drs://{self._drs_domain_name}/bd98f428-881e-501a-ac16-24f27a68ce2f'
                                           '?version=2021-02-11T23%3A11%3A45.000000Z',
                                'document_id': 'fec17064-9014-50b0-9e1a-dfaef2fbb4fc',
                                'file_type': 'analysis_file',
                                'file_format': 'loom',
                                'content_description': ['Count Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'matrix_cell_count': 9223372036854774784,
                                'matrix_cell_count_': None,
                                'file_source': 'DCP/2 Analysis',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=human adult stage;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            }
                        ]
                    }
                ],
                'contributed_analyses': [
                    {
                        'file': [
                            {
                                # 8 supplementary files. The 'strata' value was provided in
                                # the supplementary_file metadata. Source from submitter_id.
                                'uuid': '0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.5.zip',
                                'size': 15535233,
                                'size_': 15535233,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '5bf9776f',
                                'sha256': '053074e25a96a463c081e38bcd02662ba1536dd0cb71411bd111b8a2086a03e1',
                                'drs_uri': f'drs://{self._drs_domain_name}/0c5ab869-da2d-5c11-b4ae-f978a052899f'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': '0c5ab869-da2d-5c11-b4ae-f978a052899f',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '5b465aad-0981-5152-b468-e615e20f5884',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.7.zip',
                                'size': 7570475,
                                'size_': 7570475,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': 'c8c42fc3',
                                'sha256': 'af3ea779ca01a2ba65f9415720a44648ef28a6ed73c9ec30e54ed4ba9895f590',
                                'drs_uri': f'drs://{self._drs_domain_name}/5b465aad-0981-5152-b468-e615e20f5884'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': '5b465aad-0981-5152-b468-e615e20f5884',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.4.zip',
                                'size': 38722784,
                                'size_': 38722784,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '82ee1217',
                                'sha256': 'f1458913c223553d09966ff94f0ed3d87e7cdfce21904f32943d70f691d8f7a0',
                                'drs_uri': f'drs://{self._drs_domain_name}/68bda896-3b3e-5f2a-9212-f4030a0f37e2'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': '68bda896-3b3e-5f2a-9212-f4030a0f37e2',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.2.zip',
                                'size': 118250749,
                                'size_': 118250749,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '15b69eed',
                                'sha256': 'cb7beb6f4e8c684e41d25aa4dc1294dcb1e070e87f9ed852463bf651d511a36b',
                                'drs_uri': f'drs://{self._drs_domain_name}/733318e0-19c2-51e8-9ad6-d94ad562dd46'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': '733318e0-19c2-51e8-9ad6-d94ad562dd46',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': '87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.1.zip',
                                'size': 69813802,
                                'size_': 69813802,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '4f3d0c47',
                                'sha256': '331bd925c08539194eb06e197a1238e1306c3b7876b6fe13548d03824cc4b68b',
                                'drs_uri': f'drs://{self._drs_domain_name}/87f31102-ebbc-5875-abdf-4fa5cea48e8d'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': '87f31102-ebbc-5875-abdf-4fa5cea48e8d',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': 'b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'WongRetinaCelltype.csv',
                                'size': 2300969,
                                'size_': 2300969,
                                'content-type': 'application/octet-stream; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '42fcdb28',
                                'sha256': '4f515b8fbbec8bfbc72c8c0d656897ee37bfa30bab6eb50fdc641924227be674',
                                'drs_uri': f'drs://{self._drs_domain_name}/b905c8be-2e2d-592c-8481-3eb7a87c6484'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': 'b905c8be-2e2d-592c-8481-3eb7a87c6484',
                                'file_type': 'supplementary_file',
                                'file_format': 'csv',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'HCA Release',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': 'c59e2de5-01fe-56eb-be56-679ed14161bf',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.3.zip',
                                'size': 187835236,
                                'size_': 187835236,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '0209e859',
                                'sha256': '6372732e9fe9b8d58c8be8df88ea439d5c68ee9bb02e3d472c94633fadf782a1',
                                'drs_uri': f'drs://{self._drs_domain_name}/c59e2de5-01fe-56eb-be56-679ed14161bf'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': 'c59e2de5-01fe-56eb-be56-679ed14161bf',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            },
                            {
                                'uuid': 'cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                'version': '2021-02-10T16:56:40.419579Z',
                                'name': 'E-MTAB-7316.processed.6.zip',
                                'size': 17985905,
                                'size_': 17985905,
                                'content-type': 'application/zip; dcp-type=data',
                                'indexed': 0,
                                'crc32c': 'a21bdb72',
                                'sha256': '1c57cba1ade259fc9ec56b914b507507d75ccbf6ddeebf03ba00c922c30e0c6e',
                                'drs_uri': f'drs://{self._drs_domain_name}/cade4593-bfba-56ed-80ab-080d0de7d5a4'
                                           '?version=2021-02-10T16%3A56%3A40.419579Z',
                                'document_id': 'cade4593-bfba-56ed-80ab-080d0de7d5a4',
                                'file_type': 'supplementary_file',
                                'file_format': 'zip',
                                'content_description': ['Matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'file_source': 'ArrayExpress',
                                'strata': 'genusSpecies=Homo sapiens;developmentStage=adult;'
                                          'organ=eye;libraryConstructionApproach=10X v2 sequencing',
                            }
                        ]
                    }
                ]
            },
            'bd400331-54b9-4fcc-bff6-6bb8b079ee1f': {
                'matrices': [],
                'contributed_analyses': [
                    {
                        'file': [
                            # One analysis file. The 'strata' value was gathered by walking
                            # the project graph from the file. Source from file_source.
                            # File's content_description does not contain 'matrix'
                            {
                                'uuid': '8a1cead0-b0e8-4da9-a523-7adce5c69aa7',
                                'version': '2021-05-10T23:25:11.795000Z',
                                'name': 'cellinfo_updated.Rds',
                                'size': 406333,
                                'size_': 406333,
                                'content-type': 'application/gzip; dcp-type=data; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '64dddda9',
                                'sha256': '86767bd1ffcae8da5be495ce7e11a6ff0cffe05199af60c10d8124adc22ec8d3',
                                'drs_uri': f'drs://{self._drs_domain_name}/8a1cead0-b0e8-4da9-a523-7adce5c69aa7'
                                           '?version=2021-05-10T23%3A25%3A11.795000Z',
                                'document_id': '581ee2ac-fd9a-4563-b8eb-d9cfb96f65ca',
                                'file_type': 'analysis_file',
                                'file_format': 'Rds',
                                'content_description': ['cell level metadata'],
                                'is_intermediate': 9223372036854774784,
                                '_type': 'file',
                                'related_files': [],
                                'matrix_cell_count': 54140,
                                'matrix_cell_count_': 54140,
                                'file_source': 'Contributor',
                                'strata': "genusSpecies=Homo sapiens;"
                                          "developmentStage=adolescent stage,child stage,fetal stage,human adult stage;"
                                          "organ=heart;libraryConstructionApproach=10x 3' v3 sequencing",
                            },
                            # Two analysis files. The 'strata' value was gathered by walking
                            # the project graph from the file. Source from file_source.
                            # File's content_description does contain 'matrix'
                            {
                                'uuid': 'a225da4c-a0db-4411-9c1b-670c69ff3c82',
                                'version': '2021-05-10T23:25:11.836000Z',
                                'name': 'heartFYA.Rds',
                                'size': 2197439516,
                                'size_': 2197439516,
                                'content-type': 'application/gzip; dcp-type=data; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '795a29b2',
                                'sha256': '3429539fdc0ef3a8c94a8aa46a65fe8f1ad92da3584b56a7727119314463f16c',
                                'drs_uri': f'drs://{self._drs_domain_name}/a225da4c-a0db-4411-9c1b-670c69ff3c82'
                                           '?version=2021-05-10T23%3A25%3A11.836000Z',
                                'document_id': 'd3b3abc2-0da6-4163-acb8-251fe079284c',
                                'file_type': 'analysis_file',
                                'file_format': 'Rds',
                                'content_description': ['Gene expression matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'matrix_cell_count': 54140,
                                'matrix_cell_count_': 54140,
                                'file_source': 'Contributor',
                                'strata': 'genusSpecies=Homo sapiens;'
                                          'developmentStage=adolescent stage,child stage,fetal stage,human adult stage;'
                                          'organ=heart;libraryConstructionApproach=10x 3\' v3 sequencing',
                            },
                            {
                                'uuid': 'c255e795-7297-4658-8b5b-044d932efbe9',
                                'version': '2021-05-10T23:25:11.821000Z',
                                'name': 'heart-counts.Rds',
                                'size': 440041264,
                                'size_': 440041264,
                                'content-type': 'application/gzip; dcp-type=data; dcp-type=data',
                                'indexed': 0,
                                'crc32c': '3df9657f',
                                'sha256': 'b02fa88cff40f8e0fb9b3cd70c6a4d8348b55b7c80ef3ed6afbb548bd3d19db9',
                                'drs_uri': f'drs://{self._drs_domain_name}/c255e795-7297-4658-8b5b-044d932efbe9'
                                           '?version=2021-05-10T23%3A25%3A11.821000Z',
                                'document_id': '31e6cb06-0062-4096-84f5-c2d1c2621a82',
                                'file_type': 'analysis_file',
                                'file_format': 'Rds',
                                'content_description': ['Count matrix'],
                                'is_intermediate': 0,
                                '_type': 'file',
                                'related_files': [],
                                'matrix_cell_count': 54140,
                                'matrix_cell_count_': 54140,
                                'file_source': 'Contributor',
                                'strata': 'genusSpecies=Homo sapiens;'
                                          'developmentStage=adolescent stage,child stage,fetal stage,human adult stage;'
                                          'organ=heart;libraryConstructionApproach=10x 3\' v3 sequencing',
                            }
                        ]
                    }
                ]
            }
        }
        matrices = {}
        for hit in self._filter_hits(hits, DocumentType.aggregate, 'projects'):
            project_id = hit['_source']['entity_id']
            assert project_id not in matrices, project_id
            matrices[project_id] = {
                k: hit['_source']['contents'][k]
                for k in ('matrices', 'contributed_analyses')
            }
        self.assertEqual(expected_matrices, matrices)

    def test_organic_matrix_bundle(self):
        # A bundle containing an organically described CGM with a 'matrix_cell_count'
        bundle = self.bundle_fqid(uuid='04836733-0449-4e57-be2e-6f3b8fbdfb12',
                                  version='2021-05-10T23:25:12.412000Z')
        self._index_canned_bundle(bundle)
        hits = self._get_all_hits()
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            contents = hit['_source']['contents']
            for file in contents['files']:
                if file['file_format'] == 'Rds':
                    expected_source = 'Contributor'
                    expected_cell_count = 54140
                else:
                    expected_source = self.translated_str_null
                    expected_cell_count = self.translated_bool_null
                if (
                    doc_type is DocumentType.aggregate and
                    entity_type not in ('bundles', 'files')
                ):
                    expected_source = [expected_source]
                self.assertEqual(expected_source, file['file_source'])
                if 'matrix_cell_count' in file:
                    self.assertEqual(expected_cell_count, file['matrix_cell_count'])

    def test_sequence_files_with_file_source(self):
        """
        Index a bundle that contains both analysis and sequence files that have
        `file_source` values matching one of the existing `Submitter` enum.
        Verify only the expected analysis files are indexed as matrices.
        """
        bundle_fqid = self.bundle_fqid(uuid='02e69c25-71e2-48ca-a87b-e256938c6a98',
                                       version='2021-06-28T14:21:18.700000Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        files = set()
        contributed_analyses = set()
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            contents = hit['_source']['contents']
            if entity_type == 'files' and doc_type in (DocumentType.aggregate, DocumentType.contribution):
                file = one(contents['files'])
                files.add(
                    (
                        file['name'],
                        file['file_source'],
                        null_bool.from_index(file['is_intermediate'])
                    )
                )
            elif entity_type == 'projects' and doc_type is DocumentType.aggregate:
                self.assertEqual([], contents['matrices'])
                for file in one(contents['contributed_analyses'])['file']:
                    contributed_analyses.add(
                        (
                            file['name'],
                            file['file_source'],
                            file['matrix_cell_count']
                        )
                    )
        expected_files = {
            # Analysis files (organic CGM, without 'matrix' content_description)
            ('experiment2_mouse_pbs_scp_X_diffmap_pca_coords.txt', 'SCP', None),
            ('experiment2_mouse_pbs_scp_X_tsne_coords.txt', 'SCP', None),
            ('experiment2_mouse_pbs_scp_barcodes.tsv', 'SCP', None),
            ('experiment2_mouse_pbs_scp_genes.tsv', 'SCP', None),
            # Analysis files (organic CGM, with 'matrix' content_description)
            ('experiment2_mouse_pbs_scp_matrix.mtx', 'SCP', False),
            ('experiment2_mouse_pbs_scp_metadata.txt', 'SCP', False),
            # Sequence files
            ('mouse_cortex_I1.fastq', 'GEO', None),
            ('mouse_cortex_R1.fastq', 'GEO', None),
            ('mouse_cortex_R2.fastq', 'GEO', None)
        }
        self.assertEqual(expected_files, files)
        expected_contributed_analyses = {
            ('experiment2_mouse_pbs_scp_X_diffmap_pca_coords.txt', 'SCP', 3402),
            ('experiment2_mouse_pbs_scp_X_tsne_coords.txt', 'SCP', 3402),
            ('experiment2_mouse_pbs_scp_barcodes.tsv', 'SCP', 3402),
            ('experiment2_mouse_pbs_scp_genes.tsv', 'SCP', 9223372036854774784),
            ('experiment2_mouse_pbs_scp_metadata.txt', 'SCP', 3402),
            ('experiment2_mouse_pbs_scp_matrix.mtx', 'SCP', 3402),
        }
        self.assertEqual(expected_contributed_analyses, contributed_analyses)

    def test_derived_files(self):
        """
        Index an analysis bundle, which, unlike a primary bundle, has data files
        derived from other data files, and assert that the resulting `files`
        index document contains exactly one file entry.
        """
        analysis_bundle = self.bundle_fqid(uuid='d5e01f9d-615f-4153-8a56-f2317d7d9ce8',
                                           version='2018-09-06T18:57:59.326912Z')
        self._index_canned_bundle(analysis_bundle)
        hits = self._get_all_hits()
        num_files = 33
        num_expected = dict(files=num_files, samples=1, cell_suspensions=1, projects=1, bundles=1)
        self._assert_hit_counts(hits, sum(num_expected.values()))
        num_contribs, num_aggregates = Counter(), Counter()
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            source = hit['_source']
            contents = source['contents']
            if doc_type is DocumentType.aggregate:
                num_aggregates[entity_type] += 1
                bundle = one(source['bundles'])
                actual_fqid = self.bundle_fqid(uuid=bundle['uuid'],
                                               version=bundle['version'])
                self.assertEqual(analysis_bundle, actual_fqid)
                if entity_type == 'files':
                    self.assertEqual(1, len(contents['files']))
                elif entity_type == 'bundles':
                    self.assertEqual(num_files, len(contents['files']))
                else:
                    self.assertEqual(num_files, sum(file['count'] for file in contents['files']))
            elif doc_type is DocumentType.contribution:
                num_contribs[entity_type] += 1
                actual_fqid = self.bundle_fqid(uuid=source['bundle_uuid'],
                                               version=source['bundle_version'])
                self.assertEqual(analysis_bundle, actual_fqid)
                self.assertEqual(1 if entity_type == 'files' else num_files, len(contents['files']))
            else:
                assert False, doc_type
            self.assertEqual(1, len(contents['specimens']))
            self.assertEqual(1, len(contents['projects']))
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
        Indexing an old version of a bundle *after* a new version should not
        have an effect on aggregates.
        """
        self._index_canned_bundle(self.new_bundle)
        self._assert_new_bundle(num_expected_old_contributions=0)
        self._index_canned_bundle(self.old_bundle)
        self._assert_old_bundle(num_expected_new_contributions=6, ignore_aggregates=True)
        self._assert_new_bundle(num_expected_old_contributions=6)

    def _assert_old_bundle(self,
                           num_expected_new_contributions: int = 0,
                           num_expected_new_deleted_contributions: int = 0,
                           ignore_aggregates: bool = False
                           ) -> Mapping[tuple[str, DocumentType], JSON]:
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
        # Two files, one project, one cell suspension, one sample, and one bundle
        num_old_contribs = 6
        # Deletions add new contributions to the index instead of removing the old ones,
        # so they're included in the total
        num_new_contribs = num_expected_new_contributions + num_expected_new_deleted_contributions * 2
        self._assert_hit_counts(hits,
                                num_contribs=num_old_contribs + num_new_contribs,
                                num_aggs=num_old_contribs)
        hits_by_id = {}
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            if doc_type is DocumentType.aggregate and ignore_aggregates:
                continue
            source = hit['_source']
            hits_by_id[source['entity_id'], doc_type] = hit
            if doc_type is DocumentType.aggregate:
                version = one(source['bundles'])['version']
            elif doc_type is DocumentType.contribution:
                version = source['bundle_version']
            else:
                assert False, doc_type
            if (
                doc_type is DocumentType.aggregate
                or (doc_type is DocumentType.contribution and self.old_bundle.version == version)
            ):
                contents = source['contents']
                project = one(contents['projects'])
                self.assertEqual('Single cell transcriptome patterns.', get(project['project_title']))
                self.assertEqual('Single of human pancreas', get(project['project_short_name']))
                self.assertIn('John Dear', get(project['laboratory']))
                if doc_type is DocumentType.aggregate and entity_type != 'projects':
                    self.assertIn('Farmers Trucks', project['institutions'])
                elif doc_type is DocumentType.contribution:
                    self.assertIn('Farmers Trucks', [c.get('institution') for c in project['contributors']])
                donor = one(contents['donors'])
                self.assertIn('Australopithecus', donor['genus_species'])
                if doc_type is DocumentType.contribution:
                    self.assertFalse(source['bundle_deleted'])
            elif doc_type is DocumentType.contribution:
                if source['bundle_deleted']:
                    num_actual_new_deleted_contributions += 1
                else:
                    self.assertLess(self.old_bundle.version, version)
                    num_actual_new_contributions += 1
            else:
                assert False, doc_type
        # We count the deleted contributions here too since they should have a
        # corresponding addition contribution
        self.assertEqual(num_expected_new_contributions + num_expected_new_deleted_contributions,
                         num_actual_new_contributions)
        self.assertEqual(num_expected_new_deleted_contributions, num_actual_new_deleted_contributions)
        return hits_by_id

    def _assert_new_bundle(self,
                           num_expected_old_contributions: int = 0,
                           old_hits_by_id: Optional[Mapping[tuple[str, DocumentType], JSON]] = None
                           ) -> None:
        num_actual_old_contributions = 0
        hits = self._get_all_hits()
        # Two files, one project, one cell suspension, one sample, and one bundle.
        num_new_contribs = 6
        self._assert_hit_counts(hits,
                                num_contribs=num_new_contribs + num_expected_old_contributions,
                                num_aggs=num_new_contribs)

        def get_version(source, doc_type):
            if doc_type is DocumentType.aggregate:
                return one(source['bundles'])['version']
            elif doc_type is DocumentType.contribution:
                return source['bundle_version']
            else:
                assert False, doc_type

        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            source = hit['_source']
            version = get_version(source, doc_type)
            contents = source['contents']
            project = one(contents['projects'])

            if doc_type is DocumentType.contribution and version != self.new_bundle.version:
                self.assertLess(version, self.new_bundle.version)
                num_actual_old_contributions += 1
                continue

            if old_hits_by_id is not None:
                old_hit = old_hits_by_id[source['entity_id'], doc_type]
                old_source = old_hit['_source']
                old_version = get_version(old_source, doc_type)
                self.assertLess(old_version, version)
                old_contents = old_source['contents']
                old_project = one(old_contents['projects'])
                self.assertNotEqual(old_project['project_title'], project['project_title'])
                self.assertNotEqual(old_project['project_short_name'], project['project_short_name'])
                self.assertNotEqual(old_project['laboratory'], project['laboratory'])
                if doc_type is DocumentType.aggregate and entity_type != 'projects':
                    self.assertNotEqual(old_project['institutions'], project['institutions'])
                elif doc_type is DocumentType.contribution:
                    self.assertNotEqual(old_project['contributors'], project['contributors'])
                self.assertNotEqual(old_contents['donors'][0]['genus_species'],
                                    contents['donors'][0]['genus_species'])

            self.assertEqual('Single cell transcriptome analysis of human pancreas reveals transcriptional '
                             'signatures of aging and somatic mutation patterns.',
                             get(project['project_title']))
            self.assertEqual('Single cell transcriptome analysis of human pancreas',
                             get(project['project_short_name']))
            self.assertNotIn('Sarah Teichmann', project['laboratory'])
            self.assertIn('Molecular Atlas', project['laboratory'])
            if doc_type is DocumentType.aggregate and entity_type != 'projects':
                self.assertNotIn('Farmers Trucks', project['institutions'])
            elif doc_type is DocumentType.contribution:
                self.assertNotIn('Farmers Trucks', [c.get('institution') for c in project['contributors']])

        self.assertEqual(num_expected_old_contributions, num_actual_old_contributions)

    def test_concurrent_specimen_submissions(self):
        """
        Index two bundles contributing to the same specimen and project, ensure
        that conflicts are detected and handled
        """
        bundles = [
            self.bundle_fqid(uuid='9dec1bd6-ced8-448a-8e45-1fc7846d8995',
                             version='2018-03-29T15:43:19.834528Z'),
            self.bundle_fqid(uuid='56a338fe-7554-4b5d-96a2-7df127a7640b',
                             version='2018-03-29T15:35:07.198365Z')
        ]
        original_mget = Elasticsearch.mget
        latch = Latch(len(bundles))

        def mocked_mget(self, body, _source_includes):
            mget_return = original_mget(self, body=body, _source_includes=_source_includes)
            # all threads wait at the latch after reading to force conflict while writing
            latch.decrement(1)
            return mget_return

        with patch.object(Elasticsearch, 'mget', new=mocked_mget):
            with self.assertLogs(level='WARNING') as cm:
                with ThreadPoolExecutor(max_workers=len(bundles)) as executor:
                    thread_results = executor.map(self._index_canned_bundle, bundles)
                    self.assertIsNotNone(thread_results)
                    self.assertEqual(set(bundles), set(r.fqid for r in thread_results))

                self.assertIsNotNone(cm.records)
                num_hits = sum(1 for log_msg in cm.output
                               if 'There was a conflict with document' in log_msg
                               and (f'_{self.catalog}_samples_aggregate' in log_msg
                                    or f'_{self.catalog}_projects_aggregate' in log_msg))
                # One conflict for the specimen and one for the project
                self.assertEqual(2, num_hits)

        hits = self._get_all_hits()
        file_uuids = set()
        # Two bundles, each with 1 sample, 1 cell suspension, 1 project, 1 bundle and 2 files.
        num_contribs = (1 + 1 + 1 + 1 + 2) * 2
        self._assert_hit_counts(hits,
                                num_contribs=num_contribs,
                                # Both bundles share the same sample and the project, so they get aggregated only once
                                num_aggs=num_contribs - 2)
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            contents = hit['_source']['contents']
            if doc_type is DocumentType.aggregate:
                self.assertEqual(hit['_id'], hit['_source']['entity_id'])
            if entity_type == 'files':
                contents = hit['_source']['contents']
                self.assertEqual(1, len(contents['files']))
                if doc_type is DocumentType.aggregate:
                    file_uuids.add(contents['files'][0]['uuid'])
            elif entity_type in ('samples', 'projects'):
                if doc_type is DocumentType.aggregate:
                    self.assertEqual(2, len(hit['_source']['bundles']))
                    # All four files are fastqs so they are grouped together
                    self.assertEqual(4, one(contents['files'])['count'])
                elif doc_type is DocumentType.contribution:
                    self.assertEqual(2, len(contents['files']))
                else:
                    assert False, doc_type
            elif entity_type == 'bundles':
                if doc_type is DocumentType.aggregate:
                    self.assertEqual(1, len(hit['_source']['bundles']))
                    self.assertEqual(2, len(contents['files']))
                else:
                    self.assertEqual(2, len(contents['files']))
            elif entity_type == 'cell_suspensions':
                if doc_type is DocumentType.aggregate:
                    self.assertEqual(1, len(hit['_source']['bundles']))
                    self.assertEqual(1, len(contents['files']))
                elif doc_type is DocumentType.contribution:
                    self.assertEqual(2, len(contents['files']))
                else:
                    assert False, doc_type
            else:
                self.fail()
        file_document_ids = set()
        self.assertEqual(4, len(file_uuids))
        for bundle_fqid in bundles:
            bundle = self._load_canned_bundle(bundle_fqid)
            files: JSONs = bundle.metadata_files['file.json']['files']
            for file in files:
                file_document_ids.add(file['hca_ingest']['document_id'])
        self.assertEqual(file_document_ids, file_uuids)

    def test_indexing_matrix_related_files(self):
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(bundle_fqid)
        self.maxDiff = None
        hits = self._get_all_hits()
        zarrs = []
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            if entity_type == 'files':
                file = one(hit['_source']['contents']['files'])
                if len(file['related_files']) > 0:
                    self.assertEqual(file['file_format'], 'matrix')
                    zarrs.append(hit)
                elif file['file_format'] == 'matrix':
                    # Matrix of Loom or CSV format possibly
                    self.assertNotIn('.zarr', file['name'])
            elif doc_type is DocumentType.contribution:
                for file in hit['_source']['contents']['files']:
                    self.assertEqual(file['related_files'], [])

        self.assertEqual(len(zarrs), 2)  # One contribution, one aggregate
        for zarr_file in zarrs:
            zarr_file = one(zarr_file['_source']['contents']['files'])
            related_files = zarr_file['related_files']
            self.assertNotIn(zarr_file['name'], {f['name'] for f in related_files})
            self.assertEqual(len(related_files), 12)
            for related_file in related_files:
                self.assertNotIn('!', related_file['name'])

    def test_indexing_with_skipped_matrix_file(self):
        # FIXME: Remove once https://github.com/HumanCellAtlas/metadata-schema/issues/579 is resolved
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(bundle_fqid)
        self.maxDiff = None
        hits = self._get_all_hits()
        file_names, aggregate_file_names = set(), set()
        entities_with_matrix_files = set()
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            files = hit['_source']['contents']['files']
            if doc_type is DocumentType.aggregate:
                if entity_type == 'files':
                    aggregate_file_names.add(one(files)['name'])
                else:
                    for file in files:
                        # FIXME: need for one() is odd, file_format is a group field
                        #        https://github.com/DataBiosphere/azul/issues/612
                        if entity_type == 'bundles':
                            if file['file_format'] == 'matrix':
                                entities_with_matrix_files.add(hit['_source']['entity_id'])
                        else:
                            if file['file_format'] == 'matrix':
                                self.assertEqual(1, file['count'])
                                entities_with_matrix_files.add(hit['_source']['entity_id'])
            elif doc_type is DocumentType.contribution:
                for file in files:
                    file_name = file['name']
                    file_names.add(file_name)
            else:
                assert False, doc_type
        # a project, a specimen, a cell suspension and a bundle
        self.assertEqual(4, len(entities_with_matrix_files))
        self.assertEqual(aggregate_file_names, file_names)
        matrix_file_names = {file_name for file_name in file_names if '.zarr/' in file_name}
        self.assertEqual({'377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zattrs'}, matrix_file_names)

    def test_plate_bundle(self):
        bundle_fqid = self.bundle_fqid(uuid='d0e17014-9a58-4763-9e66-59894efbdaa8',
                                       version='2018-10-03T14:41:37.044509Z')
        self._index_canned_bundle(bundle_fqid)
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        counted_cell_count = 0
        # 384 wells in total, four of them empty, the rest with a single cell
        expected_cell_count = 380
        documents_with_cell_suspension = 0
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            contents = hit['_source']['contents']
            cell_suspensions = contents['cell_suspensions']
            if entity_type == 'files' and contents['files'][0]['file_format'] == 'pdf':
                # The PDF files in that bundle aren't linked to a specimen
                self.assertEqual(0, len(cell_suspensions))
            else:
                if doc_type is DocumentType.aggregate:
                    bundles = hit['_source']['bundles']
                    self.assertEqual(1, len(bundles))
                    self.assertEqual(one(contents['sequencing_protocols'])['paired_end'], [
                        self.translated_bool_true,
                    ])
                elif doc_type is DocumentType.contribution:
                    self.assertEqual({p.get('paired_end') for p in contents['sequencing_protocols']}, {
                        self.translated_bool_true,
                    })
                else:
                    assert False, doc_type
                specimens = contents['specimens']
                for specimen in specimens:
                    self.assertEqual({'bone marrow', 'temporal lobe'}, set(specimen['organ_part']))
                for cell_suspension in cell_suspensions:
                    self.assertEqual({'bone marrow', 'temporal lobe'}, set(cell_suspension['organ_part']))
                    self.assertEqual({'Plasma cells'}, set(cell_suspension['selected_cell_type']))
                if entity_type == 'cell_suspensions':
                    counted_cell_count += one(cell_suspensions)['total_estimated_cells']
                    self.assertEqual(1, len(cell_suspensions))
                else:
                    self.assertEqual(1 if doc_type is DocumentType.aggregate else 384, len(cell_suspensions))
                    self.assertEqual(expected_cell_count, sum(cs['total_estimated_cells'] for cs in cell_suspensions))
                documents_with_cell_suspension += 1
        # Times 2 for original document and aggregate
        self.assertEqual(expected_cell_count * 2, counted_cell_count)
        # Cell suspensions should be mentioned in 1 bundle, 1 project,
        # 1 specimen, 384 cell suspensions, and 2 files (one per fastq).
        # There should be one original and one aggregate document for each of
        # those. (389 * 2 = 778)
        self.assertEqual(778, documents_with_cell_suspension)

    def test_well_bundles(self):
        for bundle_fqid in [
            self.bundle_fqid(uuid='3f8176ff-61a7-4504-a57c-fc70f38d5b13',
                             version='2018-10-24T23:44:31.820615Z'),
            self.bundle_fqid(uuid='e2c3054e-9fba-4d7a-b85b-a2220d16da73',
                             version='2018-10-24T23:43:03.157920Z')
        ]:
            self._index_canned_bundle(bundle_fqid)
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            if doc_type is DocumentType.aggregate:
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # Each bundle contributes a well with one cell. The data files
                # in each bundle are derived from the cell in that well. This is
                # why each data file and bundle should only have a cell count of
                # 1. Both bundles refer to the same specimen and project, so the
                # cell count for those should be 2.
                expected_cells = 1 if entity_type in ('files', 'cell_suspensions', 'bundles') else 2
                self.assertEqual(expected_cells, cell_suspensions[0]['total_estimated_cells'])
                self.assertEqual(one(contents['analysis_protocols'])['workflow'], ['smartseq2_v2.1.0'])
            elif doc_type is DocumentType.contribution:
                self.assertEqual({p['workflow'] for p in contents['analysis_protocols']}, {'smartseq2_v2.1.0'})
            else:
                assert False, doc_type

    def test_pooled_specimens(self):
        """
        Index a bundle that combines 3 specimen_from_organism into 1 cell_suspension
        """
        bundle_fqid = self.bundle_fqid(uuid='b7fc737e-9b7b-4800-8977-fe7c94e131df',
                                       version='2018-09-12T12:11:55.846604Z')
        self._index_canned_bundle(bundle_fqid)
        self.maxDiff = None

        hits = self._get_all_hits()
        self.assertGreater(len(hits), 0)
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            if doc_type is DocumentType.aggregate:
                contents = hit['_source']['contents']
                cell_suspensions = contents['cell_suspensions']
                self.assertEqual(1, len(cell_suspensions))
                # This bundle contains three specimens which are pooled into a
                # single cell suspension with 10000 cells. Until we introduced
                # cell suspensions as an inner entity we used to associate cell
                # counts with specimen which would have inflated the total cell
                # count to 30000 in this case.
                self.assertEqual(10000, cell_suspensions[0]['total_estimated_cells'])
                sample = one(contents['samples'])
                self.assertEqual(sample['organ'], sample['effective_organ'])
                if entity_type == 'samples':
                    self.assertTrue(sample['effective_organ'] in {'Brain 1', 'Brain 2', 'Brain 3'})
                else:
                    self.assertEqual(set(sample['effective_organ']), {'Brain 1', 'Brain 2', 'Brain 3'})

    def test_diseases_field(self):
        """
        Index a bundle with a specimen `diseases` value that differs from the
        donor `diseases` value and assert that both values are represented in
        the indexed document.
        """
        bundle_fqid = self.bundle_fqid(uuid='3db604da-940e-49b1-9bcc-25699a55b295',
                                       version='2018-11-02T18:40:48.983513Z')
        self._index_canned_bundle(bundle_fqid)

        hits = self._get_all_hits()
        for hit in hits:
            source = hit['_source']
            contents = source['contents']
            specimen_diseases = contents['specimens'][0]['disease']
            donor_diseases = contents['donors'][0]['diseases']
            self.assertEqual(1, len(specimen_diseases))
            self.assertEqual('atrophic vulva (specimen_from_organism)', specimen_diseases[0])
            self.assertEqual(1, len(donor_diseases))
            self.assertEqual('atrophic vulva (donor_organism)', donor_diseases[0])

    def test_organoid_priority(self):
        """
        Index a bundle containing an Organoid and assert that the "organ" and
        "organ_part" values saved are the ones from the Organoid and not the
        SpecimenFromOrganism
        """
        bundle_fqid = self.bundle_fqid(uuid='dcccb551-4766-4210-966c-f9ee25d19190',
                                       version='2018-10-18T20:46:55.866661Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        inner_specimens, inner_cell_suspensions = 0, 0
        for hit in hits:

            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            aggregate = doc_type is DocumentType.aggregate

            if entity_type != 'files' or one(contents['files'])['file_format'] != 'pdf':
                inner_cell_suspensions += len(contents['cell_suspensions'])

            for specimen in contents['specimens']:
                inner_specimens += 1
                expect_list = aggregate and entity_type != 'specimens'
                self.assertEqual(['skin of body'] if expect_list else 'skin of body', specimen['organ'])
                self.assertEqual(['skin epidermis'], specimen['organ_part'])

            for organoid in contents['organoids']:
                self.assertEqual(['Brain'] if aggregate else 'Brain', organoid['model_organ'])
                self.assertEqual([self.translated_str_null] if aggregate else self.translated_str_null,
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
        bundle_fqid = self.bundle_fqid(uuid='fa5be5eb-2d64-49f5-8ed8-bd627ac9bc7a',
                                       version='2019-02-14T19:24:38.034764Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            project = one(contents['projects'])
            accessions_by_namespace = {
                'insdc_project': ['SRP000000', 'SRP000001'],
                'geo_series': ['GSE00000'],
                'array_express': ['E-AAAA-00'],
                'insdc_study': ['PRJNA000000']
            }
            entity_type, doc_type = self._parse_index_name(hit)
            if entity_type == 'projects':
                expected_accessions = [
                    {'namespace': namespace, 'accession': accession}
                    for namespace, accessions in accessions_by_namespace.items()
                    for accession in accessions
                ]
                self.assertEqual(expected_accessions, project['accessions'])

    def test_cell_counts(self):
        """
        Verify the cell counts found in project, cell_suspension, and file entities
        """
        # Bundles from the canned staging area, both for project 90bf705c
        # https://github.com/HumanCellAtlas/schema-test-data/
        bundle_fqid = self.bundle_fqid(uuid='4da04038-adab-59a9-b6c4-3a61242cc972',
                                       version='2021-01-01T00:00:00.000000Z')
        self._index_canned_bundle(bundle_fqid)
        bundle_fqid = self.bundle_fqid(uuid='d7b8cbff-aee9-5a05-a4a1-d8f4e720aee7',
                                       version='2021-01-01T00:00:00.000000Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()

        field_paths = [
            ('projects', 'estimated_cell_count'),
            ('cell_suspensions', 'total_estimated_cells'),
            ('files', 'matrix_cell_count')
        ]
        actual = NestedDict(2, list)
        for hit in sorted(hits, key=lambda d: d['_id']):
            entity_type, doc_type = self._parse_index_name(hit)
            contents = hit['_source']['contents']
            for inner_entity_type, field_name in field_paths:
                for inner_entity in contents[inner_entity_type]:
                    value = inner_entity[field_name]
                    insort(actual[doc_type][entity_type][inner_entity_type], value)

        expected = NestedDict(1, dict)
        for doc_type in DocumentType.contribution, DocumentType.aggregate:
            for entity_type in self.index_service.entity_types(self.catalog):
                expected[doc_type][entity_type] = {
                    'cell_suspensions': [0, 20000, 20000],
                    'files': [2100, 15000, 15000],
                    'projects': [10000, 10000, 10000]
                } if entity_type == 'cell_suspensions' else {
                    # project.estimated_cell_count is aggregated using max, not sum
                    'cell_suspensions': [40000],
                    'files': [17100],
                    'projects': [10000]
                } if doc_type is DocumentType.aggregate and entity_type == 'projects' else {
                    'cell_suspensions': [20000, 20000] if doc_type is DocumentType.aggregate else [0, 20000, 20000],
                    'files': [2100, 15000],
                    'projects': [10000, 10000]
                }

        self.assertEqual(expected.to_dict(), actual.to_dict())

    def test_no_cell_count_contributions(self):
        def assert_cell_suspension(expected: JSON, hits: list[JSON]):
            project_hit = one(self._filter_hits(hits, DocumentType.aggregate, 'projects'))
            contents = project_hit['_source']['contents']
            cell_suspension = cast(JSON, one(contents['cell_suspensions']))
            actual_result = {
                field: cell_suspension[field]
                for field in expected.keys()
            }
            self.assertEqual(expected, actual_result)

        # This bundle has a 'cell_suspension' but that `cell_suspension` does
        # not contain a `total_estimated_cells` property.
        #
        no_cells_bundle = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                           version='2018-09-14T13:33:14.453337Z')
        no_cells_bundle = self._load_canned_bundle(no_cells_bundle)
        self._index_bundle(no_cells_bundle)
        expected = {
            'total_estimated_cells': null_int.null_value,
            'total_estimated_cells_': None,
            'organ_part': ['temporal lobe']
        }
        assert_cell_suspension(expected, self._get_all_hits())

        # This bundle has a 'cell_suspension' with a 'total_estimated_cells'
        # field. The bundles are incrementally indexed to prove that the
        # estimated_cell_count in the aggregate changes from None to a value.
        #
        has_cells_bundle = self.bundle_fqid(uuid='3db604da-940e-49b1-9bcc-25699a55b295',
                                            version='2018-09-14T13:33:14.453337Z')
        has_cells_bundle = self._load_canned_bundle(has_cells_bundle)

        # We patch the project entity to ensure that the project aggregate gets
        # cell suspensions from both bundles.
        #
        target_metadata = has_cells_bundle.metadata_files
        source_metadata = no_cells_bundle.metadata_files
        target_metadata['project_0.json'] = source_metadata['project_0.json']
        self._index_bundle(has_cells_bundle)
        expected = {
            'total_estimated_cells': 10000,
            'total_estimated_cells_': 10000,
            'organ_part': ['amygdala', 'temporal lobe']
        }
        assert_cell_suspension(expected, self._get_all_hits())

    def test_imaging_bundle(self):
        bundle_fqid = self.bundle_fqid(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c',
                                       version='2019-04-03T10:34:26.471000Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        sources = defaultdict(list)
        for hit in hits:
            entity_type, doc_type = self._parse_index_name(hit)
            sources[entity_type, doc_type].append(hit['_source'])
            # bundle has 240 imaging_protocol_0.json['target'] items, each with
            # an assay_type of 'in situ sequencing'
            if doc_type is DocumentType.aggregate:
                assay_type = ['in situ sequencing']
            elif doc_type is DocumentType.contribution:
                assay_type = {'in situ sequencing': 240}
            else:
                assert False, doc_type
            self.assertEqual(one(hit['_source']['contents']['imaging_protocols'])['assay_type'], assay_type)
        for doc_type in DocumentType.aggregate, DocumentType.contribution:
            with self.subTest(doc_type=doc_type):
                self.assertEqual(
                    {
                        'bundles': 1,
                        'files': 227,
                        'projects': 1,
                        'samples': 1,
                    },
                    {
                        entity_type: len(sources)
                        for (entity_type, _doc_type), sources in sources.items()
                        if _doc_type is doc_type
                    }
                )
                # This imaging bundle contains 6 data files in JSON format
                self.assertEqual(
                    Counter({'tiff': 221, 'json': 6}),
                    Counter(one(source['contents']['files'])['file_format']
                            for source in sources['files', doc_type])
                )

    def test_cell_line_sample(self):
        """
        Index a bundle with the following structure:
        donor -> specimen -> cell_line -> cell_line -> cell_suspension -> sequence_files
        and assert the singleton sample matches the first cell_line up from the
        sequence_files and assert cell_suspension inherits the organ value from
        the nearest ancestor cell_line
        """
        bundle_fqid = self.bundle_fqid(uuid='e0ae8cfa-2b51-4419-9cde-34df44c6458a',
                                       version='2018-12-05T23:09:17.591044Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            aggregate = doc_type is DocumentType.aggregate
            contribution = doc_type is DocumentType.contribution
            if entity_type == 'samples':
                sample = one(contents['samples'])
                sample_entity_type = sample['entity_type']
                if aggregate:
                    document_ids = one(contents[sample_entity_type])['document_id']
                elif contribution:
                    document_ids = [d['document_id'] for d in contents[sample_entity_type]]
                    entity = one(d for d in contents[sample_entity_type] if d['document_id'] == sample['document_id'])
                    self.assertEqual(sample['biomaterial_id'], entity['biomaterial_id'])
                else:
                    assert False, doc_type
                self.assertTrue(sample['document_id'] in document_ids)
                self.assertEqual(one(contents['specimens'])['organ'], ['blood'] if aggregate else 'blood')
                self.assertEqual(one(contents['specimens'])['organ_part'], ['venous blood'])
                self.assertEqual(len(contents['cell_lines']), 1 if aggregate else 2)
                if aggregate:
                    cell_lines_model_organ = set(one(contents['cell_lines'])['model_organ'])
                elif contribution:
                    cell_lines_model_organ = {cl['model_organ'] for cl in contents['cell_lines']}
                else:
                    assert False, doc_type
                self.assertEqual(cell_lines_model_organ, {'blood (parent_cell_line)', 'blood (child_cell_line)'})
                self.assertEqual(one(contents['cell_suspensions'])['organ'], ['blood (child_cell_line)'])
                self.assertEqual(one(contents['cell_suspensions'])['organ_part'], [self.translated_str_null])

    def test_multiple_samples(self):
        """
        Index a bundle with a specimen_from_organism and a cell_line input into
        a cell_suspension resulting in two samples of different entity_type
        """
        bundle_fqid = self.bundle_fqid(uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                                       version='2019-10-03T10:55:24.911627Z')
        self._index_canned_bundle(bundle_fqid)
        sample_entity_types = ['cell_lines', 'specimens']
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            cell_suspension = one(contents['cell_suspensions'])
            self.assertEqual(cell_suspension['organ'], ['embryo', 'immune system'])
            self.assertEqual(cell_suspension['organ_part'], ['skin epidermis', self.translated_str_null])
            if doc_type is DocumentType.aggregate and entity_type != 'samples':
                self.assertEqual(one(contents['samples'])['entity_type'], sample_entity_types)
            elif doc_type in (DocumentType.aggregate, DocumentType.contribution):
                for sample in contents['samples']:
                    self.assertIn(sample['entity_type'], sample_entity_types)
            else:
                assert False, doc_type

    def test_sample_with_no_donor(self):
        """
        Index two bundles for the same project, one bundle has a sample that
        is connected to a donor and the other bundle has a sample that is not.
        Verify the lack of a donor is represented in the project aggregate.
        """
        # Sample (Specimen): 70d2b85a, Donor: b111e5bf
        bundle_fqid = self.bundle_fqid(uuid='1fd499c5-f397-4bff-9af0-eb42c37d5fbe',
                                       version='2021-03-18T11:38:49.884000Z')
        self._index_canned_bundle(bundle_fqid)
        # Sample (Organoid): df23c109, Donor: none
        bundle_fqid = self.bundle_fqid(uuid='0722b70c-6778-423d-8fe9-869e2a515d35',
                                       version='2021-03-18T11:38:49.863000Z')
        self._index_canned_bundle(bundle_fqid)
        donor = {
            'document_id': 'b111e5bf-e907-47f9-8eed-75b2ec5536c5',
            'biomaterial_id': 'Human_62',
            'biological_sex': 'male',
            'genus_species': ['Homo sapiens'],
            'development_stage': 'human adult stage',
            'diseases': ['normal'],
            'organism_age': '62 year',
            'organism_age_range': {
                'gte': 1955232000.0,
                'lte': 1955232000.0
            },
        }
        donor_none = {
            k: [None] if isinstance(v, list) else None
            for k, v in donor.items()
            if k != 'organism_age_range'
        }
        aggregate_donor = {
            'donor_count': 1,
            'donor_count_': 1,
            **{
                # The `organism_age_range` field will not have a `None`
                # value since the field is only added to the donor
                # inner entity if it has `organism_age_in_seconds` value.
                # FIXME: The donor inner entity of a project aggregate that
                #        includes a project without a donor should include
                #        `None` in each of the aggregated donor fields.
                #        https://github.com/databiosphere/azul/issues/3152
                k: (v if isinstance(v, list) else [v]) +
                   ([] if k == 'organism_age_range' or True else [None])
                for k, v in donor.items()
            }
        }
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            if entity_type == 'projects':
                if doc_type is DocumentType.aggregate:
                    self.assertElasticEqual([aggregate_donor], contents['donors'])
                elif doc_type is DocumentType.contribution:
                    sample_id = one(contents['samples'])['document_id']
                    if sample_id == '70d2b85a-8055-4027-a0d9-29452a49d668':
                        self.assertEqual([donor], contents['donors'])
                    elif sample_id == 'df23c109-59f0-46d3-bd09-660175b51bda':
                        # FIXME: The donor inner entity for a project without a donor
                        #        should have all the standard fields of a donor inner
                        #        entity with values of `None`.
                        #        https://github.com/databiosphere/azul/issues/3152
                        self.assertEqual([] if True else [donor_none], contents['donors'])
                    else:
                        assert False, sample_id
                else:
                    assert False, doc_type

    def test_files_content_description(self):
        bundle_fqid = self.bundle_fqid(uuid='ffac201f-4b1c-4455-bd58-19c1a9e863b4',
                                       version='2019-10-09T17:07:35.528600Z')
        self._index_canned_bundle(bundle_fqid)
        hits = self._get_all_hits()
        for hit in hits:
            contents = hit['_source']['contents']
            entity_type, doc_type = self._parse_index_name(hit)
            if doc_type is DocumentType.aggregate:
                # bundle aggregates keep individual files
                num_inner_files = 2 if entity_type == 'bundles' else 1
            elif doc_type is DocumentType.contribution:
                # one inner file per file contribution
                num_inner_files = 1 if entity_type == 'files' else 2
            else:
                assert False, doc_type
            self.assertEqual(len(contents['files']), num_inner_files)
            for file in contents['files']:
                self.assertEqual(file['content_description'], ['RNA sequence'])

    def test_related_files_field_exclusion(self):
        bundle_fqid = self.bundle_fqid(uuid='587d74b4-1075-4bbf-b96a-4d1ede0481b2',
                                       version='2018-10-10T02:23:43.182000Z')
        self._index_canned_bundle(bundle_fqid)

        # Check that the dynamic mapping has the related_files field disabled
        index = str(IndexName.create(catalog=self.catalog,
                                     entity_type='files',
                                     doc_type=DocumentType.aggregate))
        mapping = self.es_client.indices.get_mapping(index=index)
        contents = mapping[index]['mappings']['properties']['contents']
        self.assertFalse(contents['properties']['files']['properties']['related_files']['enabled'])

        # Ensure that related_files exists
        hits = self._get_all_hits()
        for hit in self._filter_hits(hits, DocumentType.aggregate, 'files'):
            file = one(hit['_source']['contents']['files'])
            self.assertIn('related_files', file)

        # … but that it can't be used for queries
        zattrs_file = '377f2f5a-4a45-4c62-8fb0-db9ef33f5cf0.zarr/.zattrs'
        hits = self.es_client.search(index=index,
                                     body={
                                         'query': {
                                             'match': {
                                                 'contents.files.related_files.name': zattrs_file
                                             }
                                         }
                                     })
        self.assertEqual({'value': 0, 'relation': 'eq'}, hits['hits']['total'])

    def test_downstream_entities(self):
        """
        Verify that samples and cell_suspensions include analysis files from
        stitched subgraphs
        """
        bundle_fqid = self.bundle_fqid(uuid='79fa91b4-f1fc-534b-a935-b57342804a70',
                                       version='2020-12-10T10:30:00.000000Z')
        bundle = self._load_canned_bundle(bundle_fqid)

        expected_cell_count = 123

        # The bundles that motivated this test case lack `estimated_cell_count`,
        # so we inject it here to avoid nulls in the index.
        for document in bundle.metadata_files.values():
            if document['describedBy'].endswith('/cell_suspension'):
                document['estimated_cell_count'] = expected_cell_count

        self._index_bundle(bundle)

        def get_aggregates(hits, type):
            for hit in self._filter_hits(hits, DocumentType.aggregate, type):
                yield hit['_source']['contents']

        hits = self._get_all_hits()
        samples = list(get_aggregates(hits, 'samples'))
        self.assertEqual(15, len(samples))

        def assert_analysis_files(hit):
            analysis_file_formats = {
                file['file_format']
                for file in hit['files']
                if 'DCP/2 Analysis' in file['file_source']
            }
            self.assertEqual({'bam', 'loom'}, analysis_file_formats)

        for sample in samples:
            assert_analysis_files(sample)
            self.assertGreater(len(sample['donors']), 0)
            self.assertGreater(len(sample['specimens']), 0)

        transformer = CellSuspensionTransformer
        field_name = 'total_estimated_cells'
        entity_type = transformer.entity_type()
        inner_entity_type = one(transformer.inner_entity_types())
        field_type = transformer.field_types()[inner_entity_type][field_name]
        cell_suspensions = list(get_aggregates(hits, entity_type))
        self.assertEqual(22, len(cell_suspensions))
        for cell_suspension in cell_suspensions:
            assert_analysis_files(cell_suspension)
            inner = one(cell_suspension[entity_type])
            cell_count = field_type.from_index(inner[field_name])
            self.assertEqual(expected_cell_count, cell_count)

    def test_mapper_parsing(self):
        """
        Verify that the tests are insensitive to whether a can from DSS or TDR
        is indexed first. Especially the dynamic mapping could be sensitive to
        subtle difference in the formatting of fields.
        """
        bundle_fqids = [
            # A bundle from TDR
            self.bundle_fqid(uuid='17a3d288-01a0-464a-9599-7375fda3353d',
                             version='2018-03-28T15:10:23.074974Z'),
            # A bundle from DSS
            self.bundle_fqid(uuid='2c7d06b8-658e-4c51-9de4-a768322f84c5',
                             version='2021-09-21T17:27:23.898000Z'),
        ]

        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                self.index_service.delete_indices(self.catalog)
                self.index_service.create_indices(self.catalog)

                for bundle_fqid in reversed(bundle_fqids) if reverse else bundle_fqids:
                    self._index_canned_bundle(bundle_fqid)

                hits = self._get_all_hits()
                self._assert_hit_counts(hits, 21)

    def test_disallow_manifest_column_joiner(self):
        bundle_fqid = self.bundle_fqid(uuid='1b6d8348-d6e9-406a-aa6a-7ee886e52bf9',
                                       version='2019-10-03T10:55:24.911627Z')
        bundle = self._load_canned_bundle(bundle_fqid)
        project = bundle.metadata_files['project_0.json']
        contributor = project['contributors'][0]
        assert contributor['institution'] == 'Lund University'
        contributor['institution'] += ' || LabMED'
        with self.assertRaisesRegex(RequirementError, "'||' is disallowed"):
            self._index_bundle(bundle)


class TestIndexManagement(AzulUnitTestCase):

    def test_check_indices(self):
        # In all but the first subtest, we vary a single aspect of the actual
        # values. The function under test must detect each varied aspect.
        for mismatch, settings_value, property_value, dynamic_value, rest_value in [
            (None, '8', 'nested', True, False),
            ('settings', '9', 'nested', True, False),
            ('properties', '8', 'foo', True, False),
            ('properties', '8', None, True, False),
            ('dynamic_templates', '8', 'nested', False, False),
            ('mappings', '8', 'nested', True, True)
        ]:
            with self.subTest(settings_value=settings_value,
                              property_value=property_value,
                              dynamic_value=dynamic_value,
                              rest_value=rest_value,
                              mismatch=mismatch):
                # A few helpers
                boolean = {'type': 'boolean', 'fields': {'keyword': {'type': 'boolean'}}}
                date = {'type': 'date', 'fields': {'keyword': {'type': 'date'}}}
                keyword = {'keyword': {'type': 'keyword', 'ignore_above': 256}}
                # The literals below are stripped down examples, that are still
                # representative of what goes on in a real deployment. The goal
                # is to have diversity without repetition but also to capture
                # insignificant differences i.e. those that should not be
                # detected as a mismatch.
                actual_settings = {
                    'index': {
                        'refresh_interval': '1s',
                        'number_of_shards': settings_value,
                        'provided_name': 'azul_v2_sandbox_dcp2_files',
                    }
                }
                expected_settings = {
                    'index': {
                        'number_of_shards': 8,
                        'refresh_interval': '1s'
                    }
                }
                expected_mappings = {
                    'numeric_detection': False,
                    'properties': {
                        'entity_id': {'type': 'text', 'fields': keyword},
                        'contents': {
                            'properties': {
                                'projects': {
                                    'properties': {
                                        'accessions': {'type': 'nested'}
                                    }
                                }
                            }
                        }
                    },
                    'dynamic_templates': [
                        {
                            'donor_age_range': {
                                'path_match': 'contents.donors.organism_age_range',
                                'mapping': {'type': 'double_range'}
                            }
                        }
                    ]
                    if dynamic_value else
                    []
                }
                actual_mappings = {
                    'dynamic_templates': [
                        {
                            'donor_age_range': {
                                'path_match': 'contents.donors.organism_age_range',
                                'mapping': {'type': 'double_range'}
                            }
                        }
                    ],
                    'numeric_detection': rest_value,
                    'date_detection': True,
                    'properties': {
                        'bundle_deleted': boolean,
                        'contents': {
                            'properties': {
                                'dates': {
                                    'properties': {
                                        'submission_date': date
                                    }
                                },
                                'projects': {
                                    'properties': {
                                        'accessions': {
                                            **({} if property_value is None else {'type': property_value}),
                                            'properties': {
                                                'accession': {
                                                    'type': 'text',
                                                    'fields': keyword
                                                },
                                                'namespace': {
                                                    'type': 'text',
                                                    'fields': keyword
                                                }
                                            }
                                        },
                                        'submission_date': date
                                    }
                                },
                            }
                        },
                        'entity_id': {
                            'type': 'text',
                            'fields': keyword
                        },
                    }
                }
                try:
                    index_service = IndexService()
                    index_service._check_index(settings=expected_settings,
                                               mappings=expected_mappings,
                                               index=dict(settings=actual_settings,
                                                          mappings=actual_mappings))
                except IndexExistsAndDiffersException as e:
                    assert e.args[0] == mismatch
                else:
                    assert mismatch is None


def get(v):
    return one(v) if isinstance(v, list) else v


if __name__ == '__main__':
    unittest.main()
