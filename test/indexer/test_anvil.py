from abc import (
    ABC,
)
from collections import (
    Counter,
    defaultdict,
)
import json
from operator import (
    attrgetter,
    itemgetter,
)
from typing import (
    Iterable,
    Type,
    cast,
)
from unittest.mock import (
    Mock,
    PropertyMock,
    patch,
)

import attrs
from furl import (
    furl,
)
from more_itertools import (
    one,
)
from urllib3 import (
    HTTPResponse,
)

from azul import (
    config,
)
from azul.indexer.cache_service import (
    UrlCacheService,
)
from azul.indexer.document import (
    CataloguedEntityReference,
    Contribution,
    Document,
    DocumentType,
    EntityReference,
)
from azul.indexer.index_service import (
    IndexService,
)
from azul.lib import (
    R,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    MutableJSON,
    json_element_mappings,
    json_str,
    not_none,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.plugins.metadata.anvil.bundle import (
    EntityGraph,
)
from azul.plugins.metadata.anvil.indexer import (
    transform,
)
from azul.plugins.metadata.anvil.indexer.transform import (
    DatasetTransformer,
    FileTransformer,
)
from azul.plugins.metadata.anvil.schema import (
    anvil_schemas,
)
from azul.plugins.repository import (
    tdr_anvil,
)
from azul.plugins.repository.tdr_anvil import (
    TDRAnvilBundle,
    _entity_id,
    fixed_version,
)
from azul.source import (
    Prefix,
    Source,
    SourceConfig,
)
from azul.terra import (
    TDRClient,
    TDRSourceRef,
    TDRSourceSpec,
)
from azul_test_case import (
    TDRTestCase,
    patch_config,
)
from indexer import (
    AnvilCannedBundleTestCase,
    IndexerTestCase,
    WithIndexesSetUpTestCase,
)
from indexer.test_tdr import (
    TDRPluginTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


log = get_test_logger(__name__)


class DUOSTestCase(TDRTestCase, ABC):

    def _mock_normal_duos(self):
        for p in self._duos_patches(self.normal_tdr_response,
                                    self.normal_duos_response):
            self.addPatch(p)

    @property
    def normal_tdr_response(self) -> MutableJSON:
        return {
            'name': self.source.ref.spec.name,
            'duosFirecloudGroup': {'duosId': 'DUOS-000000'},
            'tables': [
                {'name': name, 'rowCount': len(table['rows'])}
                for name, table in self._canned_tables().items()
            ]
        }

    @property
    def normal_duos_response(self) -> MutableJSON:
        return {
            'consentGroups': [{'datasetIdentifier': 'DUOS-000000'}],
            'studyDescription': 'Study description from DUOS'
        }

    def _duos_patches(self,
                      tdr_response: JSON,
                      duos_response: JSON | None = None
                      ) -> Iterable[patch]:
        tdr_mock = Mock(spec=HTTPResponse, status=200,
                        data=json.dumps(tdr_response))
        duos_host = 'mock_duos.lan'
        mock_url = PropertyMock(return_value=furl(f'https://{duos_host}'))
        mock_cache = Mock(spec=UrlCacheService)
        if duos_response is None:
            duos_mock = None
        else:
            duos_mock = Mock(spec=HTTPResponse, status=200,
                             data=json.dumps(duos_response))

        def get_url(url, *_args, **_kwargs):
            # The snapshot metadata and the DUOS dataset registration are
            # fetched through the same cache, and are told apart by their host
            return duos_mock if url.host == duos_host else tdr_mock

        mock_cache.get_url.side_effect = get_url
        patches = [
            patch.object(type(config), 'duos_service_url', new=mock_url),
            patch.object(TDRClient, '_url_cache', new=mock_cache),
        ]
        return patches


class AnvilIndexerTestCase(AnvilCannedBundleTestCase, IndexerTestCase):
    pass


class TestAnvilIndexer(AnvilIndexerTestCase,
                       TDRPluginTestCase[tdr_anvil.Plugin],
                       DUOSTestCase):

    @classmethod
    def _plugin_cls(cls) -> Type[tdr_anvil.Plugin]:
        return tdr_anvil.Plugin

    def test_indexing(self):
        self.maxDiff = None
        bundle = self.primary_bundle()
        canned_hits = self._load_canned_result(bundle)
        for enable_replicas in True, False:
            with patch.object(target=type(config),
                              attribute='enable_replicas',
                              new_callable=PropertyMock,
                              return_value=enable_replicas):
                with self.subTest(enable_replicas=enable_replicas):
                    if enable_replicas:
                        expected_hits = canned_hits
                    else:
                        expected_hits = [
                            h
                            for h in canned_hits
                            if self._parse_index_name(h)[1] is not DocumentType.replica
                        ]
                    self.index_service.create_indices(self.catalog)
                    try:
                        self._index_canned_bundle(bundle)
                        hits = self._get_all_hits()
                        hits.sort(key=itemgetter('_id'))
                        self.assertElasticEqual(expected_hits, hits)
                    finally:
                        self._purge_indices()

    def test_list_and_fetch_bundles(self):
        self._mock_normal_duos()
        source_ref = self.source.ref
        self._make_mock_tables(source_ref)
        canned_bundle_fqids = [
            self.primary_bundle(),
            self.supplementary_bundle(),
            self.replica_bundle(),
        ]
        expected_bundle_fqids = sorted(canned_bundle_fqids + [
            # Replica bundles for the AnVIL schema tables, which we don't can
            self.bundle_fqid(table_name='anvil_activity'),
            self.bundle_fqid(table_name='anvil_alignmentactivity'),
            self.bundle_fqid(table_name='anvil_assayactivity'),
            self.bundle_fqid(table_name='anvil_diagnosis'),
            self.bundle_fqid(table_name='anvil_donor'),
            self.bundle_fqid(table_name='anvil_sequencingactivity'),
            self.bundle_fqid(table_name='anvil_variantcallingactivity')
        ])
        plugin = self.plugin
        bundle_fqids = sorted(plugin.list_bundles(source_ref, ''))
        self.assertEqual(expected_bundle_fqids, bundle_fqids)
        for bundle_fqid in bundle_fqids:
            with self.subTest(bundle_fqid=bundle_fqid):
                bundle = plugin.fetch_bundle(bundle_fqid)
                assert isinstance(bundle, TDRAnvilBundle)
                if bundle_fqid in canned_bundle_fqids:
                    canned_bundle = self._load_canned_bundle(bundle_fqid)
                    assert isinstance(canned_bundle, TDRAnvilBundle)
                    self.assertEqual(canned_bundle.fqid, bundle.fqid)
                    self.assertEqual(canned_bundle.entities, bundle.entities)
                    self.assertEqual(canned_bundle.graph, bundle.graph)
                    self.assertEqual(canned_bundle.orphans, bundle.orphans)

    def test_links_round_trip(self):
        """
        The links of a canned bundle survive deserialization followed by
        serialization, up to their order.
        """

        def canonical(links: AnyJSON) -> list[str]:
            return sorted(json.dumps(link, sort_keys=True) for link in json_element_mappings(links))

        for bundle_fqid in [self.primary_bundle(), self.supplementary_bundle(), self.replica_bundle()]:
            with self.subTest(bundle_fqid=bundle_fqid):
                bundle_json = self._load_canned_file(bundle_fqid, TDRAnvilBundle.canning_qualifier())
                assert isinstance(bundle_json, dict), bundle_json
                bundle = self._load_canned_bundle(bundle_fqid)
                self.assertEqual(canonical(bundle_json['links']),
                                 canonical(bundle.to_json()['links']))

    def test_row_counts(self):
        tables = [
            {'name': 'anvil_biosample', 'rowCount': 3},
            {'name': 'non_schema_orphan_table', 'rowCount': 5}
        ]
        tdr_response = self.normal_tdr_response | {'tables': tables}
        with self.stacked_patches(self._duos_patches(tdr_response)):
            self.assertEqual({'anvil_biosample': 3, 'non_schema_orphan_table': 5},
                             self.plugin.tdr.row_counts(self.source.ref))

    def test_non_verbatim_tables(self):
        tables = [
            {'name': 'anvil_biosample', 'rowCount': 3},
            # Exempt from the threshold, however huge
            {'name': 'anvil_file', 'rowCount': 1000},
            # Excluded regardless of the threshold
            {'name': 'file_inventory', 'rowCount': 1},
            {'name': 'non_schema_orphan_table', 'rowCount': 1000}
        ]
        tdr_response = self.normal_tdr_response | {'tables': tables}
        with self.stacked_patches(self._duos_patches(tdr_response)):
            with patch_config('anvil_huge_table_threshold', 999):
                self.assertEqual(['datarepo_row_ids',
                                  'file_inventory',
                                  'non_schema_orphan_table'],
                                 self.plugin._non_verbatim_tables(self.source.ref))

    # FIXME: Remove this test
    #        https://github.com/DataBiosphere/azul/issues/8448

    def test_legacy_dataset_fields(self):
        """
        A dataset document indexed before `_non_verbatim_tables` replaced the
        two row count fields still carries them. Translating such a document
        must not fail, because `Document.translate_fields` iterates the keys
        of the document, not the declared field types.
        """
        document = {
            'datasets': [
                {
                    '_tdr_non_schema_row_count': 1,
                    '_tdr_non_schema_row_count_threshold': 2
                }
            ]
        }
        field_types = DatasetTransformer.field_types()
        translated = Document.translate_fields(document,
                                               field_types,
                                               forward=False)
        self.assertEqual(document, translated)

    # FIXME: Remove this test
    #        https://github.com/DataBiosphere/azul/issues/8448

    def test_legacy_dataset_fields_are_not_contributed(self):
        """
        Bundles don't carry the two row count fields anymore, so transforming
        one must not fail, and the resulting contribution must not carry them
        either. Unlike `Document.translate_fields`, `_entity` iterates the
        declared field types, and demands a value for each of them.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        contributions, _ = self.index_service.transform(self.catalog,
                                                        bundle,
                                                        delete=False)
        contents = one(
            one(contribution.contents['datasets'])
            for contribution in contributions
            if contribution.entity.entity_type == 'datasets'
        )
        self.assertNotIn('_tdr_non_schema_row_count', contents)
        self.assertNotIn('_tdr_non_schema_row_count_threshold', contents)

    def test_huge_table(self):
        """
        No replica bundles are listed for huge tables, except for the anvil_file
        table, for which replicas are always included, regardless of its size.
        """
        source_ref = self.source.ref
        self._make_mock_tables(source_ref)
        self._mock_normal_duos()
        plugin = self.plugin
        row_counts = plugin.tdr.row_counts(source_ref)
        non_schema_table = self.replica_bundle().table_name
        num_non_schema_rows = row_counts[non_schema_table]
        file_table = self.supplementary_bundle().table_name
        num_file_rows = row_counts[file_table]
        # The exemption is only exercised if the file table is huge, too, at
        # the lower of the two thresholds below. This is not asserting something
        # about the code under test. It is asserting that the test fixture is
        # suitable for this test.
        self.assertGreaterEqual(num_file_rows, num_non_schema_rows)
        for threshold, is_huge in [
            (num_non_schema_rows, False),
            (num_non_schema_rows - 1, True)
        ]:
            with self.subTest(threshold=threshold):
                with patch_config('anvil_huge_table_threshold', threshold):
                    expected = ['datarepo_row_ids']
                    if is_huge:
                        expected = sorted([*expected, non_schema_table])
                    self.assertEqual(expected, plugin._non_verbatim_tables(source_ref))
                    bundle_fqids = plugin.list_bundles(source_ref, prefix='')
                    self.assertEqual(is_huge, self.replica_bundle() not in bundle_fqids)
                    # Bundles from the tables that aren't huge remain
                    for bundle_fqid in [
                        self.primary_bundle(),
                        self.supplementary_bundle(),
                        self.bundle_fqid(table_name='anvil_donor')
                    ]:
                        self.assertIn(bundle_fqid, bundle_fqids)
                    self.assertEqual(len(bundle_fqids),
                                     plugin.count_bundles(source_ref))

    def test_absent_duos_id(self):
        source_ref = self.source.ref
        self._make_mock_tables(source_ref)
        normal = self.normal_tdr_response
        cases = {
            'Absent duosFirecloudGroup':
                {k: v for k, v in normal.items() if k != 'duosFirecloudGroup'},
            'Empty duosFirecloudGroup':
                normal | {'duosFirecloudGroup': {}},
            'Null duosId':
                normal | {'duosFirecloudGroup': {'duosId': None}},
        }
        for sub_test, tdr_response in cases.items():
            with self.subTest(sub_test):
                with self.stacked_patches(self._duos_patches(tdr_response)):
                    bundle = self.plugin.fetch_bundle(self.primary_bundle())
                    self.assertIsInstance(bundle, TDRAnvilBundle)
                    dataset_ref = one(
                        ref for ref in bundle.entities
                        if ref.entity_type == 'anvil_dataset'
                    )
                    metadata = bundle.entities[dataset_ref]
                    self.assertIsNone(metadata['duos_id'])
                    self.assertIsNone(metadata['description'])

    def test_schema_version(self):
        plugin = self.plugin
        for name, expected in [
            ('ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732', 5),
            ('ANVIL_1000G_high_coverage_2019_20230517_ANV6_202607071430', 6),
            # Some snapshots spell the prefix `AnVIL`
            ('AnVIL_1000G_2019_Dev_20230609_ANV5_202306121732', 5)
        ]:
            with self.subTest(name=name):
                self.assertEqual(expected, plugin._schema_version(self._spec(name)))
        for name in [
            # No version at all
            'anvil_snapshot',
            # A version we don't track, and therefore can't select columns for
            'ANVIL_1000G_2019_Dev_20230609_ANV4_202306121732',
            # No dataset name
            'ANVIL_20230609_ANV5_202306121732',
            # Truncated date, version and time
            'ANVIL_1000G_2019_Dev_2023060_ANV5_202306121732',
            'ANVIL_1000G_2019_Dev_20230609_ANV_202306121732',
            'ANVIL_1000G_2019_Dev_20230609_ANV5_20230612173',
            # Trailing and leading garbage
            'ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732_v2',
            'restored_ANVIL_1000G_2019_Dev_20230609_ANV5_202306121732',
            # An atlas whose snapshots don't encode a schema version at all
            'hca_prod_005d611a14d54fbf846e571a1f874f70__20220111_dcp2_20241205_dcp45'
        ]:
            with self.subTest(name=name):
                with self.assertRaises(AssertionError):
                    plugin._schema_version(self._spec(name))

    def test_entity_id(self):
        key = 'f9d40cf6-37b8-22f3-ce35-0dc614d2452b'
        spec = self._spec('ANVIL_CMG_UWASH_DS_BDIS_20230418_ANV5_202304201958')
        entity_id = _entity_id(spec, 'anvil_biosample', key)
        self.assertEqual('6f8461fd-0e84-524e-9488-7614e45e2eec', entity_id)
        # A later release of the same dataset yields the same ID, even if it
        # spells the dataset differently, or was ingested under a different
        # schema version, or the dataset was created anew …
        for name in [
            'ANVIL_CMG_UWASH_DS_BDIS_20250206_ANV6_202502201850',
            'ANVIL_cmg_uwash_ds_bdis_20250206_ANV6_202502201850',
            'AnVIL_CMG_UWash_DS_BDIS_20230418_ANV5_202304201958'
        ]:
            with self.subTest(name=name):
                self.assertEqual(entity_id,
                                 _entity_id(self._spec(name), 'anvil_biosample', key))
        # … while another dataset, or another table, does not. Primary keys are
        # only unique within a table of a snapshot, so without these
        # qualifiers, entities would share an ID.
        for other_spec, other_table in [
            (self._spec('ANVIL_CMG_UWASH_DS_HFA_20230418_ANV5_202304201932'), 'anvil_biosample'),
            (spec, 'anvil_donor')
        ]:
            with self.subTest(spec=str(other_spec), table=other_table):
                self.assertNotEqual(entity_id,
                                    _entity_id(other_spec, other_table, key))

    def test_duplicate_entity(self):
        """
        Test the detection of duplicate primary keys.
        """
        for bundle in [
            self._load_canned_bundle(self.primary_bundle()),
            self._load_canned_bundle(self.replica_bundle())
        ]:
            listed = bundle.entities or bundle.orphans
            entity, row = next(iter(listed.items()))
            for is_orphan in [False, True]:
                with self.subTest(bundle=bundle.uuid, is_orphan=is_orphan):
                    with self.assertRaises(AssertionError) as cm:
                        bundle.add_entity(entity, fixed_version, row, is_orphan=is_orphan)
                    self.assertTrue(R.caused(cm.exception))

    def test_absent_file_path(self):
        """
        Test that the `file_path` column, which version 6 of the schema added,
        is null in rows from a snapshot that was ingested under version 5.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        entity, row = next(
            (entity, row)
            for entity, row in bundle.entities.items()
            if entity.entity_type == 'anvil_file'
        )
        self.assertIsNotNone(row['file_path'])
        del row['file_path']
        bundle = TDRAnvilBundle(fqid=bundle.fqid)
        bundle.add_entity(entity, fixed_version, row)
        self.assertIsNone(bundle.entities[entity]['file_path'])

    def test_pk_column(self):
        plugin = self.plugin
        for version, schema in anvil_schemas.items():
            spec = self._spec(f'ANVIL_1000G_2019_Dev_20230609_ANV{version}_202306121732')
            for table in schema['tables']:
                table_name = table['name']
                with self.subTest(version=version, table=table_name):
                    # Every table the schema describes declares exactly one
                    # primary key, and names it after the table. The plugin
                    # used to hard-code that convention instead of reading the
                    # declaration, so this pins the two together.
                    self.assertEqual(table_name.removeprefix('anvil_') + '_id',
                                     plugin._pk_column(spec, table_name))
            # Tables absent from the schema declare no primary key, so their
            # rows are identified, partitioned and batched by their row ID
            with self.subTest(version=version, table='anvil_unknown'):
                self.assertIsNone(plugin._pk_column(spec, 'anvil_unknown'))
                self.assertEqual('datarepo_row_id',
                                 plugin._batch_column(spec, 'anvil_unknown'))

    def test_columns_per_schema_version(self):
        plugin = self.plugin
        # Version 6 added this column to the anvil_file table, so selecting it
        # from a snapshot ingested under version 5 would fail
        column = 'file_path'
        for version, expected in [(5, False), (6, True)]:
            with self.subTest(version=version):
                name = f'ANVIL_1000G_2019_Dev_20230609_ANV{version}_202306121732'
                columns = plugin._columns(self._spec(name), 'anvil_file')
                self.assertEqual(expected, column in columns)
                # Columns common to both versions are selected either way
                self.assertIn('file_name', columns)
        # Tables absent from the schema are replicated in their entirety
        name = 'ANVIL_1000G_2019_Dev_20230609_ANV6_202306121732'
        self.assertEqual({'*'}, plugin._columns(self._spec(name), 'anvil_unknown'))

    def _spec(self, name: str) -> TDRSourceSpec:
        return TDRSourceSpec.parse(f'tdr:bigquery:gcp:test_anvil_project:{name}')

    def test_duplicate_file_paths(self):
        """
        A file from a snapshot that was ingested under version 6 of the schema
        is told apart by its path, not by its name, so two files sharing a path
        are rejected.
        """
        tables = self._canned_tables()
        rows = tables['anvil_file']['rows']
        file_path = 'somedir/dup-file-path-test.txt'
        rows[0]['file_path'] = file_path
        rows[1]['file_path'] = file_path
        self._assert_duplicate_files(tables, [file_path])

    def test_duplicate_file_names(self):
        """
        A file from a snapshot that was ingested under version 5 of the schema
        has no path. A prefix of its digest tells it apart from the files it
        shares a name with, so a duplicate name is not rejected.
        """
        tables = self._canned_tables()
        rows = tables['anvil_file']['rows']
        file_name = 'dup-file-name-test.txt'
        for row in rows[:2]:
            row['file_name'] = file_name
            row['file_path'] = None
        self._assert_duplicate_files(tables, [])

    def _assert_duplicate_files(self,
                                tables: MutableJSON,
                                expected: list[str]
                                ) -> None:
        self._mock_normal_duos()
        bundle_fqid = self.primary_bundle()
        for name, table in tables.items():
            self._make_mock_table(self.source.ref.spec,
                                  name,
                                  table['rows'],
                                  table.get('schema'))
        if expected:
            with self.assertRaises(AssertionError) as error:
                self.plugin.fetch_bundle(bundle_fqid)
            self.assertEqual(R('Bundle contains duplicate file paths',
                               bundle_fqid, expected),
                             error.exception.args[0])
        else:
            bundle = self.plugin.fetch_bundle(bundle_fqid)
            self.assertEqual(bundle_fqid, bundle.fqid)


class TestAnvilIndexerWithIndexesSetUp(WithIndexesSetUpTestCase, AnvilIndexerTestCase):

    def test_dataset_description(self):
        dataset_ref = EntityReference(entity_type='anvil_dataset',
                                      entity_id='208441e2-cf28-584e-a0ac-7553c7118412')
        bundle_fqid = self.primary_bundle()
        bundle = cast(TDRAnvilBundle, self._load_canned_bundle(bundle_fqid))
        bundle.graph = EntityGraph()
        bundle.entities = {dataset_ref: bundle.entities[dataset_ref]}
        self._index_bundle(bundle, delete=False)

        hits = self._get_all_hits()
        doc_counts: dict[DocumentType, int] = defaultdict(int)
        for hit in hits:
            qualifier, doc_type = self._parse_index_name(hit)
            if qualifier == 'bundles':
                continue
            elif qualifier in {'datasets', 'replica'}:
                doc_counts[doc_type] += 1
                if qualifier == 'datasets' and doc_type is DocumentType.aggregate:
                    self.assertEqual(1, hit['_source']['num_contributions'])
                    contents = one(hit['_source']['contents']['datasets'])
                    self.assertEqual(dataset_ref.entity_id, contents['document_id'])
                    self.assertEqual(['phs000693'], contents['registered_identifier'])
                    self.assertEqual('Study description from DUOS', contents['description'])
                    self.assertEqual('DUOS-000000', contents['duos_id'])
                    self.assertEqual('52ee7665-7033-63f2-a8d9-ce8e32666739', contents['dataset_id'])
            else:
                self.fail(qualifier)
        self.assertDictEqual(doc_counts, {
            DocumentType.aggregate: 1,
            DocumentType.contribution: 1,
            **({DocumentType.replica: 1} if config.enable_replicas else {})
        })

    def test_orphans(self):
        bundle = self._index_canned_bundle(self.replica_bundle())
        assert isinstance(bundle, TDRAnvilBundle)
        dataset_entity_id = one(
            ref.entity_id
            for ref in bundle.orphans
            if ref.entity_type == 'anvil_dataset'
        )

        def is_augmentation(field: str) -> bool:
            return field.startswith('_')

        if config.enable_replicas:
            expected = dict(bundle.orphans)
            dataset_ref = EntityReference(entity_type='anvil_dataset',
                                          entity_id=dataset_entity_id)
            dataset = expected[dataset_ref]
            # The canned bundle carries the augmented fields, replicas don't
            self.assertTrue(any(map(is_augmentation, dataset)))
            expected[dataset_ref] = {
                field: value
                for field, value in dataset.items()
                if not is_augmentation(field)
            }
        else:
            expected = {}
        actual = {}
        hits = self._get_all_hits()
        for hit in hits:
            qualifier, doc_type = self._parse_index_name(hit)
            self.assertEqual(DocumentType.replica, doc_type)
            source = hit['_source']
            self.assertEqual(source['hub_ids'], [dataset_entity_id])
            ref = EntityReference(entity_type=source['replica_type'],
                                  entity_id=source['entity_id'])
            actual[ref] = source['contents']
        self.assertEqual(expected, actual)

    def _replicas(self) -> list[tuple[str, int]]:
        """
        The replicas currently in the index, one tuple per replica, sorted.
        Each tuple gives the name of the table that the replicated row came
        from, and the number of hubs the replica has.
        """
        return sorted(
            (hit['_source']['replica_type'], len(hit['_source']['hub_ids']))
            for hit in self._get_all_hits()
            if self._parse_index_name(hit)[1] is DocumentType.replica
        )

    def _fileless_bundle(self) -> TDRAnvilBundle:
        """
        The canned primary bundle, stripped of its files and of the activities
        that produced them, as the primary bundle of a biosample without files
        would look. None of the remaining entities is linked to a file, so the
        bundle transformer is the only source of their replicas.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        assert isinstance(bundle, TDRAnvilBundle)
        links = [
            link
            for link in bundle.graph.to_links()
            if not any(ref.entity_type == 'anvil_file'
                       for ref in link.all_entities)
        ]
        bundle.graph = EntityGraph.from_links(links)
        linked = set()
        for link in links:
            linked.update(link.all_entities)
        for ref in list(bundle.entities):
            if ref not in linked:
                del bundle.entities[ref]
        return bundle

    def test_excluded_table(self):
        """
        The replica of a row of an excluded table is withheld if no file links
        to that row, the bundle transformer being the only transformer that
        would otherwise have emitted it.
        """
        bundle = self._fileless_bundle()
        excluded_table = 'anvil_diagnosis'
        refs_by_type = defaultdict(list)
        for ref in bundle.entities:
            refs_by_type[ref.entity_type].append(ref)
        self.assertEqual(2, len(refs_by_type[excluded_table]))
        dataset_ref = one(refs_by_type['anvil_dataset'])
        bundle.entities[dataset_ref]['_non_verbatim_tables'] = [excluded_table]
        self._index_bundle(bundle)
        if config.enable_replicas:
            # Both diagnoses are withheld, and the dataset is the only hub of
            # the replicas that are left
            expected = [
                ('anvil_biosample', 1),
                ('anvil_dataset', 1),
                ('anvil_donor', 1)
            ]
        else:
            expected = []
        self.assertEqual(expected, self._replicas())

    def test_excluded_table_linked_to_file(self):
        """
        A row of an excluded table that is linked to a file keeps its replica,
        and that replica keeps all of its hubs, including the dataset's. The
        file transformer emits it, independently of the bundle transformer, and
        hubs it on the dataset as well as on the file.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        assert isinstance(bundle, TDRAnvilBundle)
        excluded_table = 'anvil_diagnosis'
        dataset_ref = one(
            ref
            for ref in bundle.entities
            if ref.entity_type == 'anvil_dataset'
        )
        bundle.entities[dataset_ref]['_non_verbatim_tables'] = [excluded_table]
        self._index_bundle(bundle)
        if config.enable_replicas:
            # Each diagnosis is linked to both of the bundle's files, and the
            # dataset is the third hub
            expected = [(excluded_table, 3), (excluded_table, 3)]
        else:
            expected = []
        actual = [r for r in self._replicas() if r[0] == excluded_table]
        self.assertEqual(expected, actual)


def _contribution_order(contribution: Contribution) -> tuple[str, str]:
    """
    A sort key that emulates the order requested by the indexer when it reads
    contributions from OpenSearch.
    """
    coordinates = contribution.coordinates
    return coordinates.index_name, coordinates.document_id


class TestAnvilFilePaths(AnvilCannedBundleTestCase):
    """
    The path backfilled for an AnVIL file that the snapshot gives none of its
    own, for the cases that the canned bundles don't cover.
    """

    #: The biosample that the canned primary bundle is rooted at
    #:
    biosample = 'f9d40cf6-37b8-22f3-ce35-0dc614d2452b'

    digest = '0123456789abcdef'

    def _bundle(self, *names: str, **overrides) -> TDRAnvilBundle:
        """
        The canned primary bundle, with each of its two files renamed to the
        correspondingly positioned one of the given names, stripped of the
        path the snapshot gave it, and overridden with the given fields.
        """
        bundle = self._load_canned_bundle(self.primary_bundle())
        files = sorted((e for e in bundle.entities if e.entity_type == 'anvil_file'),
                       key=attrgetter('entity_id'))
        for file, name in zip(files, names, strict=False):
            metadata = bundle.entities[file]
            metadata['file_name'] = name
            metadata['file_path'] = None
            metadata['file_md5sum'] = self.digest + self.digest
            metadata.update(overrides)
        return bundle

    def _paths(self, bundle: TDRAnvilBundle) -> set[str]:
        # Not every contribution to a file includes the file itself, so the
        # paths are taken from the copies of the files the bundle would
        # contribute, whether it does or not
        transformer = FileTransformer(bundle=bundle, deleted=False)
        return {
            json_str(transformer._file(file)['file_path'])
            for file in bundle.entities
            if file.entity_type == 'anvil_file'
        }

    def test_shared_name(self):
        """
        Two files that would be downloaded to the same path are set apart by a
        prefix of their digests, inserted before the extension.
        """
        bundle = self._bundle('reads.bam', 'reads.bam')
        self.assertEqual({f'{self.biosample}/reads-{self.digest}.bam'},
                         self._paths(bundle))

    def test_own_name(self):
        """
        A file that shares its name with no other keeps the name the snapshot
        gave it.
        """
        bundle = self._bundle('reads.bam', 'writes.bam')
        self.assertEqual({f'{self.biosample}/reads.bam',
                          f'{self.biosample}/writes.bam'},
                         self._paths(bundle))

    def test_supplementary(self):
        """
        A supplementary file belongs to the dataset rather than to any one
        biosample, and is set apart even when its name is its own, no bundle
        holding all of the files it could collide with.
        """
        bundle = self._bundle('reads.bam', 'writes.bam', is_supplementary=True)
        self.assertEqual({f'supplementary/reads-{self.digest}.bam',
                          f'supplementary/writes-{self.digest}.bam'},
                         self._paths(bundle))

    def _add_biosample(self, bundle: TDRAnvilBundle, biosample_id: str) -> None:
        """
        Record every activity in the given bundle that used the biosample the
        bundle is rooted at as having used an additional biosample with the
        given ID, too. Like every primary bundle, the given one doesn't contain
        the additional biosample, only the activities listing it.
        """
        for entity, metadata in bundle.entities.items():
            if entity.entity_type.endswith('activity'):
                used = metadata.get('used_biosample_id', [])
                if self.biosample in used:
                    metadata['used_biosample_id'] = [*used, biosample_id]

    def _reroot(self, bundle: TDRAnvilBundle, biosample_id: str) -> None:
        """
        Turn the given bundle into the primary bundle of the biosample with the
        given ID, by replacing the biosample the bundle is rooted at. This also
        changes the UUID of the bundle.
        """
        root = not_none(bundle.bundle_entity)
        clone = EntityReference(entity_type=root.entity_type,
                                entity_id=_entity_id(self.source.ref.spec,
                                                     root.entity_type,
                                                     biosample_id))
        bundle.entities[clone] = {
            **bundle.entities.pop(root),
            'biosample_id': biosample_id
        }
        bundle.graph = bundle.graph.map(lambda entity: clone if entity == root else entity)
        bundle.fqid = attrs.evolve(bundle.fqid, primary_key=biosample_id, uuid=None)
        assert clone == bundle.bundle_entity, (clone, bundle.bundle_entity)

    def test_many_biosamples(self):
        """
        A file taken from more than one biosample is grouped by the lowest of
        their IDs, so that the directory doesn't depend on their order.
        """
        for other in ['00000000-0000-0000-0000-000000000000', 'ffffffff']:
            with self.subTest(other=other):
                bundle = self._bundle('reads.bam', 'writes.bam')
                self._add_biosample(bundle, other)
                lowest = min(other, self.biosample)
                self.assertEqual({f'{lowest}/reads.bam', f'{lowest}/writes.bam'},
                                 self._paths(bundle))

    def test_unreliable_bundle(self):
        """
        A bundle leaves a group alone unless it is rooted at the biosample
        that names the group's directory, there being no guarantee that it
        holds the group in full. The bundle rooted at that biosample does.
        """
        other = '00000000-0000-0000-0000-000000000000'
        bundle = self._bundle('reads.bam', 'reads.bam')
        self._add_biosample(bundle, other)
        self.assertEqual({f'{other}/reads.bam'}, self._paths(bundle))
        self._reroot(bundle, other)
        self.assertEqual({f'{other}/reads-{self.digest}.bam'},
                         self._paths(bundle))

    def test_longer_path_wins(self):
        """
        Of the two paths that two bundles derive for a file they share, the
        one that deduplication lengthened prevails, the bundle that derived
        the other one not holding the file's namesakes in full.
        """
        other = '00000000-0000-0000-0000-000000000000'
        service = IndexService()
        contributions = []
        for primary_key in [self.biosample, other]:
            bundle = self._bundle('reads.bam', 'reads.bam')
            self._add_biosample(bundle, other)
            if primary_key != self.biosample:
                self._reroot(bundle, primary_key)
            documents = service.transform(self.catalog, bundle, delete=False)
            assert isinstance(documents, tuple), documents
            for contribution in documents[0]:
                entity = CataloguedEntityReference.for_entity(
                    self.catalog, contribution.coordinates.entity)
                coordinates = attrs.evolve(contribution.coordinates, entity=entity)
                contributions.append(attrs.evolve(contribution,
                                                  coordinates=coordinates))
        contributions.sort(key=_contribution_order)
        paths = {
            json_str(one(aggregate.contents['files'])['file_path'])
            for aggregate in service._aggregate(contributions)
            if aggregate.coordinates.entity.entity_type == 'files'
        }
        self.assertEqual({f'{other}/reads-{self.digest}.bam'}, paths)

    def test_detached(self):
        """
        A file that is neither supplementary nor taken from a biosample is
        downloaded to a directory of its own, and is set apart there whether
        it shares its name or not, no bundle being able to tell (#8386).
        """
        bundle = self._bundle('reads.bam', 'writes.bam')
        bundle.graph = EntityGraph()
        self.assertEqual({f'detached/reads-{self.digest}.bam',
                          f'detached/writes-{self.digest}.bam'},
                         self._paths(bundle))

    def test_without_digest(self):
        """
        A file without a digest is set apart by its primary key, but only in
        the one snapshot whose files lack a digest.
        """
        bundle = self._bundle('reads.bam', 'reads.bam', file_md5sum=None)
        with self.assertRaises(AssertionError):
            self._paths(bundle)
        expected = {
            f'{self.biosample}/reads-{file_id}.bam'
            for file_id in ['1e269f0443474188', '8b722e88810349c1']
        }
        # Every entity ID in the bundle is derived from the name of its
        # snapshot, so the carve-out is moved to that name instead
        with patch.object(transform,
                          'snapshot_without_md5s',
                          bundle.fqid.source.spec.name):
            self.assertEqual(expected, self._paths(bundle))


class TestAnvilSharedFile(WithIndexesSetUpTestCase, AnvilIndexerTestCase):
    """
    Tests involving files derived from two biosamples. Each parent biosample
    incurs a separate primary bundle, which is why this test uses two cans.

    One of the files the two bundles share is stripped of its path before
    they are indexed, leaving them with a mix of files that have a path and
    files that don't, a scenario we want to test explicitly. The doctoring is
    necessary because the v6 schema snapshots currently found in the wild all
    specify a non-null file_path.
    """

    #: Every entity ID in a bundle is derived from the name of the snapshot the
    #: bundle was taken from, so the cans only load under that name.
    #:
    source = Source(
        config=SourceConfig(mirror=True),
        ref=TDRSourceRef(
            id='4b4c1e4e-2a17-4b9e-9f1a-9e8e0a3b2d55',
            spec=TDRSourceSpec.parse('tdr:bigquery:gcp:test_anvil_project:'
                                     'ANVIL_1000G_high_coverage_2019'
                                     '_20230517_ANV6_202607071430'),
            prefix=Prefix.parse('/0')
        )
    )

    #: The primary key of the biosample root of each canned bundle. The first
    #: one sorts lower and therefore names the directory a backfilled path uses.
    #:
    biosamples = [
        '52cb7acf-36e4-7ee2-33b7-7772edd60b3f',
        '72ee95e4-04b7-93c1-36c7-b3f06058407f'
    ]

    #: The entity ID of the doctored file
    #:
    pathless_file = '121f8754-7639-5977-9e0f-c9f0095f2a57'

    directory = '/'.join([
        'CCDG_13607',
        'Project_CCDG_13607_B01_GRM_WGS.gVCF.2019-02-06',
        'Sample_HG02089',
        'analysis'
    ])

    name = 'HG02089.haplotypeCalls.er.raw.g.vcf.gz'

    #: The paths of all shared files, by entity ID
    #:
    expected_paths = {
        '5ea88d0a-9bbf-55b5-8df3-69bc20355333': f'{directory}/{name}',
        '045547b7-12b1-5e4d-9eb5-854c963a6522': f'{directory}/{name}.tbi',
        'b117471b-ad63-5581-b87e-139e2be35a46': f'{directory}/checksum/{name}.tbi.md5',
        pathless_file: f'{biosamples[0]}/{name}.md5'
    }

    def _index_bundles(self) -> None:
        """
        Index both canned bundles, after doctoring them as described in the
        docstring of this class.
        """
        for primary_key in self.biosamples:
            bundle = self._load_canned_bundle(self.bundle_fqid(primary_key=primary_key))
            for entity, metadata in bundle.entities.items():
                if entity.entity_id == self.pathless_file:
                    metadata['file_path'] = None
            self._index_bundle(bundle)

    def test_shared_files(self):
        """
        Each shared file gets a contribution from each bundle. The undoctored
        files end up at their original paths, but the doctored one is placed in
        a directory named after the lower-sorting of its two parent biosamples.
        """
        self._index_bundles()
        actual = {}
        for hit in self._get_all_hits():
            qualifier, doc_type = self._parse_index_name(hit)
            if qualifier == 'files' and doc_type is DocumentType.aggregate:
                contents = one(hit['_source']['contents']['files'])
                entity_id = json_str(contents['document_id'])
                if entity_id in self.expected_paths:
                    actual[entity_id] = (hit['_source']['num_contributions'],
                                         contents['file_path'])
        expected = {
            entity_id: (2, path)
            for entity_id, path in self.expected_paths.items()
        }
        self.assertEqual(expected, actual)


class TestAnvilHubActivity(AnvilCannedBundleTestCase,
                           TDRPluginTestCase[tdr_anvil.Plugin],
                           DUOSTestCase):
    """
    Primary bundles of biosamples used by activities that also use other
    biosamples. The canned tables were taken from the MOHD snapshot, in which
    a few such activities each use hundreds of biosamples and generate
    hundreds of files. The tables were stripped down to three biosamples,
    their donors and the donors' diagnoses, two of these activities and one
    ordinary activity, and the files generated by them. The biosamples,
    donors and diagnoses were redacted, except for their primary and foreign
    keys. The files were stripped of their paths, so that the paths are
    backfilled.
    """

    source = Source(
        config=SourceConfig(mirror=False),
        ref=TDRSourceRef(
            id='79591509-9db1-4b16-812e-db31c5caf086',
            spec=TDRSourceSpec.parse('tdr:bigquery:gcp:test_anvil_project:'
                                     'ANVIL_MOHD_GRU_R1_20261110_ANV6_202611101604'),
            prefix=Prefix.parse('/0')
        )
    )

    #: The primary keys of the three biosamples
    #:
    b1, b2, b3 = (
        '79860a38-62eb-4e8f-f665-9a300f5a101c',
        '190882fa-0d8a-0685-3edd-6ce187dc5e2d',
        '013f5f65-8f4d-3997-ac45-fa4245110a2e'
    )

    #: The primary keys and names of the files generated by an activity that
    #: used biosamples b1 and b2
    #:
    lipidomics = {
        '72d53b08-c3b6-3efa-80e5-2b826beadedd': 'MOHD_EL100939_quant_v0.tsv',
        '2b2b78c6-a08b-3c96-b0d7-7a787266b5a2': 'MOHD_EL101183_quant_v0.tsv'
    }

    #: The primary keys and names of the files generated by an activity that
    #: used biosamples b2 and b3
    #:
    exposomics = {
        '16edd765-7b1b-36c4-89a8-a07da45ad020': 'MOHD_EE100716_pfas-concentrations_v0.tsv',
        '3d1c29ef-df0a-341c-94c3-616212b243a2': 'MOHD_EE100733_pfas-concentrations_v0.tsv'
    }

    #: The primary key and name of the file generated by an activity that used
    #: only biosample b1
    #:
    metabolomics = {
        'b8d15a39-99c7-3101-bd25-8f9042070457': 'MOHD_EM100032_positive.mzML.gz'
    }

    @classmethod
    def _plugin_cls(cls) -> Type[tdr_anvil.Plugin]:
        return tdr_anvil.Plugin

    def _keys(self, bundle: TDRAnvilBundle, entity_type: str) -> set[str]:
        pk_column = entity_type.removeprefix('anvil_') + '_id'
        return {
            json_str(metadata[pk_column])
            for entity, metadata in bundle.entities.items()
            if entity.entity_type == entity_type
        }

    def test_bundles(self):
        """
        A primary bundle contains every file derived from its biosample, and
        every activity those files were generated by, but none of the other
        biosamples used by those activities. Each of these files is placed in
        the directory of the lowest of the biosamples its activity used, no
        matter which bundle the file is taken from.
        """
        self._mock_normal_duos()
        self._make_mock_tables(self.source.ref)
        files = {
            self.b1: self.lipidomics.keys() | self.metabolomics.keys(),
            self.b2: self.lipidomics.keys() | self.exposomics.keys(),
            self.b3: self.exposomics.keys()
        }
        names = self.lipidomics | self.exposomics | self.metabolomics
        directories = {
            **dict.fromkeys(self.lipidomics, min(self.b1, self.b2)),
            **dict.fromkeys(self.exposomics, min(self.b2, self.b3)),
            **dict.fromkeys(self.metabolomics, self.b1)
        }
        for biosample, expected_files in files.items():
            with self.subTest(biosample=biosample):
                bundle = self.plugin.fetch_bundle(self.bundle_fqid(primary_key=biosample))
                assert isinstance(bundle, TDRAnvilBundle)
                self.assertEqual({biosample}, self._keys(bundle, 'anvil_biosample'))
                self.assertEqual(expected_files, self._keys(bundle, 'anvil_file'))
                for link in bundle.graph.to_links():
                    if link.activity is not None:
                        self.assertEqual({bundle.bundle_entity}, link.inputs)
                transformer = FileTransformer(bundle=bundle, deleted=False)
                paths = {
                    json_str(file['file_id']): file['file_path']
                    for entity in bundle.entities
                    if entity.entity_type == 'anvil_file'
                    for file in [transformer._file(entity)]
                }
                expected_paths = {
                    file_id: f'{directories[file_id]}/{names[file_id]}'
                    for file_id in expected_files
                }
                self.assertEqual(expected_paths, paths)

    def _contributions(self, bundles: list[TDRAnvilBundle]) -> list[Contribution]:
        service = IndexService()
        result = []
        for bundle in bundles:
            documents = service.transform(self.catalog, bundle, delete=False)
            assert isinstance(documents, tuple), documents
            for contribution in documents[0]:
                entity = CataloguedEntityReference.for_entity(
                    self.catalog, contribution.coordinates.entity)
                coordinates = attrs.evolve(contribution.coordinates, entity=entity)
                result.append(attrs.evolve(contribution, coordinates=coordinates))
        return result

    def _inclusions(self,
                    contributions: list[Contribution]
                    ) -> Counter[tuple[EntityReference, str, str]]:
        """
        The number of contributions to each outer entity that include each of
        its inner entities, by outer entity, inner entity type and inner entity
        ID.
        """
        return Counter(
            (contribution.coordinates.entity, inner_type, json_str(inner['document_id']))
            for contribution in contributions
            for inner_type, inners in contribution.contents.items()
            for inner in json_element_mappings(inners)
        )

    def test_ownership(self):
        """
        Of the contributions to an outer entity, only one includes any given
        inner entity, except for the outer entity itself, which every
        contribution includes, unless the contents have no place for it, as is
        the case with bundles. The aggregates are the same as if every
        contribution included every inner entity linked to its outer entity.
        """
        self._mock_normal_duos()
        self._make_mock_tables(self.source.ref)
        bundles = []
        for biosample in [self.b1, self.b2, self.b3]:
            bundle = self.plugin.fetch_bundle(self.bundle_fqid(primary_key=biosample))
            assert isinstance(bundle, TDRAnvilBundle)
            bundles.append(bundle)
        owned = self._contributions(bundles)
        with patch.object(transform.BaseTransformer, '_owns', return_value=True):
            unowned = self._contributions(bundles)

        owned_inclusions, unowned_inclusions = self._inclusions(owned), self._inclusions(unowned)
        self.assertEqual(unowned_inclusions.keys(), owned_inclusions.keys())
        self.assertGreater(unowned_inclusions.total(), owned_inclusions.total())
        contributions_by_outer = Counter(
            contribution.coordinates.entity
            for contribution in owned
            if contribution.coordinates.entity.entity_type in contribution.contents
        )
        self_inclusions = {
            (outer, outer.entity_type, outer.entity_id): count
            for outer, count in contributions_by_outer.items()
        }
        self.assertLessEqual(self_inclusions.items(), owned_inclusions.items())
        duplicates = {
            (outer, inner_type, inner_id): count
            for (outer, inner_type, inner_id), count in owned_inclusions.items()
            if count > 1
        }
        self.assertLessEqual(duplicates.items(), self_inclusions.items())
        self.assertIn(len(bundles), duplicates.values())

        service = IndexService()

        def aggregates(contributions: list[Contribution]) -> dict[EntityReference, JSON]:
            contributions = sorted(contributions, key=_contribution_order)
            return {
                aggregate.coordinates.entity: aggregate.to_json()
                for aggregate in service._aggregate(contributions)
            }

        self.assertEqual(aggregates(unowned), aggregates(owned))
