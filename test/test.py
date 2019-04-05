from concurrent.futures import ThreadPoolExecutor, wait
import doctest
from itertools import chain
import json
import logging
import os
import re
from unittest import TestCase, skip
from unittest.mock import Mock
import warnings

from atomicwrites import atomic_write

from humancellatlas.data.metadata.api import (AgeRange,
                                              Biomaterial,
                                              Bundle,
                                              DonorOrganism,
                                              Project,
                                              SequenceFile,
                                              SpecimenFromOrganism,
                                              CellSuspension,
                                              LibraryPreparationProtocol,
                                              SupplementaryFile)
from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata, dss_client
from humancellatlas.data.metadata.helpers.json import as_json
from humancellatlas.data.metadata.helpers.schema_examples import download_example_bundle


def setUpModule():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s", )
    logging.getLogger('humancellatlas').setLevel(logging.DEBUG)


class TestAccessorApi(TestCase):

    def setUp(self):
        # Suppress `sys:1: ResourceWarning: unclosed <ssl.SSLSocket fd=6, family=AddressFamily.AF_INET, ...`
        warnings.simplefilter("ignore", ResourceWarning)

    def test_lymphocytes(self):
        self._test_example_bundle(directory='CD4+ cytotoxic T lymphocytes',
                                  age_range=AgeRange(min=567648000.0, max=1892160000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'Human Cell Atlas wrangler', 'external curator'},
                                  storage_methods={'frozen, liquid nitrogen'},
                                  preservation_methods={'cryopreservation, other'},
                                  library_construction_methods={'Smart-seq2'},
                                  selected_cell_type={'TEMRA'})

    def test_diabetes_pancreas(self):
        self._test_example_bundle(directory='Healthy and type 2 diabetes pancreas',
                                  age_range=AgeRange(min=1356048000.0, max=1356048000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'Human Cell Atlas wrangler', 'external curator'},
                                  library_construction_methods={'Smart-seq2'})

    def test_hpsi(self):
        self._test_example_bundle(directory='HPSI_human_cerebral_organoids',
                                  age_range=AgeRange(min=1419120000.0, max=1545264000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'principal investigator', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={"Chromium 3' Single Cell v2"},
                                  selected_cell_type={"neural cell"})

    def test_mouse(self):
        self._test_example_bundle(directory='Mouse Melanoma',
                                  age_range=AgeRange(3628800.0, 7257600.0),
                                  diseases={'subcutaneous melanoma'},
                                  project_roles={None, 'Human Cell Atlas wrangler', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'Smart-seq2'},
                                  selected_cell_type={'CD11b+ Macrophages/monocytes'})

    def test_pancreas(self):
        self._test_example_bundle(directory='Single cell transcriptome analysis of human pancreas',
                                  age_range=AgeRange(662256000.0, 662256000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'external curator', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'smart-seq2'},
                                  selected_cell_type={'pancreatic A cell'})

    def test_tissue_stability(self):
        self._test_example_bundle(directory='Tissue stability',
                                  age_range=AgeRange(1734480000.0, 1892160000.0),
                                  diseases={'normal'},
                                  storage_methods=set(),
                                  preservation_methods=set(),
                                  project_roles={None, 'Human Cell Atlas wrangler', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'10X sequencing'})

    def test_immune_cells(self):
        self._test_example_bundle(directory='1M Immune Cells',
                                  age_range=AgeRange(1639872000.0, 1639872000.0),
                                  diseases=set(),
                                  project_roles={None, 'Human Cell Atlas wrangler', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'10X sequencing'},
                                  selected_cell_type={'bone marrow hematopoietic cell'})

    def _test_example_bundle(self, directory, **kwargs):
        uuid = 'b2216048-7eaa-45f4-8077-5a3fb4204953'
        version = '2018-08-03T082009.272868Z'
        canning_directory = os.path.join('examples', directory)
        manifest, metadata_files = self._canned_bundle(canning_directory, uuid, version)
        if manifest is None:  # pragma: no cover
            manifest, metadata_files = download_example_bundle(repo='HumanCellAtlas/metadata-schema',
                                                               branch='develop',
                                                               path=f'examples/bundles/public-beta/{directory}/')
            self._can_bundle(canning_directory, uuid, version, manifest, metadata_files)
            manifest, metadata_files = self._canned_bundle(canning_directory, uuid, version)
        self._assert_bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files,
                            **kwargs)

    def _canned_bundle_path(self, directory, uuid, version):
        return os.path.join(os.path.dirname(__file__), 'cans', directory, uuid, version)

    def _can_bundle(self, directory, uuid, version, manifest, metadata_files):  # pragma: no cover
        dir_path = self._canned_bundle_path(directory, uuid, version)
        os.makedirs(dir_path, exist_ok=True)
        with atomic_write(os.path.join(dir_path, 'manifest.json'), overwrite=True) as f:
            json.dump(manifest, f)
        with atomic_write(os.path.join(dir_path, 'metadata.json'), overwrite=True) as f:
            json.dump(metadata_files, f)

    def _canned_bundle(self, directory, uuid, version):
        dir_path = self._canned_bundle_path(directory, uuid, version)
        if os.path.isdir(dir_path):
            with open(os.path.join(dir_path, 'manifest.json')) as f:
                manifest = json.load(f)
            with open(os.path.join(dir_path, 'metadata.json')) as f:
                metadata_files = json.load(f)
            return manifest, metadata_files
        else:
            return None, None

    def test_bad_content(self):
        deployment, replica, uuid = 'staging', 'aws', 'df00a6fc-0015-4ae0-a1b7-d4b08af3c5a6'
        client = dss_client(deployment)
        with self.assertRaises(TypeError) as cm:
            download_bundle_metadata(client, replica, uuid)
        self.assertRegex(cm.exception.args[0],
                         "Expecting file .* to contain a JSON object " +
                         re.escape("(<class 'dict'>), not <class 'bytes'>"))

    def test_bad_content_type(self):
        deployment, replica, uuid = 'staging', 'aws', 'df00a6fc-0015-4ae0-a1b7-d4b08af3c5a6'
        client = Mock()
        file_uuid, file_version = 'b2216048-7eaa-45f4-8077-5a3fb4204953', '2018-09-20T232924.687620Z'
        client.get_bundle.return_value = {
            'bundle': {
                'files': [
                    {
                        'name': 'name.json',
                        'uuid': file_uuid,
                        'version': file_version,
                        'indexed': True,
                        'content-type': 'bad'
                    }
                ]
            }
        }
        with self.assertRaises(NotImplementedError) as cm:
            # noinspection PyTypeChecker
            download_bundle_metadata(client, replica, uuid)
        self.assertEquals(cm.exception.args[0],
                          f"Expecting file {file_uuid}.{file_version} "
                          "to have content type 'application/json', not 'bad'")

    def test_v5_bundle(self):
        """
        A v5 bundle in production
        """
        self._test_bundle(uuid='b2216048-7eaa-45f4-8077-5a3fb4204953',
                          version='2018-03-29T142048.835519Z',
                          age_range=AgeRange(3628800.0, 7257600.0),
                          diseases={'subcutaneous melanoma'}),

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip("Test bundle no longer exists on staging")
    def test_vx_primary_cs_bundle(self):
        """
        A vx primary bundle with a cell_suspension as sequencing input
        """
        self._test_bundle(uuid='3e7c6f8e-334c-41fb-a1e5-ddd9fe70a0e2',
                          deployment='staging',
                          diseases={'glioblastoma'}),

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip("Test bundle no longer exists on staging")
    def test_vx_analysis_cs_bundle(self):
        """
        A vx analysis bundle for the primary bundle with a cell_suspension as sequencing input
        """
        self._test_bundle(uuid='859a8bd2-de3c-4c78-91dd-9e35a3418972',
                          version='2018-09-20T232924.687620Z',
                          deployment='staging',
                          diseases={'glioblastoma'}),

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip("Test bundle no longer exists on staging")
    def test_vx_analysis_specimen_bundle(self):
        """
        A vx primary bundle with a specimen_from_organism as sequencing input
        """
        self._test_bundle(uuid='3e7c6f8e-334c-41fb-a1e5-ddd9fe70a0e2',
                          version='2018-09-20T230221.622042Z',
                          deployment='staging',
                          diseases={'glioblastoma'}),

    def test_vx_specimen_v271_bundle(self):
        """
        A bundle containing a specimen_from_organism.json with a schema version of 2.7.1
        """
        self._test_bundle(uuid='70184761-70fc-4b80-8c48-f406a478d5ab',
                          version='2018-09-05T182535.846470Z',
                          deployment='staging',
                          diseases={'glioblastoma'},
                          library_construction_methods={'Smart-seq2'})

    def test_preservation_storage_bundle(self):
        """
        A bundle with preservation and storage methods provided
        """
        self._test_bundle(uuid='68bdc676-c442-4581-923e-319c1c2d9018',
                          version='2018-10-07T130111.835234Z',
                          deployment='staging',
                          age_range=AgeRange(min=567648000.0, max=1892160000.0),
                          diseases={'normal'},
                          project_roles={'Human Cell Atlas wrangler', None, 'external curator'},
                          storage_methods={'frozen, liquid nitrogen'},
                          preservation_methods={'cryopreservation, other'},
                          library_construction_methods={'Smart-seq2'},
                          selected_cell_type={'TEMRA'})

    def test_ontology_label_field(self):
        """
        A bundle in production containing a library_construction_approach field
        with "text" and "ontology_label" properties that have different values
        """
        self._test_bundle(uuid='6b498499-c5b4-452f-9ff9-2318dbb86000',
                          version='2019-01-03T163633.780215Z',
                          age_range=AgeRange(1734480000.0, 1860624000.0),
                          diseases={'normal'},
                          project_roles={None, 'principal investigator', 'Human Cell Atlas wrangler'},
                          library_construction_methods={"10X v2 sequencing"},
                          selected_cell_type={'neural cell'})

    def test_accessions_fields(self):
        self._test_bundle(uuid='eca05046-3dad-4e45-b86c-8720f33a5dde',
                          version='2019-03-17T220646.332108Z',
                          deployment='staging',
                          diseases={'H syndrome'},
                          project_roles={'principal investigator'},
                          age_range=AgeRange(630720000.0, 630720000.0),
                          library_construction_methods={'10X v2 sequencing'},
                          insdc_project_accessions={'SRP000000'},
                          geo_series_accessions={'GSE00000'},
                          array_express_accessions={'E-AAAA-00'},
                          insdc_study_accessions={'PRJNA000000'})

    def _test_bundle(self, uuid, version, replica='aws', deployment=None, **assertion_kwargs):
        canning_directory = deployment or 'prod'
        manifest, metadata_files = self._canned_bundle(canning_directory, uuid, version)
        if manifest is None:  # pragma: no cover
            client = dss_client(deployment)
            _version, manifest, metadata_files = download_bundle_metadata(client, replica, uuid, version)
            assert _version == version
            self._can_bundle(os.path.join(canning_directory), uuid, version, manifest, metadata_files)
            manifest, metadata_files = self._canned_bundle(canning_directory, uuid, version)

        self._assert_bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files,
                            **assertion_kwargs)

    def _assert_bundle(self, uuid, version, manifest, metadata_files,
                       age_range=None,
                       diseases=frozenset({None}),
                       project_roles=frozenset({None}),
                       storage_methods=frozenset({None}),
                       preservation_methods=frozenset({None}),
                       library_construction_methods=frozenset(),
                       selected_cell_type=frozenset(),
                       insdc_project_accessions=frozenset(),
                       geo_series_accessions=frozenset(),
                       array_express_accessions=frozenset(),
                       insdc_study_accessions=frozenset()):
        bundle = Bundle(uuid, version, manifest, metadata_files)
        biomaterials = bundle.biomaterials.values()
        actual_diseases = set(chain(*(bm.diseases for bm in biomaterials
                                      if isinstance(bm, (DonorOrganism, SpecimenFromOrganism)))))
        # noinspection PyDeprecation
        actual_disease = set(chain(*(bm.disease for bm in biomaterials
                                     if isinstance(bm, (DonorOrganism, SpecimenFromOrganism)))))
        self.assertEqual(actual_diseases, diseases)
        self.assertEqual(actual_diseases, actual_disease)
        self.assertEqual(str(bundle.uuid), uuid)
        self.assertEqual(bundle.version, version)
        self.assertEqual(1, len(bundle.projects))

        cell_suspension = next(x for x in bundle.biomaterials.values() if isinstance(x, CellSuspension))
        self.assertEqual(CellSuspension, type(cell_suspension))
        self.assertEqual(selected_cell_type, cell_suspension.selected_cell_type)
        # noinspection PyDeprecation
        self.assertEqual(cell_suspension.estimated_cell_count, cell_suspension.total_estimated_cells)

        project = list(bundle.projects.values())[0]
        self.assertEqual(Project, type(project))
        self.assertEqual(project_roles, {c.project_role for c in project.contributors})
        # noinspection PyDeprecation
        self.assertLessEqual(len(project.laboratory_names), len(project.contributors))
        # noinspection PyDeprecation
        self.assertEqual(project.project_short_name, project.project_shortname)

        self.assertEqual(insdc_project_accessions, project.insdc_project_accessions)
        self.assertEqual(geo_series_accessions, project.geo_series_accessions)
        self.assertEqual(array_express_accessions, project.array_express_accessions)
        self.assertEqual(insdc_study_accessions, project.insdc_study_accessions)

        root_entities = bundle.root_entities().values()
        root_entity_types = {type(e) for e in root_entities}
        self.assertIn(DonorOrganism, root_entity_types)
        self.assertTrue({DonorOrganism, SupplementaryFile}.issuperset(root_entity_types))
        root_entity = next(iter(root_entities))
        self.assertRegex(root_entity.address, 'donor_organism@.*')
        self.assertIsInstance(root_entity, DonorOrganism)
        self.assertEqual(root_entity.organism_age_in_seconds, age_range)
        self.assertTrue(root_entity.sex in ('female', 'male', 'unknown'))
        # noinspection PyDeprecation
        self.assertEqual(root_entity.sex, root_entity.biological_sex)

        sequencing_input = bundle.sequencing_input
        self.assertGreater(len(sequencing_input), 0,
                           "There should be at least one sequencing input")
        self.assertEqual(len(set(si.document_id for si in sequencing_input)), len(sequencing_input),
                         "Sequencing inputs should be distinct entities")
        self.assertEqual(len(set(si.biomaterial_id for si in sequencing_input)), len(sequencing_input),
                         "Sequencing inputs should have distinct biomaterial IDs")
        self.assertTrue(all(isinstance(si, Biomaterial) for si in sequencing_input),
                        "All sequencing inputs should be instances of Biomaterial")
        sequencing_input_schema_names = set(si.schema_name for si in sequencing_input)
        self.assertTrue({'cell_suspension', 'specimen_from_organism'}.issuperset(sequencing_input_schema_names),
                        "The sequencing inputs in the test bundle are of specific schemas")

        sequencing_output = bundle.sequencing_output
        self.assertGreater(len(sequencing_output), 0,
                           "There should be at least one sequencing output")
        self.assertEqual(len(set(so.document_id for so in sequencing_output)), len(sequencing_output),
                         "Sequencing outputs should be distinct entities")
        self.assertTrue(all(isinstance(so, SequenceFile) for so in sequencing_output),
                        "All sequencing outputs should be instances of SequenceFile")
        self.assertTrue(all(so.manifest_entry.name.endswith('.fastq.gz') for so in sequencing_output),
                        "All sequencing outputs in the test bundle are fastq files.")

        has_specimens = storage_methods or preservation_methods
        specimen_types = {type(s) for s in bundle.specimens}
        self.assertEqual({SpecimenFromOrganism} if has_specimens else set(), specimen_types)

        self.assertEqual(storage_methods, {s.storage_method for s in bundle.specimens})
        self.assertEqual(preservation_methods, {s.preservation_method for s in bundle.specimens})

        if has_specimens:
            # noinspection PyDeprecation
            self.assertRaises(AttributeError, lambda: bundle.specimens[0].organ_part)

        # Prove that as_json returns a valid JSON structure (no cycles, correct types, etc.)
        self.assertTrue(isinstance(json.dumps(as_json(bundle)), str))

        library_prep_protos = [p for p in bundle.protocols.values() if isinstance(p, LibraryPreparationProtocol)]
        library_prep_proto_types = {type(p) for p in library_prep_protos}
        has_library_preps = library_construction_methods != set() or len(library_prep_protos) > 0
        self.assertEqual({LibraryPreparationProtocol} if has_library_preps else set(), library_prep_proto_types)
        self.assertEqual(library_construction_methods, {p.library_construction_method for p in library_prep_protos})
        self.assertEqual(library_construction_methods, {p.library_construction_approach for p in library_prep_protos})

    dss_subscription_query = {
        "query": {
            "bool": {
                "must_not": [
                    {
                        "term": {
                            "admin_deleted": True
                        }
                    }
                ],
                "must": [
                    {
                        "exists": {
                            "field": "files.biomaterial_json"
                        }
                    },
                    {
                        "prefix": {
                            "uuid": "ab"
                        }
                    }
                ]
            }
        }
    }

    def test_many_bundles(self):
        client = dss_client()
        # noinspection PyUnresolvedReferences
        response = client.post_search.iterate(es_query=self.dss_subscription_query, replica="aws")
        fqids = [r['bundle_fqid'] for r in response]

        def to_json(fqid):
            uuid, _, version = fqid.partition('.')
            version, manifest, metadata_files = download_bundle_metadata(client, 'aws', uuid, version, num_workers=0)
            bundle = Bundle(uuid, version, manifest, metadata_files)
            return as_json(bundle)

        with ThreadPoolExecutor(os.cpu_count() * 16) as tpe:
            futures = {tpe.submit(to_json, fqid): fqid for fqid in fqids}
            done, not_done = wait(futures.keys())

        self.assertFalse(not_done)
        errors, bundles = {}, {}
        for future in done:
            exception = future.exception()
            if exception:
                errors[futures[future]] = exception
            else:
                bundles[futures[future]] = future.result()

        # FIXME: Assert JSON output?

        self.assertEqual({}, errors)


# noinspection PyUnusedLocal
def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite('humancellatlas.data.metadata.age_range'))
    tests.addTests(doctest.DocTestSuite('humancellatlas.data.metadata.lookup'))
    tests.addTests(doctest.DocTestSuite('humancellatlas.data.metadata.api'))
    return tests
