from collections import (
    Counter,
)
from datetime import (
    datetime,
    timezone,
)
import doctest
from itertools import (
    chain,
)
import json
import logging
import os
from pathlib import (
    Path,
)
from tempfile import (
    TemporaryDirectory,
)
from unittest import (
    skip,
)
from uuid import (
    UUID,
)

from atomicwrites import (
    atomic_write,
)
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul_test_case import (
    AzulUnitTestCase,
)
from humancellatlas.data.metadata.api import (
    Accession,
    AgeRange,
    AnalysisFile,
    AnalysisProtocol,
    Biomaterial,
    Bundle,
    CellLine,
    CellSuspension,
    DonorOrganism,
    ImagedSpecimen,
    ImagingProtocol,
    LibraryPreparationProtocol,
    Project,
    SequenceFile,
    SequencingProtocol,
    SpecimenFromOrganism,
    SupplementaryFile,
    entity_types as api_entity_types,
)
from humancellatlas.data.metadata.helpers.json import (
    as_json,
)
from humancellatlas.data.metadata.helpers.staging_area import (
    CannedStagingAreaFactory,
)


# noinspection PyPep8Naming
def setUpModule():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s', )
    logging.getLogger('humancellatlas').setLevel(logging.DEBUG)


class TestAccessorApi(AzulUnitTestCase):

    def _rename_keys(self, d, **kwargs):
        for new_name, old_name in kwargs.items():
            assert new_name != old_name
            if old_name in d:
                d[new_name] = d[old_name]
                del d[old_name]

    def test_lymphocytes(self):
        self._test_example_bundle(directory='CD4+ cytotoxic T lymphocytes',
                                  age_range=AgeRange(min=567648000.0, max=1892160000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'Human Cell Atlas wrangler', 'external curator'},
                                  storage_methods={'frozen, liquid nitrogen'},
                                  preservation_methods={'cryopreservation, other'},
                                  library_construction_methods={'Smart-seq2'},
                                  selected_cell_types={'TEMRA'})

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
                                  selected_cell_types={'neural cell'})

    def test_mouse(self):
        self._test_example_bundle(directory='Mouse Melanoma',
                                  age_range=AgeRange(3628800.0, 7257600.0),
                                  diseases={'subcutaneous melanoma'},
                                  project_roles={None, 'Human Cell Atlas wrangler', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'Smart-seq2'},
                                  selected_cell_types={'CD11b+ Macrophages/monocytes'})

    def test_pancreas(self):
        self._test_example_bundle(directory='Single cell transcriptome analysis of human pancreas',
                                  age_range=AgeRange(662256000.0, 662256000.0),
                                  diseases={'normal'},
                                  project_roles={None, 'external curator', 'Human Cell Atlas wrangler'},
                                  library_construction_methods={'smart-seq2'},
                                  selected_cell_types={'pancreatic A cell'})

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
                                  selected_cell_types={'bone marrow hematopoietic cell'})

    def _test_example_bundle(self, directory, **kwargs):
        uuid = 'b2216048-7eaa-45f4-8077-5a3fb4204953'
        version = '2018-08-03T082009.272868Z'
        canning_directory = os.path.join('examples', directory)
        manifest, metadata_files = self._canned_bundle(canning_directory, uuid, version)
        self.assertIsNotNone(manifest)
        self._assert_bundle(uuid=uuid,
                            version=version,
                            manifest=manifest,
                            metadata_files=metadata_files,
                            **kwargs)

    def _canned_bundle_path(self, directory, uuid, version):
        return os.path.join(os.path.dirname(__file__), 'cans', directory, uuid, version)

    def _can_bundle(self, directory, uuid, version, manifest, metadata_files):  # pragma: no cover
        """
        Save a bundle's manifest & metadata files to a local directory
        """
        dir_path = self._canned_bundle_path(directory, uuid, version)
        os.makedirs(dir_path, exist_ok=True)
        with atomic_write(os.path.join(dir_path, 'manifest.json'), overwrite=True) as f:
            json.dump(manifest, f)
        with atomic_write(os.path.join(dir_path, 'metadata.json'), overwrite=True) as f:
            json.dump(metadata_files, f)

    def _canned_bundle(self, directory, uuid, version):
        """
        Load a previously canned bundle
        """
        dir_path = self._canned_bundle_path(directory, uuid, version)
        if os.path.isdir(dir_path):
            with open(os.path.join(dir_path, 'manifest.json')) as f:
                manifest = json.load(f)
            with open(os.path.join(dir_path, 'metadata.json')) as f:
                metadata_files = json.load(f)
            return manifest, metadata_files
        else:
            return None, None

    def test_v5_bundle(self):
        """
        A v5 bundle in production
        """
        self._test_bundle(uuid='b2216048-7eaa-45f4-8077-5a3fb4204953',
                          version='2018-03-29T142048.835519Z',
                          age_range=AgeRange(3628800.0, 7257600.0),
                          diseases={'subcutaneous melanoma'}),

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip('Test bundle no longer exists on staging')
    def test_vx_primary_cs_bundle(self):
        """
        A vx primary bundle with a cell_suspension as sequencing input
        """
        self._test_bundle(uuid='3e7c6f8e-334c-41fb-a1e5-ddd9fe70a0e2',
                          version=None,
                          deployment='staging',
                          diseases={'glioblastoma'})

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip('Test bundle no longer exists on staging')
    def test_vx_analysis_cs_bundle(self):
        """
        A vx analysis bundle for the primary bundle with a cell_suspension as
        sequencing input
        """
        self._test_bundle(uuid='859a8bd2-de3c-4c78-91dd-9e35a3418972',
                          version='2018-09-20T232924.687620Z',
                          deployment='staging',
                          diseases={'glioblastoma'}),

    # TODO: Use bundle from production to fix test broken by missing bundle
    @skip('Test bundle no longer exists on staging')
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
        A bundle with preservation_method and storage_method fields and a
        donor_organism document with a development_stage field.
        """
        bundle = self._test_bundle(uuid='68bdc676-c442-4581-923e-319c1c2d9018',
                                   version='2018-10-07T130111.835234Z',
                                   deployment='staging',
                                   age_range=AgeRange(min=567648000.0, max=1892160000.0),
                                   diseases={'normal'},
                                   project_roles={'Human Cell Atlas wrangler', None, 'external curator'},
                                   storage_methods={'frozen, liquid nitrogen'},
                                   preservation_methods={'cryopreservation, other'},
                                   library_construction_methods={'Smart-seq2'},
                                   selected_cell_types={'TEMRA'},
                                   ncbi_taxon_ids={9606})
        donors = [bio for bio in bundle.biomaterials.values() if isinstance(bio, DonorOrganism)]
        donor_organism = one(donors)
        self.assertEqual(donor_organism.development_stage, 'adult')

    def test_ontology_label_field(self):
        """
        A bundle with a library_preparation_protocol that has a
        library_construction_approach ontology and nucleic_acid_source field.
        """
        bundle = self._test_bundle(uuid='6b498499-c5b4-452f-9ff9-2318dbb86000',
                                   version='2019-01-03T163633.780215Z',
                                   age_range=AgeRange(1734480000.0, 1860624000.0),
                                   diseases={'normal'},
                                   project_roles={None, 'principal investigator', 'Human Cell Atlas wrangler'},
                                   library_construction_methods={'10X v2 sequencing'},
                                   selected_cell_types={'neural cell'})
        protocols = [p for p in bundle.protocols.values() if isinstance(p, LibraryPreparationProtocol)]
        protocol = one(protocols)
        self.assertEqual(protocol.nucleic_acid_source, 'single cell')

    def test_accessions_fields(self):
        self._test_bundle(uuid='eca05046-3dad-4e45-b86c-8720f33a5dde',
                          version='2019-03-17T220646.332108Z',
                          deployment='staging',
                          diseases={'H syndrome'},
                          project_roles={'principal investigator'},
                          age_range=AgeRange(630720000.0, 630720000.0),
                          library_construction_methods={'10X v2 sequencing'},
                          insdc_project_accessions={'SRP000000', 'SRP000001'},
                          geo_series_accessions={'GSE00000'},
                          array_express_accessions={'E-AAAA-00'},
                          insdc_study_accessions={'PRJNA000000'},
                          accessions={
                              Accession('insdc_project', 'SRP000000'),
                              Accession('insdc_project', 'SRP000001'),
                              Accession('geo_series', 'GSE00000'),
                              Accession('array_express', 'E-AAAA-00'),
                              Accession('insdc_study', 'PRJNA000000')
                          })

    def test_imaging_bundle(self):
        self._test_bundle(uuid='94f2ba52-30c8-4de0-a78e-f95a3f8deb9c',
                          version='2019-04-03T103426.471000Z',
                          deployment='staging',
                          diseases=set(),
                          selected_cell_types=None,
                          project_roles=set(),
                          age_range=AgeRange(min=4838400.0, max=4838400.0),
                          is_sequencing_bundle=False,
                          storage_methods={'fresh'},
                          preservation_methods={'fresh'},
                          slice_thickness=[20.0])

    def test_file_core_bundle(self):
        """
        Verify file_core.content_description, provenance.submitter_id,
        provenance.submission_date, and provenance.update_date
        """
        bundle = self._test_bundle(uuid='86e7b58e-b9f0-4020-8b34-c61d6da02d44',
                                   version='2019-09-20T103932.395795Z',
                                   deployment='prod',
                                   diseases={'normal'},
                                   selected_cell_types={'neuron'},
                                   project_roles={'data curator', 'experimental scientist', 'principal investigator'},
                                   age_range=AgeRange(min=2302128000.0, max=2302128000.0),
                                   library_construction_methods={"10x 3' v3 sequencing"},
                                   content_description={'DNA sequence'})
        submitter_id_counts = Counter(file.submitter_id for file in bundle.files.values())
        submitter_id_expected = {
            None: 3,  # Sequence files without a submitter_id field
            '67a720af-4482-4619-81d7-3693b2d3cc4c': 4  # Supplementary files with submitter_id
        }
        self.assertEqual(submitter_id_expected, submitter_id_counts)
        submission_date_counts = Counter(entity.submission_date
                                         for entity in bundle.files.values())
        submission_date_expected = {
            datetime(2019, 9, 20, 8, 29, 52, ms, tzinfo=timezone.utc): 1
            for ms in (232000, 239000, 247000, 277000, 285000, 292000, 299000)
        }
        self.assertEqual(submission_date_expected, submission_date_counts)
        update_date_counts = Counter(entity.update_date
                                     for entity in bundle.files.values())
        update_date_expected = {
            None: 1,
            datetime(2019, 9, 20, 8, 38, 58, 167000, tzinfo=timezone.utc): 2,
            datetime(2019, 9, 20, 8, 38, 58, 165000, tzinfo=timezone.utc): 2,
            datetime(2019, 9, 20, 10, 13, 35, 720000, tzinfo=timezone.utc): 1,
            datetime(2019, 9, 20, 8, 50, 13, 111000, tzinfo=timezone.utc): 1
        }
        self.assertEqual(update_date_expected, update_date_counts)

    def test_sequencing_process_paired_end(self):
        uuid = '6b498499-c5b4-452f-9ff9-2318dbb86000'
        version = '2019-01-03T163633.780215Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)
        bundle = Bundle(uuid, version, manifest, metadata_files)
        sequencing_protocols = [p for p in bundle.protocols.values() if isinstance(p, SequencingProtocol)]
        self.assertEqual(len(sequencing_protocols), 1)
        self.assertEqual(sequencing_protocols[0].paired_end, True)

    def _test_bundle(self,
                     uuid,
                     version,
                     deployment='prod',
                     **assertion_kwargs
                     ) -> Bundle:

        manifest, metadata_files = self._canned_bundle(deployment, uuid, version)

        return self._assert_bundle(uuid=uuid,
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
                       selected_cell_types=frozenset(),
                       accessions=frozenset(),
                       insdc_project_accessions=frozenset(),
                       geo_series_accessions=frozenset(),
                       array_express_accessions=frozenset(),
                       insdc_study_accessions=frozenset(),
                       is_sequencing_bundle=True,
                       slice_thickness=None,
                       ncbi_taxon_ids=None,
                       content_description=None) -> Bundle:
        bundle = Bundle(uuid, version, manifest, metadata_files)

        # Every data file's manifest entry should be referenced by a metadata
        # entity that describes the data file. id() is used to work around the
        # fact that dict instances aren't hashable and to ensure that no
        # redundant copies are made.
        self.assertEqual(set(id(f.manifest_entry.json) for f in bundle.files.values()),
                         set(id(me) for me in manifest if not me['indexed']))

        biomaterials = bundle.biomaterials.values()

        if ncbi_taxon_ids is not None:
            self.assertSetEqual(ncbi_taxon_ids, set(chain(*(b.ncbi_taxon_id for b in biomaterials))))

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

        if selected_cell_types is not None:
            cell_suspension = next(x for x in bundle.biomaterials.values() if isinstance(x, CellSuspension))
            self.assertEqual(CellSuspension, type(cell_suspension))
            self.assertEqual(selected_cell_types, cell_suspension.selected_cell_types)
            # noinspection PyDeprecation
            self.assertEqual(cell_suspension.selected_cell_types, cell_suspension.selected_cell_type)
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
        self.assertEqual(accessions, project.accessions)

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

        if is_sequencing_bundle:
            sequencing_input = bundle.sequencing_input
            self.assertGreater(len(sequencing_input), 0,
                               'There should be at least one sequencing input')
            self.assertEqual(len(set(si.document_id for si in sequencing_input)), len(sequencing_input),
                             'Sequencing inputs should be distinct entities')
            self.assertEqual(len(set(si.biomaterial_id for si in sequencing_input)), len(sequencing_input),
                             'Sequencing inputs should have distinct biomaterial IDs')
            self.assertTrue(all(isinstance(si, Biomaterial) for si in sequencing_input),
                            'All sequencing inputs should be instances of Biomaterial')
            sequencing_input_schema_names = set(si.schema_name for si in sequencing_input)
            self.assertTrue({'cell_suspension', 'specimen_from_organism'}.issuperset(sequencing_input_schema_names),
                            'The sequencing inputs in the test bundle are of specific schemas')

            sequencing_output = bundle.sequencing_output
            self.assertGreater(len(sequencing_output), 0,
                               'There should be at least one sequencing output')
            self.assertEqual(len(set(so.document_id for so in sequencing_output)), len(sequencing_output),
                             'Sequencing outputs should be distinct entities')
            self.assertTrue(all(isinstance(so, SequenceFile) for so in sequencing_output),
                            'All sequencing outputs should be instances of SequenceFile')
            self.assertTrue(all(so.manifest_entry.name.endswith('.fastq.gz') for so in sequencing_output),
                            'All sequencing outputs in the test bundle are fastq files.')

        has_specimens = storage_methods or preservation_methods
        specimen_types = {type(s) for s in bundle.specimens}
        self.assertEqual({SpecimenFromOrganism} if has_specimens else set(), specimen_types)

        self.assertEqual(storage_methods, {s.storage_method for s in bundle.specimens})
        self.assertEqual(preservation_methods, {s.preservation_method for s in bundle.specimens})

        if has_specimens:
            # noinspection PyDeprecation
            self.assertRaises(AttributeError, lambda: bundle.specimens[0].organ_part)

        # Prove that as_json returns a valid JSON structure (no cycles, correct types, etc.)
        self.assertTrue(isinstance(json.dumps(as_json(bundle), default=str), str))

        library_prep_protos = [p for p in bundle.protocols.values() if isinstance(p, LibraryPreparationProtocol)]
        library_prep_proto_types = {type(p) for p in library_prep_protos}
        has_library_preps = library_construction_methods != set() or len(library_prep_protos) > 0
        self.assertEqual({LibraryPreparationProtocol} if has_library_preps else set(), library_prep_proto_types)
        self.assertEqual(library_construction_methods, {p.library_construction_method for p in library_prep_protos})
        # noinspection PyDeprecation
        self.assertEqual(library_construction_methods, {p.library_construction_approach for p in library_prep_protos})

        if slice_thickness is not None:
            self.assertEqual(slice_thickness,
                             [s.slice_thickness for s in bundle.entities.values() if isinstance(s, ImagedSpecimen)])

        if content_description is not None:
            self.assertSetEqual(content_description,
                                set(chain.from_iterable(file.content_description for file in bundle.files.values())))

        return bundle

    def test_canned_staging_area(self):
        remote_url = furl('https://github.com/HumanCellAtlas/schema-test-data.git')
        staging_area_path = Path('tests')
        ref = 'eb93f83b'
        with TemporaryDirectory() as tmpdir:
            factory = CannedStagingAreaFactory.clone_remote(remote_url,
                                                            Path(tmpdir),
                                                            ref)
            staging_area = factory.load_staging_area(staging_area_path)
        self.assertGreater(len(staging_area.links), 0)
        for link_id in staging_area.links:
            with self.subTest(link_id=link_id):
                bundle = staging_area.get_bundle(link_id)
                self.assertEqual(bundle.uuid, UUID(link_id))
                project = bundle.projects[UUID('90bf705c-d891-5ce2-aa54-094488b445c6')]
                self.assertEqual(project.estimated_cell_count, 10000)

    def test_analysis_protocol(self):
        uuid = 'ffee7f29-5c38-461a-8771-a68e20ec4a2e'
        version = '2019-02-02T065454.662896Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)
        bundle = Bundle(uuid, version, manifest, metadata_files)
        analysis_protocols = [p for p in bundle.protocols.values() if isinstance(p, AnalysisProtocol)]
        self.assertEqual(len(analysis_protocols), 1)
        self.assertEqual(str(analysis_protocols[0].document_id), 'bb17ee61-193e-4ae1-a014-4f1b1c19b8b7')
        self.assertEqual(analysis_protocols[0].protocol_id, 'smartseq2_v2.2.0')
        self.assertEqual(analysis_protocols[0].protocol_name, None)

    def test_imaging_protocol(self):
        uuid = '94f2ba52-30c8-4de0-a78e-f95a3f8deb9c'
        version = '2019-04-03T103426.471000Z'
        manifest, metadata_files = self._canned_bundle('staging', uuid, version)
        bundle = Bundle(uuid, version, manifest, metadata_files)
        imaging_protocol = one(p for p in bundle.protocols.values() if isinstance(p, ImagingProtocol))
        self.assertEqual(len(imaging_protocol.probe), 240)
        assay_types = {probe.assay_type for probe in imaging_protocol.probe}
        self.assertEqual(assay_types, {'in situ sequencing'})

    def test_cell_line(self):
        uuid = 'ffee3a9b-14de-4dda-980f-c08092b2dabe'
        version = '2019-04-17T175706.867000Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)
        bundle = Bundle(uuid, version, manifest, metadata_files)
        cell_lines = [cl for cl in bundle.biomaterials.values() if isinstance(cl, CellLine)]
        self.assertEqual(len(cell_lines), 1)
        self.assertEqual(str(cell_lines[0].document_id), '961092cd-dcff-4b59-a0d2-ceeef0aece74')
        self.assertEqual(cell_lines[0].biomaterial_id, 'cell_line_at_day_54')
        self.assertEqual(cell_lines[0].has_input_biomaterial, None)
        self.assertEqual(cell_lines[0].type, 'stem cell-derived')
        # noinspection PyDeprecation
        self.assertEqual(cell_lines[0].type, cell_lines[0].cell_line_type)
        self.assertEqual(cell_lines[0].model_organ, 'brain')

    def test_links_json_v2_0_0(self):
        """
        Test a bundle with a v2.0.0 links.json and supplementary_file links
        """
        uuid = 'cc0b5aa4-9f66-48d2-aa4f-ed019d1c9439'
        version = '2019-05-15T222432.561000Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)
        bundle = Bundle(uuid, version, manifest, metadata_files)
        for expected_count, link_type in [(6, 'process_link'), (2, 'supplementary_file_link')]:
            actual_count = sum(1 for link in bundle.links if link.link_type == link_type)
            self.assertEqual(expected_count, actual_count)
        for link in bundle.links:
            self.assertIn(link.source_type, api_entity_types)
            self.assertIn(link.source_id, bundle.entities)
            self.assertIsInstance(bundle.entities[link.source_id], api_entity_types[link.source_type])
            self.assertIn(link.destination_type, api_entity_types)
            self.assertIn(link.destination_id, bundle.entities)
            self.assertIsInstance(bundle.entities[link.destination_id], api_entity_types[link.destination_type])

    def test_project_fields(self):
        uuid = '68bdc676-c442-4581-923e-319c1c2d9018'
        version = '2018-10-07T130111.835234Z'
        manifest, metadata_files = self._canned_bundle('staging', uuid, version)

        def assert_bundle():
            bundle = Bundle(uuid, version, manifest, metadata_files)
            project = bundle.projects[UUID('519b58ef-6462-4ed3-8c0d-375b54f53c31')]
            self.assertEqual(len(project.publications), 1)
            publication = project.publications.pop()
            title = 'Precursors of human CD4+ cytotoxic T lymphocytes identified by single-cell transcriptome analysis.'
            self.assertEqual(publication.title, title)
            # noinspection PyDeprecation
            self.assertEqual(publication.doi, '10.1126/sciimmunol.aan8664')
            self.assertEqual(publication.official_hca, None)
            self.assertEqual(publication.title, publication.publication_title)
            self.assertEqual(publication.url, 'http://immunology.sciencemag.org/content/3/19/eaan8664.long')
            # noinspection PyDeprecation
            self.assertEqual(publication.url, publication.publication_url)
            project_roles = {c.project_role for c in project.contributors}
            self.assertEqual(project_roles, {None, 'external curator', 'Human Cell Atlas wrangler'})
            supplementary_links = {'https://www.ebi.ac.uk/gxa/sc/experiments/E-GEOD-106540/Results'}
            self.assertEqual(project.supplementary_links, supplementary_links)

        assert_bundle()

        for publication in metadata_files['project_0.json']['publications']:
            self._rename_keys(publication, title='publication_title', url='publication_url')
        for contributor in metadata_files['project_0.json']['contributors']:
            if 'project_role' in contributor:
                contributor['project_role'] = dict(text=contributor['project_role'])

        assert_bundle()

    def test_project_contact(self):
        uuid = '6b498499-c5b4-452f-9ff9-2318dbb86000'
        version = '2019-01-03T163633.780215Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)

        def assert_bundle():
            bundle = Bundle(uuid, version, manifest, metadata_files)
            project = bundle.projects[UUID('d96c2451-6e22-441f-a3e6-70fd0878bb1b')]
            self.assertEqual(len(project.contributors), 5)
            expected_names = {
                'Sabina,,Kanton',
                'Barbara,,Treutlein',
                'J,Gray,Camp',
                'Mallory,Ann,Freeberg',
                'Zhisong,,He'
            }
            self.assertEqual({c.name for c in project.contributors}, expected_names)
            # noinspection PyDeprecation
            self.assertEqual({c.contact_name for c in project.contributors}, expected_names)

        assert_bundle()

        for contributor in metadata_files['project_0.json']['contributors']:
            self._rename_keys(contributor, name='contact_name')

        assert_bundle()

    def test_file_format(self):
        uuid = '6b498499-c5b4-452f-9ff9-2318dbb86000'
        version = '2019-01-03T163633.780215Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)

        def assert_bundle():
            bundle = Bundle(uuid, version, manifest, metadata_files)
            self.assertEqual(len(bundle.files), 6)
            for file in bundle.files.values():
                if isinstance(file, SequenceFile):
                    self.assertEqual(file.format, 'fastq.gz')
                if isinstance(file, SupplementaryFile):
                    self.assertEqual(file.format, 'pdf')
                # noinspection PyDeprecation
                self.assertEqual(file.format, file.file_format)

        assert_bundle()

        for file_name, file_content in metadata_files.items():
            if file_name.startswith('sequence_file_') or file_name.startswith('supplementary_file_'):
                self._rename_keys(file_content['file_core'], format='file_format')

        assert_bundle()

    def test_link_destination_type(self):
        uuid = '6b498499-c5b4-452f-9ff9-2318dbb86000'
        version = '2019-01-03T163633.780215Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)

        def assert_bundle():
            bundle = Bundle(uuid, version, manifest, metadata_files)
            destination_types = {link.destination_type for link in bundle.links}
            expected_types = {
                'library_preparation_protocol',
                'sequencing_protocol',
                'dissociation_protocol',
                'differentiation_protocol',
                'ipsc_induction_protocol',
                'biomaterial',
                'process',
                'file'
            }
            self.assertEqual(destination_types, expected_types)

        assert_bundle()

        for link in metadata_files['links.json']['links']:
            for protocol in link['protocols']:
                self._rename_keys(protocol, type='protocol_type')

        assert_bundle()

    def test_missing_mandatory_checksums(self):
        uuid = '404f9663-21c6-49ff-afd0-8cfeff816949'
        checksums = []
        cases = [{}, {'crc32c': None}, {'crc32c': 'a'}, {'crc32c': 'a', 'sha1': None}]
        for case in cases:
            with self.assertRaises(TypeError) as cm:
                Bundle(uuid=uuid,
                       version='',
                       manifest=[
                           {
                               'uuid': uuid,
                               'version': '',
                               'name': '',
                               'size': 0,
                               'indexed': True,
                               'content-type': '',
                               **case
                           }
                       ],
                       metadata_files={})
            self.assertEqual(cm.exception.args[0], 'Property cannot be absent or None')
            checksums.append(cm.exception.args[1])
        self.assertEqual(['crc32c', 'crc32c', 'sha256', 'sha256'], checksums)

    def test_name_substitution(self):
        uuid = 'ffee7f29-5c38-461a-8771-a68e20ec4a2e'
        version = '2019-02-02T065454.662896Z'
        manifest, metadata_files = self._canned_bundle('prod', uuid, version)

        files_before = [f['name'] for f in manifest]
        with_bang_before = set(f for f in files_before if '!' in f)
        expected_bang_before = {
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!.zattrs',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!.zgroup',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_id!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_id!0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_numeric!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_numeric!0.0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_numeric_name!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_numeric_name!0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_string!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_string!0.0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_string_name!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!cell_metadata_string_name!0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!expression!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!expression!0.0',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!gene_id!.zarray',
            '9ea49dd1-7511-48f8-be12-237e3d0690c0.zarr!gene_id!0',
        }
        self.assertEqual(expected_bang_before, with_bang_before)
        with_slash_before = set(f for f in files_before if '/' in f)
        self.assertEqual(set(), with_slash_before)

        bundle = Bundle(uuid, version, manifest, metadata_files)

        expected_slash_after = set(f1.replace('!', '/') for f1 in with_bang_before)
        entity_json_file_names = set(e.json['file_core']['file_name']
                                     for e in bundle.entities.values()
                                     if isinstance(e, (AnalysisFile, SequenceFile)))
        for files_after in set(bundle.manifest.keys()), entity_json_file_names:
            with_bang_after = set(f1 for f1 in files_after if '!' in f1)
            self.assertEqual(set(), with_bang_after)
            with_slash_after = set(f1 for f1 in files_after if '/' in f1)
            self.assertEqual(expected_slash_after, with_slash_after)


def load_tests(_loader, tests, _ignore):
    modules = (
        'humancellatlas.data.metadata.age_range',
        'humancellatlas.data.metadata.api',
        'humancellatlas.data.metadata.datetime',
        'humancellatlas.data.metadata.lookup',
    )
    for module in modules:
        tests.addTests(doctest.DocTestSuite(module))
    return tests
