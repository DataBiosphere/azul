from concurrent.futures import ThreadPoolExecutor, wait
import doctest
import json
import logging
import os
from unittest import TestCase
import warnings

from humancellatlas.data.metadata import (AgeRange,
                                          Biomaterial,
                                          Bundle,
                                          DonorOrganism,
                                          Project,
                                          SequenceFile,
                                          SpecimenFromOrganism)

from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata, dss_client
from humancellatlas.data.metadata.helpers.json import as_json


def setUpModule():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s", )
    logging.getLogger('humancellatlas').setLevel(logging.DEBUG)


class TestAccessorApi(TestCase):
    def setUp(self):
        # Suppress `sys:1: ResourceWarning: unclosed <ssl.SSLSocket fd=6, family=AddressFamily.AF_INET, ...`
        warnings.simplefilter("ignore", ResourceWarning)

    def test_one_bundle(self):
        for deployment, replica, uuid, version, age_range in [
            # A v5 bundle
            (None, 'aws', 'b2216048-7eaa-45f4-8077-5a3fb4204953', None, AgeRange(min=3628800, max=7257600)),
            # A vx primary bundle with a cell_suspension as sequencing input
            ('staging', 'aws', '3e7c6f8e-334c-41fb-a1e5-ddd9fe70a0e2', None, None),
            # A vx analysis bundle for the primary bundle with a cell_suspension as sequencing input
            ('staging', 'aws', '859a8bd2-de3c-4c78-91dd-9e35a3418972', '2018-09-20T232924.687620Z', None),
            # A vx primary bundle with a specimen_from_organism as sequencing input
            ('staging', 'aws', '3e7c6f8e-334c-41fb-a1e5-ddd9fe70a0e2', '2018-09-20T230221.622042Z', None),
            # A vx analysis bundle for the primary with a specimen_from_organism as sequencing input
            ('staging', 'aws', '859a8bd2-de3c-4c78-91dd-9e35a3418972', '2018-09-20T232924.687620Z', None),
        ]:
            with self.subTest(deployment=deployment, replica=replica, uuid=uuid, age_range=age_range):
                client = dss_client(deployment)
                version, manifest, metadata_files = download_bundle_metadata(client, replica, uuid, version)
                bundle = Bundle(uuid, version, manifest, metadata_files)
                self.assertEqual(str(bundle.uuid), uuid)
                self.assertEqual(bundle.version, version)
                self.assertEqual(1, len(bundle.projects))
                self.assertEqual({Project}, {type(e) for e in bundle.projects.values()})
                root_entities = bundle.root_entities().values()
                self.assertEqual({DonorOrganism}, {type(e) for e in root_entities})
                root_entity = next(iter(root_entities))
                self.assertRegex(root_entity.address, 'donor_organism@.*')
                self.assertIsInstance(root_entity, DonorOrganism)
                self.assertEqual(root_entity.organism_age_in_seconds, age_range)
                self.assertTrue(root_entity.sex in ('female', 'unknown'))

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

                print(json.dumps(as_json(bundle), indent=4))

                self.assertEqual({SpecimenFromOrganism}, {type(s) for s in bundle.specimens})

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
    return tests
