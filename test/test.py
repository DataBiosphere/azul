from concurrent.futures import ThreadPoolExecutor, wait
import doctest
import json
import logging
import os
from unittest import TestCase
import warnings

from humancellatlas.data.metadata import (AgeRange,
                                          Bundle,
                                          DonorOrganism,
                                          age_range,
                                          SpecimenFromOrganism,
                                          CellSuspension,
                                          Project)

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
            (None, 'aws', 'b2216048-7eaa-45f4-8077-5a3fb4204953', None, AgeRange(min=3628800, max=7257600)),  # v5
            ('integration', 'aws', '1e276fdd-d885-4a18-b5b8-df33f1347c1a', '2018-08-03T082009.272868Z', None)  # vx
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
                self.assertEqual({CellSuspension}, {type(x) for x in bundle.sequencing_input})
                self.assertEqual({SpecimenFromOrganism}, {type(s) for s in bundle.specimens})
                self.assertTrue(all(bm.schema_name == 'cell_suspension' for bm in bundle.sequencing_input))
                self.assertTrue(all(f.manifest_entry.name.endswith('.fastq.gz') for f in bundle.sequencing_output))
                print(json.dumps(as_json(bundle), indent=4))

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
    tests.addTests(doctest.DocTestSuite(age_range))
    return tests
