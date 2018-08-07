from concurrent.futures import ThreadPoolExecutor, wait
import doctest
import json
import logging
import os
from unittest import TestCase
import warnings

from hca import HCAConfig
from hca.dss import DSSClient
from urllib3 import Timeout

from humancellatlas.data.metadata import AgeRange, Bundle, DonorOrganism, age_range
from humancellatlas.data.metadata.helpers.download import download_bundle_metadata
from humancellatlas.data.metadata.helpers.json import as_json


def setUpModule():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(threadName)s: %(message)s", )
    logging.getLogger('humancellatlas').setLevel(logging.DEBUG)


class TestAccessorApi(TestCase):
    def setUp(self):
        # Suppress `sys:1: ResourceWarning: unclosed <ssl.SSLSocket fd=6, family=AddressFamily.AF_INET, ...`
        warnings.simplefilter("ignore", ResourceWarning)
        # Work around https://github.com/HumanCellAtlas/dcp-cli/issues/142
        hca_config = HCAConfig("hca")
        hca_config['DSSClient'].swagger_url = 'https://dss.data.humancellatlas.org/v1/swagger.json'
        client = DSSClient(config=hca_config)
        client.timeout_policy = Timeout(connect=10, read=40)
        self.dss_client = client

    def test_one_bundle(self):
        uuid = "b2216048-7eaa-45f4-8077-5a3fb4204953"
        version = "2018-03-29T142048.835519Z"
        manifest, metadata_files = download_bundle_metadata(self.dss_client, uuid, version, 'aws')
        bundle = Bundle(uuid=uuid, version=version, manifest=manifest, metadata_files=metadata_files)
        self.assertEqual(str(bundle.uuid), uuid)
        root_entities = bundle.root_entities()
        root_entity = next(iter(root_entities.values()))
        self.assertEqual(root_entity.address, 'donor_organism@bf8492ad-1d45-46aa-9fe9-67058b8c2410')
        root_entity_json = as_json(root_entity)
        assert isinstance(root_entity, DonorOrganism)
        self.assertEqual(root_entity.organism_age_in_seconds, AgeRange(min=3628800, max=7257600))
        print(json.dumps(root_entity_json, indent=4))
        # FIXME: more assertions

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
        # noinspection PyUnresolvedReferences
        response = self.dss_client.post_search.iterate(es_query=self.dss_subscription_query, replica="aws")
        fqids = [r['bundle_fqid'] for r in response]

        def to_json(fqid):
            uuid, _, version = fqid.partition('.')
            manifest, metadata_files = download_bundle_metadata(client=self.dss_client,
                                                                uuid=uuid,
                                                                version=version,
                                                                replica='aws',
                                                                num_workers=0)
            bundle = Bundle(uuid=uuid, version=version, manifest=manifest, metadata_files=metadata_files)
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

        # FIXME: How to assert JSON output?

        self.assertEqual({}, errors)


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(age_range))
    return tests
