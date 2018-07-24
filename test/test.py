import json
from unittest import TestCase
import warnings

from hca.dss import DSSClient

from humancellatlas.data.metadata import Bundle, as_json, DonorOrganism, AgeRange
from humancellatlas.data.metadata.helpers.download import download_bundle_metadata


class TestAccessorApi(TestCase):
    def setUp(self):
        # Suppress `sys:1: ResourceWarning: unclosed <ssl.SSLSocket fd=6, family=AddressFamily.AF_INET, ...`
        warnings.simplefilter("ignore", ResourceWarning)
        self.dss_client = DSSClient()

    def test_one_bundle(self):
        uuid = "b2216048-7eaa-45f4-8077-5a3fb4204953"
        version = "2018-03-29T142048.835519Z"
        manifest, metadata_files = download_bundle_metadata(self.dss_client, uuid, version, 'aws')
        bundle = Bundle(uuid=uuid, version=version, manifest=manifest, metadata_files=metadata_files)
        self.assertEqual(str(bundle.uuid), uuid)
        root_entities = bundle.root_entities()
        root_entity = next(iter(root_entities.values()))
        root_entity_json = as_json(root_entity)
        assert isinstance(root_entity, DonorOrganism)
        self.assertEqual(root_entity.organism_age_in_seconds, AgeRange(min=3628800, max=7257600))
        print(json.dumps(root_entity_json, indent=4))
        # FIXME: more assertions

    # TODO: test more/all bundles in DSS
