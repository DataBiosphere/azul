import json
import os
from typing import Mapping, Any
from unittest.mock import patch
from uuid import uuid4

from azul import config
from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from azul.downloader import MetadataDownloader
from es_test_case import AzulTestCase


class IndexerTestCase(AzulTestCase):
    index_properties = None
    hca_indexer = None

    _old_dss_endpoint = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._old_dss_endpoint = os.environ.get('AZUL_DSS_ENDPOINT')
        # FIXME: https://github.com/DataBiosphere/azul/issues/134
        # FIXME: deprecate use of production server in favor of local, farm-to-table data files
        os.environ['AZUL_DSS_ENDPOINT'] = "https://dss.data.humancellatlas.org/v1"
        cls.index_properties = IndexProperties(dss_url=config.dss_endpoint,
                                               es_endpoint=config.es_endpoint)
        cls.hca_indexer = Indexer(cls.index_properties)

    @classmethod
    def tearDownClass(cls):
        if cls._old_dss_endpoint is None:
            del os.environ['AZUL_DSS_ENDPOINT']
        else:
            os.environ['AZUL_DSS_ENDPOINT'] = cls._old_dss_endpoint
        super().tearDownClass()

    def _make_fake_notification(self, uuid: str, version: str) -> Mapping[str, Any]:
        return {
            "query": {
                "match_all": {}
            },
            "subscription_id": str(uuid4()),
            "transaction_id": str(uuid4()),
            "match": {
                "bundle_uuid": uuid,
                "bundle_version": version
            }
        }

    def _get_data_files(self, filename, updated=False):
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        metadata_suffix = ".metadata.json"
        manifest_suffix = ".manifest.json"
        if updated:
            filename += ".updated"

        with open(os.path.join(data_prefix, filename + metadata_suffix), 'r') as infile:
            metadata = json.load(infile)

        with open(os.path.join(data_prefix, filename + manifest_suffix), 'r') as infile:
            manifest = json.load(infile)

        return metadata, manifest

    def _mock_index(self, test_bundles, data_pack):
        bundle_uuid, bundle_version = test_bundles
        metadata, manifest = data_pack
        fake_event = self._make_fake_notification(bundle_uuid, bundle_version)
        with patch('azul.DSSClient'):
            with patch.object(MetadataDownloader, 'extract_bundle') as mock_method:
                mock_method.return_value = metadata, manifest
                self.hca_indexer.index(fake_event)
