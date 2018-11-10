import json
import os
from typing import Any, Mapping
from unittest.mock import patch
from uuid import uuid4

from azul.plugin import Plugin
from es_test_case import ElasticsearchTestCase


class IndexerTestCase(ElasticsearchTestCase):

    def setUp(self):
        super().setUp()
        plugin = Plugin.load()
        indexer_cls = plugin.indexer_class()
        self.hca_indexer = indexer_cls(refresh='wait_for')

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

    def _get_data_files(self, filename, bundle_version, updated=False):
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        metadata_suffix = ".metadata.json"
        manifest_suffix = ".manifest.json"
        if updated:
            filename += ".updated"

        with open(os.path.join(data_prefix, filename + metadata_suffix), 'r') as infile:
            metadata = json.load(infile)

        with open(os.path.join(data_prefix, filename + manifest_suffix), 'r') as infile:
            manifest = json.load(infile)

        return bundle_version, manifest, metadata

    def _mock_index(self, test_bundle, updated=False):
        bundle_uuid, bundle_version = test_bundle
        fake_event = self._make_fake_notification(bundle_uuid, bundle_version)

        def mocked_extract_bundle(**kwargs):
            return self._get_data_files(filename=kwargs['uuid'], bundle_version=kwargs['version'], updated=updated)

        with patch('azul.DSSClient'):
            with patch('azul.indexer.download_bundle_metadata', new=mocked_extract_bundle):
                self.hca_indexer.index(fake_event)

    def _mock_delete(self, test_bundle, data_pack):
        bundle_uuid, bundle_version = test_bundle
        self._mock_index(test_bundle, data_pack)

        fake_event = self._make_fake_notification(bundle_uuid, bundle_version)
        with patch('azul.DSSClient'):
            self.hca_indexer.delete(fake_event)
