import functools
import json
import os
from typing import Mapping, Any
from unittest.mock import patch
from uuid import uuid4

from azul import config
from azul.project.hca.config import IndexProperties
from azul.project.hca.indexer import Indexer
from azul.downloader import MetadataDownloader
from es_test_case import ElasticsearchTestCase


class IndexerTestCase(ElasticsearchTestCase):
    index_properties = None
    hca_indexer = None

    _old_dss_endpoint = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.index_properties = IndexProperties(dss_url=config.dss_endpoint,
                                               es_endpoint=config.es_endpoint)
        cls.hca_indexer = Indexer(cls.index_properties)

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

    def _mock_index(self, test_bundle, updated=False):
        bundle_uuid, bundle_version = test_bundle
        fake_event = self._make_fake_notification(bundle_uuid, bundle_version)

        def mocked_extract_bundle(self_, fake_notification):
            return self._get_data_files(fake_notification["match"]["bundle_uuid"], updated=updated)

        with patch('azul.DSSClient'):
            with patch.object(MetadataDownloader, 'extract_bundle', new=mocked_extract_bundle):
                self.hca_indexer.index(fake_event)
