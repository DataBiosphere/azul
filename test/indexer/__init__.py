from copy import deepcopy
import json
import os
import threading
from typing import List, Tuple
from unittest.mock import patch
from uuid import uuid4

from azul import config
from azul.indexer import IndexWriter
from azul.plugin import Plugin
from azul.types import JSON
from es_test_case import ElasticsearchTestCase


class IndexerTestCase(ElasticsearchTestCase):

    def setUp(self):
        super().setUp()
        plugin = Plugin.load()
        self.indexer_cls = plugin.indexer_class()
        self.per_thread = threading.local()

    @property
    def hca_indexer(self):
        try:
            indexer = self.per_thread.indexer
        except AttributeError:
            indexer = self.indexer_cls()
            self.per_thread.indexer = indexer
        return indexer

    def _make_fake_notification(self, bundle_fqid) -> JSON:
        bundle_uuid, bundle_version = bundle_fqid
        return {
            "query": {
                "match_all": {}
            },
            "subscription_id": str(uuid4()),
            "transaction_id": str(uuid4()),
            "match": {
                "bundle_uuid": bundle_uuid,
                "bundle_version": bundle_version
            }
        }

    def _load_canned_file(self, bundle_fqid, extension) -> JSON:
        bundle_uuid, bundle_version = bundle_fqid
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        for suffix in '.' + bundle_version, '':
            try:
                with open(os.path.join(data_prefix, f'{bundle_uuid}{suffix}.{extension}.json'), 'r') as infile:
                    return json.load(infile)
            except FileNotFoundError:
                if not suffix:
                    raise

    def _load_canned_bundle(self, bundle_fqid) -> Tuple[List[JSON], JSON]:
        manifest = self._load_canned_file(bundle_fqid, 'manifest')
        metadata = self._load_canned_file(bundle_fqid, 'metadata')
        assert isinstance(manifest, list)
        return manifest, metadata

    def _load_canned_result(self, bundle_fqid) -> List[JSON]:
        """
        Load the canned index contents for the given canned bundle and fix the '_index' entry in each to match the
        index name used by the current deployment
        """
        expected_hits = self._load_canned_file(bundle_fqid, 'results')
        assert isinstance(expected_hits, list)
        for hit in expected_hits:
            _, _, entity_type, aggregate = config.parse_foreign_es_index_name(hit['_index'])
            hit['_index'] = config.es_index_name(entity_type, aggregate=aggregate)
        return expected_hits

    def _index_canned_bundle(self, bundle_fqid):
        manifest, metadata = self._load_canned_bundle(bundle_fqid)
        self._index_bundle(bundle_fqid, manifest, metadata)

    def _index_bundle(self, bundle_fqid, manifest, metadata):
        def mocked_get_bundle(bundle_uuid, bundle_version):
            self.assertEqual(bundle_fqid, (bundle_uuid, bundle_version))
            return deepcopy(manifest), deepcopy(metadata)

        index_writer = self._create_index_writer()
        notifaction = self._make_fake_notification(bundle_fqid)
        with patch('azul.DSSClient'):
            with patch.object(self.hca_indexer, '_get_bundle', new=mocked_get_bundle):
                self.hca_indexer.index(index_writer, notifaction)

    def _create_index_writer(self):
        return IndexWriter(refresh='wait_for', conflict_retry_limit=2, error_retry_limit=0)

    def _delete_bundle(self, bundle_fqid):
        index_writer = self._create_index_writer()
        notification = self._make_fake_notification(bundle_fqid)
        with patch('azul.DSSClient'):
            self.hca_indexer.delete(index_writer, notification)
