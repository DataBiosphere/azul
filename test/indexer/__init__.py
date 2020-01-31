from copy import deepcopy
import json
import os
import threading
from typing import (
    List,
    Tuple,
)
from unittest.mock import patch
from uuid import uuid4

from azul import config
from azul.indexer import (
    IndexWriter,
    Tallies,
)
from azul.plugin import Plugin
from azul.project.hca import Indexer
from azul.types import (
    AnyJSON,
    JSON,
)
from es_test_case import ElasticsearchTestCase


class IndexerTestCase(ElasticsearchTestCase):
    indexer_cls = None
    per_thread = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        plugin = Plugin.load()

        # noinspection PyAbstractClass
        class _Indexer(plugin.indexer_class()):

            def _create_writer(self) -> IndexWriter:
                writer = super()._create_writer()
                # With a single client thread, refresh=True is faster than refresh="wait_for". The latter would limit
                # the request rate to 1/refresh_interval. That's only one request per second with refresh_interval
                # being 1s.
                writer.refresh = True
                return writer

        cls.indexer_cls = _Indexer
        cls.per_thread = threading.local()

    @classmethod
    def get_hca_indexer(cls) -> Indexer:
        try:
            # One of the indexer tests uses multiple threads to facilitate concurrent indexing. Each of these threads
            # must use its own indexer instance because each one needs to be mock.patch'ed to a different canned
            # bundle.
            indexer = cls.per_thread.indexer
        except AttributeError:
            indexer = cls.indexer_cls()
            cls.per_thread.indexer = indexer
        return indexer

    @staticmethod
    def _make_fake_notification(bundle_fqid) -> JSON:
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

    @classmethod
    def _load_canned_file(cls, bundle_fqid, extension) -> JSON:
        bundle_uuid, bundle_version = bundle_fqid
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        for suffix in '.' + bundle_version, '':
            try:
                with open(os.path.join(data_prefix, f'{bundle_uuid}{suffix}.{extension}.json'), 'r') as infile:
                    return json.load(infile)
            except FileNotFoundError:
                if not suffix:
                    raise

    @classmethod
    def _load_canned_bundle(cls, bundle_fqid) -> Tuple[List[JSON], JSON]:
        manifest = cls._load_canned_file(bundle_fqid, 'manifest')
        metadata = cls._load_canned_file(bundle_fqid, 'metadata')
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

    @classmethod
    def _index_canned_bundle(cls, bundle_fqid, delete=False):
        manifest, metadata = cls._load_canned_bundle(bundle_fqid)
        cls._index_bundle(bundle_fqid, manifest, metadata, delete=delete)

    @classmethod
    def _index_bundle(cls, bundle_fqid, manifest, metadata, delete=False):
        def mocked_get_bundle(bundle_uuid, bundle_version):
            assert bundle_fqid == (bundle_uuid, bundle_version)
            return deepcopy(manifest), deepcopy(metadata)

        notification = cls._make_fake_notification(bundle_fqid)
        with patch('azul.dss.client'):
            indexer = cls.get_hca_indexer()
            with patch.object(indexer, '_get_bundle', new=mocked_get_bundle):
                method = indexer.delete if delete else indexer.index
                method(notification)

    @classmethod
    def _write_contributions(cls, bundle_fqid, manifest, metadata) -> Tallies:
        def mocked_get_bundle(bundle_uuid, bundle_version):
            assert bundle_fqid == (bundle_uuid, bundle_version)
            return deepcopy(manifest), deepcopy(metadata)

        indexer = cls.get_hca_indexer()
        notification = cls._make_fake_notification(bundle_fqid)
        with patch('azul.dss.client'):
            with patch.object(indexer, '_get_bundle', new=mocked_get_bundle):
                contributions = indexer.transform(notification, delete=False)
                return indexer.contribute(contributions)

    def _verify_sorted_lists(self, data: AnyJSON):
        """
        Traverse through an index document or service response to verify all
        lists of primitives are sorted. Fails if no lists to check are found.
        """

        def verify_sorted_lists(data_: AnyJSON, path: Tuple[str] = ()) -> int:
            if isinstance(data_, dict):
                return sum(verify_sorted_lists(val, path + (key,))
                           for key, val in data_.items())
            elif isinstance(data_, list):
                if data_:
                    if isinstance(data_[0], dict):
                        return sum(verify_sorted_lists(v, path + (k,))
                                   for val in data_
                                   for k, v in val.items())
                    elif isinstance(data_[0], (type(None), bool, int, float, str)):
                        self.assertEqual(data_,
                                         sorted(data_, key=lambda x: (x is None, x)),
                                         msg=f'Value at {path} is not sorted')
                        return 1
                    else:
                        assert False, str(type(data_[0]))
                else:
                    return 0
            elif isinstance(data_, (type(None), bool, int, float, str)):
                return 0
            else:
                assert False, str(type(data_))

        num_lists_counted = verify_sorted_lists(data)
        self.assertGreater(num_lists_counted, 0)
