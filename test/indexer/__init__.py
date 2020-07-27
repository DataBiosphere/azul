from copy import (
    deepcopy,
)
from dataclasses import (
    replace,
)
import json
import os
from typing import (
    Optional,
    Tuple,
    Union,
    cast,
)

from azul import (
    CatalogName,
    IndexName,
    config,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
    IndexWriter,
    Tallies,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from es_test_case import (
    ElasticsearchTestCase,
)


class ForcedRefreshIndexService(IndexService):

    def _create_writer(self, catalog: Optional[CatalogName]) -> IndexWriter:
        writer = super()._create_writer(catalog)
        # With a single client thread, refresh=True is faster than
        # refresh="wait_for". The latter would limit the request rate to
        # 1/refresh_interval. That's only one request per second with
        # refresh_interval being 1s.
        writer.refresh = True
        return writer


class IndexerTestCase(ElasticsearchTestCase):
    index_service: IndexService

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.index_service = ForcedRefreshIndexService()

    @classmethod
    def _load_canned_file(cls, bundle_fqid: BundleFQID, extension: str) -> Union[MutableJSONs, MutableJSON]:
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
    def _load_canned_bundle(cls, bundle_fqid: BundleFQID) -> Bundle:
        manifest = cast(MutableJSONs, cls._load_canned_file(bundle_fqid, 'manifest'))
        metadata_files = cls._load_canned_file(bundle_fqid, 'metadata')
        assert isinstance(manifest, list)
        return Bundle.for_fqid(bundle_fqid, manifest=manifest, metadata_files=metadata_files)

    def _load_canned_result(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        """
        Load the canned index documents for the given canned bundle and fix the
        '_index' entry in each to match the index name in the current deployment
        """
        expected_hits = self._load_canned_file(bundle_fqid, 'results')
        assert isinstance(expected_hits, list)
        for hit in expected_hits:
            index_name = IndexName.parse(hit['_index'])
            hit['_index'] = config.es_index_name(catalog=self.catalog,
                                                 entity_type=index_name.entity_type,
                                                 aggregate=index_name.aggregate)
        return expected_hits

    @classmethod
    def _index_canned_bundle(cls, bundle_fqid: BundleFQID, delete=False):
        bundle = cls._load_canned_bundle(bundle_fqid)
        cls._index_bundle(bundle, delete=delete)

    @classmethod
    def _index_bundle(cls, bundle: Bundle, delete: bool = False):
        if delete:
            cls.index_service.delete(cls.catalog, bundle)
        else:
            cls.index_service.index(cls.catalog, bundle)

    @classmethod
    def _write_contributions(cls, bundle: Bundle) -> Tallies:
        bundle = replace(bundle,
                         manifest=deepcopy(bundle.manifest),
                         metadata_files=deepcopy(bundle.metadata_files))
        contributions = cls.index_service.transform(cls.catalog, bundle, delete=False)
        return cls.index_service.contribute(cls.catalog, contributions)

    def _verify_sorted_lists(self, data: AnyJSON):
        """
        Traverse through an index document or service response to verify all
        lists of primitives are sorted. Fails if no lists to check are found.
        """

        def verify_sorted_lists(data_: AnyJSON, path: Tuple[str, ...] = ()) -> int:
            if isinstance(data_, dict):
                return sum(verify_sorted_lists(val, (*path, key))
                           for key, val in cast(JSON, data_).items())
            elif isinstance(data_, list):
                if data_:
                    if isinstance(data_[0], dict):
                        return sum(verify_sorted_lists(v, (*path, k))
                                   for val in cast(JSONs, data_)
                                   for k, v in val.items())
                    elif isinstance(data_[0], (type(None), bool, int, float, str)):
                        self.assertEqual(data_,
                                         sorted(data_, key=lambda x: (x is None, x)),
                                         msg=f'Value at {path} is not sorted: {data_}')
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
