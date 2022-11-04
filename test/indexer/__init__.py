from copy import (
    deepcopy,
)
import json
import os
from typing import (
    Optional,
    Union,
    cast,
)

import attr
from elasticsearch.helpers import (
    scan,
)

from azul import (
    CatalogName,
    IndexName,
    config,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
    SourcedBundleFQID,
)
from azul.indexer.index_service import (
    IndexService,
    IndexWriter,
    Tallies,
)
from azul.plugins import (
    FieldPath,
)
from azul.plugins.repository.dss import (
    DSSBundle,
    DSSSourceRef,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from azul_test_case import (
    AzulUnitTestCase,
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


class CannedBundleTestCase(AzulUnitTestCase):

    @classmethod
    def _load_canned_file(cls,
                          bundle: BundleFQID,
                          extension: str
                          ) -> Union[MutableJSONs, MutableJSON]:
        def load(version):
            return cls._load_canned_file_version(uuid=bundle.uuid,
                                                 version=version,
                                                 extension=extension)

        try:
            return load(bundle.version)
        except FileNotFoundError:
            return load(None)

    @classmethod
    def _load_canned_file_version(cls,
                                  *,
                                  uuid: str,
                                  version: Optional[str],
                                  extension: str
                                  ) -> Union[MutableJSONs, MutableJSON]:
        data_prefix = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')
        suffix = '' if version is None else '.' + version
        file_name = f'{uuid}{suffix}.{extension}.json'
        with open(os.path.join(data_prefix, file_name), 'r') as infile:
            return json.load(infile)

    @classmethod
    def _load_canned_bundle(cls, bundle: SourcedBundleFQID) -> Bundle:
        manifest = cast(MutableJSONs, cls._load_canned_file(bundle, 'manifest'))
        metadata_files = cls._load_canned_file(bundle, 'metadata')
        assert isinstance(manifest, list)
        return DSSBundle(fqid=bundle,
                         manifest=manifest,
                         metadata_files=metadata_files)


mock_dss_source = 'https://test:/2'


class IndexerTestCase(ElasticsearchTestCase, CannedBundleTestCase):
    index_service: IndexService
    source = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.index_service = ForcedRefreshIndexService()
        cls.source = DSSSourceRef.for_dss_source(mock_dss_source)

    @classmethod
    def bundle_fqid(cls, *, uuid, version):
        return SourcedBundleFQID(source=cls.source,
                                 uuid=uuid,
                                 version=version)

    def _get_all_hits(self):
        # Without `preserve_order`, hits are sorted by `_doc`, which is fastest
        # but causes the `sort` field in hits to vary unpredictably, based on
        # the number of shards, for example, but also under what appear to be
        # unrelated code changes. This makes asserting test results verbatim
        # impossible. Thus we set `preserve_order` to True.
        hits = list(scan(client=self.es_client,
                         index=','.join(self.index_service.index_names(self.catalog)),
                         preserve_order=True))
        for hit in hits:
            self._verify_sorted_lists(hit)
        return hits

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
    def _index_canned_bundle(cls, bundle_fqid: SourcedBundleFQID, delete=False):
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
        bundle = attr.evolve(bundle,
                             manifest=deepcopy(bundle.manifest),
                             metadata_files=deepcopy(bundle.metadata_files))
        contributions = cls.index_service.transform(cls.catalog, bundle, delete=False)
        return cls.index_service.contribute(cls.catalog, contributions)

    def _verify_sorted_lists(self, data: AnyJSON):
        """
        Traverse through an index document or service response to verify all
        lists of primitives are sorted. Fails if no lists to check are found.
        """

        def verify_sorted_lists(data: AnyJSON, path: FieldPath = ()) -> int:
            if isinstance(data, dict):
                return sum(verify_sorted_lists(val, (*path, key))
                           for key, val in cast(JSON, data).items())
            elif isinstance(data, list):
                if data:
                    if isinstance(data[0], dict):
                        return sum(verify_sorted_lists(v, (*path, k))
                                   for val in cast(JSONs, data)
                                   for k, v in val.items())
                    elif isinstance(data[0], (type(None), bool, int, float, str)):
                        if path[-2] == 'projects' and path[-1] in ('laboratory',
                                                                   'institutions',
                                                                   'contact_names',
                                                                   'publication_titles'):
                            return 0
                        else:
                            self.assertEqual(data, sorted(data, key=lambda x: (x is None, x)))
                            return 1
                    elif isinstance(data[0], list):
                        # In lieu of tuples, a range in JSON is a list of two values
                        self.assertEqual(data, list(map(list, sorted(map(tuple, data)))))
                        return 1
                    else:
                        assert False, str(type(data[0]))
                else:
                    return 0
            elif isinstance(data, (type(None), bool, int, float, str)):
                return 0
            else:
                assert False, str(type(data))

        num_lists_counted = verify_sorted_lists(data)
        self.assertGreater(num_lists_counted, 0)
