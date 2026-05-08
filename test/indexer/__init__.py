from abc import (
    ABCMeta,
    abstractmethod,
)
import json
from pathlib import (
    Path,
)
from typing import (
    ClassVar,
    Literal,
    cast,
)
from unittest.mock import (
    PropertyMock,
    patch,
)

from more_itertools import (
    one,
)
from opensearchpy.helpers import (
    scan,
)

from azul import (
    CatalogName,
    config,
)
from azul.indexer import (
    Bundle,
    BundleFQID,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    DocumentType,
    IndexName,
)
from azul.indexer.index_service import (
    IndexService,
    IndexWriter,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)
from azul.plugins import (
    FieldPath,
)
from azul.plugins.repository.dss import (
    DSSBundle,
    DSSBundleFQID,
)
from azul.plugins.repository.tdr import (
    TDRBundleFQID,
)
from azul.plugins.repository.tdr_anvil import (
    BundleType,
    TDRAnvilBundle,
    TDRAnvilBundleFQID,
)
from azul.plugins.repository.tdr_hca import (
    TDRHCABundle,
)
from azul_test_case import (
    AnvilTestCase,
    AzulUnitTestCase,
    CatalogTestCase,
    DCP1TestCase,
    DCP2TestCase,
)
from opensearch_test_case import (
    OpenSearchTestCase,
)


class ForcedRefreshIndexService(IndexService):

    def _create_writer(self,
                       doc_type: DocumentType,
                       catalog: CatalogName | None
                       ) -> IndexWriter:
        writer = super()._create_writer(doc_type, catalog)
        # With a single client thread, refresh=True is faster than
        # refresh="wait_for". The latter would limit the request rate to
        # 1/refresh_interval. That's only one request per second with
        # refresh_interval being 1s.
        writer.refresh = True
        return writer


class CannedFileTestCase(AzulUnitTestCase):
    """
    A test case that loads JSON cans. A can is a file containing test inputs or
    expected outputs.
    """

    @classmethod
    def _data_path(cls, module: Literal['service', 'indexer'], *path: str) -> Path:
        return Path(config.project_root).joinpath('test', module, 'data', *path)

    @classmethod
    def _load_canned_file(cls,
                          bundle: BundleFQID,
                          extension: str
                          ) -> MutableJSON | MutableJSONs:
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
                                  version: str | None,
                                  extension: str
                                  ) -> MutableJSON | MutableJSONs:
        suffix = '' if version is None else '.' + version
        file_name = f'{uuid}{suffix}.{extension}.json'
        with open(cls._data_path('indexer', file_name), 'r') as infile:
            return json.load(infile)


class CannedBundleTestCase[BUNDLE: Bundle](CannedFileTestCase,
                                           metaclass=ABCMeta):
    """
    A test case that loads a canned bundle, i.e. a can containing the input to
    tests involving a metadata plugin or the expected output of tests involving
    a repository plugin.
    """

    @classmethod
    @abstractmethod
    def _bundle_cls(cls) -> type[BUNDLE]:
        raise NotImplementedError

    @classmethod
    def _load_canned_bundle(cls, fqid: SourcedBundleFQID) -> BUNDLE:
        bundle_cls = cls._bundle_cls()
        bundle_json = cls._load_canned_file(fqid, bundle_cls.canning_qualifier())
        bundle_json['fqid'] = fqid.to_json()
        bundle = bundle_cls.from_json(bundle_json)
        assert bundle.fqid == fqid
        return bundle


class DCP1CannedBundleTestCase(DCP1TestCase, CannedBundleTestCase[DSSBundle]):

    @classmethod
    def _bundle_cls(cls) -> type[DSSBundle]:
        return DSSBundle

    @classmethod
    def bundle_fqid(cls, *, uuid: str, version: str) -> DSSBundleFQID:
        return DSSBundleFQID(source=cls.source.ref,
                             uuid=uuid,
                             version=version)

    @classmethod
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.bundle_fqid(uuid='aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                            version='2018-11-02T11:33:44.698028Z')
        ]


class DCP2CannedBundleTestCase(DCP2TestCase, CannedBundleTestCase[TDRHCABundle]):

    @classmethod
    def _bundle_cls(cls) -> type[TDRHCABundle]:
        return TDRHCABundle

    @classmethod
    def bundle_fqid(cls, *, uuid: str, version: str) -> TDRBundleFQID:
        return TDRBundleFQID(source=cls.source.ref,
                             uuid=uuid,
                             version=version)


class AnvilCannedBundleTestCase(AnvilTestCase,
                                CannedBundleTestCase[TDRAnvilBundle]):
    #: AnVIL doesn't use versioning and all versions are fixed
    version = '2022-06-01T00:00:00.000000Z'

    @classmethod
    def _bundle_cls(cls) -> type[TDRAnvilBundle]:
        return TDRAnvilBundle

    @classmethod
    def bundle_fqid(cls,
                    *,
                    uuid: str,
                    table_name: str = BundleType.primary.value,
                    ) -> TDRAnvilBundleFQID:
        return TDRAnvilBundleFQID(source=cls.source.ref,
                                  uuid=uuid,
                                  version=cls.version,
                                  table_name=table_name,
                                  batch_prefix='' if BundleType.is_batched(table_name) else None)

    @classmethod
    def primary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='826dea02-e274-affe-aabc-eb3db63ad068')

    @classmethod
    def supplementary_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='595c469e-604d-ab34-af39-f5b9f5d61818',
                               table_name=BundleType.supplementary.value)

    @classmethod
    def duos_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='2370f948-2783-aeb6-afea-e022897f4dcf',
                               table_name=BundleType.duos.value)

    @classmethod
    def replica_bundle(cls) -> TDRAnvilBundleFQID:
        return cls.bundle_fqid(uuid='f4b39881-d519-ab6f-99a0-7cc5089caee6',
                               table_name='non_schema_orphan_table')


class IndexerTestCase(CatalogTestCase,
                      OpenSearchTestCase,
                      CannedBundleTestCase,
                      metaclass=ABCMeta):
    index_service: ClassVar[IndexService | None] = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.index_service = ForcedRefreshIndexService()
        cls.addClassPatch(patch.object(type(config),
                                       'mirror_bucket',
                                       new_callable=PropertyMock,
                                       return_value=None))

    @classmethod
    def _purge_indices(cls):
        """
        Deletes everything and is faster than deleting indices individually
        through the service.
        """
        opensearch = OpenSearchClientFactory.get()
        opensearch.indices.delete(index='*')

    def _get_all_hits(self):
        # Without `preserve_order`, hits are sorted by `_doc`, which is fastest
        # but causes the `sort` field in hits to vary unpredictably, based on
        # the number of shards, for example, but also under what appear to be
        # unrelated code changes. This makes asserting test results verbatim
        # impossible. Thus we set `preserve_order` to True.
        hits = list(scan(client=self.opensearch,
                         index=','.join(map(str, self.index_service.index_names(self.catalog))),
                         preserve_order=True))

        def is_duos_contribution(entity_type, doc_type):
            return (
                config.is_anvil_enabled(self.catalog)
                and entity_type in {'bundles', 'datasets'}
                and doc_type is DocumentType.contribution
                and 'description' in one(hit['_source']['contents']['datasets'])
            )

        for hit in hits:
            qualifier, doc_type = self._parse_index_name(hit)
            if not (
                # Replicas may contain (intentionally) unsorted metadata
                doc_type is DocumentType.replica
                # DUOS contributions contain no lists
                or is_duos_contribution(qualifier, doc_type)
            ):
                self._verify_sorted_lists(hit['_source'])
        return hits

    def _parse_index_name(self, hit) -> tuple[str, DocumentType]:
        index_name = IndexName.parse(hit['_index'])
        index_name.validate()
        return index_name.qualifier, index_name.doc_type

    def _load_canned_result(self, bundle_fqid: BundleFQID) -> MutableJSONs:
        """
        Load the canned index documents for the given canned bundle and fix the
        '_index' entry in each to match the index name in the current deployment
        """
        expected_hits = self._load_canned_file(bundle_fqid, 'results')
        assert isinstance(expected_hits, list)
        for hit in expected_hits:
            index_name = IndexName.parse(hit['_index'])
            index_name = IndexName.create(catalog=self.catalog,
                                          qualifier=index_name.qualifier,
                                          doc_type=index_name.doc_type)
            hit['_index'] = str(index_name)
        return expected_hits

    @classmethod
    def _index_canned_bundle(cls,
                             bundle_fqid: SourcedBundleFQID,
                             *,
                             delete=False
                             ) -> Bundle:
        bundle = cls._load_canned_bundle(bundle_fqid)
        cls._index_bundle(bundle, delete=delete)
        return bundle

    @classmethod
    def _index_bundle(cls, bundle: Bundle, *, delete: bool = False) -> None:
        if delete:
            cls.index_service.delete(cls.catalog, bundle)
        else:
            cls.index_service.index(cls.catalog, bundle)

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
                        # FIXME: Field types don't express ordering requirements
                        #        https://github.com/DataBiosphere/azul/issues/4664
                        ordered_fields = {
                            'laboratory',
                            'institutions',
                            'contact_names',
                            'publication_titles'
                        }
                        if path[-2] == 'projects' and path[-1] in ordered_fields:
                            return 0
                        else:
                            self.assertEqual(data, sorted(data, key=lambda x: (x is None, x)))
                            return 1
                    elif isinstance(data[0], list):
                        # In lieu of tuples, a range in JSON is a list of two values
                        def pair(t: tuple) -> list:
                            return list(t)

                        self.assertEqual(data, list(map(pair, sorted(map(tuple, data)))))
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
