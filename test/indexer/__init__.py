from abc import (
    ABCMeta,
    abstractmethod,
)
import json
from pathlib import (
    Path,
)
from typing import (
    Generic,
    Literal,
    Optional,
    Type,
    Union,
    cast,
)

from elasticsearch.helpers import (
    scan,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    config,
)
from azul.indexer import (
    BUNDLE,
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
from azul.plugins import (
    FieldPath,
)
from azul.plugins.repository.dss import (
    DSSBundle,
)
from azul.plugins.repository.tdr_anvil import (
    TDRAnvilBundle,
)
from azul.plugins.repository.tdr_hca import (
    TDRHCABundle,
)
from azul.types import (
    AnyJSON,
    JSON,
    JSONs,
    MutableJSON,
    MutableJSONs,
)
from azul_test_case import (
    AnvilTestCase,
    AzulUnitTestCase,
    CatalogTestCase,
    DCP1TestCase,
    DCP2TestCase,
)
from es_test_case import (
    ElasticsearchTestCase,
)


class ForcedRefreshIndexService(IndexService):

    def _create_writer(self,
                       doc_type: DocumentType,
                       catalog: Optional[CatalogName]
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
    def _data_path(cls, module: Literal['service', 'indexer']) -> Path:
        return Path(config.project_root) / 'test' / module / 'data'

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
        suffix = '' if version is None else '.' + version
        file_name = f'{uuid}{suffix}.{extension}.json'
        with open(cls._data_path('indexer') / file_name, 'r') as infile:
            return json.load(infile)


class CannedBundleTestCase(CannedFileTestCase, Generic[BUNDLE]):
    """
    A test case that loads a canned bundle, i.e. a can containing the input to
    tests involving a metadata plugin or the expected output of tests involving
    a repository plugin.
    """

    @classmethod
    @abstractmethod
    def _bundle_cls(cls) -> Type[BUNDLE]:
        raise NotImplementedError

    @classmethod
    def _load_canned_bundle(cls, bundle: SourcedBundleFQID) -> BUNDLE:
        bundle_cls = cls._bundle_cls()
        bundle_json = cls._load_canned_file(bundle, bundle_cls.canning_qualifier())
        return bundle_cls.from_json(bundle, bundle_json)


class DCP1CannedBundleTestCase(DCP1TestCase, CannedBundleTestCase[DSSBundle]):

    @classmethod
    def _bundle_cls(cls) -> Type[DSSBundle]:
        return DSSBundle


class DCP2CannedBundleTestCase(DCP2TestCase, CannedBundleTestCase[TDRHCABundle]):

    @classmethod
    def _bundle_cls(cls) -> Type[TDRHCABundle]:
        return TDRHCABundle


class AnvilCannedBundleTestCase(AnvilTestCase, CannedBundleTestCase[TDRAnvilBundle]):
    #: AnVIL doesn't use versioning and all versions are fixed
    version = '2022-06-01T00:00:00.000000Z'

    @classmethod
    def _bundle_cls(cls) -> Type[TDRAnvilBundle]:
        return TDRAnvilBundle


class IndexerTestCase(CatalogTestCase,
                      ElasticsearchTestCase,
                      CannedBundleTestCase,
                      metaclass=ABCMeta):
    index_service: IndexService

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.index_service = ForcedRefreshIndexService()

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
