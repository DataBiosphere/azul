from abc import (
    ABCMeta,
)
import copy
import json
from random import (
    Random,
)
from typing import (
    Any,
    Callable,
    ClassVar,
    Optional,
    Union,
)
from unittest.mock import (
    MagicMock,
    patch,
)
import uuid

from deprecated import (
    deprecated,
)
from more_itertools import (
    flatten,
    one,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    JSON,
    cached_property,
    config,
)
from azul.indexer import (
    Bundle,
    BundleUUID,
    SourcedBundleFQID,
)
from azul.logging import (
    configure_test_logging,
    get_test_logger,
)
from azul.service.source_service import (
    NotFound,
    SourceService,
)
from azul.service.storage_service import (
    StorageService,
)
from azul.types import (
    JSONs,
)
from indexer import (
    IndexerTestCase,
)

log = get_test_logger(__name__)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class WebServiceTestCase(IndexerTestCase, LocalAppTestCase, metaclass=ABCMeta):
    """
    Although it seems weird for the webservice to inherit the testing mechanisms
    for the indexer, we need them in order to send live indexer output to the
    webservice.
    """
    indexed_bundles: ClassVar[Optional[dict[BundleUUID, Bundle]]] = None

    @classmethod
    def bundles(cls) -> list[SourcedBundleFQID]:
        return [
            cls.bundle_fqid(uuid='aaa96233-bf27-44c7-82df-b4dc15ad4d9d',
                            version='2018-11-02T11:33:44.698028Z')
        ]

    @classmethod
    def lambda_name(cls) -> str:
        return "service"

    @classmethod
    def _setup_indices(cls):
        cls.index_service.create_indices(cls.catalog)
        bundle_fqids = cls.bundles()
        bundles = {
            bundle_fqid.uuid: cls._index_canned_bundle(bundle_fqid)
            for bundle_fqid in bundle_fqids
        }
        # This class can't handle multiple versions of a bundle
        assert len(bundle_fqids) == len(bundles)
        cls.indexed_bundles = bundles

    @classmethod
    def _teardown_indices(cls):
        cls.index_service.delete_indices(cls.catalog)
        cls.indexed_bundles = None

    def _params(self, filters: Optional[JSON] = None, **params: Any) -> dict[str, Any]:
        return {
            **({} if filters is None else {'filters': json.dumps(filters)}),
            'catalog': self.catalog,
            **params
        }


class DocumentCloningTestCase(WebServiceTestCase, metaclass=ABCMeta):
    _templates: JSONs
    _random: Random

    def setUp(self):
        super().setUp()
        self._random = Random(42)

    def _setup_document_templates(self):
        hits = self._get_all_hits()
        self._templates = [hit['_source'] for hit in hits]
        self._delete_all_hits()

    def tearDown(self):
        self._teardown_indices()
        super().tearDown()

    _query = {
        'query': {
            'match_all': {}
        }
    }

    def _get_all_hits(self):
        response = self.es_client.search(index=self._index_name,
                                         body=self._query)
        return response['hits']['hits']

    def _delete_all_hits(self):
        self.es_client.delete_by_query(index=self._index_name,
                                       body=self._query,
                                       refresh=True)

    def _clone_doc(self, doc):
        """
        Duplicate the given `files` document with new identifiers.
        """
        doc = copy.deepcopy(doc)
        entity_id, file_id = str(uuid.uuid4()), str(uuid.uuid4())
        doc['entity_id'] = entity_id
        file = one(doc['contents']['files'])
        file['document_id'] = entity_id
        file['uuid'] = file_id
        return doc

    def _add_docs(self, num_docs):
        """
        Make the given number of copies of a randomly selected template
        document from the `files` index.
        """
        if num_docs > 0:
            log.info('Adding %i documents to index', num_docs)
            template = self._random.choice(self._templates)
            docs = [self._clone_doc(template) for _ in range(num_docs)]
            body = '\n'.join(
                flatten(
                    (
                        json.dumps({'create': {}}),
                        json.dumps(doc)
                    )
                    for doc in docs
                )
            )
            self.es_client.bulk(body, index=self._index_name, refresh=True)

    @property
    def _index_name(self):
        return config.es_index_name(catalog=self.catalog,
                                    entity_type='files',
                                    aggregate=True)


class StorageServiceTestMixin:
    """
    A mixin for test cases that utilize StorageService.
    """

    @cached_property
    def storage_service(self) -> StorageService:
        return StorageService()


@deprecated('Instead of decorating your test case, or its test methods in it, '
            'mix in the appropriate subclass of CatalogTestCase.')
def patch_source_cache(target: Union[None, type, Callable] = None,
                       /,
                       hit: Optional[JSONs] = None):
    """
    Patch the cache access methods of SourceService to emulate a cache miss or
    return a given set of sources.

    When used directly (without parentheses) to decorate a method (or class),
    the SourceService will produce a cache miss, while the method (or any
    method in the class) is running.

    >>> @patch_source_cache
    ... class C:
    ...     def test(self):
    ...         return SourceService()._get('foo')
    >>> C().test()
    Traceback (most recent call last):
    ...
    azul.service.source_service.NotFound: Key not found: 'foo'

    >>> class C:
    ...     @patch_source_cache
    ...     def test(self):
    ...         return SourceService()._get('foo')
    >>> C().test()
    Traceback (most recent call last):
    ...
    azul.service.source_service.NotFound: Key not found: 'foo'

    When calling it without arguments it returns a decorator that has the same
    effect.

    >>> @patch_source_cache()
    ... class C:
    ...     def test(self):
    ...         return SourceService()._get('foo')
    >>> C().test()
    Traceback (most recent call last):
    ...
    azul.service.source_service.NotFound: Key not found: 'foo'

    Alternatively, the return value can be used as a context manager, to the
    same effect.

    >>> with patch_source_cache():
    ...     SourceService()._get('key')  # noqa
    Traceback (most recent call last):
    ...
    azul.service.source_service.NotFound: Key not found: 'key'

    When called with the `hit` keyword argument, the returned decorator/context
    manager causes SourceService to produce a cache hit with the given value.

    >>> @patch_source_cache(hit=[{'foo': 'bar'}])
    ... class C:
    ...     def test(self):
    ...         return SourceService()._get('key')
    >>> C().test()
    [{'foo': 'bar'}]

    >>> with patch_source_cache(hit=[{'foo': 'bar'}]):
    ...     SourceService()._get('foo')  # noqa
    [{'foo': 'bar'}]

    While the patch is active, any items placed in the cache are discarded.

    >>> with patch_source_cache(hit=[{'foo': 'bar'}]):
    ...     service = SourceService()
    ...     service._put('key', [{}])  # noqa
    ...     service._get('key')  # noqa
    [{'foo': 'bar'}]
    """

    def not_found(key):
        raise NotFound(key)

    get = MagicMock()
    if hit is None:
        get.side_effect = not_found
    else:
        get.return_value = hit

    put = MagicMock()
    put.return_value = None
    the_patch = patch.multiple(SourceService, _get=get, _put=put)

    if target is None:
        return the_patch
    else:
        return the_patch(target)
