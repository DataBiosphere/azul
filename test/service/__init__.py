import json
import os
from typing import (
    Any,
    Callable,
    Optional,
    Type,
    Union,
)
from unittest import (
    TestCase,
    mock,
)
from unittest.mock import (
    MagicMock,
    PropertyMock,
    patch,
)

from app_test_case import (
    LocalAppTestCase,
)
from azul import (
    JSON,
    cached_property,
)
from azul.indexer import (
    SourcedBundleFQID,
)
from azul.logging import (
    configure_test_logging,
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
import indexer
from indexer import (
    IndexerTestCase,
)


# noinspection PyPep8Naming
def setUpModule():
    configure_test_logging()


class WebServiceTestCase(IndexerTestCase, LocalAppTestCase):
    """
    Although it seems weird for the webservice to inherit the testing mechanisms
    for the indexer, we need them in order to send live indexer output to the
    webservice.
    """

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
        for bundle in cls.bundles():
            cls._index_canned_bundle(bundle)

    @classmethod
    def _teardown_indices(cls):
        cls.index_service.delete_indices(cls.catalog)

    def _params(self, filters: Optional[JSON] = None, **params: Any) -> dict[str, Any]:
        return {
            **({} if filters is None else {'filters': json.dumps(filters)}),
            'catalog': self.catalog,
            **params
        }


class DSSUnitTestCase(TestCase):
    """
    A mixin for test cases that depend on certain DSS-related environment
    variables.
    """

    _dss_mock = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._dss_mock = mock.patch.dict(os.environ,
                                        AZUL_DSS_SOURCE='https://dss.data.humancellatlas.org/v1:2/2')
        cls._dss_mock.start()

    @classmethod
    def tearDownClass(cls):
        cls._dss_mock.stop()
        super().tearDownClass()


class StorageServiceTestCase(TestCase):
    """
    A mixin for test cases that utilize StorageService.
    """

    @cached_property
    def storage_service(self) -> StorageService:
        return StorageService()


patch_dss_source = patch('azul.Config.dss_source',
                         new=PropertyMock(return_value=indexer.mock_dss_source))


def patch_source_cache(arg: Union[Type, Callable, JSONs]):
    """
    Patch the cache access methods of SourceService to emulate a cache miss or
    return a given set of sources.

    May be invoked directly as a decorator (no parentheses, emulating a cache
    miss) or as a decorator factory accepting exactly one argument (the sources
    to return from the patched cache).

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

    >>> @patch_source_cache()
    ... class C:
    ...     def test(self):
    ...         return SourceService()._get('foo')
    Traceback (most recent call last):
    ...
    TypeError: patch_source_cache() missing 1 required positional argument: 'arg'

    >>> class C:
    ...     @patch_source_cache()
    ...     def test(self):
    ...         return SourceService()._get('foo')
    Traceback (most recent call last):
    ...
    TypeError: patch_source_cache() missing 1 required positional argument: 'arg'

    >>> @patch_source_cache([{'foo': 'bar'}])
    ... class C:
    ...     def test(self):
    ...         return SourceService()._get('foo')
    >>> C().test()
    [{'foo': 'bar'}]

    >>> class C:
    ...     @patch_source_cache([{'foo': 'bar'}])
    ...     def test(self):
    ...         return SourceService()._get('foo')
    >>> C().test()
    [{'foo': 'bar'}]
    """
    get_mock = MagicMock()

    def nested_patch(target):
        get_patch = patch.object(SourceService,
                                 '_get',
                                 new=get_mock)
        put_patch = patch.object(SourceService,
                                 '_put',
                                 new=MagicMock())
        return put_patch(get_patch(target))

    if isinstance(arg, JSONs.__origin__):
        get_mock.return_value = arg
        return nested_patch
    else:
        def not_found(key):
            raise NotFound(key)

        get_mock.side_effect = not_found
        return nested_patch(arg)
