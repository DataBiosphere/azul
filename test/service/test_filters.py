from typing import (
    Optional,
)
from unittest.mock import (
    MagicMock,
)

from chalice import (
    ForbiddenError,
)

from azul.plugins import (
    SpecialFields,
)
from azul.service import (
    FilterOperator,
    Filters,
    FiltersJSON,
)
from azul_test_case import (
    AzulTestCase,
)


class TestFilterReification(AzulTestCase):
    accessible_source = '123'
    accessible_sources = {accessible_source, '456', '789'}
    inaccessible_source = '321'

    assert inaccessible_source not in accessible_sources

    special_fields = SpecialFields(
        source_id='sourceId',
        source_spec=MagicMock(),
        bundle_uuid=MagicMock(),
        bundle_version=MagicMock(),
        implicit_hub_id=MagicMock()
    )

    @property
    def plugin(self):
        return MagicMock(special_fields=self.special_fields)

    def _get_filters(self,
                     *,
                     limit_access: bool,
                     explicit_sources: Optional[list[str]] = None,
                     explicit_access: Optional[list[Optional[bool]]] = None
                     ) -> FiltersJSON:
        explicit = {
            'cellCount': {
                'within': [[10000, 1000000000]]
            },
            **({} if explicit_sources is None else {
                self.special_fields.source_id: {
                    'is': explicit_sources
                }
            }),
            **({} if explicit_access is None else {
                self.special_fields.accessible: {
                    'is': explicit_access
                }
            })
        }
        filters = Filters(explicit=explicit, source_ids=self.accessible_sources)
        return filters.reify(plugin=self.plugin, limit_access=limit_access)

    def _test_filters(self,
                      expected_source_id_filter: Optional[FilterOperator],
                      *,
                      limit_access: bool,
                      explicit_sources: Optional[list[str]] = None,
                      explicit_access: Optional[list[Optional[bool]]] = None
                      ):
        expected_filters = {
            'cellCount': {'within': [[10000, 1000000000]]},
            **({} if expected_source_id_filter is None else {
                self.special_fields.source_id: expected_source_id_filter
            })
        }
        with self.subTest(explicit_sources=explicit_sources,
                          explicit_access=explicit_access,
                          limit_access=limit_access):
            filters = self._get_filters(limit_access=limit_access,
                                        explicit_sources=explicit_sources,
                                        explicit_access=explicit_access)
            self.assertEqual(expected_filters, filters)

    def test_implicit(self):
        self._test_filters({'is': sorted(self.accessible_sources)},
                           limit_access=True)
        self._test_filters(None,
                           limit_access=False)

    def test_explicit_accessible_sources(self):
        for limit_access in True, False:
            self._test_filters({'is': [self.accessible_source]},
                               limit_access=limit_access,
                               explicit_sources=[self.accessible_source])

    def test_explicit_inaccessible_sources(self):
        for sources in [
            [self.inaccessible_source],
            [self.accessible_source, self.inaccessible_source]
        ]:
            with self.assertRaises(ForbiddenError):
                self._get_filters(limit_access=True,
                                  explicit_sources=sources)

        self._test_filters({'is': [self.inaccessible_source]},
                           limit_access=False,
                           explicit_sources=[self.inaccessible_source])

    def test_explicit_access(self):
        for limit_access in True, False:
            self._test_filters({'is': sorted(self.accessible_sources)},
                               limit_access=limit_access,
                               explicit_access=[True])
            self._test_filters({'is': []},
                               explicit_access=[],
                               limit_access=limit_access)

        self._test_filters({'is_not': sorted(self.accessible_sources)},
                           explicit_access=[False],
                           limit_access=False)
        self._test_filters({'is': []},
                           explicit_access=[False],
                           limit_access=True)
        self._test_filters(None,
                           explicit_access=[True, False],
                           limit_access=False)
        self._test_filters({'is': sorted(self.accessible_sources)},
                           explicit_access=[True, False],
                           limit_access=True)

    def test_explicit_access_with_explicit_public_source(self):
        for limit_access in True, False:
            for explicit_access in [[True], [False, True]]:
                self._test_filters({'is': [self.accessible_source]},
                                   explicit_sources=[self.accessible_source],
                                   explicit_access=explicit_access,
                                   limit_access=limit_access)
            for explicit_access in [[], [False]]:
                self._test_filters({'is': []},
                                   explicit_sources=[self.accessible_source],
                                   explicit_access=explicit_access,
                                   limit_access=limit_access)

    def test_explicit_access_with_explicit_protected_source(self):
        for explicit_access in [[], [True], [False], [True, False]]:
            with self.assertRaises(ForbiddenError):
                self._get_filters(limit_access=True,
                                  explicit_sources=[self.inaccessible_source],
                                  explicit_access=explicit_access)
            self._test_filters({'is': [self.inaccessible_source] if False in explicit_access else []},
                               explicit_sources=[self.inaccessible_source],
                               limit_access=False,
                               explicit_access=explicit_access)
