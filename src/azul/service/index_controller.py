from functools import (
    partial,
)
import json
import logging
from typing import (
    Any,
    Mapping,
)

import attr
from chalice.app import (
    BadRequestError,
    ChaliceViewError,
    NotFoundError,
)
from furl import (
    furl,
)

from azul.indexer.document import (
    EntityType,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.functions import (
    iif,
)
from azul.lib.strings import (
    format_and_dedent as fd,
)
from azul.lib.types import (
    JSON,
    PrimitiveJSON,
    check_type,
)
from azul.lib.uuids import (
    InvalidUUIDError,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.controller import (
    validate_params,
)
from azul.service.index_service import (
    EntityNotFoundError,
    IndexService,
)
from azul.service.query_controller import (
    QueryController,
)
from azul.service.query_service import (
    IndexNotFoundError,
    Pagination,
    SortKey,
)

log = logging.getLogger(__name__)


class IndexController(QueryController):

    @cached_property
    def _service(self) -> IndexService:
        return IndexService()

    _min_page_size = 1

    _generic_object_schema = schema.object(additionalProperties=True)

    _array_of_objects_schema = schema.array(_generic_object_schema)

    _hit_schema = schema.object(
        additionalProperties=True,
        protocols=_array_of_objects_schema,
        entryId=str,
        sources=_array_of_objects_schema,
        samples=_array_of_objects_schema,
        specimens=_array_of_objects_schema,
        cellLines=_array_of_objects_schema,
        donorOrganisms=_array_of_objects_schema,
        organoids=schema.array(str),
        cellSuspensions=_array_of_objects_schema
    )

    def _search_entity_spec(self):
        search_spec_link = '#operations-Index-get_index__entity_type_'
        return {
            'summary': 'Detailed information on a particular entity.',
            'tags': ['Index'],
            'parameters': [
                self._catalog_param_spec,
                params.path('entity_type', str, description='The type of the desired entity'),
                params.path('entity_id', str, description='The UUID of the desired entity')
            ],
            'responses': {
                '200': {
                    'description': fd(f'''
                        This response describes a single entity. To search the index
                        for multiple entities, see the [corresponding search
                        endpoint]({search_spec_link}).

                        The properties that are common to all entity types are
                        listed in the schema below; however, additional properties
                        may be present for certain entity types. With the exception
                        of the entity's unique identifier, all properties are
                        arrays, even in cases where only one value is present.

                        The structures of the objects within these arrays are not
                        perfectly consistent, since they may represent either
                        singleton entities or aggregations depending on context.

                        For example, any biomaterial that yields a cell suspension
                        which yields a sequence file will be considered a "sample".
                        Therefore, the `samples` field is polymorphic, and each
                        sample may be either a specimen, an organoid, or a cell line
                        (the field `sampleEntityType` can be used to discriminate
                        between these cases).
                    '''),
                    **responses.json_content(self._hit_schema)
                }
            }
        }

    def _search_entities_spec(self, *, post: bool):
        id_spec_link = '#operations-Index-get_index__entity_type___entity_id_'
        return {
            'summary': fd(f'''
                Search an index for entities of interest
                {", with filters provided in the request body" if post else ""}.
            '''),
            'deprecated': post,
            'description':
                iif(post, self._parameter_hoisting_note('GET', '/index/files', 'POST') + fd('''

                Note that the Swagger UI can't currently be used to pass a body.

                Please also note that this endpoint should be considered beta and
                may change or disappear in the future. That is the reason for the
                deprecation.
            ''')),
            'tags': ['Index'],
            'parameters': self._search_entities_params_spec(),
            'responses': {
                '200': {
                    'description': fd(f'''
                        Paginated list of entities that meet the search criteria
                        ("hits"). The structure of these hits is documented under
                        the [corresponding endpoint for a specific
                        entity]({id_spec_link}).

                        The `pagination` section describes the total number of hits
                        and total number of pages, as well as user-supplied search
                        parameters for page size and sorting behavior. It also
                        provides links for navigating forwards and backwards between
                        pages of results.

                        The `termFacets` section tabulates the occurrence of unique
                        values within nested fields of the `hits` section across all
                        entities meeting the filter criteria (this includes entities
                        not listed on the current page, meaning that this section
                        will be invariable across all pages from the same search).
                        Not every nested field is tabulated, but the set of
                        tabulated fields is consistent between entity types.
                    '''),
                    **responses.json_content(
                        schema.object(
                            hits=schema.array(self._hit_schema),
                            pagination=self._generic_object_schema,
                            termFacets=self._generic_object_schema
                        )
                    )
                }
            }
        }

    def _search_entities_params_spec(self):
        return [
            self._catalog_param_spec,
            self._filters_param_spec,
            params.path(
                'entity_type',
                schema.enum(*self._metadata_plugin.exposed_indices.keys()),
                description='Which index to search.'
            ),
            params.query(
                'size',
                schema.optional(schema.default(10, form=schema.range(self._min_page_size, None))),
                description=fd('''
                    The number of hits included per page. The maximum size allowed
                    depends on the catalog and entity type.
                ''')
            ),
            params.query(
                'sort',
                schema.optional(schema.enum(*self._organic_fields)),
                description=fd('''
                    The field to sort the hits by. The default value depends on the
                    entity type.
                ''')
            ),
            params.query(
                'order',
                schema.optional(schema.enum('asc', 'desc')),
                description=fd('''
                    The ordering of the sorted hits, either ascending or descending.
                    The default value depends on the entity type.
                ''')
            ),
            *[
                params.query(
                    param,
                    schema.optional(str),
                    description=fd('''
                        Use the `next` and `previous` properties of the
                        `pagination` response element to navigate between pages.
                    '''),
                    deprecated=True)
                for param in [
                    'search_before',
                    'search_before_uid',
                    'search_after',
                    'search_after_uid'
                ]
            ]
        ]

    def _head_spec(self, for_summary: bool = False):
        search_spec_link = f'#operations-Index-get_index_{"summary" if for_summary else "_entity_type_"}'
        return {
            'summary': 'Perform a query without returning its result.',
            'tags': ['Index'],
            'responses': {
                '200': {
                    'description': fd(f'''
                        The HEAD method can be used to test whether an index is
                        operational, or to check the validity of query parameters
                        for the [GET method]({search_spec_link}).
                    ''')
                }
            }
        }

    @property
    def _summary_spec(self):
        return {
            'tags': ['Index'],
            'parameters': [self._catalog_param_spec, self._filters_param_spec]
        }

    def handlers(self) -> dict[str, Any]:
        @self.app.route(
            '/index/{entity_type}',
            methods=['GET'],
            spec=self._search_entities_spec(post=False),
            cors=True
        )
        # FIXME: Properly document the POST version of /index
        #        https://github.com/DataBiosphere/azul/issues/5900
        @self.app.route(
            '/index/{entity_type}',
            methods=['POST'],
            content_types=['application/json'],
            spec=self._search_entities_spec(post=True),
            cors=True
        )
        @self.app.route(
            '/index/{entity_type}',
            methods=['HEAD'],
            spec={
                **self._head_spec(),
                'parameters': self._search_entities_params_spec()
            },
            cors=True
        )
        @self.app.route(
            '/index/{entity_type}/{entity_id}',
            methods=['GET'],
            spec=self._search_entity_spec(),
            cors=True
        )
        def get_head_post_index_entity(entity_type: str, entity_id: str | None = None) -> str | JSON:
            return self.search(entity_type, entity_id)

        @self.app.route(
            '/index/summary',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Statistics on the data present across all entities.',
                'responses': {
                    '200': {
                        # FIXME: Add 'projects' to API documentation & schema
                        #        https://github.com/DataBiosphere/azul/issues/3917
                        'description': fd('''
                            Counts the total number and total size in bytes of assorted
                            entities, subject to the provided filters.

                            `fileTypeSummaries` provides the count and total size in
                            bytes of files grouped by their format, e.g. "fastq" or
                            "matrix." `fileCount` and `totalFileSize` compile these
                            figures across all file formats. Likewise,
                            `cellCountSummaries` counts cells and their associated
                            documents grouped by organ type, with `organTypes` listing
                            all referenced organs.

                            Total counts of unique entities are also provided for other
                            entity types such as projects and tissue donors. These
                            values are not grouped/aggregated.
                        '''),
                        **responses.json_content(
                            schema.object(
                                additionalProperties=True,
                                organTypes=schema.array(str),
                                totalFileSize=float,
                                fileTypeSummaries=self._array_of_objects_schema,
                                cellCountSummaries=self._array_of_objects_schema,
                                donorCount=int,
                                fileCount=int,
                                labCount=int,
                                projectCount=int,
                                speciesCount=int,
                                specimenCount=int
                            )
                        )
                    }
                },
                **self._summary_spec
            }
        )
        @self.app.route(
            '/index/summary',
            methods=['HEAD'],
            spec={
                **self._head_spec(for_summary=True),
                **self._summary_spec
            }
        )
        def get_head_index_summary():
            return self.summary()

        return locals()

    def search(self, entity_type: str, entity_id: str | None = None) -> str | JSON:
        request = self.current_request
        query_params = self._hoist_parameters(request)
        validate_params(query_params,
                        catalog=self._validate_catalog,
                        filters=self._validate_filters,
                        order=self._validate_order,
                        search_after=partial(self._validate_json_param, 'search_after'),
                        search_after_uid=str,
                        search_before=partial(self._validate_json_param, 'search_before'),
                        search_before_uid=str,
                        size=partial(self._validate_size, entity_type),
                        sort=self._validate_field)
        self._validate_entity_type(entity_type)
        filters = query_params.get('filters')
        pagination = self._pagination(entity_type)
        authentication = self._authentication(request)
        filters = self._prepare_filters(self.app.catalog, authentication, filters)
        try:
            response = self._service.search(catalog=self.app.catalog,
                                            entity_type=entity_type,
                                            file_url_func=self._file_url,
                                            item_id=entity_id,
                                            filters=filters,
                                            pagination=pagination)
        except (BadArgumentException, InvalidUUIDError) as e:
            raise BadRequestError(e)
        except (EntityNotFoundError, IndexNotFoundError) as e:
            raise NotFoundError(e)
        return '' if request.method == 'HEAD' else response

    def summary(self):
        request = self.current_request
        query_params = self._query_params(request)
        validate_params(query_params,
                        filters=str,
                        catalog=self._validate_catalog)
        filters = query_params.get('filters', '{}')
        self._validate_filters(filters)
        authentication = self._authentication(request)
        filters = self._prepare_filters(self.app.catalog, authentication, filters)
        try:
            response = self._service.summary(self.app.catalog, filters)
        except BadArgumentException as e:
            raise BadRequestError(e)
        return '' if request.method == 'HEAD' else response

    def _validate_entity_type(self, entity_type: str):
        entity_types = self._metadata_plugin.exposed_indices.keys()
        if entity_type not in entity_types:
            raise BadRequestError(f'Entity type {entity_type!r} is invalid for catalog '
                                  f'{self.app.catalog!r}. Must be one of {set(entity_types)}.')

    def _validate_size(self, entity_type: EntityType, size: str | int):
        sorting = self._metadata_plugin.exposed_indices[entity_type]
        try:
            size = int(size)
        except BaseException:
            raise BadRequestError('Invalid value for parameter `size`')
        else:
            if size > sorting.max_page_size:
                raise BadRequestError(f'Invalid value for parameter `size`, '
                                      f'must not be greater than {sorting.max_page_size}')
            elif size < self._min_page_size:
                raise BadRequestError('Invalid value for parameter `size`, must be greater than 0')

    def _validate_order(self, order: str):
        supported_orders = ('asc', 'desc')
        if order not in supported_orders:
            raise BadRequestError(f'Unknown order `{order}`. Must be one of {supported_orders}')

    @attr.s(kw_only=True, auto_attribs=True, frozen=True)
    class _Pagination(Pagination):
        self_url: furl

        def link(self, *, previous: bool, **params: str) -> furl | None:
            search_key = self.search_before if previous else self.search_after
            if search_key is None:
                return None
            else:
                before_or_after = 'before' if previous else 'after'
                params = {
                    **params,
                    f'search_{before_or_after}': json.dumps(search_key),
                    'sort': self.sort,
                    'order': self.order,
                    'size': str(self.size)
                }
            return furl(url=self.self_url, args=params)

    def _pagination(self, entity_type: str) -> _Pagination:
        default_sorting = self._metadata_plugin.exposed_indices[entity_type]
        request = self.current_request
        params = self._query_params(request)
        try:
            sb, sa = self._pagination_params(params)
        except AssertionError as e:
            if R.caused(e):
                raise R.propagate(e, ChaliceViewError)
            else:
                raise
        else:
            return self._Pagination(order=params.get('order', default_sorting.order),
                                    size=int(params.get('size', '10')),
                                    sort=params.get('sort', default_sorting.field_name),
                                    search_before=sb,
                                    search_after=sa,
                                    self_url=self.app.self_url)

    def _pagination_params(self,
                           params: Mapping[str, str]
                           ) -> tuple[SortKey, None] | tuple[None, SortKey] | tuple[None, None]:
        sb, sa = params.get('search_before'), params.get('search_after')
        if sb is None:
            if sa is None:
                return None, None
            else:
                return None, self._sort_key(sa)
        else:
            if sa is None:
                return self._sort_key(sb), None
            else:
                assert False, R('Only one of search_after or search_before may be set')

    def _sort_key(self, sort_key: str) -> SortKey:
        sort_key = json.loads(sort_key)
        assert isinstance(sort_key, list), R(
            'Not a list', sort_key)
        assert len(sort_key) == 2, R(
            'Not a tuple with two elements', sort_key)
        a, b = sort_key
        assert check_type(PrimitiveJSON, a), R(
            'First sort key element is not primitive JSON', sort_key)
        assert isinstance(b, str), R(
            'Second sort key element not a string', sort_key)
        return a, b
