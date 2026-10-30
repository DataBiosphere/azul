import base64
from functools import (
    partial,
)
import hashlib
import hmac
import json
import logging
from typing import (
    Any,
    ClassVar,
    Mapping,
    Self,
)

import attr
from chalice.app import (
    BadRequestError,
    NotFoundError,
)
from furl import (
    furl,
)

from azul.deployment import (
    aws,
)
from azul.filters import (
    Filters,
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
    MutableJSON,
    PrimitiveJSON,
    check_type,
    json_dict,
    json_list,
)
from azul.lib.uuids import (
    InvalidUUIDError,
    validate_uuid,
)
from azul.openapi import (
    params,
    responses,
    schema,
)
from azul.plugins import (
    Sorting,
)
from azul.service import (
    BadArgumentException,
)
from azul.service.controller import (
    validate_params,
)
from azul.service.index_service import (
    IndexService,
    SummaryResponseStage,
)
from azul.service.query_controller import (
    QueryController,
)
from azul.service.query_service import (
    Pagination,
    SortKey,
    sort_key_to_json,
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
                },
                '404': {
                    'description': fd('''
                        No entity of the given type has the given UUID, in the
                        given catalog and among the sources the request is
                        authorized to access.
                    ''')
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
            'description':
                iif(post, self._parameter_hoisting_note('GET', '/index/files', 'POST') + fd('''

                Note that the Swagger UI can't currently be used to pass a body.
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
                            pagination=self._generic_object_schema,
                            termFacets=self._generic_object_schema,
                            hits=schema.array(self._hit_schema)
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

    @cached_property
    def _summary_response_stage(self) -> SummaryResponseStage:
        return self._metadata_plugin.summary_response_stage()

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
        def get_head_post_index_entity_type(entity_type: str) -> str | JSON:
            return self.search(entity_type)

        @self.app.route(
            '/index/{entity_type}/{entity_id}',
            methods=['GET'],
            spec=self._search_entity_spec(),
            cors=True
        )
        def get_index_entity_type_and_id(entity_type: str, entity_id: str) -> JSON:
            return self.retrieve(entity_type, entity_id)

        @self.app.route(
            '/index/summary',
            methods=['GET'],
            cors=True,
            spec={
                'summary': 'Statistics on the data present across all entities.',
                'responses': {
                    '200': {
                        'description': self._summary_response_stage.response_description,
                        **responses.json_content(self._summary_response_stage.response_schema)
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

    def search(self, entity_type: str) -> str | JSON:
        request = self.current_request
        query_params = self._hoist_parameters(request)
        # Validate `catalog` first since `_validate_entity_type` requires a
        # valid catalog to load the metadata plugin.
        validate_params(query_params,
                        catalog=self._validate_catalog,
                        allow_extra_params=True)
        # Likewise, `_validate_size` requires a valid entity type.
        self._validate_entity_type(entity_type)
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
        filters = query_params.get('filters')
        authentication = self._authentication(request)
        filters = self._prepare_filters(self.app.catalog, authentication, filters)
        pagination = self._Pagination.from_request(self, entity_type, filters)
        response = self._search(entity_type, filters, pagination, aggregate=True)
        return '' if request.method == 'HEAD' else response

    def retrieve(self, entity_type: str, entity_id: str) -> JSON:
        """
        Return the one entity of the given type with the given ID. Unlike
        :py:meth:`search`, this endpoint takes no query parameters other than
        `catalog`, so that the entity returned is determined by the path alone.
        """
        request = self.current_request
        query_params = self._query_params(request)
        # `_validate_entity_type` requires a valid catalog to load the plugin
        validate_params(query_params, catalog=self._validate_catalog)
        self._validate_entity_type(entity_type)
        try:
            validate_uuid(entity_id)
        except InvalidUUIDError as e:
            raise BadRequestError(e)
        sort = Sorting(field_name='entryId')
        pagination = Pagination(sort=sort.field_name, order=sort.order, size=1)
        authentication = self._authentication(request)
        filters = self._prepare_filters(self.app.catalog, authentication, None)
        filters = filters.set_explicit({'entryId': {'is': [entity_id]}})
        response = self._search(entity_type, filters, pagination, aggregate=False)
        hits = json_list(response['hits'])
        match hits:
            case []:
                raise NotFoundError(
                    f'No {entity_type!r} entity with UUID {entity_id!r} was found'
                )
            case [hit]:
                return json_dict(hit)
            case _:
                assert False, len(hits)

    def _search(self,
                entity_type: str,
                filters: Filters,
                pagination: Pagination,
                *,
                aggregate: bool
                ) -> MutableJSON:
        try:
            response = self._service.search(catalog=self.app.catalog,
                                            entity_type=entity_type,
                                            file_url_func=self._file_url,
                                            aggregate=aggregate,
                                            filters=filters,
                                            pagination=pagination)
        except BadArgumentException as e:
            raise BadRequestError(e)
        return response

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
        controller: IndexController

        #: The explicit filters of the request, in the normalized form
        #: produced by :py:func:`azul.filters.parse_filters`
        #:
        filters: str

        #: How much of the HMAC digest to keep in a signature. A forgery
        #: means guessing a MAC for a chosen context, and the only way to
        #: test a guess is to send a request, so the bound is on how many
        #: requests an attacker can make, not on their compute. At 72 bits,
        #: a one in a million chance of a single forgery takes on the order
        #: of 10^16 requests. A multiple of three bytes encodes to base64
        #: without wasting any bits.
        #:
        _signature_bytes: ClassVar[int] = 9

        @classmethod
        def from_request(cls,
                         controller: IndexController,
                         entity_type: str,
                         filters: Filters
                         ) -> Self:
            default_sorting = controller._metadata_plugin.exposed_indices[entity_type]
            request = controller.current_request
            params = controller._query_params(request)
            # The sort key is verified against the instance it belongs to, so
            # that instance must exist before the key is parsed
            self = cls(order=params.get('order', default_sorting.order),
                       size=int(params.get('size', '10')),
                       sort=params.get('sort', default_sorting.field_name),
                       controller=controller,
                       filters=json.dumps(filters.explicit))
            try:
                sb, sa = self._pagination_params(params)
            except AssertionError as e:
                if R.caused(e):
                    raise R.propagate(e, BadRequestError)
                else:
                    raise
            else:
                return self.with_sort_keys(search_before=sb, search_after=sa)

        def _pagination_params(self,
                               params: Mapping[str, str]
                               ) -> tuple[SortKey, None] | tuple[None, SortKey] | tuple[None, None]:
            sb, sa = params.get('search_before'), params.get('search_after')
            if sb is None:
                if sa is None:
                    return None, None
                else:
                    return None, self._sort_key(sa, previous=False)
            else:
                if sa is None:
                    return self._sort_key(sb, previous=True), None
                else:
                    assert False, R('Only one of search_after or search_before may be set')

        def _sort_key(self, sort_key: str, *, previous: bool) -> SortKey:
            sort_key = json.loads(sort_key)
            assert isinstance(sort_key, list), R(
                'Not a list', sort_key)
            assert len(sort_key) == 3, R(
                'Not a tuple with three elements', sort_key)
            a, b, signature = sort_key
            assert check_type(PrimitiveJSON, a), R(
                'First sort key element is not primitive JSON', sort_key)
            assert isinstance(b, str), R(
                'Second sort key element not a string', sort_key)
            assert isinstance(signature, str), R(
                'Third sort key element not a string', sort_key)
            expected = self._sign(previous=previous, sort_key=(a, b))
            assert hmac.compare_digest(signature, expected), R(
                'Sort key signature does not match the other request parameters')
            return a, b

        def _sign(self, *, previous: bool, sort_key: SortKey) -> str:
            """
            Return a signature tying the given sort key to the request
            parameters that determine the result set into which that sort key
            points. A client that sends the key back with any of those
            parameters altered can be detected, and the request rejected, before
            it is translated into a query.
            """
            app = self.controller.app
            key, key_id = aws.get_hmac_key_and_id()
            # The page size is absent from the signature because changing it
            # can neither skip nor duplicate hits.
            context = [
                str(app.self_url.path),
                app.catalog,
                self.filters,
                self.sort,
                self.order,
                previous,
                sort_key_to_json(sort_key)
            ]
            payload = json.dumps(context, separators=(',', ':'))
            digest = hmac.new(key, payload.encode(), hashlib.sha256).digest()
            digest = digest[:self._signature_bytes]
            return base64.urlsafe_b64encode(digest).decode().rstrip('=')

        def link(self, *, previous: bool) -> furl | None:
            sort_key = self.search_before if previous else self.search_after
            if sort_key is None:
                return None
            else:
                before_or_after = 'before' if previous else 'after'
                # The signature travels inside the sort keys as opposed to being
                # a separate parameter, because clients are known to reverse
                # engineer the paging links, discarding every other parameter
                # (DataBiosphere/data-browser#5014).
                #
                # FIXME: Make the signature a separate request parameter
                #        https://github.com/DataBiosphere/azul/issues/8458
                #
                signature = self._sign(previous=previous, sort_key=sort_key)
                signed_key = [*sort_key, signature]
                params = {
                    'catalog': self.controller.app.catalog,
                    'filters': self.filters,
                    f'search_{before_or_after}': json.dumps(signed_key),
                    'sort': self.sort,
                    'order': self.order,
                    'size': str(self.size)
                }
            return furl(url=self.controller.app.self_url, args=params)
