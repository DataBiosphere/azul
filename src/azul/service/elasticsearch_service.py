from abc import (
    ABCMeta,
    abstractmethod,
)
import json
import logging
from typing import (
    Any,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    TypedDict,
)

import attr
from elasticsearch import (
    Elasticsearch,
)
from elasticsearch_dsl import (
    A,
    Q,
    Search,
)
from elasticsearch_dsl.aggs import (
    Agg,
    Terms,
)
from elasticsearch_dsl.query import (
    Query,
)
from elasticsearch_dsl.response import (
    Response,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    cached_property,
    config,
    reject,
    require,
)
from azul.es import (
    ESClientFactory,
)
from azul.indexer.document import (
    Nested,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.plugins import (
    DocumentSlice,
    FieldPath,
    MetadataPlugin,
    dotted,
)
from azul.service import (
    Filters,
    FiltersJSON,
)
from azul.types import (
    JSON,
    JSONs,
    MutableJSON,
    PrimitiveJSON,
)

log = logging.getLogger(__name__)


class IndexNotFoundError(Exception):

    def __init__(self, missing_index: str):
        super().__init__(f'Index `{missing_index}` was not found')


R1 = TypeVar('R1')
R2 = TypeVar('R2')


class ElasticsearchStage(Generic[R1, R2], metaclass=ABCMeta):
    """
    A stage in a chain of responsibility to prepare an Elasticsearch request and
    to process the response to that request. If an implementation modifies the
    argument in place, it must return the argument.
    """

    @abstractmethod
    def prepare_request(self, request: Search) -> Search:
        """
        Modify the given request and return the argument or convert the given
        request and return the result of the conversion.
        """
        raise NotImplementedError

    @abstractmethod
    def process_response(self, response: R1) -> R2:
        """
        Handle the given response and return the result of the processing.
        If an implementation modifies the argument in place it must return the
        argument.
        """
        raise NotImplementedError


R0 = TypeVar('R0')


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ElasticsearchChain(ElasticsearchStage[R0, R2]):
    """
    The result of wrapping a stage or chain in another stage.
    """

    inner: ElasticsearchStage[R0, R1]
    outer: ElasticsearchStage[R1, R2]

    def __attrs_post_init__(self):
        reject(isinstance(self.outer, ElasticsearchChain),
               'Outer stage must not be a chain', type(self.outer))

    def prepare_request(self, request: Search) -> Search:
        request = self.inner.prepare_request(request)
        request = self.outer.prepare_request(request)
        return request

    def process_response(self, response0: R0) -> R2:
        response1: R1 = self.inner.process_response(response0)
        response2: R2 = self.outer.process_response(response1)
        return response2

    def stages(self) -> Iterable[ElasticsearchStage]:
        yield self.outer
        if isinstance(self.inner, ElasticsearchChain):
            yield from self.inner.stages()
        else:
            yield self.inner


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class _ElasticsearchStage(ElasticsearchStage[R1, R2], metaclass=ABCMeta):
    """
    A base implementation of a stage.
    """
    service: DocumentService
    catalog: CatalogName
    entity_type: str

    @cached_property
    def plugin(self) -> MetadataPlugin:
        return self.service.metadata_plugin(self.catalog)

    def wrap(self, other: ElasticsearchStage[R0, R1]) -> ElasticsearchChain[R0, R2]:
        return ElasticsearchChain(inner=other, outer=self)


TranslatedFilters = Mapping[FieldPath, Mapping[str, Sequence[PrimitiveJSON]]]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class FilterStage(_ElasticsearchStage[Response, Response]):
    """
    Converts the given filters to an Elasticsearch query and adds that query as
    either a `query` or `post_filter` property to the request.
    """
    filters: Filters
    post_filter: bool

    def prepare_request(self, request: Search) -> Search:
        query = self.prepare_query()
        if self.post_filter:
            request = request.post_filter(query)
        else:
            request = request.query(query)
        return request

    def process_response(self, response: Response) -> Response:
        return response

    @cached_property
    def prepared_filters(self) -> TranslatedFilters:
        return self._translate_filters(self._reify_filters())

    def _reify_filters(self):
        return self.filters.reify(self.plugin)

    def _translate_filters(self, filters: FiltersJSON) -> TranslatedFilters:
        """
        Translate the field values in the given filter JSON to their respective
        Elasticsearch form, using the field types, the field names to field
        paths.
        """
        catalog = self.catalog
        field_mapping = self.plugin.field_mapping
        translated_filters = {}
        for field, filter in filters.items():
            field = field_mapping[field]
            operator, values = one(filter.items())
            field_type = self.service.field_type(catalog, field)
            if isinstance(field_type, Nested):
                nested_object = one(values)
                assert isinstance(nested_object, dict)
                query_filters = {}
                for nested_field, nested_value in nested_object.items():
                    nested_type = field_type.properties[nested_field]
                    to_index = nested_type.to_index
                    value = one(values)[nested_field]
                    query_filters[nested_field] = to_index(value)
                translated_filters[field] = {operator: [query_filters]}
            else:
                to_index = field_type.to_index
                translated_filters[field] = {
                    operator: [(to_index(start), to_index(end)) for start, end in values]
                    if operator in ('contains', 'within', 'intersects') else
                    list(map(to_index, values))
                }
        return translated_filters

    def prepare_query(self, skip_field_paths: Tuple[FieldPath] = ()) -> Query:
        """
        Converts the given filters into an Elasticsearch DSL Query object.
        """
        filter_list = []
        for field_path, values in self.prepared_filters.items():
            if field_path not in skip_field_paths:
                relation, value = one(values.items())
                if relation == 'is':
                    field_type = self.service.field_type(self.catalog, field_path)
                    if isinstance(field_type, Nested):
                        term_queries = []
                        for nested_field, nested_value in one(value).items():
                            nested_body = {dotted(field_path, nested_field, 'keyword'): nested_value}
                            term_queries.append(Q('term', **nested_body))
                        query = Q('nested', path=dotted(field_path), query=Q('bool', must=term_queries))
                    else:
                        query = Q('terms', **{dotted(field_path, 'keyword'): value})
                        translated_none = field_type.to_index(None)
                        if translated_none in value:
                            # Note that at this point None values in filters have already
                            # been translated eg. {'is': ['~null']} and if the filter has a
                            # None our query needs to find fields with None values as well
                            # as absent fields
                            absent_query = Q('bool', must_not=[Q('exists', field=dotted(field_path))])
                            query = Q('bool', should=[query, absent_query])
                    filter_list.append(query)
                elif relation in ('contains', 'within', 'intersects'):
                    for min_value, max_value in value:
                        range_value = {
                            'gte': min_value,
                            'lte': max_value,
                            'relation': relation
                        }
                        filter_list.append(Q('range', **{dotted(field_path): range_value}))
                else:
                    assert False

        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=f) for f in filter_list]

        return Q('bool', must=query_list)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class AggregationStage(_ElasticsearchStage[MutableJSON, MutableJSON]):
    """
    Cooperate with the given filter stage to augment the request with an
    `aggregation` property containing an aggregation for each of the facet
    fields configured in the current metadata plugin. If this aggregation stage
    is to be part of a chain, the chain should include the given filter stage.
    """
    filter_stage: FilterStage

    @classmethod
    def create_and_wrap(cls,
                        chain: ElasticsearchChain[R0, MutableJSON]
                        ) -> ElasticsearchChain[R0, MutableJSON]:
        """
        Creates and adds an aggregation stage to the specified chain. The chain
        must contain a filter stage.
        """
        filter_stage = one(s for s in chain.stages() if isinstance(s, FilterStage))
        aggregation_stage = cls(service=filter_stage.service,
                                catalog=filter_stage.catalog,
                                entity_type=filter_stage.entity_type,
                                filter_stage=filter_stage)
        return aggregation_stage.wrap(chain)

    def prepare_request(self, request: Search) -> Search:
        field_mapping = self.plugin.field_mapping
        for facet in self.plugin.facets:
            # FIXME: Aggregation filters may be redundant when post_filter is false
            #        https://github.com/DataBiosphere/azul/issues/3435
            aggregate = self._prepare_aggregation(facet=facet,
                                                  facet_path=field_mapping[facet])
            request.aggs.bucket(facet, aggregate)
        self._annotate_aggs_for_translation(request)
        return request

    def process_response(self, response: MutableJSON) -> MutableJSON:
        try:
            aggs = response['aggregations']
        except KeyError:
            pass
        else:
            self._translate_response_aggs(aggs)
        return response

    def _prepare_aggregation(self, *, facet: str, facet_path: FieldPath) -> Agg:
        """
        Creates an aggregation to be used in a Elasticsearch search request.
        """
        # Create a filter agg using a query that represents all filters
        # except for the current facet.
        query = self.filter_stage.prepare_query(skip_field_paths=(facet_path,))
        agg = A('filter', query)

        # Make an inner agg that will contain the terms in question
        path = dotted(facet_path, 'keyword')
        # FIXME: Approximation errors for terms aggregation are unchecked
        #        https://github.com/DataBiosphere/azul/issues/3413
        agg.bucket(name='myTerms',
                   agg_type='terms',
                   field=path,
                   size=config.terms_aggregation_size)
        agg.bucket('untagged', 'missing', field=path)
        return agg

    def _annotate_aggs_for_translation(self, request: Search):
        """
        Annotate the aggregations in the given Elasticsearch search request so
        we can later translate substitutes for None in the aggregations part of
        the response.
        """

        def annotate(agg: Agg):
            if isinstance(agg, Terms):
                path = agg.field.split('.')
                if path[-1] == 'keyword':
                    path.pop()
                if not hasattr(agg, 'meta'):
                    agg.meta = {}
                agg.meta['path'] = path
            if hasattr(agg, 'aggs'):
                subs = agg.aggs
                for sub_name in subs:
                    annotate(subs[sub_name])

        for agg_name in request.aggs:
            annotate(request.aggs[agg_name])

    def _translate_response_aggs(self, aggs: MutableJSON):
        """
        Translate substitutes for None in the aggregations part of an
        Elasticsearch response.
        """

        def translate(k, v: MutableJSON):
            try:
                buckets = v['buckets']
            except KeyError:
                for k, v in v.items():
                    if isinstance(v, dict):
                        translate(k, v)
            else:
                try:
                    path = v['meta']['path']
                except KeyError:
                    pass
                else:
                    field_type = self.service.field_type(self.catalog, tuple(path))
                    for bucket in buckets:
                        bucket['key'] = field_type.from_index(bucket['key'])
                        translate(k, bucket)

        for k, v in aggs.items():
            translate(k, v)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SlicingStage(_ElasticsearchStage[Response, Response]):
    """
    Augments the request with a document slice (known as a *source filter* in
    Elasticsearch land) to restrict the set of properties in each hit in the
    response. If the given document slice is None, the default one from the
    plugin is used. If that is None, too, each hit will contain all properties.
    """
    document_slice: Optional[DocumentSlice]

    def prepare_request(self, request: Search) -> Search:
        document_slice = self._prepared_slice()
        if document_slice is not None:
            request = request.source(**document_slice)
        return request

    def process_response(self, response: Response) -> Response:
        return response

    def _prepared_slice(self) -> Optional[DocumentSlice]:
        if self.document_slice is None:
            return self.plugin.document_slice(self.entity_type)
        else:
            return None


# FIXME: Elminate Eliminate reliance on Elasticsearch DSL
#        https://github.com/DataBiosphere/azul/issues/4111

@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ToDictStage(_ElasticsearchStage[Response, MutableJSON]):

    def prepare_request(self, request: Search) -> Search:
        return request

    def process_response(self, response: Response) -> MutableJSON:
        return response.to_dict()


SortKey = Tuple[Any, str]


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Pagination:
    order: str
    size: int
    sort: str
    search_before: Optional[SortKey] = None
    search_after: Optional[SortKey] = None

    def __attrs_post_init__(self):
        self._check_sort_key(self.search_before)
        self._check_sort_key(self.search_after)

    def _check_sort_key(self, sort_key):
        if sort_key is not None:
            require(isinstance(sort_key, tuple), 'Not a tuple', sort_key)
            require(len(sort_key) == 2, 'Not a tuple with two elements', sort_key)
            require(isinstance(sort_key[1], str), 'Second sort key element not a string', sort_key)

    def advance(self,
                *,
                search_before: Optional[SortKey],
                search_after: Optional[SortKey]
                ) -> 'Pagination':
        return attr.evolve(self,
                           search_before=search_before,
                           search_after=search_after)

    def link(self, *, previous: bool, **params: str) -> Optional[str]:
        """
        Return the URL of the next or previous page in this pagination or None
        if there is no such page.

        :param previous: True, for a link to the previous page, False for a link
                         to the next one.

        :param params: Additional query parameters to embed in the the URL
        """
        return None


class ResponsePagination(TypedDict):
    count: int
    total: int
    size: int
    pages: int
    next: Optional[str]
    previous: Optional[str]
    sort: str
    order: str


ResponseTriple = Tuple[JSONs, ResponsePagination, JSON]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class PaginationStage(_ElasticsearchStage[JSON, ResponseTriple]):
    """
    Handles the pagination of search results
    """
    pagination: Pagination

    #: If True, request one more hit so that _generate_paging_dict can know if
    #: there is another page. Use this to prevent a last page that's empty.
    peek_ahead: bool

    filters: Filters

    def prepare_request(self, request: Search) -> Search:
        sort_order = self.pagination.order
        sort_field = self.plugin.field_mapping[self.pagination.sort]
        field_type = self.service.field_type(self.catalog, sort_field)
        sort_mode = field_type.es_sort_mode
        sort_field = dotted(sort_field, 'keyword')

        def sort(order):
            assert order in ('asc', 'desc'), order
            return (
                {
                    sort_field: {
                        'order': order,
                        'mode': sort_mode,
                        'missing': '_last' if order == 'asc' else '_first',
                        **(
                            {}
                            if field_type.es_type is None else
                            {'unmapped_type': field_type.es_type}
                        )
                    }
                },
                # This secondary sort field serves as the tie breaker for when
                # the primary sort field is not unique across documents.
                # Otherwise it's redundant, especially its the same as the
                # primary sort field. However, always having a secondary
                # simplifies the code and most real-world use cases use sort
                # fields that are not unique.
                {
                    'entity_id.keyword': {
                        'order': order
                    }
                }
            )

        # Using search_after/search_before pagination
        if self.pagination.search_after is not None:
            request = request.extra(search_after=self.pagination.search_after)
            request = request.sort(*sort(sort_order))
        elif self.pagination.search_before is not None:
            request = request.extra(search_after=self.pagination.search_before)
            rev_order = 'asc' if sort_order == 'desc' else 'desc'
            request = request.sort(*sort(rev_order))
        else:
            request = request.sort(*sort(sort_order))

        # FIXME: Remove this or change to 10000 (the default)
        #        https://github.com/DataBiosphere/azul/issues/3770
        request = request.extra(track_total_hits=True)

        assert isinstance(self.peek_ahead, bool), type(self.peek_ahead)
        # fetch one more than needed to see if there's a "next page".
        request = request.extra(size=self.pagination.size + self.peek_ahead)

        return request

    def process_response(self, response: JSON) -> ResponseTriple:
        """
        Returns hits and pagination as dict
        """
        # The slice is necessary because we may have fetched an extra entry to
        # determine if there is a previous or next page.
        hits = self._extract_hits(response)
        hits = self._translate_hits(hits)
        pagination = self._process_pagination(response)
        aggregations = response.get('aggregations', {})
        return hits, pagination, aggregations

    def _extract_hits(self, response):
        hits = response['hits']['hits'][0:self.pagination.size]
        if self.pagination.search_before is not None:
            hits = reversed(hits)
        hits = [hit['_source'] for hit in hits]
        return hits

    def _translate_hits(self, hits):
        hits = self.service.translate_fields(self.catalog, hits, forward=False)
        return hits

    def _process_pagination(self, response: JSON) -> MutableJSON:
        total = response['hits']['total']
        # FIXME: Handle other relations
        #        https://github.com/DataBiosphere/azul/issues/3770
        assert total['relation'] == 'eq'
        pages = -(-total['value'] // self.pagination.size)

        # ... else use search_after/search_before pagination
        hits: JSONs = response['hits']['hits']
        count = len(hits)
        if self.pagination.search_before is None:
            # hits are normal sorted
            if count > self.pagination.size:
                # There is an extra hit, indicating a next page.
                count -= 1
                search_after = tuple(hits[count - 1]['sort'])
            else:
                # No next page
                search_after = None
            if self.pagination.search_after is not None:
                search_before = tuple(hits[0]['sort'])
            else:
                search_before = None
        else:
            # hits are reverse sorted
            if count > self.pagination.size:
                # There is an extra hit, indicating a previous page.
                count -= 1
                search_before = tuple(hits[count - 1]['sort'])
            else:
                # No previous page
                search_before = None
            search_after = tuple(hits[0]['sort'])

        pagination = self.pagination.advance(search_before=search_before,
                                             search_after=search_after)

        def page_link(*, previous):
            url = pagination.link(previous=previous,
                                  catalog=self.catalog,
                                  filters=json.dumps(self.filters.explicit))
            return None if url is None else str(url)

        return ResponsePagination(count=count,
                                  total=total['value'],
                                  size=pagination.size,
                                  next=page_link(previous=False),
                                  previous=page_link(previous=True),
                                  pages=pages,
                                  sort=pagination.sort,
                                  order=pagination.order)


class ElasticsearchService(DocumentService):

    @cached_property
    def _es_client(self) -> Elasticsearch:
        return ESClientFactory.get()

    def create_chain(self,
                     *,
                     catalog: CatalogName,
                     entity_type: str,
                     filters: Filters,
                     post_filter: bool,
                     document_slice: Optional[DocumentSlice]
                     ) -> ElasticsearchChain[Response, Response]:
        """
        Create a chain for a basic Elasticsearch `search` request for documents
        matching the given filter, optionally restricting the set of properties
        returned for each matching document.
        """
        plugin = self.metadata_plugin(catalog)

        # noinspection PyArgumentList
        chain = plugin.filter_stage(service=self,
                                    catalog=catalog,
                                    entity_type=entity_type,
                                    filters=filters,
                                    post_filter=post_filter)
        chain = SlicingStage(service=self,
                             catalog=catalog,
                             entity_type=entity_type,
                             document_slice=document_slice).wrap(chain)
        return chain

    def create_request(self, catalog, entity_type) -> Search:
        """
        Create an Elasticsearch request against the index containing aggregate
        documents for the given entity type in the given catalog.
        """
        return Search(using=self._es_client,
                      index=config.es_index_name(catalog=catalog,
                                                 entity_type=entity_type,
                                                 aggregate=True))
