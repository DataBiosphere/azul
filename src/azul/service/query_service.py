from abc import (
    ABCMeta,
    abstractmethod,
)
from collections import (
    defaultdict,
)
from collections.abc import (
    Iterable,
    Mapping,
)
import json
import logging
from typing import (
    Any,
    Self,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    one,
)
from opensearchpy import (
    A,
    OpenSearch,
    Q,
    Search,
)
from opensearchpy.helpers.aggs import (
    Agg,
    Terms,
)
from opensearchpy.helpers.query import (
    Query,
)
from opensearchpy.helpers.response import (
    Response,
)

from azul import (
    CatalogName,
    config,
)
from azul.field_type import (
    Nested,
)
from azul.indexer.document import (
    DocumentType,
    FieldPath,
    IndexName,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.lib import (
    R,
    cached_property,
)
from azul.lib.types import (
    AnyJSON,
    JSON,
    JSONArray,
    JSONTypedDict,
    JSONs,
    MutableJSON,
    PrimitiveJSON,
    json_dict,
    json_dict_of_dicts,
    json_element_dicts,
    json_element_strings,
    json_int,
    json_item_sequences,
    json_list_of_dicts,
    json_mapping,
    json_primitive,
    json_sequence,
    json_sequence_of_mappings,
    json_str,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)
from azul.plugins import (
    DocumentSlice,
    MetadataPlugin,
    dotted,
)
from azul.service import (
    FilterJSON,
    Filters,
    FiltersJSON,
)

log = logging.getLogger(__name__)


class IndexNotFoundError(Exception):

    def __init__(self, missing_index: str):
        super().__init__(f'Index `{missing_index}` was not found')


class OpenSearchStage[R1, R2](metaclass=ABCMeta):
    """
    A stage in a chain of responsibility to prepare an OpenSearch request and
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


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class OpenSearchChain[R0, R1, R2](OpenSearchStage[R0, R2]):
    """
    The result of wrapping a stage or chain in another stage.
    """

    inner: OpenSearchStage[R0, R1]
    outer: OpenSearchStage[R1, R2]

    def __attrs_post_init__(self):
        assert not isinstance(self.outer, OpenSearchChain), R(
            'Outer stage must not be a chain', type(self.outer))

    def prepare_request(self, request: Search) -> Search:
        request = self.inner.prepare_request(request)
        request = self.outer.prepare_request(request)
        return request

    def process_response(self, response0: R0) -> R2:
        response1: R1 = self.inner.process_response(response0)
        response2: R2 = self.outer.process_response(response1)
        return response2

    def stages(self) -> Iterable[OpenSearchStage]:
        yield self.outer
        if isinstance(self.inner, OpenSearchChain):
            yield from self.inner.stages()
        else:
            yield self.inner


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class _OpenSearchStage[R1, R2](OpenSearchStage[R1, R2], metaclass=ABCMeta):
    """
    A base implementation of a stage.
    """
    service: DocumentService
    catalog: CatalogName
    entity_type: str

    @cached_property
    def plugin(self) -> MetadataPlugin:
        return self.service.metadata_plugin(self.catalog)

    def wrap[R0](self, other: OpenSearchStage[R0, R1]) -> OpenSearchChain[R0, R1, R2]:
        return OpenSearchChain(inner=other, outer=self)


TranslatedFilters = Mapping[FieldPath, Mapping[str, JSONArray]]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class FilterStage(_OpenSearchStage[Response, Response]):
    """
    Converts the given filters to an OpenSearch query and adds that query as
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
        limit_access = self.service.always_limit_access or self._limit_access
        filters_json = self.filters.reify(self.plugin, limit_access=limit_access)
        return self._translate_filters(filters_json)

    @property
    @abstractmethod
    def _limit_access(self) -> bool:
        """
        Whether to enforce the managed access controls during filter
        reification, provided that the service allows such conditional
        enforcement of access. If it doesn't, the return value should be
        ignored, and access must be enforced unconditionally.
        """
        raise NotImplementedError

    def _translate_filters(self, filters: FiltersJSON) -> TranslatedFilters:
        """
        Translate the field values in the given filter JSON to their respective
        OpenSearch form, using the field types, the field names to field
        paths.
        """
        catalog = self.catalog
        field_mapping = self.plugin.field_mapping

        def translate_filter(field_name: str,
                             filter: FilterJSON
                             ) -> tuple[FieldPath, Mapping[str, JSONArray]]:
            field_path = field_mapping[field_name]
            operator, values = one(filter.items())
            field_type = self.service.field_type(catalog, field_path)
            # FIXME: remove `type: ignore`
            #        https://github.com/DataBiosphere/azul/issues/6821
            values: JSONArray = list(field_type.filter(operator, values))  # type: ignore
            return field_path, {operator: values}

        return dict(
            translate_filter(field, filter)
            for field, filter in filters.items()
        )

    def prepare_query(self, skip_field_paths: tuple[FieldPath, ...] = ()) -> Query:
        """
        Converts the given filters into an OpenSearch DSL Query object.
        """
        filter_list = []
        for field_path, filter in self.prepared_filters.items():
            if field_path not in skip_field_paths:
                operator, values = one(json_item_sequences(filter))
                # Note that `is_not` is only used internally (for filtering by
                # inaccessible sources)
                if operator in ('is', 'is_not'):
                    field_type = self.service.field_type(self.catalog, field_path)
                    if isinstance(field_type, Nested):
                        term_queries = []
                        for nested_field, nested_value in json_mapping(one(values)).items():
                            nested_body = {dotted(field_path, nested_field, 'keyword'): nested_value}
                            term_queries.append(Q('term', **nested_body))
                        query = Q('nested', path=dotted(field_path), query=Q('bool', must=term_queries))
                    else:
                        query = Q('terms', **{dotted(field_path, 'keyword'): values})
                        translated_none = field_type.to_index(None)
                        if translated_none in values:
                            # Note that at this point None values in filters have already
                            # been translated e.g. {'is': ['~null']} and if the filter has a
                            # None our query needs to find fields with None values as well
                            # as absent fields
                            absent_query = Q('bool', must_not=[Q('exists', field=dotted(field_path))])
                            query = Q('bool', should=[query, absent_query])
                    if operator == 'is_not':
                        query = Q('bool', must_not=[query])
                    filter_list.append(query)
                elif operator in ('contains', 'within', 'intersects'):
                    for value in values:
                        value = {**json_mapping(value), 'relation': operator}
                        filter_list.append(Q('range', **{dotted(field_path): value}))
                else:
                    assert False

        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=f) for f in filter_list]

        return Q('bool', must=query_list)


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class AggregationStage(_OpenSearchStage[MutableJSON, MutableJSON]):
    """
    Cooperate with the given filter stage to augment the request with an
    `aggregation` property containing an aggregation for each of the facet
    fields configured in the current metadata plugin. If this aggregation stage
    is to be part of a chain, the chain should include the given filter stage.
    """
    filter_stage: FilterStage

    @classmethod
    def create_and_wrap[R0](cls,
                            chain: OpenSearchChain[R0, MutableJSON, MutableJSON]
                            ) -> OpenSearchChain[R0, MutableJSON, MutableJSON]:
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
            aggs = json_dict(response['aggregations'])
        except KeyError:
            pass
        else:
            self._flatten_nested_aggs(aggs)
            self._translate_response_aggs(aggs)
            self._populate_accessible(aggs)
        return response

    def _prepare_aggregation(self, *, facet: str, facet_path: FieldPath) -> Agg:
        """
        Creates an aggregation to be used in an OpenSearch search request.
        """
        # Create a filter agg using a query that represents all filters
        # except for the current facet.
        query = self.filter_stage.prepare_query(skip_field_paths=(facet_path,))
        agg = A('filter', query)

        field_type = self.service.field_type(self.catalog, facet_path)
        if isinstance(field_type, Nested):
            nested_agg = agg.bucket(name='nested',
                                    agg_type='nested',
                                    path=dotted(facet_path))
            facet_path = (*facet_path, field_type.agg_property)
        else:
            nested_agg = agg
        # Make an inner agg that will contain the terms in question
        path = dotted(facet_path, 'keyword')
        # FIXME: Approximation errors for terms aggregation are unchecked
        #        https://github.com/DataBiosphere/azul/issues/3413
        nested_agg.bucket(name='myTerms',
                          agg_type='terms',
                          field=path,
                          size=config.terms_aggregation_size)
        nested_agg.bucket('untagged', 'missing', field=path)
        return agg

    def _annotate_aggs_for_translation(self, request: Search):
        """
        Annotate the aggregations in the given OpenSearch search request so
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

    def _flatten_nested_aggs(self, aggs: MutableJSON):
        for facet, agg in json_dict_of_dicts(aggs).items():
            try:
                nested_agg = json_dict(agg.pop('nested'))
            except KeyError:
                pass
            else:
                agg.update(nested_agg)

    def _translate_response_aggs(self, aggs: MutableJSON):
        """
        Translate substitutes for None in the aggregations part of an
        OpenSearch response.
        """

        def translate(k: str, v: MutableJSON):
            try:
                buckets = v['buckets']
            except KeyError:
                for ki, vi in v.items():
                    if isinstance(vi, dict):
                        translate(ki, vi)
            else:
                try:
                    path = json_dict(v['meta'])['path']
                except KeyError:
                    pass
                else:
                    field_type = self.service.field_type(self.catalog,
                                                         tuple(json_element_strings(path)))
                    for bucket in json_element_dicts(buckets):
                        bucket['key'] = field_type.from_index(bucket['key'])
                        translate(k, bucket)

        for k, v in aggs.items():
            translate(k, json_dict(v))

    def _populate_accessible(self, aggs: MutableJSON) -> None:
        # Because the value of the `accessible` field depends on the provided
        # authentication, we have to synthesize the field and its corresponding
        # facet from the `sourceId` field.
        source_ids = self.filter_stage.filters.source_ids
        plugin = self.service.metadata_plugin(self.catalog)
        special_fields = plugin.special_fields
        agg = json_dict(aggs.pop(special_fields.source_id.name))
        counts_by_accessibility: dict[bool, int] = defaultdict(int)
        terms = json_dict(agg['myTerms'])
        buckets = json_list_of_dicts(terms['buckets'])
        for bucket in buckets:
            accessible = bucket['key'] in source_ids
            counts_by_accessibility[accessible] += json_int(bucket['doc_count'])
        terms['buckets'] = [
            {'key': accessible, 'doc_count': count}
            for accessible, count in counts_by_accessibility.items()
        ]
        aggs[special_fields.accessible.name] = agg


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SlicingStage(_OpenSearchStage[Response, Response]):
    """
    Augments the request with a document slice (known as a *source filter* in
    OpenSearch land) to restrict the set of properties in each hit in the
    response. If the given document slice is None, the default one from the
    plugin is used. If that is None, too, each hit will contain all properties.
    """
    document_slice: DocumentSlice | None

    def prepare_request(self, request: Search) -> Search:
        document_slice = self._prepared_slice()
        if document_slice is not None:
            request = request.source(**document_slice)
        return request

    def process_response(self, response: Response) -> Response:
        return response

    def _prepared_slice(self) -> DocumentSlice | None:
        if self.document_slice is None:
            return self.plugin.document_slice(self.entity_type)
        else:
            return self.document_slice


# FIXME: Elminate Eliminate reliance on Elasticsearch DSL
#        https://github.com/DataBiosphere/azul/issues/4111

@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class ToDictStage(_OpenSearchStage[Response, MutableJSON]):

    def prepare_request(self, request: Search) -> Search:
        return request

    def process_response(self, response: Response) -> MutableJSON:
        return response.to_dict()


type SortKey = tuple[PrimitiveJSON, str]


def sort_key_from_json(s: AnyJSON) -> SortKey:
    a, b = json_sequence(s)
    return json_primitive(a), json_str(b)


def sort_key_to_json(s: SortKey) -> AnyJSON:
    return list(s)


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Pagination:
    order: str
    size: int
    sort: str
    search_before: SortKey | None = None
    search_after: SortKey | None = None

    def advance(self,
                *,
                search_before: SortKey | None,
                search_after: SortKey | None
                ) -> Self:
        return attr.evolve(self,
                           search_before=search_before,
                           search_after=search_after)

    def link(self, *, previous: bool, **params: str) -> furl | None:
        """
        Return the URL of the next or previous page in this pagination or None
        if there is no such page.

        :param previous: True, for a link to the previous page, False for a link
                         to the next one.

        :param params: Additional query parameters to embed in the URL
        """
        return None


class ResponsePagination(JSONTypedDict):
    count: int
    total: int
    size: int
    pages: int
    next: str | None
    previous: str | None
    sort: str
    order: str


ResponseTriple = tuple[JSONs, ResponsePagination, JSON]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class PaginationStage(_OpenSearchStage[JSON, ResponseTriple]):
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
        sort_field_path = self.plugin.field_mapping[self.pagination.sort]
        field_type = self.service.field_type(self.catalog, sort_field_path)
        sort_mode = field_type.es_sort_mode
        sort_field = dotted(sort_field_path, 'keyword')

        def sort(order: str) -> tuple[JSON, JSON]:
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
                # This secondary sort field serves as the tiebreaker for when
                # the primary sort field is not unique across documents.
                # Otherwise it's redundant, especially if it's the same as the
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
        hits, total = self._extract_hits(response)
        pagination = self._process_pagination(hits, total)
        hits = self._translate_hits(hits)
        aggregations = json_mapping(response.get('aggregations', {}))
        return hits, pagination, aggregations

    def _extract_hits(self, response: JSON) -> tuple[JSONs, int]:
        hits = json_mapping(response['hits'])
        total = json_mapping(hits['total'])
        # FIXME: Handle other relations
        #        https://github.com/DataBiosphere/azul/issues/3770
        assert total['relation'] == 'eq'
        return json_sequence_of_mappings(hits['hits']), json_int(total['value'])

    def _translate_hits(self, hits: JSONs) -> JSONs:
        # The slice is necessary because we may have fetched an extra entry to
        # determine if there is a previous or next page.
        hits = hits[0:self.pagination.size]
        hits = iter(hits) if self.pagination.search_before is None else reversed(hits)
        return [
            self.service.translate_fields(self.catalog,
                                          json_mapping(hit['_source']),
                                          forward=False)
            for hit in hits
        ]

    def _process_pagination(self, hits: JSONs, total: int) -> ResponsePagination:
        pages = -(-total // self.pagination.size)

        # ... else use search_after/search_before pagination
        count = len(hits)
        if self.pagination.search_before is None:
            # hits are normal sorted
            if count > self.pagination.size:
                # There is an extra hit, indicating a next page.
                count -= 1
                search_after = sort_key_from_json(hits[count - 1]['sort'])
            else:
                # No next page
                search_after = None
            if self.pagination.search_after is not None:
                search_before = sort_key_from_json(hits[0]['sort'])
            else:
                search_before = None
        else:
            # hits are reverse sorted
            if count > self.pagination.size:
                # There is an extra hit, indicating a previous page.
                count -= 1
                search_before = sort_key_from_json(hits[count - 1]['sort'])
            else:
                # No previous page
                search_before = None
            search_after = sort_key_from_json(hits[0]['sort'])

        pagination = self.pagination.advance(search_before=search_before,
                                             search_after=search_after)

        def page_link(*, previous: bool) -> str | None:
            url = pagination.link(previous=previous,
                                  catalog=self.catalog,
                                  filters=json.dumps(self.filters.explicit))
            return None if url is None else str(url)

        return ResponsePagination(count=count,
                                  total=total,
                                  size=pagination.size,
                                  next=page_link(previous=False),
                                  previous=page_link(previous=True),
                                  pages=pages,
                                  sort=pagination.sort,
                                  order=pagination.order)


class QueryService(DocumentService):

    @cached_property
    def _opensearch(self) -> OpenSearch:
        return OpenSearchClientFactory.get()

    def create_chain(self,
                     *,
                     catalog: CatalogName,
                     entity_type: str,
                     filters: Filters,
                     post_filter: bool,
                     document_slice: DocumentSlice | None
                     ) -> OpenSearchChain[Response, Any, Response]:
        """
        Create a chain for a basic OpenSearch `search` request for documents
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

    def create_request(self,
                       catalog: CatalogName,
                       entity_type: str,
                       doc_type: DocumentType = DocumentType.aggregate
                       ) -> Search:
        """
        Create an OpenSearch request against the index containing documents
        of the given entity and document types, in the given catalog.
        """
        return Search(using=self._opensearch,
                      index=str(IndexName.create(catalog=catalog,
                                                 qualifier=entity_type,
                                                 doc_type=doc_type)))
