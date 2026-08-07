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
    Sequence,
)
from functools import (
    partial,
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
    MultiTerms,
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
from azul.filters import (
    Filters,
    FiltersJSON,
)
from azul.indexer.document import (
    DocumentType,
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
    JSONTypedDict,
    JSONs,
    MutableJSON,
    PrimitiveJSON,
    json_str,
)
from azul.opensearch import (
    OpenSearchClientFactory,
)
from azul.plugins import (
    DocumentSlice,
    DottedFieldPath,
    FieldPath,
    MetadataPlugin,
    dotted,
    undotted,
)

log = logging.getLogger(__name__)

#: The name of the bucket holding one entry per distinct facet value, with its
#: document count.
#:
values_agg_name = 'myTerms'

#: The name of the `nested` aggregation bucket, used only for facets backed by a
#: nested field.
#:
nested_agg_name = 'myNested'

#: The name of the bucket counting documents with no value for the facet.
#:
untagged_agg_name = 'myUntagged'


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


TranslatedFilters = Mapping[FieldPath, Mapping[str, Sequence[PrimitiveJSON]]]


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
        translated_filters = {}
        for field, filter in filters.items():
            field = field_mapping[field]
            operator, values = one(filter.items())
            field_type = self.service.field_type(catalog, field)
            values = field_type.filter(operator, values)
            translated_filters[field] = {operator: list(values)}
        return translated_filters

    def prepare_query(self, skip_field_paths: tuple[FieldPath] = ()) -> Query:
        """
        Converts the given filters into an OpenSearch DSL Query object.
        """
        filter_list = []
        for field_path, filter in self.prepared_filters.items():
            if field_path not in skip_field_paths:
                operator, values = one(filter.items())
                # Note that `is_not` is only used internally (for filtering by
                # inaccessible sources)
                if operator in ('is', 'is_not'):
                    field_type = self.service.field_type(self.catalog, field_path)
                    if isinstance(field_type, Nested):
                        nested_queries = []
                        for value in values:
                            term_queries = []
                            for nested_field, nested_value in value.items():
                                nested_body = {dotted(field_path, nested_field, 'keyword'): nested_value}
                                term_queries.append(Q('term', **nested_body))
                            nested_queries.append(Q('nested',
                                                    path=dotted(field_path),
                                                    query=Q('bool', must=term_queries)))
                        query = Q('bool', should=nested_queries)
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
                        value = value | {'relation': operator}
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
            aggs = response['aggregations']
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
            dotted_facet_path = dotted(facet_path)
            # Aggregate over the values of the nested field. The values are flat
            # JSON, i.e. dictionaries consisting of properties whose values are
            # primitive. OpenSearch refers to the values of the nested field as
            # "nested documents". By itself, a `nested` aggregation only counts
            # the number of nested documents.
            nested_agg = agg.bucket(name=nested_agg_name,
                                    agg_type='nested',
                                    path=dotted_facet_path)
            # In order to aggregate over the properties of the nested documents,
            # a child aggregation must be added. We use the `multi_terms` child
            # aggregation to produce a result bucket for every distinct nested
            # document, based on the values of its properties. For each bucket,
            # the number of occurrences of such a nested document is returned.
            # Duplicate nested documents, either in a single containing document
            # or spread out over multiple containing documents, would be counted
            # individually. We do want to count duplicates occurring in
            # different containing documents, but we don't want to count
            # duplicates occurring in the same containing document. To
            # eliminate the latter, we deduplicated at indexing time.
            nested_agg.bucket(name=values_agg_name,
                              agg_type='multi_terms',
                              terms=[
                                  {'field': dotted(facet_path, field, 'keyword')}
                                  for field in field_type.properties
                              ],
                              size=config.terms_aggregation_size)
            # We use a sibling aggregation in order to count the documents that
            # don't contain any nested documents for this field. For normal
            # fields we can use the `missing` aggregation, but this is currently
            # not possible in combination with the `nested` aggregation:
            # https://github.com/elastic/elasticsearch/issues/9571
            #
            # As a workaround, we use a `filter` aggregation instead.
            agg.bucket(name=untagged_agg_name,
                       agg_type='filter',
                       filter=Q('bool',
                                must_not=[
                                    Q('nested',
                                      path=dotted_facet_path,
                                      query=Q('exists', field=dotted_facet_path))
                                ]))
        else:
            dotted_facet_path = dotted(facet_path, 'keyword')
            agg.bucket(name=values_agg_name,
                       agg_type='terms',
                       field=dotted_facet_path,
                       size=config.terms_aggregation_size)
            agg.bucket(untagged_agg_name, 'missing', field=dotted_facet_path)
        return agg

    def _annotate_aggs_for_translation(self, request: Search):
        """
        Annotate the aggregations in the given OpenSearch search request so
        we can later translate substitutes for None in the aggregations part of
        the response.
        """

        def convert_path(path: DottedFieldPath) -> FieldPath:
            p = undotted(path)
            assert p[-1] == 'keyword', path
            return p[:-1]

        def annotate(agg: Agg):
            if isinstance(agg, (Terms, MultiTerms)):
                if not hasattr(agg, 'meta'):
                    agg.meta = {}
                agg.meta['paths'] = []
                if isinstance(agg, Terms):
                    # A Terms agg is for a single field, so we only need to
                    # annotate with the one FieldPath for the field.
                    agg.meta['paths'].append(convert_path(agg.field))
                else:
                    # A MultiTerms agg contains multiple fields, so we need the
                    # FieldPath of each one. By storing these in the same order
                    # that the fields occur in `agg.terms`, we can later pair
                    # these FieldPaths to the values in the aggregation buckets.
                    for term in agg.terms:
                        agg.meta['paths'].append(convert_path(term['field']))
            if hasattr(agg, 'aggs'):
                subs = agg.aggs
                for sub_name in subs:
                    annotate(subs[sub_name])

        for agg_name in request.aggs:
            annotate(request.aggs[agg_name])

    def _flatten_nested_aggs(self, aggs: MutableJSON):
        """
        Hoist the contents of each facet's nested aggregation bucket into its
        parent, so downstream response parsing is oblivious to nesting.
        """
        for facet, agg in aggs.items():
            try:
                nested_agg = agg.pop(nested_agg_name)
            except KeyError:
                pass
            else:
                # The value buckets are expected to account for every nested
                # document the `nested` aggregation counted. This relies on each
                # nested document populating all the `multi_terms` fields, since
                # a nested document missing any of them is omitted from the
                # buckets but still counted by the `nested` aggregation.
                doc_count = sum(bucket['doc_count']
                                for bucket in nested_agg[values_agg_name]['buckets'])
                assert nested_agg['doc_count'] == doc_count, R(
                    'Nested value buckets do not account for the nested total',
                    facet, nested_agg['doc_count'], doc_count)
                agg.update(nested_agg)
                # The `nested` aggregation only counts documents that have the
                # field, so its doc_count omits those tallied by `filter`
                # aggregation we use to count the missing fields.
                agg['doc_count'] += agg[untagged_agg_name]['doc_count']

    def _translate_response_aggs(self, aggs: MutableJSON):
        """
        Translate substitutes for None in the aggregations part of an
        OpenSearch response.
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
                    # `paths` is a key we added to `meta` to have available here
                    # when processing the response. Each path is a FieldPath
                    # (e.g. ['contents', 'projects', 'document_id']). There will
                    # be only one FieldPath in the case of a Terms aggregation,
                    # and multiple in the case of a MultiTerms aggregation.
                    paths = v['meta']['paths']
                except KeyError:
                    pass
                else:
                    for i, path in enumerate(paths):
                        field_type = self.service.field_type(self.catalog, tuple(path))
                        for bucket in buckets:
                            if isinstance(bucket['key'], list):
                                # The bucket is from a MultiTerms aggregation
                                bucket['key'][i] = field_type.from_index(bucket['key'][i])
                            else:
                                # The bucket is from a Terms aggregation
                                bucket['key'] = field_type.from_index(bucket['key'])
                    for bucket in buckets:
                        translate(k, bucket)

        for k, v in aggs.items():
            translate(k, v)

    def _populate_accessible(self, aggs: MutableJSON) -> None:
        # Because the value of the `accessible` field depends on the provided
        # authentication, we have to synthesize the field and its corresponding
        # facet from the `sourceId` field.
        source_ids = self.filter_stage.filters.source_ids
        plugin = self.service.metadata_plugin(self.catalog)
        special_fields = plugin.special_fields
        agg = aggs.pop(special_fields.source_id.name)
        counts_by_accessibility: dict[bool, int] = defaultdict(int)
        for bucket in agg[values_agg_name]['buckets']:
            accessible = bucket['key'] in source_ids
            counts_by_accessibility[accessible] += bucket['doc_count']
        agg[values_agg_name]['buckets'] = [
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


type SortKey = tuple[PrimitiveJSON, str] | tuple[PrimitiveJSON]


def sort_key_from_json(s: AnyJSON) -> SortKey:
    match s:
        case [a, b]:
            return (a, json_str(b))
        case [a]:
            return (a,)
        case _:
            assert False, s


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
        f = partial(self.service.translate_fields, self.catalog, forward=False)
        hits = list(map(f, hits))
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
