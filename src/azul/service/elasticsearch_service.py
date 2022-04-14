import logging
from typing import (
    Any,
    Optional,
    Tuple,
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
    AggResponse,
    Response,
)
from elasticsearch_dsl.response.aggs import (
    Bucket,
    BucketData,
    FieldBucket,
    FieldBucketData,
)
from more_itertools import (
    one,
)

from azul import (
    CatalogName,
    cached_property,
    config,
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
)
from azul.service import (
    AbstractService,
    Filters,
    FiltersJSON,
)
from azul.types import (
    JSON,
)

log = logging.getLogger(__name__)


class IndexNotFoundError(Exception):

    def __init__(self, missing_index: str):
        super().__init__(f'Index `{missing_index}` was not found')


SortKey = Tuple[Any, str]


@attr.s(auto_attribs=True, kw_only=True, frozen=False)
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


class ElasticsearchService(DocumentService, AbstractService):

    @cached_property
    def _es_client(self) -> Elasticsearch:
        return ESClientFactory.get()

    def _translate_filters(self,
                           catalog: CatalogName,
                           filters: FiltersJSON,
                           field_mapping: JSON
                           ) -> FiltersJSON:
        """
        Translate the field values in the given filter JSON to their respective
        Elasticsearch form, using the field types, the field names to field
        paths.
        """
        translated_filters = {}
        for field, filter in filters.items():
            field = field_mapping[field]
            operator, values = one(filter.items())
            field_type = self.field_type(catalog, tuple(field.split('.')))
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

    def _create_query(self,
                      catalog: CatalogName,
                      filters: FiltersJSON,
                      skip_field_paths: Tuple[str] = ()
                      ) -> Query:
        """
        Converts the given filters into an Elasticsearch DSL Query object.
        """
        filter_list = []
        for field_path, values in filters.items():
            if field_path not in skip_field_paths:
                relation, value = one(values.items())
                if relation == 'is':
                    field_type = self.field_type(catalog, tuple(field_path.split('.')))
                    if isinstance(field_type, Nested):
                        term_queries = []
                        for nested_field, nested_value in one(value).items():
                            nested_body = {'.'.join((field_path, nested_field, 'keyword')): nested_value}
                            term_queries.append(Q('term', **nested_body))
                        query = Q('nested', path=field_path, query=Q('bool', must=term_queries))
                    else:
                        query = Q('terms', **{field_path + '.keyword': value})
                        translated_none = field_type.to_index(None)
                        if translated_none in value:
                            # Note that at this point None values in filters have already
                            # been translated eg. {'is': ['~null']} and if the filter has a
                            # None our query needs to find fields with None values as well
                            # as absent fields
                            absent_query = Q('bool', must_not=[Q('exists', field=field_path)])
                            query = Q('bool', should=[query, absent_query])
                    filter_list.append(query)
                elif relation in ('contains', 'within', 'intersects'):
                    for min_value, max_value in value:
                        range_value = {
                            'gte': min_value,
                            'lte': max_value,
                            'relation': relation
                        }
                        filter_list.append(Q('range', **{field_path: range_value}))
                else:
                    assert False

        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=f) for f in filter_list]

        return Q('bool', must=query_list)

    def _create_aggregate(self,
                          *,
                          catalog: CatalogName,
                          filters: FiltersJSON,
                          facet: str,
                          facet_path: str
                          ) -> Agg:
        """
        Creates an aggregation to be used in a Elasticsearch search request.
        """
        # Create a filter aggregate using a query that represents all filters
        # except for the current facet.
        query = self._create_query(catalog, filters, skip_field_paths=(facet_path,))
        aggregate = A('filter', query)

        # Make an inner aggregate that will contain the terms in question
        path = facet_path + '.keyword'
        plugin = self.metadata_plugin(catalog)
        # FIXME: Approximation errors for terms aggregation are unchecked
        #        https://github.com/DataBiosphere/azul/issues/3413
        bucket = aggregate.bucket(name='myTerms',
                                  agg_type='terms',
                                  field=path,
                                  size=config.terms_aggregation_size)
        if facet == 'project':
            sub_path = plugin.field_mapping['projectId'] + '.keyword'
            bucket.bucket(name='myProjectIds',
                          agg_type='terms',
                          field=sub_path,
                          size=config.terms_aggregation_size)
        aggregate.bucket('untagged', 'missing', field=path)
        if facet == 'fileFormat':
            # FIXME: Use of shadow field is brittle
            #        https://github.com/DataBiosphere/azul/issues/2289
            def set_summary_agg(field: str, bucket: str) -> None:
                path = plugin.field_mapping[field] + '_'
                aggregate.aggs['myTerms'].metric(bucket, 'sum', field=path)
                aggregate.aggs['untagged'].metric(bucket, 'sum', field=path)

            set_summary_agg(field='fileSize', bucket='size_by_type')
            set_summary_agg(field='matrixCellCount', bucket='matrix_cell_count_by_type')
        return aggregate

    def _annotate_aggs_for_translation(self, es_search: Search):
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

        for agg_name in es_search.aggs:
            annotate(es_search.aggs[agg_name])

    def _translate_response_aggs(self, catalog: CatalogName, es_response: Response):
        """
        Translate substitutes for None in the aggregations part of an
        Elasticsearch response.
        """

        def translate(agg: AggResponse):
            if isinstance(agg, FieldBucketData):
                field_type = self.field_type(catalog, tuple(agg.meta['path']))
                for bucket in agg:
                    bucket['key'] = field_type.from_index(bucket['key'])
                    translate(bucket)
            elif isinstance(agg, BucketData):
                for name in dir(agg):
                    value = getattr(agg, name)
                    if isinstance(value, AggResponse):
                        translate(value)
            elif isinstance(agg, (FieldBucket, Bucket)):
                for sub in agg:
                    translate(sub)

        for agg in es_response.aggs:
            translate(agg)

    def _create_request(self,
                        *,
                        catalog: CatalogName,
                        entity_type: str,
                        filters: Filters,
                        post_filter: bool,
                        enable_aggregation: bool,
                        document_slice: DocumentSlice = None
                        ) -> Search:
        """
        Prepare an Elasticsearch DSL request object for searching entities of
        the given type in the given catalog, restricting the search to entities
        matching the filter and optionally (enable_aggregation) aggregating all
        (post_filter=True), or only the matching entities (post_filter=False).

        Optionally restrict the set of fields returned for each entity using a
        set of field path patterns (document_slice).
        """
        plugin = self.metadata_plugin(catalog)
        field_mapping = plugin.field_mapping
        es_search = Search(using=self._es_client,
                           index=config.es_index_name(catalog=catalog,
                                                      entity_type=entity_type,
                                                      aggregate=True))
        if entity_type == 'projects':
            filters = filters.explicit
        else:
            filters = filters.reify(plugin)
        filters = self._translate_filters(catalog, filters, field_mapping)

        es_query = self._create_query(catalog, filters)

        if post_filter:
            es_search = es_search.post_filter(es_query)
        else:
            es_search = es_search.query(es_query)

        if document_slice is None:
            document_slice = plugin.document_slice(entity_type)

        if document_slice is not None:
            es_search = es_search.source(**document_slice)

        if enable_aggregation:
            for facet in plugin.facets:
                # FIXME: Aggregation filters may be redundant when post_filter is false
                #        https://github.com/DataBiosphere/azul/issues/3435
                aggregate = self._create_aggregate(catalog=catalog,
                                                   filters=filters,
                                                   facet=facet,
                                                   facet_path=field_mapping[facet])
                es_search.aggs.bucket(facet, aggregate)

        return es_search

    def apply_paging(self,
                     catalog: CatalogName,
                     es_search: Search,
                     pagination: Pagination,
                     peek_ahead: bool = True
                     ) -> Search:
        """
        Set sorting and paging parameters for the given ES search request.

        :param catalog: The name of the catalog to search in

        :param es_search: The Elasticsearch request object

        :param pagination: The sorting and paging settings to apply

        :param peek_ahead: If True, request one more hit so that
                           _generate_paging_dict can know if there is another
                           page. Use this to prevent a last page that's empty.
        """
        sort_field = pagination.sort + '.keyword'
        sort_order = pagination.order

        field_type = self.field_type(catalog, tuple(pagination.sort.split('.')))
        sort_mode = field_type.es_sort_mode

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
        if pagination.search_after is not None:
            es_search = es_search.extra(search_after=pagination.search_after)
            es_search = es_search.sort(*sort(sort_order))
        elif pagination.search_before is not None:
            es_search = es_search.extra(search_after=pagination.search_before)
            rev_order = 'asc' if sort_order == 'desc' else 'desc'
            es_search = es_search.sort(*sort(rev_order))
        else:
            es_search = es_search.sort(*sort(sort_order))

        # FIXME: Remove this or change to 10000 (the default)
        #        https://github.com/DataBiosphere/azul/issues/3770
        es_search = es_search.extra(track_total_hits=True)

        assert isinstance(peek_ahead, bool), type(peek_ahead)
        # fetch one more than needed to see if there's a "next page".
        es_search = es_search.extra(size=pagination.size + peek_ahead)
        return es_search
