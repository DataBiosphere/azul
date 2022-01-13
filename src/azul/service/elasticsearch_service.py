import json
import logging
from typing import (
    List,
    Optional,
)
from urllib.parse import (
    urlencode,
)

import attr
import elasticsearch
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
)
from azul.es import (
    ESClientFactory,
)
from azul.indexer.document_service import (
    DocumentService,
)
from azul.plugins import (
    ServiceConfig,
)
from azul.service import (
    AbstractService,
    BadArgumentException,
    Filters,
    FiltersJSON,
    MutableFiltersJSON,
)
from azul.service.hca_response_v5 import (
    AutoCompleteResponse,
    FileSearchResponse,
    KeywordSearchResponse,
)
from azul.service.utilities import (
    json_pp,
)
from azul.types import (
    JSON,
    MutableJSON,
)

logger = logging.getLogger(__name__)


class IndexNotFoundError(Exception):

    def __init__(self, missing_index: str):
        super().__init__(f'Index `{missing_index}` was not found')


SourceFilters = List[str]


@attr.s(auto_attribs=True, kw_only=True, frozen=False)
class Pagination:
    order: str
    size: int
    sort: str
    self_url: str
    search_after: Optional[List[str]] = None
    search_before: Optional[List[str]] = None


class ElasticsearchService(DocumentService, AbstractService):

    @cached_property
    def es_client(self) -> Elasticsearch:
        return ESClientFactory.get()

    def __init__(self, service_config: Optional[ServiceConfig] = None):
        self._service_config = service_config

    def service_config(self, catalog: CatalogName):
        return self._service_config or self.metadata_plugin(catalog).service_config()

    def _translate_filters(self,
                           catalog: CatalogName,
                           filters: FiltersJSON,
                           field_mapping: JSON
                           ) -> MutableFiltersJSON:
        """
        Function for translating the filters

        :param catalog: the name of the catalog to translate filters for.
                        Different catalogs may use different field types,
                        resulting in differently translated filter.

        :param filters: Raw filters from the filters param. That is, in 'browserForm'

        :param field_mapping: Mapping config json with '{'browserKey': 'es_key'}' format

        :return: Returns translated filters with 'es_keys'
        """
        translated_filters = {}
        for field, filter in filters.items():
            field = field_mapping[field]
            operator, values = one(filter.items())
            field_type = self.field_type(catalog, tuple(field.split('.')))
            to_index = field_type.to_index
            filter = {
                operator:
                    [(to_index(start), to_index(end)) for start, end in values]
                    if operator in ('contains', 'within', 'intersects') else
                    list(map(to_index, values))
            }
            translated_filters[field] = filter
        return translated_filters

    def _create_query(self, catalog: CatalogName, filters):
        """
        Creates a query object based on the filters argument

        :param catalog: The catalog against which to create the query for.

        :param filters: filter parameter from the /files endpoint with
        translated (es_keys) keys
        :return: Returns Query object with appropriate filters
        """
        filter_list = []
        for facet, values in filters.items():
            relation, value = one(values.items())
            if relation == 'is':
                query = Q('terms', **{facet + '.keyword': value})
                field_type = self.field_type(catalog, tuple(facet.split('.')))
                translated_none = field_type.to_index(None)
                if translated_none in value:
                    # Note that at this point None values in filters have already
                    # been translated eg. {'is': ['~null']} and if the filter has a
                    # None our query needs to find fields with None values as well
                    # as absent fields
                    absent_query = Q('bool', must_not=[Q('exists', field=facet)])
                    query = Q('bool', should=[query, absent_query])
                filter_list.append(query)
            elif relation in ('contains', 'within', 'intersects'):
                for min_value, max_value in value:
                    range_value = {
                        'gte': min_value,
                        'lte': max_value,
                        'relation': relation
                    }
                    filter_list.append(Q('range', **{facet: range_value}))
            else:
                assert False
        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=f) for f in filter_list]

        return Q('bool', must=query_list)

    def _create_aggregate(self, catalog: CatalogName, filters: MutableFiltersJSON, facet_config, agg):
        """
        Creates the aggregation to be used in ElasticSearch

        :param catalog: The name of the catalog to create the aggregations for

        :param filters: Translated filters from 'files/' endpoint call

        :param facet_config: Configuration for the facets (i.e. facets on which
               to construct the aggregate) in '{browser:es_key}' form

        :param agg: Current aggregate where this aggregation is occurring.
                    Syntax in browser form

        :return: returns an Aggregate object to be used in a Search query
        """
        # Pop filter of current Aggregate
        excluded_filter = filters.pop(facet_config[agg], None)
        # Create the appropriate filters
        filter_query = self._create_query(catalog, filters)
        # Create the filter aggregate
        aggregate = A('filter', filter_query)
        # Make an inner aggregate that will contain the terms in question
        _field = f'{facet_config[agg]}.keyword'
        service_config = self.service_config(catalog)
        # FIXME: Approximation errors for terms aggregation are unchecked
        #        https://github.com/DataBiosphere/azul/issues/3413
        if agg == 'project':
            _sub_field = service_config.translation['projectId'] + '.keyword'
            aggregate.bucket('myTerms', 'terms', field=_field, size=config.terms_aggregation_size).bucket(
                'myProjectIds', 'terms', field=_sub_field, size=config.terms_aggregation_size)
        else:
            aggregate.bucket('myTerms', 'terms', field=_field, size=config.terms_aggregation_size)
        aggregate.bucket('untagged', 'missing', field=_field)
        if agg == 'fileFormat':
            # FIXME: Use of shadow field is brittle
            #        https://github.com/DataBiosphere/azul/issues/2289
            def set_summary_agg(field: str, bucket: str) -> None:
                field_full = service_config.translation[field] + '_'
                aggregate.aggs['myTerms'].metric(bucket, 'sum', field=field_full)
                aggregate.aggs['untagged'].metric(bucket, 'sum', field=field_full)

            set_summary_agg(field='fileSize', bucket='size_by_type')
            set_summary_agg(field='matrixCellCount', bucket='matrix_cell_count_by_type')
        # If the aggregate in question didn't have any filter on the API
        #  call, skip it. Otherwise insert the popped
        # value back in
        if excluded_filter is not None:
            filters[facet_config[agg]] = excluded_filter
        return aggregate

    def _annotate_aggs_for_translation(self, es_search: Search):
        """
        Annotate the aggregations in the given Elasticsearch search request so we can later translate substitutes for
        None in the aggregations part of the response.
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
        Translate substitutes for None in the aggregations part of an Elasticsearch response.
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
                        catalog: CatalogName,
                        filters: FiltersJSON,
                        post_filter: bool = False,
                        source_filter: SourceFilters = None,
                        enable_aggregation: bool = True,
                        entity_type='files') -> Search:
        """
        This function will create an ElasticSearch request based on
        the filters and facet_config passed into the function
        :param filters: The 'filters' parameter.
        Assumes to be translated into es_key terms
        :param post_filter: Flag for doing either post_filter or regular
        querying (i.e. faceting or not)
        :param List source_filter: A list of "foo.bar" field paths (see
               https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-source-filtering.html)
        :param enable_aggregation: Flag for enabling query aggregation (and
               effectively ignoring facet configuration)
        :param entity_type: the string referring to the entity type used to get
        the ElasticSearch index to search
        :return: Returns the Search object that can be used for executing
        the request
        """
        service_config = self.service_config(catalog)
        field_mapping = service_config.translation
        facet_config = {key: field_mapping[key] for key in service_config.facets}
        es_search = Search(using=self.es_client, index=config.es_index_name(catalog=catalog,
                                                                            entity_type=entity_type,
                                                                            aggregate=True))
        filters = self._translate_filters(catalog, filters, field_mapping)

        es_query = self._create_query(catalog, filters)

        if post_filter:
            es_search = es_search.post_filter(es_query)
        else:
            es_search = es_search.query(es_query)

        if source_filter:
            es_search = es_search.source(includes=source_filter)
        elif entity_type not in ("files", "bundles"):
            es_search = es_search.source(excludes="bundles")

        if enable_aggregation:
            for agg, translation in facet_config.items():
                # FIXME: Aggregation filters may be redundant when post_filter is false
                #        https://github.com/DataBiosphere/azul/issues/3435
                es_search.aggs.bucket(agg, self._create_aggregate(catalog, filters, facet_config, agg))

        return es_search

    def _create_autocomplete_request(self,
                                     catalog: CatalogName,
                                     filters: FiltersJSON,
                                     es_client,
                                     _query,
                                     search_field,
                                     entity_type='files'):
        """
        This function will create an ElasticSearch request based on
        the filters passed to the function.

        :param catalog: The name of the catalog to create the ES request for.

        :param filters: The 'filters' parameter from '/keywords'.

        :param es_client: The ElasticSearch client object used to configure the
                          Search object

        :param _query: The query (string) to use for querying.

        :param search_field: The field to do the query on.

        :param entity_type: the string referring to the entity type used to get
                            the ElasticSearch index to search

        :return: Returns the Search object that can be used for executing the
                 request
        """
        service_config = self.service_config(catalog)
        field_mapping = service_config.autocomplete_translation[entity_type]
        es_search = Search(using=es_client,
                           index=config.es_index_name(catalog=catalog,
                                                      entity_type=entity_type,
                                                      aggregate=True))
        filters = self._translate_filters(catalog, filters, field_mapping)
        search_field = field_mapping[search_field] if search_field in field_mapping else search_field
        es_filter_query = self._create_query(catalog, filters)
        es_search = es_search.post_filter(es_filter_query)
        es_search = es_search.query(Q('prefix', **{str(search_field): _query}))
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
                {
                    '_uid': {
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

        if peek_ahead:
            # fetch one more than needed to see if there's a "next page".
            es_search = es_search.extra(size=pagination.size + 1)
        return es_search

    def _generate_paging_dict(self,
                              catalog: CatalogName,
                              filters: FiltersJSON,
                              es_response: JSON,
                              pagination: Pagination
                              ) -> MutableJSON:

        def page_link(**kwargs) -> str:
            params = dict(catalog=catalog,
                          filters=json.dumps(filters),
                          sort=pagination.sort,
                          order=pagination.order,
                          size=pagination.size,
                          **kwargs)
            return pagination.self_url + '?' + urlencode(params)

        pages = -(-es_response['hits']['total'] // pagination.size)

        # ... else use search_after/search_before pagination
        es_hits = es_response['hits']['hits']
        count = len(es_hits)
        if pagination.search_before is None:
            # hits are normal sorted
            if count > pagination.size:
                # There is an extra hit, indicating a next page.
                count -= 1
                search_after = es_hits[count - 1]['sort']
            else:
                # No next page
                search_after = [None, None]
            if pagination.search_after is not None:
                search_before = es_hits[0]['sort']
            else:
                search_before = [None, None]
        else:
            # hits are reverse sorted
            if count > pagination.size:
                # There is an extra hit, indicating a previous page.
                count -= 1
                search_before = es_hits[count - 1]['sort']
            else:
                # No previous page
                search_before = [None, None]
            search_after = es_hits[0]['sort']

        page_field = {
            'count': count,
            'total': es_response['hits']['total'],
            'size': pagination.size,
            'next': (
                None
                if search_after[1] is None else
                # Encode value in JSON such that its type is not lost
                page_link(search_after=json.dumps(search_after[0]),
                          search_after_uid=search_after[1])
            ),
            'previous': (
                None
                if search_before[1] is None else
                page_link(search_before=json.dumps(search_before[0]),
                          search_before_uid=search_before[1])
            ),
            'pages': pages,
            'sort': pagination.sort,
            'order': pagination.order
        }

        return page_field

    def transform_summary(self,
                          catalog: CatalogName,
                          filters=None,
                          entity_type=None):
        if not filters:
            filters = {}
        es_search = self._create_request(catalog=catalog,
                                         filters=filters,
                                         post_filter=False,
                                         entity_type=entity_type)

        if entity_type == 'files':
            # Add a total file size aggregate
            es_search.aggs.metric('totalFileSize',
                                  'sum',
                                  field='contents.files.size_')
        elif entity_type == 'cell_suspensions':
            # Add a cell count aggregate per organ
            es_search.aggs.bucket(
                'cellCountSummaries',
                'terms',
                field='contents.cell_suspensions.organ.keyword',
                size=config.terms_aggregation_size
            ).bucket(
                'cellCount',
                'sum',
                field='contents.cell_suspensions.total_estimated_cells_'
            )
            # Add a total cell count aggregate
            es_search.aggs.metric('totalCellCount',
                                  'sum',
                                  field='contents.cell_suspensions.total_estimated_cells_')
        elif entity_type == 'samples':
            # Add an organ aggregate to the Elasticsearch request
            es_search.aggs.bucket('organTypes',
                                  'terms',
                                  field='contents.samples.effective_organ.keyword',
                                  size=config.terms_aggregation_size)
        elif entity_type == 'projects':
            # Add a project cell count aggregate
            es_search.aggs.metric('projectEstimatedCellCount',
                                  'sum',
                                  field='contents.projects.estimated_cell_count_')
        else:
            assert False, entity_type

        cardinality_aggregations = {
            'samples': {
                'specimenCount': 'contents.specimens.document_id',
                'speciesCount': 'contents.donors.genus_species',
                'donorCount': 'contents.donors.document_id',
            },
            'projects': {
                'labCount': 'contents.projects.laboratory',
            }
        }.get(entity_type, {})

        threshold = config.precision_threshold
        for agg_name, cardinality in cardinality_aggregations.items():
            es_search.aggs.metric(agg_name,
                                  'cardinality',
                                  field=cardinality + '.keyword',
                                  precision_threshold=str(threshold))

        self._annotate_aggs_for_translation(es_search)
        es_search = es_search.extra(size=0)
        es_response = es_search.execute(ignore_cache=True)
        assert len(es_response.hits) == 0
        self._translate_response_aggs(catalog, es_response)

        if config.debug == 2 and logger.isEnabledFor(logging.DEBUG):
            logger.debug('Elasticsearch request: %s', json.dumps(es_search.to_dict(), indent=4))

        result = es_response.aggs.to_dict()
        for agg_name in cardinality_aggregations:
            agg_value = result[agg_name]['value']
            assert agg_value <= threshold / 2, (agg_name, agg_value, threshold)

        return result

    def transform_request(self,
                          catalog: CatalogName,
                          entity_type: str,
                          filters: Filters,
                          pagination: Optional[Pagination] = None) -> MutableJSON:
        """
        This function does the whole transformation process. It takes the path
        of the config file, the filters, and pagination, if any. Excluding
        filters will do a match_all request. Excluding pagination will exclude
        pagination from the output.

        :param catalog: The name of the catalog to query

        :param entity_type: the string referring to the entity type used to get
                            the ElasticSearch index to search

        :param filters: Filter parameter from the API to be used in the query.

        :param pagination: Pagination to be used for the API

        :return: Returns the transformed request
        """
        service_config = self.service_config(catalog)
        translation = service_config.translation
        inverse_translation = {v: k for k, v in translation.items()}

        for facet in filters.explicit.keys():
            if facet not in translation:
                raise BadArgumentException(f"Unable to filter by undefined facet {facet}.")

        if pagination is not None:
            facet = pagination.sort
            if facet not in translation:
                raise BadArgumentException(f"Unable to sort by undefined facet {facet}.")

        es_search = self._create_request(catalog=catalog,
                                         filters=filters.reify(explicit_only=entity_type == 'projects'),
                                         post_filter=True,
                                         entity_type=entity_type)

        if pagination is None:
            # It's a single file search
            self._annotate_aggs_for_translation(es_search)
            es_response = es_search.execute(ignore_cache=True)
            self._translate_response_aggs(catalog, es_response)
            es_response_dict = es_response.to_dict()
            hits = [hit['_source'] for hit in es_response_dict['hits']['hits']]
            hits = self.translate_fields(catalog, hits, forward=False)
            final_response = KeywordSearchResponse(hits, entity_type, catalog)
        else:
            # It's a full file search
            # Translate the sort field if there is any translation available
            if pagination.sort in translation:
                pagination.sort = translation[pagination.sort]
            es_search = self.apply_paging(catalog, es_search, pagination)
            self._annotate_aggs_for_translation(es_search)
            try:
                es_response = es_search.execute(ignore_cache=True)
            except elasticsearch.NotFoundError as e:
                raise IndexNotFoundError(e.info["error"]["index"])
            self._translate_response_aggs(catalog, es_response)
            es_response_dict = es_response.to_dict()
            # Extract hits and facets (aggregations)
            es_hits = es_response_dict['hits']['hits']
            # If the number of elements exceed the page size, then we fetched one too many
            # entries to determine if there is a previous or next page.  In that case,
            # return one fewer hit.
            list_adjustment = 1 if len(es_hits) > pagination.size else 0
            if pagination.search_before is not None:
                hits = reversed(es_hits[0:len(es_hits) - list_adjustment])
            else:
                hits = es_hits[0:len(es_hits) - list_adjustment]
            hits = [hit['_source'] for hit in hits]
            hits = self.translate_fields(catalog, hits, forward=False)

            facets = es_response_dict['aggregations'] if 'aggregations' in es_response_dict else {}
            pagination.sort = inverse_translation[pagination.sort]
            paging = self._generate_paging_dict(catalog,
                                                filters.reify(explicit_only=True),
                                                es_response_dict,
                                                pagination)
            final_response = FileSearchResponse(hits, paging, facets, entity_type, catalog)

        final_response = final_response.apiResponse.to_json_no_copy()

        return final_response

    def transform_autocomplete_request(self,
                                       catalog: CatalogName,
                                       pagination: Pagination,
                                       filters=None,
                                       _query='',
                                       search_field='fileId',
                                       entry_format='file'):
        """
        This function does the whole transformation process. It takes the path
        of the config file, the filters, and pagination, if any. Excluding
        filters will do a match_all request. Excluding pagination will exclude
        pagination from the output.

        :param catalog: The name of the catalog to transform the autocomplete
                        request for.

        :param filters: Filter parameter from the API to be used in the query

        :param pagination: Pagination to be used for the API

        :param _query: String query to use on the search

        :param search_field: Field to perform the search on

        :param entry_format: Tells the method which _type of entry format to use

        :return: Returns the transformed request
        """
        service_config = self.service_config(catalog)
        mapping_config = service_config.autocomplete_mapping_config
        # Get the right autocomplete mapping configuration
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Entry is: %s', entry_format)
            logger.debug('Printing the mapping_config: \n%s', json_pp(mapping_config))
        mapping_config = mapping_config[entry_format]
        if not filters:
            filters = {}

        entity_type = 'files' if entry_format == 'file' else 'donor'
        es_search = self._create_autocomplete_request(
            catalog,
            filters,
            self.es_client,
            _query,
            search_field,
            entity_type=entity_type)
        # Handle pagination
        logger.info('Handling pagination')
        pagination.sort = '_score'
        pagination.order = 'desc'
        es_search = self.apply_paging(catalog, es_search, pagination)
        # Executing ElasticSearch request
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Printing ES_SEARCH request dict:\n %s', json.dumps(es_search.to_dict()))
        es_response = es_search.execute(ignore_cache=True)
        es_response_dict = es_response.to_dict()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Printing ES_SEARCH response dict:\n %s', json.dumps(es_response_dict))
        # Extracting hits
        hits = [x['_source'] for x in es_response_dict['hits']['hits']]
        # Generating pagination
        logger.debug('Generating pagination')
        paging = self._generate_paging_dict(catalog, filters, es_response_dict, pagination)
        # Creating AutocompleteResponse
        logger.info('Creating AutoCompleteResponse')
        final_response = AutoCompleteResponse(
            mapping_config,
            hits,
            paging,
            _type=entry_format)
        final_response = final_response.apiResponse.to_json()
        logger.info('Returning the final response for transform_autocomplete_request')
        return final_response
