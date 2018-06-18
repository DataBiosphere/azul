#!/usr/bin/python
from aws_requests_auth import boto_utils
from aws_requests_auth.aws_auth import AWSRequestsAuth
from chalicelib import config
from copy import deepcopy
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search, Q, A
import json
import logging
import os
from chalicelib.responseobjects.hca_response_v5 import KeywordSearchResponse, \
    FileSearchResponse, SummaryResponse, ManifestResponse, \
    AutoCompleteResponse
from chalicelib.responseobjects.utilities import json_pp

module_logger = logging.getLogger("dashboardService.elastic_request_builder")

# The minimum total number of hits for which search_after pagination
# will be used instead of standard from/to pagination.
SEARCH_AFTER_THRESHOLD = 10000


class BadArgumentException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class ElasticTransformDump(object):
    """
    This class works as the highest abstraction, serving as the top layer
    between the webservice and ElasticSearch

    Attributes:
        es_client: The ElasticSearch client which will be used to connect
        to ElasticSearch
    """

    def __init__(
            self,
            es_domain='localhost',
            es_port=9200,
            es_protocol='http'):
        """
        The constructor simply initializes the ElasticSearch client object
        to be used for making requests.
        :param es_domain: Domain where ElasticSearch is living
        :param es_port: Port where ElasticSearch is listening
        :param es_protocol: Protocol for ElasticSearch. Must be 'http' or
        'https'
        """
        self.logger = logging.getLogger(
            'dashboardService.elastic_request_builder.ElasticTransformDump')
        assert es_protocol in ['http', 'https'], \
            "Protocol must be 'http' or 'https'"
        self.logger.debug('ElasticSearch url: {}://{}:{}/'.format(
            es_protocol, es_domain, es_port))

        if es_domain.endswith('.es.amazonaws.com'):
            awsauth = AWSRequestsAuth(
                aws_host=es_domain,
                aws_region='us-east-1',
                aws_service='es',
                **boto_utils.get_credentials()
            )
            self.es_client = Elasticsearch(
                hosts=[{'host': es_domain, 'port': 443}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=90
            )
        else:
            self.es_client = Elasticsearch(
                ['{}://{}:{}/'.format(es_protocol, es_domain, es_port)],
                timeout=90)
        self.logger.info('Creating an instance of ElasticTransformDump')

    @staticmethod
    def translate_filters(filters, field_mapping):
        """
        Function for translating the filters
        :param filters: Raw filters from the filters /files param.
        That is, in 'browserForm'
        :param field_mapping: Mapping config json with
        '{'browserKey': 'es_key'}' format
        :return: Returns translated filters with 'es_keys'
        """
        # Translate the fields to the appropriate ElasticSearch Index.
        # Probably can be edited later to do not just files but donors, etc.
        # for key, value in filters['file'].items():
        iterate_filters = deepcopy(filters)
        for key, value in iterate_filters.items():
            if key in field_mapping:
                # Get the corrected term within ElasticSearch
                corrected_term = field_mapping[key]
                # Replace the key in the filter with the name within
                # ElasticSearch
                filters[corrected_term] = filters.pop(key)
        return filters

    @staticmethod
    def create_query(filters):
        """
        Creates a query object based on the filters argument
        :param filters: filter parameter from the /files endpoint with
        translated (es_keys) keys
        :return: Returns Query object with appropriate filters
        """
        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=Q(
            'terms', **{'{}__keyword'.format(
                facet.replace(".", "__")): values.get('is', {})}))
                      for facet, values in filters.items()]
        #        Return a Query object. Make it match_all
        return Q('bool', must=query_list) if len(query_list) > 0 else Q()

    @staticmethod
    def create_aggregate(filters, facet_config, agg):
        """
        Creates the aggregation to be used in ElasticSearch
        :param filters: Translated filters from 'files/' endpoint call
        :param facet_config: Configuration for the facets (i.e. facets
        on which to construct the aggregate) in '{browser:es_key}' form
        :param agg: Current aggregate where this aggregation is occurring.
        Syntax in browser form
        :return: returns an Aggregate object to be used in a Search query
        """
        # Pop filter of current Aggregate
        excluded_filter = filters.pop(facet_config[agg], None)
        # Create the appropriate filters
        filter_query = ElasticTransformDump.create_query(filters)
        # Create the filter aggregate
        aggregate = A('filter', filter_query)
        # Make an inner aggregate that will contain the terms in question
        # _field = '{}.keyword'.format(facet_config[agg])
        # HACK
        if facet_config[agg] != 'pairedEnds':
            _field = '{}.keyword'.format(facet_config[agg])
        else:
            _field = facet_config[agg]
        aggregate.bucket(
            'myTerms',
            'terms',
            field=_field,
            size=99999)
        # If the aggregate in question didn't have any filter on the API
        #  call, skip it. Otherwise insert the popped
        # value back in
        if excluded_filter is not None:
            filters[facet_config[agg]] = excluded_filter
        return aggregate

    @staticmethod
    def create_request(
            filters, es_client,
            req_config,
            post_filter=False,
            index='ES_FILE_INDEX'):
        """
        This function will create an ElasticSearch request based on
        the filters and facet_config passed into the function
        :param filters: The 'filters' parameter from '/files'.
        Assumes to be translated into es_key terms
        :param es_client: The ElasticSearch client object used
         to configure the Search object
        :param req_config: The
        {'translation: {'browserKey': 'es_key'}, 'facets': ['facet1', ...]}
        config
        :param post_filter: Flag for doing either post_filter or regular
        querying (i.e. faceting or not)
        :param index: the string referring to the environmental variable
        containing the ElasticSearch index to search
        :return: Returns the Search object that can be used for executing
        the request
        """
        # Get the field mapping and facet configuration from the config
        field_mapping = req_config['translation']
        facet_config = {key: field_mapping[key]
                        for key in req_config['facets']}
        # Create the Search Object
        es_search = Search(
            using=es_client,
            index=os.getenv(index, 'browser_files_dev'))
        # Translate the filters keys
        filters = ElasticTransformDump.translate_filters(
            filters, field_mapping)
        # Get the query from 'create_query'
        es_query = ElasticTransformDump.create_query(filters)
        # Do a post_filter using the returned query
        es_search = es_search.query(
            es_query) if not post_filter else es_search.post_filter(es_query)
        # Iterate over the aggregates in the facet_config
        for agg, translation in facet_config.items():
            # Create a bucket aggregate for the 'agg'.
            # Call create_aggregate() to return the appropriate aggregate query
            es_search.aggs.bucket(
                agg,
                ElasticTransformDump.create_aggregate(
                    filters, facet_config, agg))
        return es_search

    @staticmethod
    def create_autocomplete_request(
            filters,
            es_client,
            req_config,
            _query,
            search_field,
            index='ES_FILE_INDEX'):
        """
        This function will create an ElasticSearch request based on
         the filters passed to the function
        :param filters: The 'filters' parameter from '/keywords'.
        :param es_client: The ElasticSearch client object used to
         configure the Search object
        :param req_config: The
        {'translation': {'browserKey': 'es_key'}, 'facets': ['facet1', ...]}
         config
        :param _query: The query (string) to use for querying.
        :param search_field: The field to do the query on.
        :param index: the string referring to the environmental variable
         containing the ElasticSearch index to search
        :return: Returns the Search object that can be used for
        executing the request
        """
        # Get the field mapping and facet configuration from the config
        field_mapping = req_config['autocomplete-translation'][index]
        # Create the Search Object
        es_search = Search(
            using=es_client,
            index=os.getenv(index, 'fb_index'))
        # Translate the filters keys
        filters = ElasticTransformDump.translate_filters(
            filters,
            field_mapping)
        # Translate the search_field
        search_field = field_mapping[search_field] \
            if search_field in field_mapping else search_field
        # Get the query from 'create_query'
        es_filter_query = ElasticTransformDump.create_query(filters)
        # Do a post_filter using the filter query
        es_search = es_search.post_filter(es_filter_query)
        # Apply a prefix query with the query string
        es_search = es_search.query(
            Q('prefix', **{'{}'.format(search_field): _query}))
        return es_search

    @staticmethod
    def open_and_return_json(file_path):
        """
        Opens and returns the contents of the json file given in file_path
        :param file_path: Path of a json file to be opened
        :return: Returns an obj with the contents of the json file
        """
        with open(file_path, 'r') as file_:
            loaded_file = json.load(file_)
            file_.close()
        return loaded_file

    @staticmethod
    def apply_paging(es_search, pagination):
        """
        Applies the pagination to the ES Search object
        :param es_search: The ES Search object
        :param pagination: Dictionary with raw entries from the GET Request.
        It has: 'size', 'sort', 'order', and one of 'search_after', 'search_before', or 'from'.
        :return: An ES Search object where pagination has been applied
        """
        # Extract the fields for readability (and slight manipulation)

        _sort = pagination['sort'] + ".keyword"
        _order = pagination['order']
        # Apply order
        if 'from' in pagination:
            # Using to-from pagination
            _from = pagination['from'] - 1
            _to = pagination['size'] + _from
            if _to > SEARCH_AFTER_THRESHOLD:
                raise BadArgumentException("from+to must not be greater than " + str(SEARCH_AFTER_THRESHOLD))
            es_search = es_search[_from:_to]
            es_search = es_search.sort({_sort: {"order": _order}},
                                       {'_uid': {"order": 'desc'}})
        else:
            # Using search_after/search_before pagination
            if 'search_after' in pagination:
                es_search = es_search.extra(search_after=pagination['search_after'])
                es_search = es_search.sort({_sort: {"order": _order}},
                                           {'_uid': {"order": 'desc'}})
            elif 'search_before' in pagination:
                es_search = es_search.extra(search_after=pagination['search_before'])
                rev_order = 'asc' if _order == 'desc' else 'desc'
                es_search = es_search.sort({_sort: {"order": rev_order}},
                                           {'_uid': {"order": 'asc'}})

            # fetch one more than needed to see if there's a "next page".
            es_search = es_search.extra(size=pagination['size'] + 1)

        logging.debug("es_search is " + str(es_search))
        return es_search

    @staticmethod
    def generate_paging_dict(es_response, pagination):
        """
        Generates the right dictionary for the final response.
        :param es_response: The raw dictionary response from ElasticSearch
        :param pagination: The pagination as coming from the GET request
        (or the defaults)
        :return: Modifies and returns the pagination updated with the
        new required entries.
        """
        pages = -(-es_response['hits']['total'] // pagination['size'])
        if es_response['hits']['total'] < SEARCH_AFTER_THRESHOLD:
            # Use from/to pagination
            page_field = {
                'count': len(es_response['hits']['hits']),
                'total': es_response['hits']['total'],
                'size': pagination['size'],
                'from': pagination['from'],
                'page': ((pagination['from'] - 1) / pagination['size']) + 1,
                'pages': pages,
                'sort': pagination['sort'],
                'order': pagination['order']
            }
        else:
            # ...else use search_after/search_before pagination
            es_hits = es_response['hits']['hits']
            count = len(es_hits)

            logging.debug("count=" + str(count) + " and size=" + str(pagination['size']))

            if 'search_before' in pagination:
                # hits are reverse sorted
                if count > pagination['size']:
                    # There is an extra hit, indicating a previous page.
                    count = count - 1
                    search_before = es_hits[count - 1]['sort']
                else:
                    # No previous page
                    search_before = [None, None]
                search_after = es_hits[0]['sort']
            elif 'search_after' in pagination:
                # hits are normal sorted
                if count > pagination['size']:
                    # There is an extra hit, indicating a next page.
                    count = count - 1
                    search_after = es_hits[count - 1]['sort']
                else:
                    # No next page
                    search_after = [None, None]
                search_before = es_hits[0]['sort']
            else:
                # No search_after/before args were supplied, so assume it is the first page
                search_after = es_hits[count - 1]['sort'] if pages > 1 else []
                search_before = [None, None]

            page_field = {
                'count': count,
                'total': es_response['hits']['total'],
                'size': pagination['size'],
                'search_after': search_after[0],
                'search_after_uid': search_after[1],
                'search_before': search_before[0],
                'search_before_uid': search_before[1],
                'pages': pages,
                'sort': pagination['sort'],
                'order': pagination['order']
            }

        return page_field

    def transform_summary(
            self,
            request_config_file='request_config.json',
            filters=None):
        # Use this as the base to construct the paths
        # stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        self.logger.info('Transforming /summary request')
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the request_config_file
        request_config_path = "{}/{}".format(
            config_folder, request_config_file)
        # Get the Json Objects from the mapping_config and the request_config
        self.logger.debug('Getting the request_config file')
        request_config = self.open_and_return_json(request_config_path)
        if not filters:
            filters = {"file": {}}
        filters = filters["file"]
        # Create a request to ElasticSearch
        self.logger.info('Creating request to ElasticSearch')
        es_search = self.create_request(
            filters, self.es_client,
            request_config,
            post_filter=False)
        # Add a total_size aggregate to the ElasticSearch request
        es_search.aggs.metric(
            'total_size',
            'sum',
            field=request_config['translation']['fileSize'])
        # Override the aggregates for Samples,
        # Primary site count, and project count
        for field, agg_name in (
                ('specimenId',
                 'specimenCount'),
                ('organ', 'organCount'),
                ('project', 'projectCode')):
            cardinality = request_config['translation'][field]
            es_search.aggs.metric(
                agg_name, 'cardinality',
                field='{}.keyword'.format(cardinality),
                precision_threshold="40000")
        # Execute ElasticSearch request
        self.logger.info('Executing request to ElasticSearch')
        es_response = es_search.execute(ignore_cache=True)
        # Create the SummaryResponse object,
        #  which has the format for the summary request
        self.logger.info('Creating a SummaryResponse object')
        final_response = SummaryResponse(es_response.to_dict())
        self.logger.info(
            'Returning the final response for transform_summary()')
        return final_response.apiResponse.to_json()

    def transform_request(self,
                          request_config_file='request_config.json',
                          mapping_config_file='mapping_config.json',
                          filters=None,
                          pagination=None,
                          post_filter=False,
                          index="ES_FILE_INDEX"):
        """
        This function does the whole transformation process. It takes
        the path of the config file, the filters, and
        pagination, if any. Excluding filters will do a match_all request.
        Excluding pagination will exclude pagination
        from the output.
        :param filters: Filter parameter from the API to be used in the query.
        Defaults to None
        :param request_config_file: Path containing the requests config to be
        used for aggregates. Relative to the 'config' folder.
        :param mapping_config_file: Path containing the mapping to the API
        response fields. Relative to the 'config' folder.
        :param pagination: Pagination to be used for the API
        :param post_filter: Flag to indicate whether to do a post_filter
        call instead of the regular query.
        :return: Returns the transformed request
        """
        # Use this as the base to construct the paths
        # stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module

        self.logger.info('Transforming /specimens request')
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the mapping config file
        mapping_config_path = "{}/{}".format(
            config_folder, mapping_config_file)
        # Create the path for the config_path
        request_config_path = "{}/{}".format(
            config_folder, request_config_file)

        # Get the Json Objects from the mapping_config and the request_config
        self.logger.debug(
            'Getting the request_config and mapping_config file: {}'.format(
                request_config_path,
                mapping_config_path))
        request_config = self.open_and_return_json(request_config_path)
        self.logger.debug('Handling empty filters')
        if not filters:
            filters = {"file": {}}
        filters = filters['file']
        # No faceting (i.e. do the faceting on the filtered query)
        self.logger.debug('Handling presence or absence of faceting')
        if post_filter is False:
            # Create request structure
            es_search = self.create_request(
                filters, self.es_client,
                request_config,
                post_filter=False)
        # It's a full faceted search
        else:
            # Create request structure
            es_search = self.create_request(
                filters,
                self.es_client,
                request_config,
                post_filter=post_filter,
                index=index)
        # Handle pagination
        self.logger.debug('Handling pagination')

        if pagination is None:
            # It's a single file search
            es_response = es_search.execute(ignore_cache=True)
            es_response_dict = es_response.to_dict()
            hits = [x['_source']
                    for x in es_response_dict['hits']['hits']]
            # Create a KeywordSearchResponse object
            self.logger.info('Creating KeywordSearchResponse')
            final_response = KeywordSearchResponse(hits)
        else:
            # It's a full file search
            # Translate the sort field if there is any translation available
            if pagination['sort'] in request_config['translation']:
                pagination['sort'] = request_config[
                    'translation'][pagination['sort']]
            # Apply paging
            es_search = self.apply_paging(es_search, pagination)
            # Execute ElasticSearch request
            es_response = es_search.execute(ignore_cache=True)
            es_response_dict = es_response.to_dict()
            self.logger.debug("Printing ES_SEARCH response dict:\n {}".format(
                json.dumps(es_response_dict)))
            # Extract hits and facets (aggregations)
            es_hits = es_response_dict['hits']['hits']
            self.logger.info("length of es_hits: " + str(len(es_response_dict['hits']['hits'])))
            # If the number of elements exceed the page size, then we fetched one too many
            # entries to determine if there is a previous or next page.  In that case,
            # return one fewer hit.
            list_adjustment = 1 if len(es_hits) > pagination['size'] else 0
            if 'search_before' in pagination:
                hits = [x['_source'] for x in
                        reversed(es_hits[0:len(es_hits) - list_adjustment])]
            else:
                hits = [x['_source'] for x in es_hits[0:len(es_hits) - list_adjustment]]

            facets = es_response_dict['aggregations'] if 'aggregations' in es_response_dict else {}
            paging = self.generate_paging_dict(es_response_dict, pagination)
            # Creating FileSearchResponse object
            self.logger.info('Creating FileSearchResponse')

            final_response = FileSearchResponse(
                hits,
                paging,
                facets)
        self.logger.info(
            'Returning the final response for transform_request()')
        final_response = final_response.apiResponse.to_json()
        return final_response

    def transform_manifest(
            self,
            request_config_file='request_config.json',
            filters=None):
        """
        This function does the whole transformation process for a manifest
        request. It takes the path of the config file and the filters
        Excluding filters will do a match_all request.
        :param filters: Filter parameter from the API to be used in
        the query. Defaults to None
        :param request_config_file: Path containing the requests config
        to be used for aggregates. Relative to the'config' folder.
        :return: Returns the transformed manifest request
        """
        # Use this as the base to construct the paths
        # stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        self.logger.info('Transforming /export request')
        # Create the path for the config_path
        request_config_path = "{}/{}".format(
            config_folder, request_config_file)
        # Get the Json Objects from the request_config
        self.logger.debug(
            'Getting the request_config file: {}'.format(request_config_path))
        request_config = self.open_and_return_json(request_config_path)
        if not filters:
            filters = {"file": {}}
        # Create an ElasticSearch request
        filters = filters['file']

        es_search = self.create_request(
            filters,
            self.es_client,
            request_config,
            post_filter=False)

        manifest = ManifestResponse(es_search, request_config['manifest'], request_config['translation'])

        return manifest.return_response()

    def transform_autocomplete_request(
            self,
            pagination,
            request_config_file='request_config.json',
            mapping_config_file='autocomplete_mapping_config.json',
            filters=None,
            _query='',
            search_field='fileId',
            entry_format='file'):
        """
        This function does the whole transformation process. It
        takes the path of the config file, the filters, and pagination,
        if any. Excluding filters will do a match_all request.
        Excluding pagination will exclude pagination from the output.
        :param filters: Filter parameter from the API to be used in
        the query. Defaults to None
        :param request_config_file: Path containing the requests
        config to be used for aggregates. Relative to the config' folder.
        :param mapping_config_file: Path containing the mapping to the
        API response fields. Relative to the 'config' folder.
        :param pagination: Pagination to be used for the API
        :param _query: String query to use on the search.
        :param search_field: Field to perform the search on.
        :param entry_format: Tells the method which _type of
        entry format to use.
        :return: Returns the transformed request
        """
        # Use this as the base to construct the paths
        # stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        self.logger.info('Transforming /keywords request')
        # Create the path for the mapping config file
        mapping_config_path = "{}/{}".format(
            config_folder, mapping_config_file)
        # Create the path for the config_path
        request_config_path = "{}/{}".format(
            config_folder, request_config_file)
        # Get the Json Objects from the mapping_config and the request_config
        self.logger.debug(
            'Getting the request_config and mapping_config file: {}'.format(
                request_config_path,
                mapping_config_path))
        mapping_config = self.open_and_return_json(mapping_config_path)
        request_config = self.open_and_return_json(request_config_path)
        # Get the right autocomplete mapping configuration
        self.logger.debug("Entry is: {}".format(entry_format))
        self.logger.debug(
            "Printing the mapping_config: \n{}".format(
                json_pp(mapping_config)))
        mapping_config = mapping_config[entry_format]
        if not filters:
            filters = {"file": {}}

        index = 'ES_FILE_INDEX' if entry_format == 'file' \
            else 'ES_DONOR_INDEX'
        # Create an ElasticSearch autocomplete request
        es_search = self.create_autocomplete_request(
            filters,
            self.es_client,
            request_config,
            _query,
            search_field,
            index=index)
        # Handle pagination
        self.logger.info("Handling pagination")
        pagination['sort'] = '_score'
        pagination['order'] = 'desc'
        es_search = self.apply_paging(es_search, pagination)
        # Executing ElasticSearch request
        self.logger.debug(
            "Printing ES_SEARCH request dict:\n {}".format(
                json.dumps(es_search.to_dict())))
        es_response = es_search.execute(ignore_cache=True)
        es_response_dict = es_response.to_dict()
        self.logger.debug(
            "Printing ES_SEARCH response dict:\n {}".format(
                json.dumps(es_response_dict)))
        # Extracting hits
        hits = [x['_source'] for x in es_response_dict['hits']['hits']]
        # Generating pagination
        self.logger.debug("Generating pagination")
        paging = self.generate_paging_dict(es_response_dict, pagination)
        # Creating AutocompleteResponse
        self.logger.info("Creating AutoCompleteResponse")
        final_response = AutoCompleteResponse(
            mapping_config,
            hits,
            paging,
            _type=entry_format)
        final_response = final_response.apiResponse.to_json()
        self.logger.info(
            "Returning the final response for transform_autocomplete_request")
        return final_response
