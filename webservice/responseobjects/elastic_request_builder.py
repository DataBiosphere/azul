#!/usr/bin/python
import config
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
import json
import os
from responseobjects.api_response import KeywordSearchResponse, FileSearchResponse, SummaryResponse, ManifestResponse


class ElasticTransformDump(object):
    """
    This class works as the highest abstraction, serving as the top layer between the
    webservice and ElasticSearch

    Attributes:
        es_client: The ElasticSearch client which will be used to connect to ElasticSearch
    """

    def __init__(self, es_domain='localhost', es_port=9200, es_protocol='http'):
        """
        The constructor simply initializes the ElasticSearch client object
        to be used for making requests.
        :param es_domain: Domain where ElasticSearch is living
        :param es_port: Port where ElasticSearch is listening
        :param es_protocol: Protocol for ElasticSearch. Must be 'http' or 'https'
        """
        assert es_protocol in ['http', 'https'], "Protocol must be 'http' or 'https'"
        self.es_client = Elasticsearch(['{}://{}:{}/'.format(es_protocol, es_domain, es_port)])

    @staticmethod
    def translate_filters(filters, field_mapping):
        """
        Function for translating the filters
        :param filters: Raw filters from the filters /files param. That is, in 'browserForm'
        :param field_mapping: Mapping config json with '{'browserKey': 'es_key'}' format
        :return: Returns translated filters with 'es_keys'
        """
        # Translate the fields to the appropriate ElasticSearch Index.
        for key, value in filters['file'].items():  # Probably can be edited later to do not just files but donors, etc.
            if key in field_mapping:
                # Get the corrected term within ElasticSearch
                corrected_term = field_mapping[key]
                # Replace the key in the filter with the name within ElasticSearch
                filters['file'][corrected_term] = filters['file'].pop(key)
        return filters

    @staticmethod
    def create_query(filters):
        """
        Creates a query object based on the filters argument
        :param filters: filter parameter from the /files endpoint with translated (es_keys) keys
        :return: Returns Query object with appropriate filters
        """
        # Each iteration will AND the contents of the list
        query_list = [Q('constant_score', filter=Q('terms', **{facet: values['is']}))
                      for facet, values in filters['file'].iteritems()]
        # Return a Query object. Make it match_all
        return Q('bool', must=query_list) if len(query_list) > 0 else Q()

    @staticmethod
    def create_aggregate(filters, facet_config, agg):
        """
        Creates the aggregation to be used in ElasticSearch
        :param filters: Translated filters from 'files/' endpoint call
        :param facet_config: Configuration for the facets (i.e. facets on which to construct the aggregate)
        in '{browser:es_key}' form
        :param agg: Current aggregate where this aggregation is occurring. Syntax in browser form
        :return: returns an Aggregate object to be used in a Search query
        """
        # Pop filter of current Aggregate
        excluded_filter = filters['file'].pop(facet_config[agg], None)
        # Create the appropriate filters
        filter_query = ElasticTransformDump.create_query(filters)
        # Create the filter aggregate
        aggregate = A('filter', filter_query)
        # Make an inner aggregate that will contain the terms in question
        aggregate.bucket('myTerms', 'terms', field=facet_config[agg], size=99999)
        # If the aggregate in question didn't have any filter on the API call, skip it. Otherwise insert the popped
        # value back in
        if excluded_filter is not None:
            filters['file'][facet_config[agg]] = excluded_filter
        return aggregate

    @staticmethod
    def create_request(filters, es_client, req_config, post_filter=False):
        """
        This function will create an ElasticSearch request based on the filters and facet_config
        passed into the function
        :param filters: The 'filters' parameter from '/files'. Assumes to be translated into es_key terms
        :param es_client: The ElasticSearch client object used to configure the Search object
        :param req_config: The {'translation: {'browserKey': 'es_key'}, 'facets': ['facet1', ...]} config
        :param post_filter: Flag for doing either post_filter or regular querying (i.e. faceting or not)
        :return: Returns the Search object that can be used for executing the request
        """
        # Get the field mapping and facet configuration from the config
        field_mapping = req_config['translation']
        facet_config = {key: field_mapping[key] for key in req_config['facets']}
        # Create the Search Object
        es_search = Search(using=es_client, index=os.getenv('ES_FILE_INDEX', 'fb_index'))
        # Translate the filters keys
        filters = ElasticTransformDump.translate_filters(filters, field_mapping)
        # Get the query from 'create_query'
        es_query = ElasticTransformDump.create_query(filters)
        # Do a post_filter using the returned query
        es_search = es_search.query(es_query) if not post_filter else es_search.post_filter(es_query)
        # Iterate over the aggregates in the facet_config
        for agg, translation in facet_config.iteritems():
            # Create a bucket aggregate for the 'agg'. Call create_aggregate() to return the appropriate aggregate query
            es_search.aggs.bucket(agg, ElasticTransformDump.create_aggregate(filters, facet_config, agg))
        return es_search

    @staticmethod
    def open_and_return_json(file_path):
        """
        Opens and returns the contents of the json file given in file_path
        :param file_path: Path of a json file to be opened
        :return: Returns an obj with the contents of the json file
        """
        with open(file_path) as file_:
            loaded_file = json.load(file_)
            file_.close()
        return loaded_file

    @staticmethod
    def apply_paging(es_search, pagination):
        """
        Applies the pagination to the ES Search object
        :param es_search: The ES Search object
        :param pagination: Dictionary with raw entries from the GET Request. It has: 'from', 'size', 'sort', 'order'
        :return: An ES Search object where pagination has been applied
        """
        # Extract the fields for readability (and slight manipulation)
        _from = pagination['from'] - 1
        _to = pagination['size'] + _from
        _sort = pagination['sort']
        _order = pagination['order']
        # Apply order
        es_search = es_search.sort({_sort: {"order": _order}})
        # Apply paging
        es_search = es_search[_from:_to]
        return es_search

    @staticmethod
    def generate_paging_dict(es_response, pagination):
        """
        Generates the right dictionary for the final response.
        :param es_response: The raw dictionary response from ElasticSearch
        :param pagination: The pagination as coming from the GET request (or the defaults)
        :return: Modifies and returns the pagination updated with the new required entries.
        """
        page_field = {
            'count': len(es_response['hits']['hits']),
            'total': es_response['hits']['total'],
            'size': pagination['size'],
            'from': pagination['from'],
            'page': ((pagination['from'] - 1) / pagination['size']) + 1,
            'pages': -(-es_response['hits']['total'] // pagination['size']),
            'sort': pagination['sort'],
            'order': pagination['order']
        }
        return page_field

    def transform_summary(self, request_config_file='request_config.json', filters=None):
        # Use this as the base to construct the paths
        # https://stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the request_config_file
        request_config_path = "{}/{}".format(config_folder, request_config_file)
        # Get the Json Objects from the mapping_config and the request_config
        request_config = self.open_and_return_json(request_config_path)
        # Handle empty filters
        if filters is None:
            filters = {"file": {}}
        # Create a request to ElasticSearch
        es_search = self.create_request(filters, self.es_client, request_config, post_filter=False)
        es_search.aggs.metric('total_size', 'sum', field=request_config['translation']['fileSize'])
        # Add an aggregate for Donors
        cardinality = request_config['translation']['donorId']
        es_search.aggs.metric("donor", 'cardinality', field=cardinality, precision_threshold="40000")
        es_response = es_search.execute(ignore_cache=True)
        final_response = SummaryResponse(es_response.to_dict())
        return final_response.apiResponse.to_json()

    def transform_request(self, request_config_file='request_config.json',
                          mapping_config_file='mapping_config.json', filters=None, pagination=None,
                          post_filter=False):
        """
        This function does the whole transformation process. It takes the path of the config file, the filters, and
        pagination, if any. Excluding filters will do a match_all request. Excluding pagination will exclude pagination
        from the output.
        :param filters: Filter parameter from the API to be used in the query. Defaults to None
        :param request_config_file: Path containing the requests config to be used for aggregates. Relative to the
            'config' folder.
        :param mapping_config_file: Path containing the mapping to the API response fields. Relative to the
            'config' folder.
        :param pagination: Pagination to be used for the API
        :param post_filter: Flag to indicate whether to do a post_filter call instead of the regular query.
        :return: Returns the transformed request
        """
        # Use this as the base to construct the paths
        # https://stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the mapping config file
        mapping_config_path = "{}/{}".format(config_folder, mapping_config_file)
        # Create the path for the config_path
        request_config_path = "{}/{}".format(config_folder, request_config_file)
        # Get the Json Objects from the mapping_config and the request_config
        mapping_config = self.open_and_return_json(mapping_config_path)
        request_config = self.open_and_return_json(request_config_path)
        # Handle empty filters
        if filters is None:
            filters = {"file": {}}
        # No faceting (i.e. do the faceting on the filtered query)
        if post_filter is False:
            # Create request structure
            es_search = self.create_request(filters, self.es_client, request_config, post_filter=False)
        # It's a full faceted search
        else:
            # Create request structure
            es_search = self.create_request(filters, self.es_client, request_config, post_filter=post_filter)
        # Handle pagination
        if pagination is None:
            # It's a single file search
            es_response = es_search.execute(ignore_cache=True)
            es_response_dict = es_response.to_dict()
            hits = [x['_source'] for x in es_response_dict['hits']['hits']]
            final_response = KeywordSearchResponse(mapping_config, hits)
        else:
            # It's a full file search
            # Translate the sort field if there is any translation available
            if pagination['sort'] in request_config['translation']:
                pagination['sort'] = request_config['translation'][pagination['sort']]
            es_search = self.apply_paging(es_search, pagination)
            # TODO: NEED TO APPROPRIATELY LOG, PLEASE DELETE PRINT STATEMENT
            print "Printing ES_SEARCH request dict:\n {}".format(json.dumps(es_search.to_dict()))
            es_response = es_search.execute(ignore_cache=True)
            es_response_dict = es_response.to_dict()
            print "Printing ES_SEARCH response dict:\n {}".format(json.dumps(es_response_dict))
            hits = [x['_source'] for x in es_response_dict['hits']['hits']]
            facets = es_response_dict['aggregations']
            paging = self.generate_paging_dict(es_response_dict, pagination)
            final_response = FileSearchResponse(mapping_config, hits, paging, facets)
        final_response = final_response.apiResponse.to_json()
        return final_response

    def transform_manifest(self, request_config_file='request_config.json', filters=None):
        """
        This function does the whole transformation process for a manifest request. It takes the path of the
        config file and the filters Excluding filters will do a match_all request.
        :param filters: Filter parameter from the API to be used in the query. Defaults to None
        :param request_config_file: Path containing the requests config to be used for aggregates. Relative to the
            'config' folder.
        :return: Returns the transformed manifest request
        """
        # Use this as the base to construct the paths
        # https://stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the config_path
        request_config_path = "{}/{}".format(config_folder, request_config_file)
        # Get the Json Objects from the request_config
        request_config = self.open_and_return_json(request_config_path)
        # Handle empty filters
        if filters is None:
            filters = {"file": {}}
        es_search = self.create_request(filters, self.es_client, request_config, post_filter=False)
        # TODO: This will break beyond 10,000 entries in ElasticSearch. This needs to be addressed in the near future
        # Get as many files as simple paging allows
        es_search = es_search[0:9999]
        # Execute the ElasticSearch Request
        es_response = es_search.execute(ignore_cache=True)
        # Transform to a raw dictionary
        es_response_dict = es_response.to_dict()
        # Get the ManifestResponse object
        manifest = ManifestResponse(es_response_dict, request_config['manifest'], request_config['translation'])
        return manifest.return_response()

    def transform_autocomplete_request(self, request_config_file='request_config.json',
                                       mapping_config_file='mapping_config.json', filters=None, pagination=None,
                                       post_filter=False):
        """
        This function does the whole transformation process. It takes the path of the config file, the filters, and
        pagination, if any. Excluding filters will do a match_all request. Excluding pagination will exclude pagination
        from the output.
        :param filters: Filter parameter from the API to be used in the query. Defaults to None
        :param request_config_file: Path containing the requests config to be used for aggregates. Relative to the
            'config' folder.
        :param mapping_config_file: Path containing the mapping to the API response fields. Relative to the
            'config' folder.
        :param pagination: Pagination to be used for the API
        :param post_filter: Flag to indicate whether to do a post_filter call instead of the regular query.
        :return: Returns the transformed request
        """
        # Use this as the base to construct the paths
        # https://stackoverflow.com/questions/247770/retrieving-python-module-path
        # Use that to get the path of the config module
        config_folder = os.path.dirname(config.__file__)
        # Create the path for the mapping config file
        mapping_config_path = "{}/{}".format(config_folder, mapping_config_file)
        # Create the path for the config_path
        request_config_path = "{}/{}".format(config_folder, request_config_file)
        # Get the Json Objects from the mapping_config and the request_config
        mapping_config = self.open_and_return_json(mapping_config_path)
        request_config = self.open_and_return_json(request_config_path)
        # Handle empty filters
        if filters is None:
            filters = {"file": {}}
        es_search = self.create_request(filters, self.es_client, request_config, post_filter=post_filter)
        # Handle pagination

        # It's a full file search
        # Translate the sort field if there is any translation available
        if pagination['sort'] in request_config['translation']:
            pagination['sort'] = request_config['translation'][pagination['sort']]
        es_search = self.apply_paging(es_search, pagination)
        # TODO: NEED TO APPROPRIATELY LOG, PLEASE DELETE PRINT STATEMENT
        print "Printing ES_SEARCH request dict:\n {}".format(json.dumps(es_search.to_dict()))
        es_response = es_search.execute(ignore_cache=True)
        es_response_dict = es_response.to_dict()
        print "Printing ES_SEARCH response dict:\n {}".format(json.dumps(es_response_dict))
        hits = [x['_source'] for x in es_response_dict['hits']['hits']]
        facets = es_response_dict['aggregations']
        paging = self.generate_paging_dict(es_response_dict, pagination)
        final_response = FileSearchResponse(mapping_config, hits, paging, facets)
        final_response = final_response.apiResponse.to_json()
        return final_response