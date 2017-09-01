#!/usr/bin/python
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, A
import sys
from responseobjects.api_response import KeywordSearchResponse, FileSearchResponse


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
    def create_request(filters, es_client, config):
        """
        This function will create an ElasticSearch request based on the filters and facet_config passed into the function
        :param filters: The 'filters' parameter from '/files'. Assumes to be translated into es_key terms
        :param es_client: The ElasticSearch client object used to configure the Search object
        :param config: The {'translation: {'browserKey': 'es_key'}, 'facets': ['facet1', ...]} config
        :return: Returns the Search object that can be used for executing the request
        """
        # Get the field mapping and facet configuration from the config
        field_mapping = config['translation']
        facet_config = {key: field_mapping[key] for key in config['facets']}
        # Create the Search Object
        es_search = Search(using=es_client)
        # Translate the filters keys
        filters = ElasticTransformDump.translate_filters(filters, field_mapping)
        # Get the query from 'create_query'
        es_query = ElasticTransformDump.create_query(filters)
        # Do a post_filter using the returned query
        es_search.post_filter(es_query)  # This should be eventually handled depending on what endpoint is being hit
        # Iterate over the aggregates in the facet_config
        for agg, translation in facet_config.iteritems():
            # Create a bucket aggregate for the 'agg'. Call create_aggregate() to return the appropriate aggregate query
            es_search.aggs.bucket(agg, ElasticTransformDump.create_aggregate(filters, facet_config, agg))
        return es_search

    def transform_request(self, request_config_path='request_config.json',
                          mapping_config_path='config',filters=None, pagination=None):
        """
        This function does the whole transformation process. It takes the path of the config file, the filters, and
        pagination, if any. Excluding filters will do a match_all request. Excluding pagination will exclude pagination
        from the output.
        :param filters: Filter parameter from the API to be used in the query. Defaults to None
        :param request_config_path: Path containing the requests config to be used for aggregates
        :param mapping_config_path: Path containing the mapping to the API response fields.
        :param pagination: Pagination to be used for
        :return:
        """

        # https://stackoverflow.com/questions/247770/retrieving-python-module-path Use that to get the path of the config module.



