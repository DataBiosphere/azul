from flask import Flask, jsonify, request, session, Blueprint
from flask_login import LoginManager, login_required, \
    current_user, UserMixin
# from flask import current_app as app
import json
# from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
# from flask.ext.elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch
import ast
# from decimal import Decimal
import copy

import os
# from models import Billing, db
# from utility import get_compute_costs, get_storage_costs, create_analysis_costs_json, create_storage_costs_json
import datetime
# import calendar
# import click
# TEST database call
# from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select
import logging
from database import db, login_db, login_manager
import config

from responseobjects.elastic_request_builder import ElasticTransformDump as EsTd

logging.basicConfig(level=logging.DEBUG)

webservicebp = Blueprint('webservicebp', 'webservicebp')

apache_path = os.environ.get("APACHE_PATH", "")
es_service = os.environ.get("ES_SERVICE", "localhost")
es = Elasticsearch(['http://' + es_service + ':9200/'])


@webservicebp.route('/repository/files')
@webservicebp.route('/repository/files/')
@webservicebp.route('/repository/files/<file_id>')
@cross_origin()
def get_data(file_id=None):
    """
    Returns a dictionary with entries that can be used by the browser to display the data and facets
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
        - name: order
          in: query
          type: string
          description: Whether it should be in ascending or descending order
        - name: sort
          in: integer
          type: string
          description: Which field to sort by
    :return: Returns a dictionary with the entries to be used when generating the facets and/or table data
    """
    # Get all the parameters from the URL
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    # Make the default pagination
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "order": request.args.get('order', 'desc'),
        "size": request.args.get('size', 5, type=int),
        "sort":    request.args.get('sort', 'center_name'),
    }
    # Handle <file_id> request form
    if file_id is not None:
        filters['file']['fileId'] = {"is": [file_id]}
    # Create and instance of the ElasticTransformDump
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    response = es_td.transform_request(filters=filters, pagination=pagination, post_filter=True)
    # Returning a single response if <file_id> request form is used
    if file_id is not None:
        response = response['hits'][0]
    return jsonify(response)


@webservicebp.route('/repository/files/piecharts')
@cross_origin()
def get_data_pie():
    """
    Returns a dictionary with entries that can be used by the browser to generate piecharts
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
        - name: order
          in: query
          type: string
          description: Whether it should be in ascending or descending order
        - name: sort
          in: integer
          type: string
          description: Which field to sort by
    :return: Returns a dictionary with the entries to be used when generating a pie chart
    """
    # Get all the parameters from the URL
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    # Make the default pagination
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "order": request.args.get('order', 'desc'),
        "size": request.args.get('size', 5, type=int),
        "sort":    request.args.get('sort', 'center_name'),
    }
    # Create and instance of the ElasticTransformDump
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    response = es_td.transform_request(filters=filters, pagination=pagination, post_filter=False)
    # Returning a single response if <file_id> request form is used
    return jsonify(response)


@webservicebp.route('/repository/files/summary')
@cross_origin()
def get_summary():
    """
    Returns a summary based on the filters passed on to the call. Based on the
    ICGC endpoint.
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
    :return: Returns a jsonified Summary API response
    """
    # Get the filters from the URL
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    # Create and instance of the ElasticTransformDump
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    response = es_td.transform_summary(filters=filters)
    # Returning a single response if <file_id> request form is used
    return jsonify(response)


@webservicebp.route('/keywords')
@cross_origin()
def get_search():
    """
    Creates and returns a dictionary with entries that best match the query passed in to the endpoint
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when calling ElasticSearch
        - name: q
          in: query
          type: string
          description: String query to use when calling ElasticSearch
        - name: from
          in: query
          type: integer
          description: From where should we start returning the results
        - name: size
          in: integer
          type: string
          description: Size of the page being returned
    :return: A dictionary with entries that best match the query passed in to the endpoint
    """
    # Get all the parameters from the URL
    # TODO: This try except block should be logged appropriately
    # Get the query to use for searching
    _query = request.args.get('q', '')
    # Get the filters
    filters = request.args.get('filters', '{"file": {}}')
    try:
        # Set up the default filter if it is returned as an empty dictionary
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    # Generate the pagination dictionary out of the endpoint parameters
    pagination = {
        "from": request.args.get('from', 1, type=int),
        "size": request.args.get('size', 5, type=int)
    }
    # Get the entry format and search field
    _type = request.args.get('type', 'file'),
    # Get the field to search
    field = request.args.get('field', 'fileId')
    # HACK: Adding this small check to make sure the search bar works with
    if _type in {'donor', 'file-donor'}:
        field = 'donor'
    # Create and instance of the ElasticTransformDump
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN", "elasticsearch1"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    response = es_td.transform_autocomplete_request(pagination, filters=filters,
                                                    _query=_query,
                                                    search_field=field,
                                                    entry_format=_type)
    return jsonify(response)


@webservicebp.route('/repository/files/order')
@cross_origin()
def get_order():
    """
    Get the order of the facets from the order_config file
    :return: A dictionary with a list containing the order of the facets
    """
    with open('{}/order_config'.format(os.path.dirname(config.__file__))) as order:
        order_list = [line.rstrip('\n') for line in order]
    return jsonify({'order': order_list})


@webservicebp.route('/repository/files/export')
@cross_origin()
def get_manifest():
    """
    Creates and returns a manifest based on the filters pased on to this endpoint
    parameters:
        - name: filters
          in: query
          type: string
          description: Filters to be applied when generating the manifest
    :return: A manifest that the user can use to download the files in there
    """
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    # Create and instance of the ElasticTransformDump
    es_td = EsTd(es_domain=os.getenv("ES_DOMAIN"),
                 es_port=os.getenv("ES_PORT", 9200),
                 es_protocol=os.getenv("ES_PROTOCOL", "http"))
    # Get the response back
    response = es_td.transform_manifest(filters=filters)
    # Return the excel file
    return response
