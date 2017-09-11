from flask import Flask, jsonify, request, session, Blueprint
from flask_login import LoginManager, login_required, \
    current_user, UserMixin
# from flask import current_app as app
import json
# from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS, cross_origin
# from flask_migrate import Migrate
import flask_excel as excel
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

from responseobjects.elastic_request_builder import ElasticTransformDump as EsTd

logging.basicConfig(level=logging.DEBUG)

webservicebp = Blueprint('webservicebp', 'webservicebp')

apache_path = os.environ.get("APACHE_PATH", "")
es_service = os.environ.get("ES_SERVICE", "localhost")
es = Elasticsearch(['http://' + es_service + ':9200/'])


def parse_ES_response(es_dict, the_size, the_from, the_sort, the_order, key_search=False):
    protoDict = {'hits': []}
    for hit in es_dict['hits']['hits']:
        if '_source' in hit:
            protoDict['hits'].append({
                'id': hit['_source']['file_id'],
                'objectID': hit['_source']['file_id'],
                'access': hit['_source']['access'],
                'center_name': hit['_source']['center_name'],
                'study': [hit['_source']['study']],
                'program': hit['_source']['program'],  ###Added source
                'dataCategorization': {
                    'dataType': hit['_source']['file_type'],
                    'experimentalStrategy': hit['_source']['experimentalStrategy']  # ['workflow']
                },
                'fileCopies': [{
                    'repoDataBundleId': hit['_source']['repoDataBundleId'],
                    'repoDataSetIds': [],
                    'repoCode': hit['_source']['repoCode'],
                    'repoOrg': hit['_source']['repoOrg'],
                    'repoName': hit['_source']['repoName'],
                    'repoType': hit['_source']['repoType'],
                    'repoCountry': hit['_source']['repoCountry'],
                    'repoBaseUrl': hit['_source']['repoBaseUrl'],
                    'repoDataPath': '',  ###Empty String
                    'repoMetadatapath': '',  ###Empty String
                    'fileName': hit['_source']['title'],
                    'fileFormat': hit['_source']['file_type'],
                    'fileSize': hit['_source']['fileSize'],
                    'fileMd5sum': hit['_source']['fileMd5sum'],
                    'lastModified': hit['_source']['lastModified']
                }],
                'donors': [{
                    'donorId': hit['_source']['donor'],
                    'primarySite': hit['_source']['submitterDonorPrimarySite'],
                    'projectCode': hit['_source']['project'],
                    'study': hit['_source']['study'],  ###
                    'sampleId': [hit['_source']['sampleId']],  ###
                    'specimenType': [hit['_source']['specimen_type']],
                    'submittedDonorId': hit['_source']['submittedDonorId'],  ###
                    'submittedSampleId': [hit['_source']['submittedSampleId']],  ###
                    'submittedSpecimenId': [hit['_source']['submittedSpecimenId']],  ###
                    'otherIdentifiers': {
                        'RedwoodDonorUUID': [hit['_source']['redwoodDonorUUID']],  ###
                    }

                }],

                'analysisMethod': {
                    'analysisType': hit['_source']['analysis_type'],
                    'software': hit['_source']['software'] + ':' + hit['_source']['workflowVersion']
                ###  #Concatenated the version for the software/workflow
                },
                'referenceGenome': {
                    'genomeBuild': '',  ###Blank String
                    'referenceName': '',  ###Blank String
                    'downloadUrl': ''  ###Blank String
                }
            })

        else:
            try:
                protoDict['hits'].append(hit['fields'])
            except:
                pass
    # If returning only one term based on file_id, break and return.
    if key_search:
        return protoDict

    protoDict['pagination'] = {
        'count': len(es_dict['hits']['hits']),  # 25,
        'total': es_dict['hits']['total'],
        'size': the_size,
        'from': the_from + 1,
        'page': (the_from / (the_size)) + 1,  # (the_from/(the_size+1))+1
        'pages': -(-es_dict['hits']['total'] // the_size),
        'sort': the_sort,
        'order': the_order
    }

    protoDict['termFacets'] = {}  # es_dict['aggregations']
    for x, y in es_dict['aggregations'].items():
        protoDict['termFacets'][x] = {'type': 'terms',
                                      'terms': map(lambda x: {"term": x["key"], 'count': x['doc_count']},
                                                   y['myTerms']['buckets'])}  # Added myTerms key

    # Get the total for all the terms
    for section in protoDict['termFacets']:
        m_sum = 0
        # print section
        for term in protoDict['termFacets'][section]['terms']:
            m_sum += term['count']
        protoDict['termFacets'][section]['total'] = m_sum

    return protoDict


@webservicebp.route('/repository/files')
@webservicebp.route('/repository/files/')
@webservicebp.route('/repository/files/<file_id>')
@cross_origin()
def get_data(file_id=None):
    # Get all the parameters from the URL
    fields = request.args.get('field')
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
        filters = {"file": {}} if filters == {} else filters
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    include = request.args.get('include', 'facets')
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
    # Get all the parameters from the URL
    fields = request.args.get('field')
    filters = request.args.get('filters', '{"file": {}}')
    # TODO: This try except block should be logged appropriately
    try:
        filters = ast.literal_eval(filters)
    except Exception, e:
        print str(e)
        return "Malformed filters parameters"
    include = request.args.get('include', 'facets')
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

##*********************************************************************************************************************************##


# This will return a summary of the facets
@webservicebp.route('/repository/files/facets')
@cross_origin()
def get_facets():
    # Get the order of the keys for the facet list
    f_order = []
    d_order = []
    with open(apache_path + 'order_file') as file_order:
        f_order = file_order.readlines()
        f_order = [x.strip() for x in f_order]
    with open(apache_path + 'order_donor') as donor_order:
        d_order = donor_order.readlines()
        d_order = [x.strip() for x in d_order]

    # Search the aggregates.
    # Parse them
    # Return it as a JSON output.
    # Things I need to know: The final format of the indexes stored in ES
    facets_list = {}
    with open(apache_path + 'supported_facets.json') as my_facets:
        facets_list = json.load(my_facets)
        mText = es.search(index='fb_alias', body={"query": {"match_all": {}}, "aggs": {
            "centerName": {
                "terms": {"field": "center_name",
                          "min_doc_count": 0,
                          "size": 99999}
            },
            "projectCode": {
                "terms": {
                    "field": "project",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "specimenType": {
                "terms": {
                    "field": "specimen_type",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "fileFormat": {
                "terms": {
                    "field": "file_type",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "workFlow": {
                "terms": {
                    "field": "workflow",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "analysisType": {
                "terms": {
                    "field": "analysis_type",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "study": {
                "terms": {
                    "field": "study",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "experimental_design": {
                "terms": {
                    "field": "experimentalStrategy",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "data_type": {
                "terms": {
                    "field": "file_type",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "repository": {
                "terms": {
                    "field": "repoName",
                    "min_doc_count": 0,
                    "size": 99999
                }
            },
            "access_type": {
                "terms": {
                    "field": "access",
                    "min_doc_count": 0,
                    "size": 99999
                }
            }

        }})

        facets_list["DonorLevel"]['project']['values'] = [x['key'] for x in
                                                          mText['aggregations']['projectCode']['buckets']]
        facets_list["DonorLevel"]['data_types_available']['values'] = [x['key'] for x in
                                                                       mText['aggregations']['fileFormat']['buckets']]
        facets_list["DonorLevel"]['specimen_type']['values'] = [x['key'] for x in
                                                                mText['aggregations']['specimenType']['buckets']]
        facets_list["DonorLevel"]['study']['values'] = [x['key'] for x in mText['aggregations']['study']['buckets']]
        facets_list["DonorLevel"]['experimental_design']['values'] = [x['key'] for x in
                                                                      mText['aggregations']['experimental_design'][
                                                                          'buckets']]

        facets_list["FileLevel"]['file_format']['values'] = [x['key'] for x in
                                                             mText['aggregations']['fileFormat']['buckets']]
        facets_list["FileLevel"]['specimen_type']['values'] = [x['key'] for x in
                                                               mText['aggregations']['specimenType']['buckets']]
        facets_list["FileLevel"]['workflow']['values'] = [x['key'] for x in
                                                          mText['aggregations']['workFlow']['buckets']]
        facets_list["FileLevel"]['repository']['values'] = [x['key'] for x in
                                                            mText['aggregations']['repository']['buckets']]
        facets_list["FileLevel"]['data_type']['values'] = [x['key'] for x in
                                                           mText['aggregations']['data_type']['buckets']]
        facets_list["FileLevel"]['experimental_design']['values'] = [x['key'] for x in
                                                                     mText['aggregations']['experimental_design'][
                                                                         'buckets']]
        facets_list["FileLevel"]['access_type']['values'] = [x['key'] for x in
                                                             mText['aggregations']['access_type']['buckets']]

    array_facet_list = {'DonorLevel': [], 'FileLevel': []}
    for x in f_order:
        array_facet_list['FileLevel'].append({x: facets_list["FileLevel"][x]})
    for x in d_order:
        array_facet_list['DonorLevel'].append({x: facets_list["DonorLevel"][x]})

    return jsonify(array_facet_list)


# return jsonify(facets_list)

# This will return a summary as the one from the ICGC endpoint
# Takes filters as parameter.
@webservicebp.route('/repository/files/summary')
@cross_origin()
def get_summary():
    my_summary = {"fileCount": None, "totalFileSize": None, "donorCount": None, "projectCount": None,
                  "primarySiteCount": "DUMMY"}
    m_filters = request.args.get('filters')

    # Dictionary for getting a reference to the aggs key
    idSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path + 'reference_aggs.json') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path + 'inverse_aggs.json') as my_aggs:
        # with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    try:
        m_filters = ast.literal_eval(m_filters)
        # Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                # This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key == "id":
                # idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                # idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["donor"] = m_filters['file'].pop(key)
        # Functions for calling the appropriates query filters
        matchValues = lambda x, y: {"filter": {"terms": {x: y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x, y in m_filters['file'].items()]
        mQuery = {"bool": {"must": [filt_list]}}
        # Add the mechanism to incorporate idSearch. See if it works!
        # if idSearch:
        # mQuery["constant_score"] = {"filter":{"terms":{"file_id":idSearch}}}
        # mQuery["bool"]["filter"] = {"terms":{"file_id":idSearch}}
    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all": {}}
        pass
    # Need to pass on the arguments for this.
    print mQuery
    mText = es.search(index='fb_alias', body={"query": mQuery, "aggs": {
        "centerName": {
            "terms": {"field": "center_name",
                      # "min_doc_count" : 0,
                      "size": 99999}
        },
        "projectCode": {
            "terms": {
                "field": "project",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "specimenType": {
            "terms": {
                "field": "specimen_type",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "fileFormat": {
            "terms": {
                "field": "file_type",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "workFlow": {
            "terms": {
                "field": "workflow",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "analysisType": {
            "terms": {
                "field": "analysis_type",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "donor": {
            "terms": {
                "field": "donor",
                # "min_doc_count" : 0,
                "size": 99999
            }
        },
        "submitterDonorPrimarySite": {
            "terms": {
                "field": "submitterDonorPrimarySite",
                # "min_doc_count": 0,
                "size": 99999
            }
        },
        "total_size": {
            "sum": {"field": "fileSize"}
        }
    }})

    print mText['aggregations']['donor']

    my_summary['fileCount'] = mText['hits']['total']
    my_summary['donorCount'] = len(mText['aggregations']['donor']['buckets'])
    my_summary['projectCount'] = len(mText['aggregations']['projectCode']['buckets'])
    my_summary['totalFileSize'] = mText['aggregations']['total_size']['value']
    my_summary['primarySiteCount'] = len(mText['aggregations']['submitterDonorPrimarySite']['buckets'])
    # To remove once this endpoint has some functionality
    return jsonify(my_summary)


# return "still working on this endpoint, updates soon!!"



###Methods for executing the search endpoint
# Searches keywords in the fb_alias index
def searchFile(_query, _filters, _from, _size):
    # Body of the query search
    query_body = {"prefix": {
        "file_id": _query}}  # {"query_string":{"query":_query, "default_field":"file_id", "analyzer":"my_analyzer"}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter": _filters}

    mResult = es.search(index='fb_alias', body=body, from_=_from, size=_size)

    # Now you have the brute results from the ES query. All you need to do now is to parse the data
    # and put it in a pretty dictionary, and return it.

    # This variable will hold the response to be returned
    searchResults = {"hits": [], "pagination": {}}

    for hit in mResult['hits']['hits']:
        if '_source' in hit:
            searchResults['hits'].append({
                "id": hit['_source']['file_id'],
                "type": "file",
                "donorId": [hit['_source']['redwoodDonorUUID']],
                "fileName": [hit['_source']['title']],
                "dataType": hit['_source']['file_type'],
                "projectCode": [hit['_source']['project']],
                # "fileObjectId": hit['_source']['file_type'], #Probabbly we don't have this
                "fileBundleId": hit['_source']['repoDataBundleId']
            })

    searchResults['pagination']['count'] = len(mResult['hits']['hits'])
    searchResults['pagination']['total'] = mResult['hits']['total']
    searchResults['pagination']['size'] = _size
    searchResults['pagination']['from'] = _from
    searchResults['pagination']['page'] = (_from / (_size)) + 1
    searchResults['pagination']['pages'] = -(-mResult['hits']['total'] // _size)
    searchResults['pagination']['sort'] = "_score"  # Will alaways be sorted by score
    searchResults['pagination']['order'] = "desc"  # Will always be descendent order

    return searchResults


def searchFilesDonors(_query, _filters, _from, _size):
    # Body of the query search
    query_body = {"prefix": {"donor_uuid": _query}}  # {"query_string":{"query":_query}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter": _filters}

    mResult = es.search(index='analysis_index', body=body, from_=_from, size=_size)

    # Now you have the brute results from the ES query. All you need to do now is to parse the data
    # and put it in a pretty dictionary, and return it.
    searchResults = {"hits": []}
    reader = [x['_source'] for x in mResult['hits']['hits']]
    for obj in reader:
        donorEntry = {}
        donor_id = obj['donor_uuid']  # This is the id
        donor_type = 'donor'  # this is the type
        # Iterate through the specimens
        donorEntry['id'] = donor_id
        donorEntry['type'] = donor_type

        searchResults['hits'].append(donorEntry)

    return searchResults


# Searches keywords in the analysis_index
def searchDonors(_query, _filters, _from, _size):
    # Body of the query search
    query_body = {"prefix": {"donor_uuid": _query}}  # {"query_string":{"query":_query}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter": _filters}

    mResult = es.search(index='analysis_index', body=body, from_=_from, size=_size)

    # Now you have the brute results from the ES query. All you need to do now is to parse the data
    # and put it in a pretty dictionary, and return it.
    searchResults = {"hits": [], "pagination": {}}
    reader = [x['_source'] for x in mResult['hits']['hits']]
    for obj in reader:
        donorEntry = {}
        donor_id = obj['donor_uuid']  # This is the id
        donor_type = 'donor'  # this is the type
        donor_submitteId = obj['submitter_donor_id']  # This is the submittedId
        donor_projectId = obj['project']
        # This are the scpecimen and sample lists.
        donor_specimenIds = []
        donor_submitteSpecimenIds = []
        donor_sampleIds = []
        donor_submitteSampleIds = []
        # Iterate through the specimens
        for speci in obj['specimen']:
            donor_specimenIds.append(speci['specimen_uuid'])
            donor_submitteSpecimenIds.append(speci['submitter_specimen_id'])
            for sample in speci['samples']:
                donor_sampleIds.append(sample['sample_uuid'])
                donor_submitteSampleIds.append(sample['submitter_sample_id'])

        donorEntry['id'] = donor_id
        donorEntry['projectId'] = donor_projectId
        donorEntry['type'] = donor_type
        donorEntry['submittedId'] = donor_submitteId
        donorEntry['specimenIds'] = donor_specimenIds
        donorEntry['submittedSpecimenIds'] = donor_submitteSpecimenIds
        donorEntry['sampleIds'] = donor_sampleIds
        donorEntry['submittedSampleIds'] = donor_submitteSampleIds

        searchResults['hits'].append(donorEntry)

    searchResults['pagination']['count'] = len(mResult['hits']['hits'])
    searchResults['pagination']['total'] = mResult['hits']['total']
    searchResults['pagination']['size'] = _size
    searchResults['pagination']['from'] = _from
    searchResults['pagination']['page'] = (_from / (_size)) + 1
    searchResults['pagination']['pages'] = -(-mResult['hits']['total'] // _size)
    searchResults['pagination']['sort'] = "_score"  # Will alaways be sorted by score
    searchResults['pagination']['order'] = "desc"  # Will always be descendent order

    return searchResults


# This will return a search list
# Takes filters as parameter.
@webservicebp.route('/keywords')
@cross_origin()
def get_search():
    # Get the parameters
    m_query = request.args.get('q')
    m_filters = request.args.get('filters')
    m_from = request.args.get('from', 1, type=int)
    m_size = request.args.get('size', 5, type=int)
    m_type = request.args.get('type', 'file')
    # Won't implement this one just yet.
    m_field = request.args.get('field', 'file')

    # References
    referenceAggs = {}
    inverseAggs = {}
    m_from -= 1
    # Holder for the keyword result
    keywordResult = {}

    with open(apache_path + 'reference_aggs.json') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path + 'inverse_aggs.json') as my_aggs:
        # with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)
    # Get the filters in an appropriate format
    try:
        m_filters = ast.literal_eval(m_filters)
        # Check if the string is in the other format. Change it as appropriate.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                corrected_term = referenceAggs[key]
                # print corrected_term
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            # Adding the filters from before, just in case the do keyword search combining donor and file ids
            if key == "fileId" or key == "id":
                # idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                # idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["donor"] = m_filters['file'].pop(key)

                # print m_filters

        # Functions for calling the appropriates query filters
        matchValues = lambda x, y: {"filter": {"terms": {x: y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x, y in m_filters['file'].items()]
        filterQuery = {
            "bool": {"must": filt_list}}  # Removed the brackets; Make sure it doesn't break anything down the line


    except Exception, e:
        print str(e)
        m_filters = None
        filterQuery = {}

    # If the query is empty
    if not m_query:
        keywordResult = {'hits': []}
    # return "Query is Empty. Change this to an empty array"
    # If the query is for files
    if m_type == 'file':
        keywordResult = searchFile(m_query, filterQuery, m_from, m_size)

    # If the query is for donors
    elif m_type == 'donor':  # or m_type == 'file-donor':
        keywordResult = searchDonors(m_query, filterQuery, m_from, m_size)
    elif m_type == 'file-donor':
        keywordResult = searchFilesDonors(m_query, filterQuery, m_from, m_size)

    # Need to have two methods. One executes depending on whether the type is either 'file' or 'file-donor'

    return jsonify(keywordResult)


# return "Comming soon!"


# This will simply return the desired order of the facets
# Takes filters as parameter.
@webservicebp.route('/repository/files/order')
@cross_origin()
def get_order2():
    with open(apache_path + 'order_config') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        # referenceAggs = json.load(my_aggs)
        order = [line.rstrip('\n') for line in my_aggs]
    return jsonify({'order': order})


@webservicebp.route('/repository/files/meta')
@cross_origin()
def get_order3():
    with open(apache_path + 'f_donor') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        # referenceAggs = json.load(my_aggs)
        order_donor = [{'name': line.rstrip('\n'), 'category': 'donor'} for line in my_aggs]
    # order = [line.rstrip('\n') for line in my_aggs]

    with open(apache_path + 'f_file') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        # referenceAggs = json.load(my_aggs)
        order_file = [{'name': line.rstrip('\n'), 'category': 'file'} for line in my_aggs]
    # order = [line.rstrip('\n') for line in my_aggs]

    order_final = order_file + order_donor

    return jsonify(order_final)

# Get the manifest. You need to pass on the filters
@webservicebp.route('/repository/files/export')
@cross_origin()
def get_manifest():
    m_filters = request.args.get('filters')
    m_size = request.args.get('size', 25, type=int)
    mQuery = {}

    # Dictionary for getting a reference to the aggs key
    idSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path + 'reference_aggs.json') as my_aggs:
        # with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path + 'inverse_aggs.json') as my_aggs:
        # with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    try:
        m_filters = ast.literal_eval(m_filters)
        # Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                # This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key == "id":
                # idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                m_filters['file']["donor"] = m_filters['file'].pop(key)
        # Functions for calling the appropriates query filters
        matchValues = lambda x, y: {"filter": {"terms": {x: y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x, y in m_filters['file'].items()]
        mQuery = {"bool": {"must": [filt_list]}}
    # if idSearch:
    # mQuery["constant_score"] = {"filter":{"terms":{"file_id":idSearch}}}

    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all": {}}
        pass
    # Added the scroll variable. Need to put the scroll variable in a config file.
    scroll_config = ''
    with open(apache_path + 'scroll_config') as _scroll_config:
        # with open('scroll_config') as _scroll_config:
        scroll_config = _scroll_config.readline().strip()
        # print scroll_config

    mText = es.search(index='fb_alias', body={"query": mQuery}, size=9999, scroll=scroll_config)  # '2m'

    # Set the variables to do scrolling. This should fix the problem with the small amount of
    sid = mText['_scroll_id']
    scroll_size = mText['hits']['total']
    # reader = [x['_source'] for x in mText['hits']['hits']]

    # MAKE SURE YOU TEST THIS
    while (scroll_size > 0):
        print "Scrolling..."
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the Scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        # Extend the result list
        # reader.extend([x['_source'] for x in page['hits']['hits']])
        mText['hits']['hits'].extend([x for x in page['hits']['hits']])
        print len(mText['hits']['hits'])
        print "Scroll Size: " + str(scroll_size)

    protoList = []
    for hit in mText['hits']['hits']:
        if '_source' in hit:
            protoList.append(hit['_source'])
    goodFormatList = []
    goodFormatList.append(
        ['Program', 'Project', 'Center Name', 'Submitter Donor ID', 'Donor UUID', "Submitter Donor Primary Site",
         'Submitter Specimen ID', 'Specimen UUID', 'Submitter Specimen Type', 'Submitter Experimental Design',
         'Submitter Sample ID', 'Sample UUID', 'Analysis Type', 'Workflow Name', 'Workflow Version', 'File Type',
         'File Path', 'Upload File ID', 'Data Bundle UUID', 'Metadata.json'])
    for row in protoList:
        currentRow = [row['program'], row['project'], row['center_name'], row['submittedDonorId'], row['donor'],
                      row['submitterDonorPrimarySite'], row['submittedSpecimenId'], row['specimenUUID'],
                      row['specimen_type'], row['experimentalStrategy'], row['submittedSampleId'], row['sampleId'],
                      row['analysis_type'], row['software'], row['workflowVersion'], row['file_type'], row['title'],
                      row['file_id'], row['repoDataBundleId'], row['metadataJson']]
        goodFormatList.append(currentRow)

    return excel.make_response_from_array(goodFormatList, 'tsv', file_name='manifest')
