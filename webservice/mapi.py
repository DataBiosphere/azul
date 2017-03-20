from flask import Flask, jsonify, request
import json
from flask_cors import CORS, cross_origin
from flask_migrate import Migrate
import flask_excel as excel
from flask.ext.elasticsearch import Elasticsearch
import ast
from decimal import Decimal
import copy

import os
from models import Billing,db
from utility import get_compute_costs, get_storage_costs, create_analysis_costs_json, create_storage_costs_json
import datetime
import calendar
import click
#TEST database call
from sqlalchemy import create_engine, MetaData, String, Table, Float, Column, select
import logging
logging.basicConfig()

# import json
class Config(object):
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")


apache_path = os.environ.get("APACHE_PATH", "")
es_service = os.environ.get("ES_SERVICE", "localhost")

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)
migrate = Migrate(app, db)
#es = Elasticsearch()
es = Elasticsearch(['http://'+es_service+':9200/'])
@app.route('/invoices')
@cross_origin()
def find_invoices():
    project = str(request.args.get('project'))
    if project:
        invoices = [invoice.to_json() for invoice in Billing.query.filter(Billing.project == project).order_by(
            Billing.end_date.desc()).all()]
        return jsonify(invoices)
    else:
        return None, 401

@app.route('/projects')
@cross_origin()
def get_projects():
    es_resp = es.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
        "projects":{
            "terms":{
                "field": "project.keyword",
                "size": 9999
            }
        }
    }}, size=0)

    projects = []
    for project in es_resp['aggregations']['projects']['buckets']:
        projects.append(project['key'])
    return jsonify(projects)




def parse_ES_response(es_dict, the_size, the_from, the_sort, the_order, key_search=False):
    protoDict = {'hits':[]}
    for hit in es_dict['hits']['hits']:
        if '_source' in hit:
            protoDict['hits'].append({
                'id' : hit['_source']['file_id'],
                'objectID' : hit['_source']['file_id'],
                'access' : hit['_source']['access'],
                'center_name': hit['_source']['center_name'],
                'study' : [hit['_source']['study']],
                'program': hit['_source']['program'], ###Added source
                'dataCategorization' : {
                    'dataType' : hit['_source']['file_type'],
                    'experimentalStrategy' : hit['_source']['experimentalStrategy']#['workflow']
                },
                'fileCopies' : [{
                    'repoDataBundleId' : hit['_source']['repoDataBundleId'],
                    'repoDataSetIds' :[],
                    'repoCode' : hit['_source']['repoCode'],
                    'repoOrg' : hit['_source']['repoOrg'],
                    'repoName' : hit['_source']['repoName'],
                    'repoType' : hit['_source']['repoType'],
                    'repoCountry' : hit['_source']['repoCountry'],
                    'repoBaseUrl' : hit['_source']['repoBaseUrl'],
                    'repoDataPath' : '', ###Empty String
                    'repoMetadatapath' : '', ###Empty String
                    'fileName' : hit['_source']['title'],
                    'fileFormat' : hit['_source']['file_type'],
                    'fileSize' : hit['_source']['fileSize'],
                    'fileMd5sum' : hit['_source']['fileMd5sum'],
                    'lastModified' : hit['_source']['lastModified']
                }],
                'donors' : [{
                    'donorId' : hit['_source']['donor'],
                    'primarySite' : hit['_source']['submitterDonorPrimarySite'],
                    'projectCode' : hit['_source']['project'],
                    'study' : hit['_source']['study'], ###
                    'sampleId' : [hit['_source']['sampleId']], ###
                    'specimenType' : [hit['_source']['specimen_type']],
                    'submittedDonorId' : hit['_source']['submittedDonorId'], ###
                    'submittedSampleId' : [hit['_source']['submittedSampleId']], ###
                    'submittedSpecimenId' : [hit['_source']['submittedSpecimenId']], ###
                    'otherIdentifiers' : {
                        'RedwoodDonorUUID' : [hit['_source']['redwoodDonorUUID']], ###
                    }

                }],

                'analysisMethod' : {
                    'analysisType' : hit['_source']['analysis_type'],
                    'software' : hit['_source']['software']+':'+hit['_source']['workflowVersion']###  #Concatenated the version for the software/workflow
                },
                'referenceGenome' : {
                    'genomeBuild' : '', ###Blank String
                    'referenceName' : '', ###Blank String
                    'downloadUrl' : ''###Blank String
                }
            })

        else:
            try:
                protoDict['hits'].append(hit['fields'])
            except:
                pass
    #If returning only one term based on file_id, break and return. 
    if key_search:
        return protoDict

    protoDict['pagination'] = {
        'count' : len(es_dict['hits']['hits']),#25,
        'total' : es_dict['hits']['total'],
        'size' : the_size,
        'from' : the_from+1,
        'page' : (the_from/(the_size))+1, #(the_from/(the_size+1))+1
        'pages' : -(-es_dict['hits']['total'] // the_size),
        'sort' : the_sort,
        'order' : the_order
    }

    protoDict['termFacets'] = {}#es_dict['aggregations']
    for x, y in es_dict['aggregations'].items():
        protoDict['termFacets'][x] = {'type':'terms', 'terms': map(lambda x:{"term":x["key"], 'count':x['doc_count']}, y['myTerms']['buckets'])} #Added myTerms key

    #Get the total for all the terms
    for section in protoDict['termFacets']:
        m_sum = 0
        #print section
        for term in protoDict['termFacets'][section]['terms']:
            m_sum += term['count']
        protoDict['termFacets'][section]['total'] = m_sum


    return protoDict



#This returns the agreggate terms and the list of hits from ElasticSearch
@app.route('/repository/files')
@app.route('/repository/files/')
@app.route('/repository/files/<file_id>')
@cross_origin()
def get_data(file_id=None):
    print "Getting data"
    #Get all the parameters from the URL
    m_field = request.args.get('field')
    m_filters = request.args.get('filters')
    m_From = request.args.get('from', 1, type=int)
    m_Size = request.args.get('size', 5, type=int)
    m_Sort = request.args.get('sort', 'center_name')
    m_Order = request.args.get('order', 'desc')
    m_Include = request.args.get('include', 'facets') #Need to work on this parameter

    #Didctionary for getting a reference to the aggs key
    #referenceAggs = {"centerName":"center_name", "projectCode":"project", "specimenType":"specimen_type", "fileFormat":"file_type", "workFlow":"workflow", "analysisType":"analysis_type", "program":"program"}
    #inverseAggs = {"center_name":"centerName", "project":"projectCode", "specimen_type":"specimenType", "file_type":"fileFormat", "workflow":"workFlow", "analysis_type":"analysisType", "program":"program"}
    #Dictionary for getting a reference to the aggs key
    idSearch = None #To use when searching a FileID using the regular endpoint structure i.e. file:{is:"x"}
    idDonorSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)


    #Will hold the query that will be used when calling ES
    mQuery = {}
    #Gets the index in [0 - (N-1)] form to communicate with ES
    m_From -= 1
    try:
        m_fields_List = [x.strip() for x in m_field.split(',')]
    except:
        m_fields_List = [] #Changed it from None to an empty list
    #Get a list of all the Filters requested
    try:
        m_filters = ast.literal_eval(m_filters)
        #Check if the string is in the other format. Change it as appropriate. #TESTING
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                corrected_term = referenceAggs[key]
                #print corrected_term
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key =="id":
                idSearch = m_filters['file'].pop(key)["is"]
            if key == "donorId":
                idDonorSearch = m_filters['file'].pop(key)["is"]
            #print m_filters

        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        mQuery = {"bool":{"must":filt_list}} #Removed the brackets; Make sure it doesn't break anything down the line
        mQuery2 = {"bool":{"must":filt_list}}

    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all":{}}
        mQuery2 = {}
        pass
    #The json with aggs to call ES
    aggs_list = {}
    with open(apache_path+'aggs.json') as my_aggs:
        #with open('aggs.json') as my_aggs:
        aggs_list = json.load(my_aggs)
    #Add the appropriate filters to the aggs_list
    if "match_all" not in mQuery:
        for key, value in aggs_list.items():
            aggs_list[key]['filter'] = copy.deepcopy(mQuery2)
            #print "Printing mQuery2", mQuery2
            #print "Printing filter field", aggs_list[key]['filter']
            for index, single_filter in enumerate(aggs_list[key]['filter']['bool']['must']):
                #print single_filter
                one_item_list = single_filter['constant_score']['filter']['terms'].items()
                if inverseAggs[one_item_list[0][0]] == key:
                    #print aggs_list[key]['filter']['bool']['must']
                    aggs_list[key]['filter']['bool']['must'].pop(index)
                    #In case there is no filter condition present
                    if len(aggs_list[key]['filter']['bool']['must']) == 0:
                        aggs_list[key]['filter'] = {}

                    #print aggs_list


                    # for agg_filter in mQuery['bool']['must']:
                    # 	#agg_filter['constant_score']['filter']['terms']
                    # 	for key, value in agg_filter['constant_score']['filter']['terms'].items():
                    # 		if inverseAggs[key] in aggs_list:
                    # 			aggs_list[inverseAggs[key]]['filter'] = mQuery
                    # 				for agg_little_filter in aggs_list[inverseAggs[key]]['filter']['bool']['must']:
                    # 					#agg_filter['constant_score']['filter']['terms']
                    # 					#Check if the field is the same as your aggregate.
                    # 					for key2, value2 in agg_little_filter['constant_score']['filter']['terms'].items():

                    # 					for key, value in agg_filter['constant_score']['filter']['terms'].items():



    #print "This is what get's into ES", {"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}

    if file_id:
        query_body = {"prefix":{"file_id":file_id}}
        body = {"query": query_body}
        mText = es.search(index='fb_alias', body=body, from_=0, size=5)
        result = parse_ES_response(mText, m_Size, m_From, m_Sort, m_Order, key_search=True)
        return jsonify(result['hits'][0])
    elif idSearch:
        mText = es.search(index='fb_alias', body={"query": {
                                            "constant_score":{"filter":{"terms":{"file_id":idSearch}}}}, 
                                            "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, 
                                            from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order)
    elif idDonorSearch:
        mText = es.search(index='fb_alias', body={"query": {
                                            "constant_score":{"filter":{"terms":{"donor":idDonorSearch}}}},
                                            "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List},
                                            from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order)

    else: 
        mText = es.search(index='fb_alias', body={"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order) #Changed "fields" to "_source"
    return jsonify(parse_ES_response(mText, m_Size, m_From, m_Sort, m_Order))


###********************************TEST FOR THE PIECHARTS FACETS ENDPOINT**********************************************##
@app.route('/repository/files/piecharts')
@cross_origin()
def get_data_pie():
    print "Getting data"
    #Get the filters from the URL
    m_filters = request.args.get('filters')
    #Just add these filters so it doesn't break the application for now.
    m_From = request.args.get('from', 1, type=int)
    m_Size = request.args.get('size', 5, type=int)
    m_Sort = request.args.get('sort', 'center_name')
    m_Order = request.args.get('order', 'desc')
    #Will hold the query that will be used when calling ES
    mQuery = {}
    #Gets the index in [0 - (N-1)] form to communicate with ES
    m_From -= 1

    #Dictionary for getting a reference to the aggs key
    idSearch = None
    idDonorSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    #Get a list of all the Filters requested
    try:
        m_filters = ast.literal_eval(m_filters)
        #Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                #This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key =="id":
                idSearch = m_filters['file'].pop(key)["is"]
            if key == "donorId":
                idDonorSearch = m_filters['file'].pop(key)["is"]
            #print m_filters

        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        mQuery = {"bool":{"must":filt_list}} #Removed the brackets; Make sure it doesn't break anything down the line
        mQuery2 = {"bool":{"must":filt_list}}

    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all":{}}
        mQuery2 = {}
        pass
    #Didctionary for getting a reference to the aggs key
    referenceAggs = {"centerName":"center_name", "projectCode":"project", "specimenType":"specimen_type", "fileFormat":"file_type", "workFlow":"workflow", "analysisType":"analysis_type", "program":"program"}
    inverseAggs = {"center_name":"centerName", "project":"projectCode", "specimen_type":"specimenType", "file_type":"fileFormat", "workflow":"workFlow", "analysis_type":"analysisType", "program":"program"}
    #The json with aggs to call ES
    aggs_list = {}
    with open(apache_path+'aggs.json') as my_aggs:
        #with open('aggs.json') as my_aggs:
        aggs_list = json.load(my_aggs)
    #Add the appropriate filters to the aggs_list
    if "match_all" not in mQuery:
        for key, value in aggs_list.items():
            aggs_list[key]['filter'] = copy.deepcopy(mQuery2)
        #Remove these lines below, since the numbering in the piecharts is exclusively what's present in the table result.
        #print "Printing mQuery2", mQuery2
        #print "Printing filter field", aggs_list[key]['filter']
        #for index, single_filter in enumerate(aggs_list[key]['filter']['bool']['must']):
        #print single_filter
        #one_item_list = single_filter['constant_score']['filter']['terms'].items()
        # if inverseAggs[one_item_list[0][0]] == key:
        # 	#print aggs_list[key]['filter']['bool']['must']
        # 	aggs_list[key]['filter']['bool']['must'].pop(index)
        # 	#In case there is no filter condition present
        # 	if len(aggs_list[key]['filter']['bool']['must']) == 0:
        # 		aggs_list[key]['filter'] = {}

        #print aggs_list


        # for agg_filter in mQuery['bool']['must']:
        # 	#agg_filter['constant_score']['filter']['terms']
        # 	for key, value in agg_filter['constant_score']['filter']['terms'].items():
        # 		if inverseAggs[key] in aggs_list:
        # 			aggs_list[inverseAggs[key]]['filter'] = mQuery
        # 				for agg_little_filter in aggs_list[inverseAggs[key]]['filter']['bool']['must']:
        # 					#agg_filter['constant_score']['filter']['terms']
        # 					#Check if the field is the same as your aggregate.
        # 					for key2, value2 in agg_little_filter['constant_score']['filter']['terms'].items():

        # 					for key, value in agg_filter['constant_score']['filter']['terms'].items():



    #print "This is what get's into ES", {"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}
    if idSearch:
        mText = es.search(index='fb_alias', body={"query": {
                                            "constant_score":{"filter":{"terms":{"file_id":idSearch}}}}, 
                                            "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, 
                                            from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order)
    elif idDonorSearch:
        mText = es.search(index='fb_alias', body={"query": {
                                            "constant_score":{"filter":{"terms":{"donor":idDonorSearch}}}},
                                            "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List},
                                            from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order)
    else:
        mText = es.search(index='fb_alias', body={"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list}, from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order) #Changed "fields" to "_source"
    return jsonify(parse_ES_response(mText, m_Size, m_From, m_Sort, m_Order))

##*********************************************************************************************************************************##








#Get the manifest. You need to pass on the filters
@app.route('/repository/files/exportNew')
@cross_origin()
def get_manifes_newt():

	m_filters = request.args.get('filters')
	m_Size = request.args.get('size', 25, type=int)
	mQuery = {}

	#Dictionary for getting a reference to the aggs key
	idSearch = None
	referenceAggs = {}
	inverseAggs = {}
	with open(apache_path+'reference_aggs.json') as my_aggs:
	#with open('reference_aggs.json') as my_aggs:
		referenceAggs = json.load(my_aggs)

	with open(apache_path+'inverse_aggs.json') as my_aggs:
	#with open('inverse_aggs.json') as my_aggs:
		inverseAggs = json.load(my_aggs)

	try:
		m_filters = ast.literal_eval(m_filters)
		#Change the keys to the appropriate values. 
		for key, value in m_filters['file'].items():
			if key in referenceAggs:
				#This performs the change.
				corrected_term = referenceAggs[key]
				m_filters['file'][corrected_term] = m_filters['file'].pop(key)
			if key == "fileId" or key =="id":
				idSearch = m_filters['file'].pop(key)["is"]

		#Functions for calling the appropriates query filters
		matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
                filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
                mQuery = {"bool":{"must":[filt_list]}}

	except Exception, e:
		print str(e)
		m_filters = None
		mQuery = {"match_all":{}}
		pass
	#Added the scroll variable. Need to put the scroll variable in a config file.
	scroll_config = '' 	
	with open(apache_path+'scroll_config') as _scroll_config:
	#with open('scroll_config') as _scroll_config:
		scroll_config = _scroll_config.readline().strip()
		#print scroll_config

	if idSearch:
		mText = es.search(index='fb_alias', body={"query": {
						"constant_score":{"filter":{"terms":{"file_id":idSearch}}}}, 
						"post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, 
						from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order)
		
	else:
		mText = es.search(index='fb_alias', body={"query": mQuery}, size=9999, scroll=scroll_config) #'2m'

	#Set the variables to do scrolling. This should fix the problem with the small amount of
	sid = mText['_scroll_id']
	scroll_size = mText['hits']['total']
	#reader = [x['_source'] for x in mText['hits']['hits']]

	#MAKE SURE YOU TEST THIS 
	while(scroll_size > 0):
		print "Scrolling..."
		page = es.scroll(scroll_id = sid, scroll = '2m')
		#Update the Scroll ID
		sid = page['_scroll_id']
		#Get the number of results that we returned in the last scroll
		scroll_size = len(page['hits']['hits'])
		#Extend the result list
		#reader.extend([x['_source'] for x in page['hits']['hits']])
		mText['hits']['hits'].extend([x for x in page['hits']['hits']])
		print len(mText['hits']['hits'])
		print "Scroll Size: " + str(scroll_size)	

	protoList = []
	for hit in mText['hits']['hits']:
		if '_source' in hit:
			protoList.append(hit['_source'])
			#protoList[-1]['_analysis_type'] = protoList[-1].pop('analysis_type')
			#protoList[-1]['_center_name'] = protoList[-1].pop('center_name')
			#protoList[-1]['_file_id'] = protoList[-1].pop('file_id')
	goodFormatList = []
	goodFormatList.append(['Program', 'Project', 'File ID','Center Name', 'Submitter Donor ID', 'Donor UUID', 'Submitter Specimen ID', 'Specimen UUID', 'Submitter Specimen Type', 'Submitter Experimental Design', 'Submitter Sample ID', 'Sample UUID', 'Analysis Type', 'Workflow Name', 'Workflow Version', 'File Type', 'File Path', 'Bundle UUID'])
	for row in protoList:
		currentRow = [row['program'], row['project'], row['file_id'], row['center_name'], row['submittedDonorId'], row['donor'], row['submittedSpecimenId'], row['specimenUUID'], row['specimen_type'], row['experimentalStrategy'], row['submittedSampleId'], row['sampleId'], row['analysis_type'], row['software'], row['workflowVersion'], row['file_type'], row['title'], row['repoDataBundleId']]
		goodFormatList.append(currentRow)
		#pass
	

	#print protoList
        #with open("manifest.tsv", "w") as manifest:
		#manifest.write("Program\tProject\tCenter Name\tSubmitter Donor ID\tDonor UUID\tSubmitter Specimen ID\tSpecimen UUID\tSubmitter Specimen Type\tSubmitter Experimental Design\tSubmitter Sample ID\tSample UUID\tAnalysis Type\tWorkflow Name\tWorkflow Version\tFile Type\tFile Path\n")
		#my_file = manifest

	return excel.make_response_from_array(goodFormatList, 'tsv', file_name='manifest')

	#return excel.make_response_from_records(protoList, 'tsv', file_name = 'manifest')


#This will return a summary of the facets
@app.route('/repository/files/facets')
@cross_origin()
def get_facets():

    #Get the order of the keys for the facet list
    f_order = []
    d_order = []
    with open(apache_path+'order_file') as file_order:
        f_order = file_order.readlines()
        f_order = [x.strip() for x in f_order]
    with open(apache_path+'order_donor') as donor_order:
        d_order = donor_order.readlines()
        d_order = [x.strip() for x in d_order]


    #Search the aggregates.
    #Parse them
    #Return it as a JSON output.
    #Things I need to know: The final format of the indexes stored in ES
    facets_list = {}
    with open(apache_path+'supported_facets.json') as my_facets:
        facets_list = json.load(my_facets)
        mText = es.search(index='fb_alias', body={"query": {"match_all":{}}, "aggs" : {
            "centerName" : {
                "terms" : { "field" : "center_name",
                            "min_doc_count" : 0,
                            "size" : 99999}
            },
            "projectCode":{
                "terms":{
                    "field" : "project",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "specimenType":{
                "terms":{
                    "field" : "specimen_type",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "fileFormat":{
                "terms":{
                    "field" : "file_type",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "workFlow":{
                "terms":{
                    "field" : "workflow",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "analysisType":{
                "terms":{
                    "field" : "analysis_type",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "study":{
                "terms":{
                    "field" : "study",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "experimental_design":{
                "terms":{
                    "field" : "experimentalStrategy",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "data_type":{
                "terms":{
                    "field" : "file_type",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "repository":{
                "terms":{
                    "field" : "repoName",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            },
            "access_type":{
                "terms":{
                    "field" : "access",
                    "min_doc_count" : 0,
                    "size" : 99999
                }
            }



        }})

        facets_list["DonorLevel"]['project']['values'] = [x['key'] for x in mText['aggregations']['projectCode']['buckets']]
        facets_list["DonorLevel"]['data_types_available']['values'] = [x['key'] for x in mText['aggregations']['fileFormat']['buckets']]
        facets_list["DonorLevel"]['specimen_type']['values'] = [x['key'] for x in mText['aggregations']['specimenType']['buckets']]
        facets_list["DonorLevel"]['study']['values'] = [x['key'] for x in mText['aggregations']['study']['buckets']]
        facets_list["DonorLevel"]['experimental_design']['values'] = [x['key'] for x in mText['aggregations']['experimental_design']['buckets']]

        facets_list["FileLevel"]['file_format']['values'] = [x['key'] for x in mText['aggregations']['fileFormat']['buckets']]
        facets_list["FileLevel"]['specimen_type']['values'] = [x['key'] for x in mText['aggregations']['specimenType']['buckets']]
        facets_list["FileLevel"]['workflow']['values'] = [x['key'] for x in mText['aggregations']['workFlow']['buckets']]
        facets_list["FileLevel"]['repository']['values'] = [x['key'] for x in mText['aggregations']['repository']['buckets']]
        facets_list["FileLevel"]['data_type']['values'] = [x['key'] for x in mText['aggregations']['data_type']['buckets']]
        facets_list["FileLevel"]['experimental_design']['values'] = [x['key'] for x in mText['aggregations']['experimental_design']['buckets']]
        facets_list["FileLevel"]['access_type']['values'] = [x['key'] for x in mText['aggregations']['access_type']['buckets']]


    array_facet_list = {'DonorLevel':[], 'FileLevel':[]}
    for x in f_order:
        array_facet_list['FileLevel'].append({x:facets_list["FileLevel"][x]})
    for x in d_order:
        array_facet_list['DonorLevel'].append({x:facets_list["DonorLevel"][x]})

    return jsonify(array_facet_list)
#return jsonify(facets_list)

#This will return a summary as the one from the ICGC endpoint
#Takes filters as parameter.
@app.route('/repository/files/summary')
@cross_origin()
def get_summary():
    my_summary = {"fileCount": None, "totalFileSize": None, "donorCount": None, "projectCount":None, "primarySiteCount":"DUMMY"}
    m_filters = request.args.get('filters')

    #Dictionary for getting a reference to the aggs key
    idSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    try:
        m_filters = ast.literal_eval(m_filters)
        #Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                #This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key =="id":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["donor"] = m_filters['file'].pop(key)
        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        mQuery = {"bool":{"must":[filt_list]}}
        #Add the mechanism to incorporate idSearch. See if it works!
        #if idSearch:
            #mQuery["constant_score"] = {"filter":{"terms":{"file_id":idSearch}}}
            #mQuery["bool"]["filter"] = {"terms":{"file_id":idSearch}}
    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all":{}}
        pass
    #Need to pass on the arguments for this.
    print mQuery
    mText = es.search(index='fb_alias', body={"query": mQuery, "aggs":{
        "centerName" : {
            "terms" : { "field" : "center_name",
                        #"min_doc_count" : 0,
                        "size" : 99999}
        },
        "projectCode":{
            "terms":{
                "field" : "project",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "specimenType":{
            "terms":{
                "field" : "specimen_type",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "fileFormat":{
            "terms":{
                "field" : "file_type",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "workFlow":{
            "terms":{
                "field" : "workflow",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "analysisType":{
            "terms":{
                "field" : "analysis_type",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "donor":{
            "terms":{
                "field" : "donor",
                #"min_doc_count" : 0,
                "size" : 99999
            }
        },
        "submitterDonorPrimarySite":{
            "terms":{
                "field": "submitterDonorPrimarySite",
                #"min_doc_count": 0,
                "size" : 99999
            }
        },
        "total_size":{
            "sum" : { "field" : "fileSize" }
        }
    }})

    print mText['aggregations']['donor']

    my_summary['fileCount'] = mText['hits']['total']
    my_summary['donorCount'] = len(mText['aggregations']['donor']['buckets'])
    my_summary['projectCount'] = len(mText['aggregations']['projectCode']['buckets'])
    my_summary['totalFileSize'] = mText['aggregations']['total_size']['value']
    my_summary['primarySiteCount'] = len(mText['aggregations']['submitterDonorPrimarySite']['buckets'])
    #To remove once this endpoint has some functionality
    return jsonify(my_summary)
#return "still working on this endpoint, updates soon!!"



###Methods for executing the search endpoint
#Searches keywords in the fb_alias index
def searchFile(_query, _filters, _from, _size):
    #Body of the query search
    query_body = {"prefix":{"file_id":_query}}   #{"query_string":{"query":_query, "default_field":"file_id", "analyzer":"my_analyzer"}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter":_filters}

    mResult = es.search(index='fb_alias', body=body, from_=_from, size=_size)

    #Now you have the brute results from the ES query. All you need to do now is to parse the data
    #and put it in a pretty dictionary, and return it.

    #This variable will hold the response to be returned
    searchResults = {"hits":[], "pagination":{}}

    for hit in mResult['hits']['hits']:
        if '_source' in hit:
            searchResults['hits'].append({
                "id": hit['_source']['file_id'],
                "type": "file",
                "donorId":[hit['_source']['redwoodDonorUUID']],
                "fileName":[hit['_source']['title']],
                "dataType": hit['_source']['file_type'],
                "projectCode":[hit['_source']['project']],
                #"fileObjectId": hit['_source']['file_type'], #Probabbly we don't have this
                "fileBundleId": hit['_source']['repoDataBundleId']
            })

    searchResults['pagination']['count'] = len(mResult['hits']['hits'])
    searchResults['pagination']['total'] = mResult['hits']['total']
    searchResults['pagination']['size'] = _size
    searchResults['pagination']['from'] = _from
    searchResults['pagination']['page'] = (_from/(_size))+1
    searchResults['pagination']['pages'] = -(-mResult['hits']['total'] // _size)
    searchResults['pagination']['sort'] = "_score" #Will alaways be sorted by score
    searchResults['pagination']['order'] = "desc" #Will always be descendent order

    return searchResults

def searchFilesDonors(_query, _filters, _from, _size):
    #Body of the query search
    query_body = {"prefix":{"donor_uuid":_query}} #{"query_string":{"query":_query}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter":_filters}

    mResult = es.search(index='analysis_index', body=body, from_=_from, size=_size)

    #Now you have the brute results from the ES query. All you need to do now is to parse the data
    #and put it in a pretty dictionary, and return it.
    searchResults = {"hits":[]}
    reader = [x['_source'] for x in mResult['hits']['hits']]
    for obj in reader:
        donorEntry = {}
        donor_id = obj['donor_uuid'] #This is the id
        donor_type = 'donor' #this is the type
        #Iterate through the specimens
        donorEntry['id'] = donor_id
        donorEntry['type'] = donor_type

        searchResults['hits'].append(donorEntry)

    return searchResults

#Searches keywords in the analysis_index
def searchDonors(_query, _filters, _from, _size):
    #Body of the query search
    query_body = {"prefix":{"donor_uuid":_query}} #{"query_string":{"query":_query}}
    if not bool(_filters):
        body = {"query": query_body}
    else:
        body = {"query": query_body, "post_filter":_filters}

    mResult = es.search(index='analysis_index', body=body, from_=_from, size=_size)

    #Now you have the brute results from the ES query. All you need to do now is to parse the data
    #and put it in a pretty dictionary, and return it.
    searchResults = {"hits":[], "pagination":{}}
    reader = [x['_source'] for x in mResult['hits']['hits']]
    for obj in reader:
        donorEntry = {}
        donor_id = obj['donor_uuid'] #This is the id
        donor_type = 'donor' #this is the type
        donor_submitteId = obj['submitter_donor_id'] #This is the submittedId
	donor_projectId = obj['project']
        #This are the scpecimen and sample lists.
        donor_specimenIds = []
        donor_submitteSpecimenIds = []
        donor_sampleIds = []
        donor_submitteSampleIds = []
        #Iterate through the specimens
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
    searchResults['pagination']['page'] = (_from/(_size))+1
    searchResults['pagination']['pages'] = -(-mResult['hits']['total'] // _size)
    searchResults['pagination']['sort'] = "_score" #Will alaways be sorted by score
    searchResults['pagination']['order'] = "desc" #Will always be descendent order

    return searchResults




#This will return a search list
#Takes filters as parameter.
@app.route('/keywords')
@cross_origin()
def get_search():
    #Get the parameters
    m_Query = request.args.get('q')
    m_filters = request.args.get('filters')
    m_From = request.args.get('from', 1, type=int)
    m_Size = request.args.get('size', 5, type=int)
    m_Type = request.args.get('type', 'file')
    #Won't implement this one just yet.
    m_Field = request.args.get('field', 'file')

    #References
    referenceAggs = {}
    inverseAggs = {}
    m_From -=1
    #Holder for the keyword result
    keywordResult = {}

    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)
    #Get the filters in an appropriate format
    try:
        m_filters = ast.literal_eval(m_filters)
        #Check if the string is in the other format. Change it as appropriate.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                corrected_term = referenceAggs[key]
                #print corrected_term
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            #Adding the filters from before, just in case the do keyword search combining donor and file ids
            if key == "fileId" or key =="id":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["donor"] = m_filters['file'].pop(key)

            #print m_filters

        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        filterQuery = {"bool":{"must":filt_list}} #Removed the brackets; Make sure it doesn't break anything down the line


    except Exception, e:
        print str(e)
        m_filters = None
        filterQuery = {}

    #If the query is empty
    if not m_Query:
        keywordResult = {'hits':[]}
    #return "Query is Empty. Change this to an empty array"
    #If the query is for files
    if m_Type == 'file':
        keywordResult = searchFile(m_Query, filterQuery, m_From, m_Size)

    #If the query is for donors
    elif m_Type == 'donor': #or m_Type == 'file-donor':
        keywordResult = searchDonors(m_Query, filterQuery, m_From, m_Size)
    elif m_Type == 'file-donor':
        keywordResult = searchFilesDonors(m_Query, filterQuery, m_From, m_Size)

    #Need to have two methods. One executes depending on whether the type is either 'file' or 'file-donor'

    return jsonify(keywordResult)
#return "Comming soon!"


#This will simply return the desired order of the facets
#Takes filters as parameter.
@app.route('/repository/files/order')
@cross_origin()
def get_order2():
    with open(apache_path+'order_config') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        #referenceAggs = json.load(my_aggs)
        order = [line.rstrip('\n') for line in my_aggs]
    return jsonify({'order': order })


@app.route('/repository/files/meta')
@cross_origin()
def get_order3():
    with open(apache_path+'f_donor') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        #referenceAggs = json.load(my_aggs)
        order_donor = [{'name':line.rstrip('\n'), 'category':'donor'} for line in my_aggs]
    #order = [line.rstrip('\n') for line in my_aggs]

    with open(apache_path+'f_file') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        #referenceAggs = json.load(my_aggs)
        order_file = [{'name':line.rstrip('\n'), 'category':'file'} for line in my_aggs]
    #order = [line.rstrip('\n') for line in my_aggs]

    order_final = order_file + order_donor

    return jsonify(order_final)


#Get the manifest. You need to pass on the filters
@app.route('/repository/files/exportOld')
@cross_origin()
def get_manifest_old():
    m_filters = request.args.get('filters')
    m_Size = request.args.get('size', 25, type=int)
    mQuery = {}

    #Dictionary for getting a reference to the aggs key
    idSearch = None 
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    try:
        m_filters = ast.literal_eval(m_filters)
        #Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                #This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key =="id":
                idSearch = m_filters['file'].pop(key)["is"]

        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        mQuery = {"bool":{"must":[filt_list]}}

    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all":{}}
        pass
    #Added the scroll variable. Need to put the scroll variable in a config file.
    scroll_config = ''
    with open(apache_path+'scroll_config') as _scroll_config:
        #with open('scroll_config') as _scroll_config:
        scroll_config = _scroll_config.readline().strip()
    #print scroll_config

    mText = es.search(index='fb_alias', body={"query": mQuery}, size=9999, scroll=scroll_config) #'2m'

    #Set the variables to do scrolling. This should fix the problem with the small amount of
    sid = mText['_scroll_id']
    scroll_size = mText['hits']['total']
    #reader = [x['_source'] for x in mText['hits']['hits']]

    #MAKE SURE YOU TEST THIS
    while(scroll_size > 0):
        print "Scrolling..."
        page = es.scroll(scroll_id = sid, scroll = '2m')
        #Update the Scroll ID
        sid = page['_scroll_id']
        #Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        #Extend the result list
        #reader.extend([x['_source'] for x in page['hits']['hits']])
        mText['hits']['hits'].extend([x for x in page['hits']['hits']])
        print len(mText['hits']['hits'])
        print "Scroll Size: " + str(scroll_size)

    protoList = []
    for hit in mText['hits']['hits']:
        if '_source' in hit:
            protoList.append(hit['_source'])
            protoList[-1]['_analysis_type'] = protoList[-1].pop('analysis_type')
            protoList[-1]['_center_name'] = protoList[-1].pop('center_name')
            protoList[-1]['_file_id'] = protoList[-1].pop('file_id')

    #print protoList
    return excel.make_response_from_records(protoList, 'tsv', file_name = 'manifest')


#Get the manifest. You need to pass on the filters
@app.route('/repository/files/export')
@cross_origin()
def get_manifest():
    m_filters = request.args.get('filters')
    m_Size = request.args.get('size', 25, type=int)
    mQuery = {}

    #Dictionary for getting a reference to the aggs key
    idSearch = None
    referenceAggs = {}
    inverseAggs = {}
    with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
        referenceAggs = json.load(my_aggs)

    with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
        inverseAggs = json.load(my_aggs)

    try:
        m_filters = ast.literal_eval(m_filters)
        #Change the keys to the appropriate values.
        for key, value in m_filters['file'].items():
            if key in referenceAggs:
                #This performs the change.
                corrected_term = referenceAggs[key]
                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
            if key == "fileId" or key =="id":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["file_id"] = m_filters['file'].pop(key)
            if key == "donorId":
                #idSearch = m_filters['file'].pop(key)["is"]
                m_filters['file']["donor"] = m_filters['file'].pop(key)

        #Functions for calling the appropriates query filters
        matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
        filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
        mQuery = {"bool":{"must":[filt_list]}}
        #if idSearch:
        #    mQuery["constant_score"] = {"filter":{"terms":{"file_id":idSearch}}}

    except Exception, e:
        print str(e)
        m_filters = None
        mQuery = {"match_all":{}}
        pass
    #Added the scroll variable. Need to put the scroll variable in a config file.
    scroll_config = ''
    with open(apache_path+'scroll_config') as _scroll_config:
        #with open('scroll_config') as _scroll_config:
        scroll_config = _scroll_config.readline().strip()
    #print scroll_config

    mText = es.search(index='fb_alias', body={"query": mQuery}, size=9999, scroll=scroll_config) #'2m'

    #Set the variables to do scrolling. This should fix the problem with the small amount of
    sid = mText['_scroll_id']
    scroll_size = mText['hits']['total']
    #reader = [x['_source'] for x in mText['hits']['hits']]

    #MAKE SURE YOU TEST THIS
    while(scroll_size > 0):
        print "Scrolling..."
        page = es.scroll(scroll_id = sid, scroll = '2m')
        #Update the Scroll ID
        sid = page['_scroll_id']
        #Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        #Extend the result list
        #reader.extend([x['_source'] for x in page['hits']['hits']])
        mText['hits']['hits'].extend([x for x in page['hits']['hits']])
        print len(mText['hits']['hits'])
        print "Scroll Size: " + str(scroll_size)

    protoList = []
    for hit in mText['hits']['hits']:
        if '_source' in hit:
            protoList.append(hit['_source'])
        #protoList[-1]['_analysis_type'] = protoList[-1].pop('analysis_type')
        #protoList[-1]['_center_name'] = protoList[-1].pop('center_name')
        #protoList[-1]['_file_id'] = protoList[-1].pop('file_id')
    goodFormatList = []
    goodFormatList.append(['Program', 'Project', 'File ID','Center Name', 'Submitter Donor ID', 'Donor UUID', 'Submitter Specimen ID', 'Specimen UUID', 'Submitter Specimen Type', 'Submitter Experimental Design', 'Submitter Sample ID'])
    for row in protoList:
        currentRow = [row['program'], row['project'], row['file_id'], row['center_name'], row['submittedDonorId'], row['donor'], row['submittedSpecimenId'], row['specimenUUID'], row['specimen_type'], row['experimentalStrategy'], row['submittedSampleId']]
        goodFormatList.append(currentRow)
    #pass


    #print protoList
    #with open("manifest.tsv", "w") as manifest:
    #manifest.write("Program\tProject\tCenter Name\tSubmitter Donor ID\tDonor UUID\tSubmitter Specimen ID\tSpecimen UUID\tSubmitter Specimen Type\tSubmitter Experimental Design\tSubmitter Sample ID\tSample UUID\tAnalysis Type\tWorkflow Name\tWorkflow Version\tFile Type\tFile Path\n")
    #my_file = manifest

    return excel.make_response_from_array(goodFormatList, 'tsv', file_name='manifest')
def get_projects_list():
    with app.app_context():
        es_resp = es.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
            "projects":{
                "terms":{
                    "field": "project.keyword",
                    "size": 9999
                }
            }
        }}, size=0)

        projects = []
        for project in es_resp['aggregations']['projects']['buckets']:
            projects.append(project['key'])
        return projects

def make_search_filter_query(timefrom, timetil, project):
    """

    :param timefrom: datetime object, filters all values less than this
    :param timetil: datetime object, filters all values greater than or equal to this
    :param project: string, this is the name of the particular project that we are trying to generate for
    :return:
    """
    with app.app_context():
        timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
        timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
        es_resp = es.search(index='billing_idx', body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "project.keyword": project
                            }
                        },
                        {
                            "nested": {
                                "path": "specimen.samples.analysis",
                                "score_mode": "max",
                                "query": {
                                    "range": {
                                        "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                            "gte": timestartstring,
                                            "lt": timeendstring,
                                            "format": "yyy-MM-dd'T'HH:mm:ss"
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "filtered_nested_timestamps": {
                    "nested": {
                        "path": "specimen.samples.analysis"
                    },
                    "aggs": {
                        "filtered_range": {
                            "filter": {
                                "range": {
                                    "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                        "gte": timestartstring,
                                        "lt": timeendstring,
                                        "format": "yyy-MM-dd'T'HH:mm:ss"
                                    }}
                            },
                            "aggs": {
                                "vmtype": {
                                    "terms": {
                                        "field": "specimen.samples.analysis.host_metrics.vm_instance_type.raw",
                                        "size": 9999
                                    },
                                    "aggs": {
                                        "regions": {
                                            "terms": {
                                                "field": "specimen.samples.analysis.host_metrics.vm_region.raw",
                                                "size": 9999
                                            },
                                            "aggs": {
                                                "totaltime": {
                                                    "sum": {
                                                        "field": "specimen.samples.analysis.timing_metrics.overall_walltime_seconds"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }, size=9999)

        return es_resp

def get_previous_file_sizes (timeend, project):
    timeendstring = timeend.strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "project.keyword": project
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "lt": timeendstring,
                            }
                        }
                    }
                ]

            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "sum_sizes": {
                        "sum": {
                            "field": "specimen.samples.analysis.workflow_outputs.file_size"
                        }
                    }
                }
            }
        }
    }, size=9999)
    return es_resp

def get_months_uploads(project, timefrom, timetil):
    with app.app_context():
        timestartstring = timefrom.strftime('%Y-%m-%dT%H:%M:%S')
        timeendstring = timetil.strftime('%Y-%m-%dT%H:%M:%S')
        es_resp = es.search(index='billing_idx', body =
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": timestartstring,
                                    "lt": timeendstring
                                }
                            }
                        },
                        {
                            "term": {
                                "project.keyword": project
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "filtered_nested_timestamps": {
                    "nested": {
                        "path": "specimen.samples.analysis"
                    },
                    "aggs": {
                        "times": {
                            "terms": {
                                "field": "specimen.samples.analysis.timestamp"
                            },
                            "aggs": {
                                "sum_sizes": {
                                    "sum": {
                                        "field": "specimen.samples.analysis.workflow_outputs.file_size"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }, size=9999)
        return es_resp

@click.command()
@click.option("--date", default="", type=str)
def generate_daily_reports(date):
    # Need to pass app context around because of how flask works
    # can take a single argument date as follows
    # flask generate_daily_reports --date 2017/01/31 will compute the billings for jan 2017, up to the 31st day of
    # January

    with app.app_context():
        try:
            timeend = datetime.datetime.strptime(date, '%Y/%m/%d')
        except:
            timeend = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


        # HANDLE CLOSING OUT BILLINGS at end of month
        if timeend.day == 1:
            projects = get_projects_list()
            for project in projects:
                bill = Billing.query.filter(Billing.end_data.month == (timeend.month-1) % 12) \
                    .filter(Billing.closed_out is False).filter(Billing.project == project).first()
                if bill:
                    bill.update(end_date=timeend, closed_out=True)


        monthstart = timeend.replace(day=1)
        projects = get_projects_list()
        seconds_into_month = (timeend-monthstart).total_seconds()
        daysinmonth = calendar.monthrange(timeend.year, timeend.month)[1]
        portion_of_month = Decimal(seconds_into_month)/Decimal(daysinmonth*3600*24)

        for project in projects:
            print(project)
            file_size = get_previous_file_sizes(monthstart, project=project)
            this_months_files = get_months_uploads(project, monthstart, timeend)
            compute_cost_search =  make_search_filter_query(monthstart,timeend,project)
            compute_costs = get_compute_costs(compute_cost_search)
            analysis_compute_json = create_analysis_costs_json(compute_cost_search['hits']['hits'], monthstart, timeend)

            all_proj_files = get_previous_file_sizes(timeend, project)['hits']['hits']
            analysis_storage_json = create_storage_costs_json(all_proj_files, monthstart, timeend, daysinmonth*3600*24)
            storage_costs = get_storage_costs( file_size, portion_of_month,
                                               this_months_files, timeend, daysinmonth*3600*24)

            bill = Billing.query.filter(Billing.project == project).filter(Billing.start_date == monthstart).first()
            itemized_costs = {
                "itemized_compute_costs": analysis_compute_json,
                "itemized_storage_costs": analysis_storage_json
            }
            if bill:
                bill.update(compute_cost=compute_costs, storage_cost=storage_costs, end_date=timeend,
                            cost_by_analysis=itemized_costs)
            else:
                Billing.create(compute_cost=compute_costs, storage_cost=storage_costs, start_date=monthstart, \
                               end_date=timeend, project=project, closed_out=False,
                               cost_by_analysis=itemized_costs)

app.cli.add_command(generate_daily_reports)

#Get the manifest. You need to pass on the filters
@app.route('/repository/files/exportFull')
@cross_origin()
def get_manifes_full():
        m_filters = request.args.get('filters')
        m_Size = request.args.get('size', 25, type=int)
        mQuery = {}

        #Dictionary for getting a reference to the aggs key
	idSearch = None
        referenceAggs = {}
        inverseAggs = {}
        with open(apache_path+'reference_aggs.json') as my_aggs:
        #with open('reference_aggs.json') as my_aggs:
                referenceAggs = json.load(my_aggs)

        with open(apache_path+'inverse_aggs.json') as my_aggs:
        #with open('inverse_aggs.json') as my_aggs:
                inverseAggs = json.load(my_aggs)

        try:
                m_filters = ast.literal_eval(m_filters)
                #Change the keys to the appropriate values. 
                for key, value in m_filters['file'].items():
                        if key in referenceAggs:
                                #This performs the change.
                                corrected_term = referenceAggs[key]
                                m_filters['file'][corrected_term] = m_filters['file'].pop(key)
			if key == "fileId" or key =="id":
				#idSearch = m_filters['file'].pop(key)["is"]
				m_filters['file']["file_id"] = m_filters['file'].pop(key)
			if key == "donorId":
				m_filters['file']["donor"] = m_filters['file'].pop(key)
                #Functions for calling the appropriates query filters
                matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
                filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
                mQuery = {"bool":{"must":[filt_list]}}
		#if idSearch:
			#mQuery["constant_score"] = {"filter":{"terms":{"file_id":idSearch}}}
			
        except Exception, e:
                print str(e)
                m_filters = None
                mQuery = {"match_all":{}}
                pass
        #Added the scroll variable. Need to put the scroll variable in a config file.
        scroll_config = ''
        with open(apache_path+'scroll_config') as _scroll_config:
        #with open('scroll_config') as _scroll_config:
                scroll_config = _scroll_config.readline().strip()
                #print scroll_config

        mText = es.search(index='fb_alias', body={"query": mQuery}, size=9999, scroll=scroll_config) #'2m'

        #Set the variables to do scrolling. This should fix the problem with the small amount of
        sid = mText['_scroll_id']
        scroll_size = mText['hits']['total']
        #reader = [x['_source'] for x in mText['hits']['hits']]

        #MAKE SURE YOU TEST THIS 
        while(scroll_size > 0):
                print "Scrolling..."
                page = es.scroll(scroll_id = sid, scroll = '2m')
                #Update the Scroll ID
                sid = page['_scroll_id']
                #Get the number of results that we returned in the last scroll
                scroll_size = len(page['hits']['hits'])
                #Extend the result list
                #reader.extend([x['_source'] for x in page['hits']['hits']])
                mText['hits']['hits'].extend([x for x in page['hits']['hits']])
                print len(mText['hits']['hits'])
                print "Scroll Size: " + str(scroll_size)

        protoList = []
        for hit in mText['hits']['hits']:
                if '_source' in hit:
                        protoList.append(hit['_source'])
                        #protoList[-1]['_analysis_type'] = protoList[-1].pop('analysis_type')
                        #protoList[-1]['_center_name'] = protoList[-1].pop('center_name')
                        #protoList[-1]['_file_id'] = protoList[-1].pop('file_id')
        goodFormatList = []
        goodFormatList.append(['Program', 'Project', 'Center Name', 'Submitter Donor ID', 'Donor UUID', "Submitter Donor Primary Site", 'Submitter Specimen ID', 'Specimen UUID', 'Submitter Specimen Type', 'Submitter Experimental Design', 'Submitter Sample ID', 'Sample UUID', 'Analysis Type', 'Workflow Name', 'Workflow Version', 'File Type', 'File Path', 'Upload File ID' ,'Data Bundle UUID', 'Metadata.json'])
        for row in protoList:
                currentRow = [row['program'], row['project'], row['center_name'], row['submittedDonorId'], row['donor'], row['submitterDonorPrimarySite'],row['submittedSpecimenId'], row['specimenUUID'], row['specimen_type'], row['experimentalStrategy'], row['submittedSampleId'], row['sampleId'], row['analysis_type'], row['software'], row['workflowVersion'], row['file_type'], row['title'], row['file_id'], row['repoDataBundleId'], row['metadataJson']]
                goodFormatList.append(currentRow)
                #pass


        #print protoList
        #with open("manifest.tsv", "w") as manifest:
                #manifest.write("Program\tProject\tCenter Name\tSubmitter Donor ID\tDonor UUID\tSubmitter Specimen ID\tSpecimen UUID\tSubmitter Specimen Type\tSubmitter Experimental Design\tSubmitter Sample ID\tSample UUID\tAnalysis Type\tWorkflow Name\tWorkflow Version\tFile Type\tFile Path\n")
                #my_file = manifest

        return excel.make_response_from_array(goodFormatList, 'tsv', file_name='manifest')

        #return excel.make_response_from_records(protoList, 'tsv', file_name = 'manifest')

#Get the manifest. You need to pass on the filters
@app.route('/action/service')
@cross_origin()
def get_action_service():
	db = create_engine('postgresql://{}:{}@db/monitor'.format(os.getenv("POSTGRES_USER"), os.getenv("POSTGRES_PASSWORD")), echo=False)
	conn = db.connect()
	metadata = MetaData(db)
	luigi = Table('luigi', metadata,
		Column("luigi_job", String(100), primary_key=True),
		Column("status", String(20)),
		Column("submitter_specimen_id", String(100)),
		Column("specimen_uuid", String(100)),
		Column("workflow_name", String(100)),
		Column("center_name", String(100)),
		Column("submitter_donor_id", String(100)),
		Column("consonance_job_uuid", String(100)),
		Column("submitter_donor_primary_site", String(100)),
		Column("project", String(100)),
		Column("analysis_type", String(100)),
		Column("program", String(100)),
		Column("donor_uuid", String(100)),
		Column("submitter_sample_id", String(100)),
		Column("submitter_experimental_design", String(100)),
		Column("submitter_specimen_type", String(100)),
		Column("workflow_version", String(100)),
		Column("sample_uuid", String(100)),
		Column("start_time", Float),
		Column("last_updated", Float)
	)
	select_query = select([luigi])
	select_result = conn.execute(select_query)
	result_list = [dict(row) for row in select_result]
	return jsonify(result_list)

#app.cli.add_command(generate_daily_reports)

if __name__ == '__main__':
  app.run() #Quit the debu and added Threaded
