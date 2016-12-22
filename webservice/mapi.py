from flask import Flask, jsonify, request
#import the FlaskElasticsearch package
from flask.ext.elasticsearch import Elasticsearch
#import json
import ast, json
#import the cors tools
from flask_cors import CORS, cross_origin
#import the flask functionality
import flask_excel as excel

import copy

app = Flask(__name__)
es = Elasticsearch()

#Assumption: objectId is download_id
#Parses the elasticSearch response 
def parse_ES_response(es_dict, the_size, the_from, the_sort, the_order):
	protoDict = {'hits':[]}
	for hit in es_dict['hits']['hits']:
		if '_source' in hit:
			protoDict['hits'].append({
			'id' : 'DUMMY',
			'objectID' : hit['_source']['file_id'],
			'access' : 'DUMMY',
			'center_name': hit['_source']['center_name'],
			'study' : ['DUMMY'],
			'dataCategorization' : {
				'dataType' : hit['_source']['analysis_type'],
				'experimentalStrategy' : hit['_source']['workflow']
			},
			'fileCopies' : [{
				'repoDataBundleId' : 'DUMMY',
				'repoDataSetIds' :[],
				'repoCode' : 'DUMMY',
				'repoOrg' : 'DUMMY',
				'repoName' : 'DUMMY',
				'repoType' : 'DUMMY',
				'repoCountry' : 'DUMMY',
				'repoBaseUrl' : 'DUMMY',
				'repoDataPath' : 'DUMMY',
				'repoMetadatapath' : 'DUMMY',
				'indexFile' : {
					'id' : 'DUMMY',
					'objectId' : hit['_source']['download_id'],
					'fileName' : hit['_source']['title'],
					'fileFormat' : hit['_source']['file_type'],
					'fileMd5sum' : 'DUMMY',
					'fileSize' : 'DUMMY'
				},
				'fileName' : hit['_source']['title'],
				'fileFormat' : hit['_source']['file_type'],
				'fileMd5sum' : 'DUMMY',
				'lastModified' : 'DUMMY'
			}],
			'donors' : [{
				'donorId' : hit['_source']['donor'],
				'primarySite' : 'DUMMY',
				'projectCode' : hit['_source']['project'],
				'study' : 'DUMMY',
				'sampleId' : ['DUMMY'],
				'specimenType' : [hit['_source']['specimen_type']],
				'submittedDonorId' : "DUMMY",
				'submittedSampleId' : ['DUMMY'],
				'submittedSpecimenId' : ['DUMMY'],
				'otherIdentifiers' : {
					'tcgaSampleBarcode' : ['DUMMY'],
					'tcgaAliquotBarcode' : ['DUMMY']
				}

			}],

			'analysisMethod' : {
				'analysisType' : hit['_source']['analysis_type'],
				'software' : 'DUMMY'
			},
			'referenceGenome' : {
				'genomeBuild' : 'DUMMY',
				'referenceName' : 'DUMMY',
				'downloadUrl' : 'DUMMY'
			}
		})

		else:
			try:
				protoDict['hits'].append(hit['fields'])
			except:
				pass

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
@app.route('/files/')
@cross_origin()
def get_data():
	print "Getting data"
	#Get all the parameters from the URL
	m_field = request.args.get('field')
	m_filters = request.args.get('filters')
	m_From = request.args.get('from', 1, type=int)
	m_Size = request.args.get('size', 5, type=int)
	m_Sort = request.args.get('sort', 'center_name')
	m_Order = request.args.get('order', 'desc')

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
	with open('/var/www/html/dcc-dashboard-service/aggs.json') as my_aggs:
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
	mText = es.search(index='fb_alias', body={"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order) #Changed "fields" to "_source"
	return jsonify(parse_ES_response(mText, m_Size, m_From, m_Sort, m_Order))


###********************************TEST FOR THE PIECHARTS FACETS ENDPOINT**********************************************##
@app.route('/files/piecharts')
@cross_origin()
def get_data():
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
	#Get a list of all the Filters requested
	try:
		m_filters = ast.literal_eval(m_filters)
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
	with open('/var/www/html/dcc-dashboard-service/aggs.json') as my_aggs:
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
	mText = es.search(index='fb_alias', body={"query": {"match_all":{}}, "post_filter": mQuery2, "aggs" : aggs_list, "_source":m_fields_List}, from_=m_From, size=m_Size, sort=m_Sort+":"+m_Order) #Changed "fields" to "_source"
	return jsonify(parse_ES_response(mText, m_Size, m_From, m_Sort, m_Order))

##*********************************************************************************************************************************##








#Get the manifest. You need to pass on the filters
@app.route('/files/export')
@cross_origin()
def get_manifest():
	m_filters = request.args.get('filters')
	m_Size = request.args.get('size', 25, type=int)
	mQuery = {}
	try:
		m_filters = ast.literal_eval(m_filters)
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
	with open('/var/www/html/dcc-dashboard-service/scroll_config') as _scroll_config:
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


#This will return a summary of the facets
@app.route('/files/facets')
@cross_origin()
def get_facets():
	#Search the aggregates.
	#Parse them
	#Return it as a JSON output.
	#Things I need to know: The final format of the indexes stored in ES
	facets_list = {}
	with open('/var/www/html/dcc-dashboard-service/supported_facets.json') as my_facets:
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
        }


    }})
	
		facets_list["DonorLevel"]['project']['values'] = [x['key'] for x in mText['aggregations']['projectCode']['buckets']]
		facets_list["DonorLevel"]['data_types_available']['values'] = [x['key'] for x in mText['aggregations']['fileFormat']['buckets']]
		facets_list["DonorLevel"]['specimen_type']['values'] = [x['key'] for x in mText['aggregations']['specimenType']['buckets']]
		facets_list["FileLevel"]['file_format']['values'] = [x['key'] for x in mText['aggregations']['fileFormat']['buckets']]
		facets_list["FileLevel"]['specimen_type']['values'] = [x['key'] for x in mText['aggregations']['specimenType']['buckets']]
		facets_list["FileLevel"]['workflow']['values'] = [x['key'] for x in mText['aggregations']['workFlow']['buckets']]

	return jsonify(facets_list)

#This will return a summary as the one from the ICGC endpoint
#Takes filters as parameter. 
@app.route('/files/summary')
@cross_origin()
def get_summary():
	my_summary = {"fileCount": None, "totalFileSize": "DUMMY", "donorCount": None, "projectCount":None, "primarySite":"DUMMY"}
	m_filters = request.args.get('filters')
	
	try:
		m_filters = ast.literal_eval(m_filters)
		#Functions for calling the appropriates query filters
		matchValues = lambda x,y: {"filter":{"terms": {x:y['is']}}}
		filt_list = [{"constant_score": matchValues(x, y)} for x,y in m_filters['file'].items()]
		mQuery = {"bool":{"must":[filt_list]}}

	except Exception, e:
		print str(e)
		m_filters = None
		mQuery = {"match_all":{}}
		pass
	#Need to pass on the arguments for this. 
	mText = es.search(index='fb_alias', body={"query": mQuery, "aggs":{
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
        "donor":{
        	"terms":{
        		"field" : "donor",
        		"min_doc_count" : 0,
                "size" : 99999
        	}
        }
        }})

	
	
	my_summary['fileCount'] = mText['hits']['total'] 
	my_summary['donorCount'] = len(mText['aggregations']['donor']['buckets'])
	my_summary['projectCount'] = len(mText['aggregations']['projectCode']['buckets'])

	#To remove once this endpoint has some functionality
	return jsonify(my_summary)
	#return "still working on this endpoint, updates soon!!"
	
	

if __name__ == '__main__':
  app.run() #Quit the debu and added Threaded










