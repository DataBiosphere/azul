from elasticsearch import Elasticsearch
import json

server_info = [{'host':'localhost','port':9200}]
es = Elasticsearch(server_info)
print "Elasticsearch server at %s:%s exists?: %s\n" % \
		(server_info[0]['host'], str(server_info[0]['port']), str(es.ping()))

query_body = {
	'query': {
		'match': {
			'sample_id': 11
		}
	}
}

json_return = es.search(index='luigi_index', body=query_body)
# hits hits 0 source gets through arbitrary elasticsearch juju to 
# the returns from the Luigi Job
json_return = json_return["hits"]["hits"]

if not json_return:
	print "This is empty!"

#for key in json_return:
#	print key

# 
# elasticsearch_server is a json dictionary of the form  
# {
# 	'host': /*host here*/
# 	'port': /*port here*/
# }
# 
# jobject should be a json dictionary of with information
# from Luigi. It should have the following attributes:
# 	* status 
# 	* job_id
# 	* start_time 
# 	* project 
# 	* donor_id 
# 	* pipeline_name 
# 	* sample_id 
# 	* error_text
# 
# Returns 0 if successful. 1 if 
# 
def update_index(es_server, jobject):
	server_info = [].append(es_server)
	es = Elasticsearch(server_info)
	server_up = es.ping()

	if not server_up:
		print "Server's down, head for the hills."
		return 1

	# DEBUG: Is server up?
	# print "Elasticsearch server at %s:%s exists?: %s\n" % (es_server['host'], str(es_server['port']), )

	# Okay, now we have a jobject to add to the es_server
	query_body = {
		'query': {
			'match': {
				'job_id': jobject['job_id']
			}
		}
	}

	json_return = es.search(index='luigi_index', body=query_body)
	json_return = json_return["hits"]["hits"]

	if json_return:
		# The job exists! 
		 es.update(index='luigi_index', doc_type='job', id=jobject['job_id'], body={"doc": jobject})
	else:
		 # Job doesn't exist. Insert the jobject, go on your merry way
		 es.index(index='luigi_index', doc_type='job', id=jobject['job_id'], body=jobject)


