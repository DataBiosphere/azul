from elasticsearch import Elasticsearch
import json

server_info = [{'host':'localhost','port':9200}]
es = Elasticsearch(server_info)
print "Elasticsearch server at %s:%s exists?: %s\n" % \
		(server_info[0]['host'], str(server_info[0]['port']), str(es.ping()))

query_body = {
	'query': {
		'match': {
			'sample_id': 5
		}
	}
}

json_return = es.search(index='luigi_index', body=query_body)
# hits hits 0 source gets through arbitrary elasticsearch juju to 
# the returns from the Luigi Job
json_return = json_return["hits"]["hits"][0]["_source"]
string_of_return = json.dumps(json_return)

print string_of_return

for key in json_return:
	print key

# 
# elasticsearch_server is a json dictionary of the form  
# {
# 	'host':
# 	'port': 
# }
# 
# jobject should be a json dictionary of with information
# from Luigi. It should have the following attributes:
# 	* status 
# 	* name 
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
		return 1

	
	# DEBUG: Is server up?
	# print "Elasticsearch server at %s:%s exists?: %s\n" % (es_server['host'], str(es_server['port']), )









