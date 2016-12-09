#!/bin/bash
#now=$(date +"%T")
#Go into the appropriate folder
cd dcc-metadata-indexer
#Activate the virtualenv
source metadaindex/bin/activate
#Download new data from Redwood; create ES .jsonl file
python metadata_indexer.py  --storage-access-token 5f1017f0-d7e9-41c9-b9c4-b013b2ea3015  --client-path ../redwood-client/ucsc-storage-client/ --metadata-schema metadata_schema.json --server-host storage.ucsc-cgl.org > testfile.txt
deactivate
####Index the data in analysis_index. NOTE: Should check first that the schema will match.
curl -XDELETE http://localhost:9200/analysis_index
curl -XPUT http://localhost:9200/analysis_index/_bulk?pretty --data-binary @elasticsearch.jsonl
#Run the python script
cd ../dcc-dashboard-service
. env/bin/activate
python2.7 es_filebrowser_index.py
deactivate
###Store data in fb_buffer.
#Delete and Create the fb_buffer, storing the mapping in it as well. 
curl -XDELETE http://localhost:9200/fb_buffer/
curl -XPUT http://localhost:9200/fb_buffer/
curl -XPUT http://localhost:9200/fb_buffer/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/fb_buffer/_bulk?pretty --data-binary @fb_index.jsonl

###Change alias to point to fb_buffer
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_index", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_buffer", "alias" : "fb_alias" } } ] }'

####Index/Update the data in fb_index
curl -XDELETE http://localhost:9200/fb_index/
curl -XPUT http://localhost:9200/fb_index/
curl -XPUT http://localhost:9200/fb_index/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/fb_index/_bulk?pretty --data-binary @fb_index.jsonl

#Change alias one last time
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_buffer", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_index", "alias" : "fb_alias" } } ] }'

####MISSING THE INDEXING ON ELASTICSEARCH####

#touch myTest/$now.txt


