#!/usr/bin/env bash

# Populate the ElasticSearch index
echo "Updating fb_index"
curl -XDELETE $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_buffer/
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_buffer/ -d @test/fb_settings.json
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_buffer/_mapping/meta?update_all_types  -d @test/fb_mapping.json
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_buffer/_bulk?pretty --data-binary @test/fb_index.jsonl

# Change alias to point to fb_buffer
curl -XPOST $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_index", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_buffer", "alias" : "fb_alias" } } ] }'

# Index/Update the data in fb_index
curl -XDELETE $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_index/
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_index/ -d @test/fb_settings.json
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_index/_mapping/meta?update_all_types  -d @test/fb_mapping.json
curl -XPUT $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/fb_index/_bulk?pretty --data-binary @test/fb_index.jsonl

#Change alias one last time
curl -XPOST $ES_PROTOCOL://$ES_SERVICE:$ES_PORT/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_buffer", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_index", "alias" : "fb_alias" } } ] }'
