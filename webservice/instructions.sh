#!/bin/bash
#now=$(date +"%T")
#Add shell script code

set -o errexit

access_token(){
    access_token=$1
}
access(){
    access=$1
}
repoBaseUrl(){
    repoBaseUrl=$1
}
repoCode(){
    repoCode=$1
}
repoCountry(){
    repoCountry=$1
}
repoName(){
    repoName=$1
}
repoOrg(){
    repoOrg=$1
}
repoType(){
    repoType=$1
}
flaskApp(){
    flaskApp=$1
}
helpmenu(){
    echo "--help | -h for help 
--access_token | -t for the access token 
--access | -a for the access value 
--repoBaseUrl | -u for the repo base url
--repoCode | -c for the repo code
--repoCountry | -u for the repo country
--repoName | -n for the repo name
--repoOrg | -o for the repo org
--repoType | -y for the repo type
"
}
ARGS=()
empty_arg(){
    if [[ -z "$1" ]]
    then
        echo "Missing value. Please enter a non-empty value"
        exit
    fi
    ARGS+=($2 $1)
    #echo ${ARGS[@]}
}
while [ ! $# -eq 0 ]
do
    case "$1" in
        --help | -h)
            helpmenu
            exit
            ;;
        --access_token | -t)
            # empty_arg $2 $1
            access_token $2

            ;;
        --access | -a)
            empty_arg $2 $1
            access $2
            ;;
        --repoBaseUrl | -u)
            empty_arg $2 $1
            repoBaseUrl $2
            ;;
        --repoCode | -c)
            empty_arg $2 $1
            repoCode $2
            ;;
        --repoCountry | -u)
            empty_arg $2 $1
            repoCountry $2
            ;;
        --repoName | -n)
            empty_arg $2 $1
            repoName $2
            ;;
        --repoOrg | -o)
            empty_arg $2 $1
            repoOrg $2
            ;;
        --repoType | -y)
            empty_arg $2 $1
            repoType $2
            ;;
        --flaskApp | -f)
            #empty_arg $2 $1
            if [[ -z "$2" ]]
                 then
                      echo "Missing value. Please enter a non-empty value"
                      exit
            fi
            flaskApp $2
            ;;
    esac
    shift
done


#Go into the appropriate folder
cd dcc-metadata-indexer
#Activate the virtualenv
#source metadaindex/bin/activate
source env/bin/activate
#Download new data from Redwood; create ES .jsonl file

echo "Starting the metadata_indexer.py; Stdout on log_metadata_indexer.txt"
python metadata_indexer.py --skip-program TEST --skip-project TEST  --storage-access-token $access_token  --client-path ../redwood-client/ucsc-storage-client/ --metadata-schema metadata_schema.json --server-host storage.ucsc-cgl.org --skip-uuid-directory redacted > log_metadata_indexer.txt

deactivate
####Index the data in analysis_index. NOTE: Should check first that the schema will match.
#curl -XDELETE http://localhost:9200/analysis_index
#curl -XPUT http://localhost:9200/analysis_index/_bulk?pretty --data-binary @elasticsearch.jsonl
echo "Updating analysis_index"
curl -XDELETE http://localhost:9200/analysis_buffer/
curl -XPUT http://localhost:9200/analysis_buffer/ -d @analysis_settings.json
curl -XPUT http://localhost:9200/analysis_buffer/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/analysis_buffer/_bulk?pretty --data-binary @elasticsearch.jsonl

#####Change buffer to point to alias
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_real", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_buffer", "alias" : "analysis_index" } } ] }'

##Update real index analysis
curl -XDELETE http://localhost:9200/analysis_real/
curl -XPUT http://localhost:9200/analysis_real/ -d @analysis_settings.json
curl -XPUT http://localhost:9200/analysis_real/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/analysis_real/_bulk?pretty --data-binary @elasticsearch.jsonl

#Change alias one last time from buffer to real
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "analysis_buffer", "alias" : "analysis_index" } }, { "add" : { "index" : "analysis_real", "alias" : "analysis_index" } } ] }'

#Run the python script
cd ../dcc-dashboard-service
. env/bin/activate
echo "Starting es_filebrowser_index.py; Stdout on log_es_fb_index.txt"
python2.7 es_filebrowser_index.py ${ARGS[@]} > log_es_fb_index.txt
deactivate
###Store data in fb_buffer.
#Delete and Create the fb_buffer, storing the mapping in it as well. 
echo "Updating fb_index"
curl -XDELETE http://localhost:9200/fb_buffer/
curl -XPUT http://localhost:9200/fb_buffer/ -d @fb_settings.json
curl -XPUT http://localhost:9200/fb_buffer/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/fb_buffer/_bulk?pretty --data-binary @fb_index.jsonl

###Change alias to point to fb_buffer
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_index", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_buffer", "alias" : "fb_alias" } } ] }'

####Index/Update the data in fb_index
curl -XDELETE http://localhost:9200/fb_index/
curl -XPUT http://localhost:9200/fb_index/ -d @fb_settings.json
curl -XPUT http://localhost:9200/fb_index/_mapping/meta?update_all_types  -d @mapping.json
curl -XPUT http://localhost:9200/fb_index/_bulk?pretty --data-binary @fb_index.jsonl

#Change alias one last time
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "fb_buffer", "alias" : "fb_alias" } }, { "add" : { "index" : "fb_index", "alias" : "fb_alias" } } ] }'

####MISSING THE INDEXING ON ELASTICSEARCH####

#touch myTest/$now.txt

###NOW HANDLE CREATING THE NEW BILLINGINDEX###

cd $HOME/dcc-metadata-indexer
#Activate the virtualenv
#source metadaindex/bin/activate
. env/bin/activate
#Download new data from Redwood; create ES .jsonl file
python metadata_indexer.py -preserve-version --skip-program TEST --skip-project TEST  --storage-access-token $access_token --client-path ../redwood-client/ucsc-storage-client/ --metadata-schema metadata_schema.json --server-host storage.ucsc-cgl.org

deactivate

#Populate the billing index
#Change the Buffer index
echo "Updating billing_idx"
curl -XDELETE http://localhost:9200/billing_buffer/
curl -XPUT http://localhost:9200/billing_buffer/
curl -XPUT http://localhost:9200/billing_buffer/_mapping/meta?update_all_types  -d @$HOME/dcc-dashboard-service/billing_mapping.json
curl -XPUT http://localhost:9200/billing_buffer/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change aliases
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_real", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_buffer", "alias" : "billing_idx" } } ] }'

#Update real billing_index

curl -XDELETE http://localhost:9200/billing_real/
curl -XPUT http://localhost:9200/billing_real/
curl -XPUT http://localhost:9200/billing_real/_mapping/meta?update_all_types  -d @$HOME/dcc-dashboard-service/billing_mapping.json
curl -XPUT http://localhost:9200/billing_real/_bulk?pretty --data-binary @duped_elasticsearch.jsonl

#Change the alias again, so that billing_idx points again to the real billing_real index
curl -XPOST http://localhost:9200/_aliases?pretty -d' { "actions" : [ { "remove" : { "index" : "billing_buffer", "alias" : "billing_idx" } }, { "add" : { "index" : "billing_real", "alias" : "billing_idx" } } ] }'



cd $HOME/dcc-dashboard-service
#Activate the virtual environment
. env/bin/activate

# now run the command, need to have FLASK_APP env var set to app.py
export FLASK_APP=$flaskApp
flask generate_daily_reports

