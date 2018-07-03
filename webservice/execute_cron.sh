#!/bin/bash

set -o errexit
set -o nounset
#source $HOME/.profile

echo "Executing the instructions.sh script; stdout on log_instructiosn.txt"

./dcc-dashboard-service/instructions.sh --access public --repoBaseUrl storage.ucsc-cgl.org --repoCountry US --repoName Redwood-AWS-Oregon --repoOrg UCSC --repoType Redwood --access_token $REDWOOD_ACCESS_TOKEN --flaskApp $FLASK_APP> log_instructions.txt 2>&1
echo "Copying the validated.jsonl to dcc-dashboard"
cp `pwd`/dcc-metadata-indexer/validated.jsonl `pwd`/dcc-dashboard/
echo "Removing existing validated.jsonl.gz"
rm `pwd`/dcc-dashboard/validated.jsonl.gz
echo "Gzip validated.jsonl"
gzip `pwd`/dcc-dashboard/validated.jsonl
