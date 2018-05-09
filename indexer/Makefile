#SET NEW_ELASTICSEARCH='N' if passing in an existing service or NEW_ELASTICSEARCH='Y' if you want it to be setup as well
SHELL=/bin/bash
include config/config.env
export $(shell sed 's/=.*//' config/config.env)

PYTHON := $(shell command -v python3 2> /dev/null)

all: setup_elasticsearch

setup_virtual_env:
	#create the virtualenv - NOTE: source cannot be activated within a makefile
	@if [$(PYTHON) == '']; then	sudo apt-get update && sudo apt-get install python3.6; fi
	virtualenv -p python3 $(VIRTUALENV_NAME)

configure_aws: setup_virtual_env
	#install AWS CLI within the virtualenv
	$(VIRTUALENV_NAME)/bin/pip install awscli --upgrade
	#aws configure cli does not support passing in access key ID and secret access key as parameters
	export AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID)
	export AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY)

setup_chalice: configure_aws
	#install chalice
	$(VIRTUALENV_NAME)/bin/pip install chalice
	#create a chalice project
	$(VIRTUALENV_NAME)/bin/chalice new-project $(CHALICE_PROJECT)
	#inital deploy of chalice project
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/chalice deploy --profile $(AWS_PROFILE) >output_log.txt
	#obtain the url of the newly deployed chalice lambda
	$(eval CHALICE_URL=$(shell grep -o 'https://[0-9a-zA-Z.-]*/api/' ${CHALICE_PROJECT}/output_log.txt))

update_chalice_with_default_files: 
	@if [ ! -d "${VIRTUALENV_NAME}" || ! -f "${CHALICE_PROJECT}/output_log.txt" ]; then $(MAKE) setup_chalice; fi
	#copy files from the repo and update chalice
	cd $(CHALICE_PROJECT) && rm app.py && rm requirements.txt
	cp -a chalicelib/. $(CHALICE_PROJECT)/
	cp app.py $(CHALICE_PROJECT)/
	cp requirements.txt $(CHALICE_PROJECT)/
	#install requirements
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/pip install -r requirements.txt

edit_env_variables: update_chalice_with_default_files
	#change the config file of chalice and replace with right values
	cd $(CHALICE_PROJECT) && rm .chalice/config.json && cp ../config/chalice/config.json .chalice/ && cd .chalice &&\
	rpl -e '<INDEXER_LAMBDA_APPLICATION_NAME>' '${CHALICE_PROJECT}' config.json && rpl -e '<INDEX_TO_USE>' '${ES_INDEX}' config.json &&\
	rpl -e '<AWS_ACCOUNT_ID>' '${AWS_ACCOUNT_ID}' config.json && rpl -e '<ELASTICSEARCH_ENDPOINT>' '${ES_ENDPOINT}' config.json &&\
	rpl -e '<BB_ENDPOINT>' '${BB_ENDPOINT}' config.json
	#update lambda's environment variables
	$(VIRTUALENV_NAME)/bin/aws lambda --profile $(AWS_PROFILE) update-function-configuration \
	--function-name "$(CHALICE_PROJECT)-dev" --environment "Variables={ES_ENDPOINT=$(ES_ENDPOINT),BLUE_BOX_ENDPOINT=$(BB_ENDPOINT),\
	ES_INDEX=$(ES_INDEX),INDEXER_NAME=$(CHALICE_PROJECT),HOME=/tmp}"

change_es_lambda_policy: edit_env_variables
	#edit elasticsearch and lambda policy

redeploy_chalice: change_es_lambda_policy
	#redeploy chalice
	$(VIRTUALENV_NAME)/bin/chalice deploy --no-autogen-policy
	#write generated URLs and config values to a file for easy reference
	echo "CALLBACK_URL=" $(CALLBACK_URL) "\n" "ES_ENDPOINT=" $(ES_ENDPOINT) > values_generated.txt

setup_elasticsearch:
	#if Elasticsearch endpoint supplied, use it. If not, setup a new elasticsearch service instance
	@if [$(ES_ENDPOINT) == '' ]; then \
		$(MAKE) new_elasticsearch; \
	else $(MAKE) redeploy_chalice; fi;\

new_elasticsearch: setup_chalice
	#create new elasticsearch service domain
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWS_PROFILE) create-elasticsearch-domain --domain-name $(ES_DOMAIN_NAME) \
	--elasticsearch-cluster-config  file://"config/elasticsearch-config.json" \
	--access-policies file://"config/elasticsearch-policy.json" \
	--ebs-options file://"config/ebs-config.json" 
	
	#obtain elasticsearch end-point - takes 10 minutes to setup on AWS. Check every 2 minutes
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWS_PROFILE) describe-elasticsearch-domain --domain-name $(ES_DOMAIN_NAME) |\
	jq '.DomainStatus.Endpoint' > eq_endpoint.txt
	while [ $(shell head -n 1 eq_endpoint.txt) == null ]; do \
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWS_PROFILE) describe-elasticsearch-domain --domain-name $(ES_DOMAIN_NAME) |\
	jq '.DomainStatus.Endpoint' > eq_endpoint.txt; \
	$(eval ES_ENDPOINT=$(shell head -n 1 eq_endpoint.txt)) \
	sleep 120 ; done;
	$(MAKE) redeploy_chalice

clean:
	#delete elasticsearch domain
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWS_PROFILE) delete-elasticsearch-domain --domain-name $(ES_DOMAIN_NAME)
	#delete chalice lambda
	#clean all local files
	rm -r $(CHALICE_PROJECT)
	rm -r $(VIRTUALENV_NAME)
	rm eq_endpoint.txt
	rm values_generated.txt
