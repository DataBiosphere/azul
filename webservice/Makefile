# Sample usage
# make testme -- Run tests using chalice in localmode against an ES instance as defined in docker-compose.yml
#
# make travistest -- Run tests using chalice in localmode against an ES instance residing on the docker host, as defined in docker-compose-hostnetworking.yml

# make ES_DOMAIN_NAME=... ES_ENDPOINT=... ES_ARN=... ES_INDEX=... VIRTUALENV_NAME=... AWS_ACCOUNT_ID=... AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
# AWSPROFILE=... CHALICE_PROJECT=... NEW_ELASTICSEARCH='N'
# 	-- Deploy chalice to AWS, and use the elasticsearch configuration specified by the ES_* variables.  Pass NEW_ELASTICSEARCH='Y' if you want to create \
#      the ES instance.

SHELL=/bin/bash

PYTHON := $(shell command -v python2.7 2> /dev/null)
STAGE ?= dev
STAGE_SUFFIX = -dev

all: setup_elasticsearch

setup_virtual_env:
	#create the virtualenv - NOTE: source cannot be activated within a makefile
	@if [$(PYTHON) == '']; then	sudo apt-get update && sudo apt-get install python2.7; fi
	virtualenv -p python2.7 $(VIRTUALENV_NAME)

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
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/chalice deploy --profile $(AWSPROFILE) >output_log.txt
	#obtain the url of the newly deployed chalice lambda
	$(eval CHALICE_URL=$(shell grep -o 'https://[0-9a-zA-Z.-]*/api/' ${CHALICE_PROJECT}/output_log.txt))

update_chalice_with_default_files:
	# update chalice with default files
	@if [ ! -d "${VIRTUALENV_NAME}" -o ! -f "${CHALICE_PROJECT}/output_log.txt" ]; then $(MAKE) setup_chalice; fi
	#copy files from the repo and update chalice
	cd $(CHALICE_PROJECT) && rm app.py && rm requirements.txt
	cp -a chalicelib $(CHALICE_PROJECT)/
	cp app.py $(CHALICE_PROJECT)/
	cp requirements.txt $(CHALICE_PROJECT)/
	#install requirements
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/pip install -r requirements.txt

edit_env_variables: update_chalice_with_default_files
	#change the config file of chalice and replace with right values
	$(eval ES_ENDPOINT = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.Endpoint"))
	cd $(CHALICE_PROJECT) && rm .chalice/config.json && cp ../config/chalice/config.json .chalice/ && cd .chalice &&\
	rpl '<DASHBOARD_LAMBDA_APPLICATION_NAME>' '${CHALICE_PROJECT}' config.json && rpl '<INDEX_TO_USE>' '${ES_INDEX}' config.json &&\
	rpl '<AWS_ACCOUNT_ID>' '${AWS_ACCOUNT_ID}' config.json && rpl '<ELASTICSEARCH_ENDPOINT>' '${ES_ENDPOINT}' config.json 
	#update lambda's environment variables
	$(VIRTUALENV_NAME)/bin/aws lambda --profile $(AWSPROFILE) update-function-configuration \
	--function-name "$(CHALICE_PROJECT)$(STAGE_SUFFIX)" --environment "Variables={ES_ENDPOINT=$(ES_ENDPOINT),\
	ES_INDEX=$(ES_INDEX),DASHBOARD_NAME=$(CHALICE_PROJECT)$(STAGE_SUFFIX),HOME=/tmp}"

change_es_lambda_policy: edit_env_variables
	#edit elasticsearch and lambda policy
	$(eval ES_ARN = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.ARN"))
	rpl '<ELASTICSEARCH_ARN>' '${ES_ARN}' config/lambda-policy.json
	$(eval POLICY_NAME = $(shell ${VIRTUALENV_NAME}/bin/aws iam --profile ${AWSPROFILE} list-role-policies --role-name "${CHALICE_PROJECT}$(STAGE_SUFFIX)" | jq ".PolicyNames[0]"))
	$(VIRTUALENV_NAME)/bin/aws iam --profile $(AWSPROFILE) put-role-policy --role-name "$(CHALICE_PROJECT)$(STAGE_SUFFIX)" --policy-name "$(POLICY_NAME)" --policy-document file://"config/lambda-policy.json"

redeploy_chalice: change_es_lambda_policy
	#redeploy chalice
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/chalice deploy --no-autogen-policy --profile $(AWSPROFILE)
	#write generated URLs and config values to a file for easy reference
	$(eval ES_ENDPOINT = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.Endpoint"))
	$(eval ES_ARN = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.ARN"))
	$(eval CHALICE_URL=$(shell grep -o 'https://[0-9a-zA-Z.-]*/api/' ${CHALICE_PROJECT}/output_log.txt))
	echo "CALLBACK_URL="$(CHALICE_URL) "\n" "ES_ENDPOINT="$(ES_ENDPOINT) "\n" \
	"ES_INDEX="$(ES_INDEX) "\n" "DASHBOARD_NAME="$(CHALICE_PROJECT)$(STAGE_SUFFIX) "\n" "HOME=/tmp" > values_generated.txt

setup_elasticsearch:
	#Make sure all required variables are set.
	ifndef ES_DOMAIN_NAME
	$(error ES_DOMAIN_NAME is undefined);
	endif

	ifndef ES_ENDPOINT
	$(error ES_ENDPOINT is undefined);
	endif

	ifndef ES_ARN
	$(error ES_ARN is undefined);
	endif

	ifndef ES_INDEX
	$(error ES_INDEX is undefined);
	endif

	ifndef VIRTUALENV_NAME
	$(error VIRTUALENV_NAME is undefined);
	endif

	ifndef AWS_ACCOUNT_ID
	$(error AWS_ACCOUNT_ID is undefined);
	endif

	ifndef AWS_ACCESS_KEY_ID
	$(error AWS_ACCESS_KEY_ID is undefined);
	endif

	ifndef AWS_SECRET_ACCESS_KEY
	$(error AWS_SECRET_ACCESS_KEY is undefined);
	endif

	ifndef AWSPROFILE
	$(error AWSPROFILE is undefined);
	endif

	ifndef CHALICE_PROJECT
	$(error CHALICE_PROJECT is undefined);
	endif
	#if Elasticsearch endpoint supplied, use it. If not, setup a new elasticsearch service instance
	@if [ -z "$(ES_ENDPOINT)" ]; then \
		$(MAKE) new_elasticsearch; \
	else $(MAKE) redeploy_chalice; fi;

new_elasticsearch: setup_chalice
	#create new elasticsearch service domain
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWSPROFILE) create-elasticsearch-domain --domain-name "$(ES_DOMAIN_NAME)" \
	--elasticsearch-cluster-config  file://"config/elasticsearch-config.json" \
	--access-policies file://"config/elasticsearch-policy.json" \
	--ebs-options file://"config/ebs-config.json" \
	--elasticsearch-version "5.5"
	#pause to give AWS extra seconds to get it configured
	sleep 60
	#obtain elasticsearch end-point - takes 10 minutes to setup on AWS. Check every 2 minutes
	./poll_elasticsearch.sh "$(VIRTUALENV_NAME)" "$(AWSPROFILE)" "$(ES_DOMAIN_NAME)"
	$(eval ES_ENDPOINT = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.Endpoint"))
	$(eval ES_ARN = $(shell ${VIRTUALENV_NAME}/bin/aws es --profile ${AWSPROFILE} describe-elasticsearch-domain --domain-name \
	$(ES_DOMAIN_NAME) | jq ".DomainStatus.ARN"))
	$(MAKE) redeploy_chalice

run-travis:
	# Start chalice in localmode with host-mode networking
	docker-compose -f docker-compose-hostnetworking.yml up -d --build --force-recreate

populate:
	docker-compose exec dcc-dashboard-service /app/test/data_generator/make_fake_data.sh

reset:
	docker-compose stop
	docker-compose rm -f

stop:
	docker-compose down --rmi 'all'

travistest: stop reset run-travis populate
	# Run tests locally, against an already-existing ES instance located
	# on the docker host and listening on 127.0.0.1.  Test data will be 
	# generated and loaded into the db. (ES connection configured in 
	# docker-compose-hostnetworking.yml)
	echo "Sleeping 60 seconds before unit testing"
	sleep 60
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

run:
	# Run a chalice instance locally, in local mode, with bridge mode networking
	docker-compose up -d --build --force-recreate

testme: stop reset run
	# Run tests locally, against an already-existing ES instance populated with data, as
	# set in the ES_DOMAIN variable of docker-compose.yml.  (e.g. this could be at AWS)
	echo "Sleeping 30 seconds before populating ES"
	sleep 30
	$(MAKE) populate
	echo "Sleeping 60 seconds before unit testing"
	sleep 60
	docker-compose exec dcc-dashboard-service py.test -p no:cacheprovider -s -x

clean_elastic:
	#delete elasticsearch domain
	$(VIRTUALENV_NAME)/bin/aws es --profile $(AWSPROFILE) delete-elasticsearch-domain --domain-name $(ES_DOMAIN_NAME)

clean:	
	#delete chalice lambda
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/chalice delete 
	#clean all local files
	rm -r $(CHALICE_PROJECT)
	rm -r $(VIRTUALENV_NAME)
	rm eq_endpoint.txt
	rm values_generated.txt
