#SET NEW_ELASTICSEARCH='N' if passing in an existing service or NEW_ELASTICSEARCH='Y' if you want it to be setup as well
include config.env
export $(shell sed 's/=.*//' config/config.env)

PYTHON := $(shell command -v python3 2> /dev/null)

all: setup_elasticsearch

setup_virtual_env:
	@if [$(PYTHON) == '']; then	sudo apt-get update && sudo apt-get install python3.6; fi
	virtualenv -p python3 $(VIRTUALENV_NAME)
	#source $($(VIRTUALENV_NAME))/bin/activate - source cannot be activated within a makefile

configure_aws: setup_virtual_env
	$(VIRTUALENV_NAME)/bin/pip install awscli --upgrade
	#aws configure cli does not support passing in access key ID and secret access key as parameters
	export AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID)
	export AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY)
	#aws configure

setup_chalice: configure_aws
	$(VIRTUALENV_NAME)/bin/pip install chalice
	$(VIRTUALENV_NAME)/bin/chalice new-project $(CHALICE_PROJECT)
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/chalice deploy --profile hca >output_log.txt
	$(eval CHALICE_URL=$(shell grep -o 'https://[0-9a-zA-Z.-]*/api/' ${CHALICE_PROJECT}/output_log.txt))
	@echo $(CHALICE_URL) 

update_chalice_with_default_files: setup_chalice
	cd $(CHALICE_PROJECT) && rm app.py && rm requirements.txt
	cp -a chalicelib/. $(CHALICE_PROJECT)/
	cp app.py $(CHALICE_PROJECT)/
	cp requirements.txt $(CHALICE_PROJECT)/
	cd $(CHALICE_PROJECT) && ../$(VIRTUALENV_NAME)/bin/pip install -r requirements.txt

setup_elasticsearch:
	ifeq($(NEW_ELASTICSEARCH),'Y')
	$(MAKE) new_elasticsearch
	else
	@echo('Elasticsearch service instance supplied')
	$(MAKE) update_chalice_with_default_files

new_elasticsearch:
	aws es create-elasticsearch-domain --domain-name make-test-es-domain \
	--elasticsearch-cluster-config  file://"config/elasticsearch-config.json" \
	--access-policies file://"config/elasticsearch-policy.json" \
	--ebs-options file://"config/ebs-config.json"
