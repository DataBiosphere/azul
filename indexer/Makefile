include config.env
export $(shell sed 's/=.*//' config.env)

PYTHON := $(shell command -v python3 2> /dev/null)

all: setup_chalice

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
	cd $(CHALICE_PROJECT) &&\
	../$(VIRTUALENV_NAME)/bin/chalice deploy