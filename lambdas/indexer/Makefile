include ../../common.mk

all: deploy

config: .chalice/config.json lambda-policy.json

state: config
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py indexer download

deploy: state
	python -m azul.changelog vendor
	python $(AZUL_HOME)/scripts/manage_lambda_iam_role.py indexer lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py indexer upload

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py indexer upload

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config state deploy delete local clean
