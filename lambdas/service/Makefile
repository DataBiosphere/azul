include ../../common.mk

all: deploy

config: .chalice/config.json lambda-policy.json

state: config
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service download

deploy: state
	python -m azul.changelog vendor
	python $(AZUL_HOME)/scripts/manage_lambda_iam_role.py service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service upload

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service upload

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config state deploy delete local clean
