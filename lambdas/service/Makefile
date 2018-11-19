include ../../common.mk

all: deploy

config: .chalice/config.json lambda-policy.json

state: config
	python $(azul_home)/scripts/manage_chalice_deployed.py service download

deploy: state vendors
	python -m azul.changelog vendor
	python $(azul_home)/scripts/manage_lambda_iam_role.py service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(azul_home)/scripts/manage_chalice_deployed.py service upload

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(azul_home)/scripts/manage_chalice_deployed.py service upload

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config state deploy delete local clean

vendors:
	unzip $(AZUL_HOME)/wheels/cffi-1.11.5-cp36-cp36m-manylinux1_x86_64.whl -d vendor
	unzip $(AZUL_HOME)/wheels/cryptography-2.4.2-cp34-abi3-manylinux1_x86_64.whl -d vendor
