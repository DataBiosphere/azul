all: deploy

include ../../common.mk

config: .chalice/config.json lambda-policy.json

state: check_branch config
	python $(azul_home)/scripts/manage_chalice_deployed.py service download

wheels:
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp36-cp36m-linux_x86_64.whl -d vendor

deploy: state wheels
	python -m azul.changelog vendor
	python $(azul_home)/scripts/manage_lambda_iam_role.py create service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(azul_home)/scripts/manage_chalice_deployed.py service upload

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(azul_home)/scripts/manage_lambda_iam_role.py delete service
	python $(azul_home)/scripts/manage_chalice_deployed.py service upload

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config state deploy delete local clean
