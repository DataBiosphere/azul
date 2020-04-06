all: deploy

include ../../common.mk

config: .chalice/config.json

wheels:
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp36-cp36m-linux_x86_64.whl -d vendor

# TODO: Rename target: https://github.com/DataBiosphere/azul/issues/1654
deploy: config wheels
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(azul_home)/scripts/prepare_lambda_deployment.py indexer

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config deploy local clean
