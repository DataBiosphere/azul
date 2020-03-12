all: deploy

include ../../common.mk

config: .chalice/config.json lambda-policy.json

state: check_branch config
	python $(azul_home)/scripts/manage_chalice_deployed.py indexer download

wheels:
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp36-cp36m-linux_x86_64.whl -d vendor

# TODO: Rename me
deploy: state wheels
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform $(azul_home)/terraform/indexer
	python $(azul_home)/scripts/transform_chalice_tf.py $(azul_home)/terraform/indexer/chalice.tf.json $(azul_home)/terraform/indexer/chalice.tf.json

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(azul_home)/scripts/manage_lambda_iam_role.py delete indexer
	python $(azul_home)/scripts/manage_chalice_deployed.py indexer upload

local: config
	chalice local

clean:
	git clean -Xdf

.PHONY: all config state deploy delete local clean
