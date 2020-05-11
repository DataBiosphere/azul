.PHONY: all
all: package

include ../../common.mk

.PHONY: config
config: .chalice/config.json

.PHONY: wheels
wheels: check_env
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp36-cp36m-linux_x86_64.whl -d vendor

.PHONY: package
package: check_branch check_python check_aws config wheels
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(azul_home)/scripts/prepare_lambda_deployment.py service

.PHONY: local
local: check_python config
	chalice local

.PHONY: clean
clean: check_env
	git clean -Xdf
