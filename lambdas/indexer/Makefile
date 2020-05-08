.PHONY: all
all: package

include ../../common.mk

.PHONY: config
config: .chalice/config.json

.PHONY: wheels
wheels: check_env
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp38-cp38-linux_x86_64.whl -d vendor

.PHONY: package
package: check_branch check_python check_aws config wheels
	python -m azul.changelog vendor
	# Ensure a .pyc file is present for every .py file. Set literals will
	# compile in a non-deterministic order unless PYTHONHASHSEED is set. For a
	# full explanation see http://benno.id.au/blog/2013/01/15/python-determinism
	PYTHONHASHSEED=0 python -m compileall -q --invalidation-mode checked-hash vendor vendor/azul
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(azul_home)/scripts/prepare_lambda_deployment.py indexer

.PHONY: local
local: check_python config
	chalice local

.PHONY: clean
clean: check_env
	git clean -Xdf
