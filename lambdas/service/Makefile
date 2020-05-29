.PHONY: all
all: package

include ../../common.mk
include ../lambdas.mk

.PHONY: package
package: check_branch check_python check_aws config
	python -m azul.changelog vendor
	# Ensure a .pyc file is present for every .py file. Set literals will
	# compile in a non-deterministic order unless PYTHONHASHSEED is set. For a
	# full explanation see http://benno.id.au/blog/2013/01/15/python-determinism
	PYTHONHASHSEED=0 python -m compileall -q --invalidation-mode checked-hash vendor vendor/azul
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(project_root)/scripts/prepare_lambda_deployment.py service
