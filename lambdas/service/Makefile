.PHONY: all
all: package

include ../../common.mk
include ../lambdas.mk

.PHONY: package
package: check_branch check_python check_aws config environ compile
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(project_root)/scripts/prepare_lambda_deployment.py service
