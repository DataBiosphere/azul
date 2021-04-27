.PHONY: all
all: package openapi

include ../../common.mk
include ../lambdas.mk

.PHONY: package
package: check_branch check_python check_aws config compile
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform
	python $(project_root)/scripts/prepare_lambda_deployment.py service

openapi:
	python -c "from app import app; import json; \
               print(json.dumps(app.spec(static=True), indent=4))" > openapi.json
