include ../../common.mk

templates = .chalice/config.json .chalice/deployed.json lambda-policy.json

all: deploy

install:
	rsync -aL $(AZUL_HOME)/src/utils $(AZUL_HOME)/src/azul chalicelib/

deploy: $(templates) install
	PYTHONPATH=$(AZUL_HOME)/src python $(AZUL_HOME)/scripts/manage_lambda_iam_role.py service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)

delete: $(templates)
	chalice delete
	test -e .chalice/deployed.json && rm .chalice/deployed.json || true

clean:
	git clean -Xdf

.PHONY: all deploy delete clean install
