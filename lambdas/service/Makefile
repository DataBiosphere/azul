include ../../common.mk

templates = .chalice/config.json .chalice/deployed.json lambda-policy.json

all: deploy

deploy: $(templates)
	python $(AZUL_HOME)/scripts/manage_lambda_iam_role.py service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)

delete: $(templates)
	chalice delete
	test -e .chalice/deployed.json && rm .chalice/deployed.json || true

local: $(templates)
	chalice local

clean:
	git clean -Xdf

.PHONY: all deploy delete clean
