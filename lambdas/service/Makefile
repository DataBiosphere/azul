include ../../common.mk

templates = .chalice/config.json .chalice/deployed/$(AZUL_DEPLOYMENT_STAGE).json lambda-policy.json

.chalice/deployed/$(AZUL_DEPLOYMENT_STAGE).json: .chalice/deployed.json.template.py .FORCE
	python $< $@

all: deploy

state: $(templates)
	test -e .chalice/deployed.json && rm .chalice/deployed.json || true
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service download

deploy: state
	python $(AZUL_HOME)/scripts/manage_lambda_iam_role.py service lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service upload

delete: state
	chalice delete --stage $(AZUL_DEPLOYMENT_STAGE)
	python $(AZUL_HOME)/scripts/manage_chalice_deployed.py service upload

local: $(templates)
	chalice local

clean:
	git clean -Xdf

.PHONY: all deploy delete clean state
