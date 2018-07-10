include ../../common.mk

templates = .chalice/config.json .chalice/deployed.json lambda-policy.json

all: deploy

install:
	rsync -a ../../utils ../../project chalicelib/

deploy: $(templates) install
	PYTHONPATH=$(AZUL_HOME) python manage_iam_role.py $(AZUL_INDEXER_NAME) lambda-policy.json
	chalice deploy --stage $(AZUL_DEPLOYMENT_STAGE)

delete: $(templates)
	chalice delete
	test -e .chalice/deployed.json && rm .chalice/deployed.json || true

clean:
	git clean -Xdf

.PHONY: all deploy delete clean install
