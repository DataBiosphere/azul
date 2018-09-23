include common.mk

all:
	@echo Looking good!
	@echo '`make deploy`' deploys the AWS Lambda functions
	@echo '`make terraform`' creates the necessary cloud infrastructure that the Lambda functions depend on

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas

subscribe:
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py --shared; fi

reindex:
	python scripts/reindex.py

delete_and_reindex:
	python scripts/reindex.py --delete

clean:
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

.PHONY: all terraform deploy subscribe everything reindex clean test travis
