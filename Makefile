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

everything:
	$(MAKE) terraform
	$(MAKE) deploy
	$(MAKE) terraform  # for custom domain names
	$(MAKE) subscribe
	$(MAKE) reindex

clean:
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	$(MAKE) -C test/service testme

travis:
	$(MAKE) -C test/service travistest

.PHONY: all terraform deploy subscribe everything reindex clean test travis
