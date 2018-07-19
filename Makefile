include common.mk

all:
	@echo Looking good!
	@echo '`make deploy`' deploys the AWS Lambda functions
	@echo '`make terraform`' creates the necessary cloud infrastructure that the Lambda functions depend on

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py --shared; fi

clean:
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	make -C test/service testme

travis:
	make -C test/service travistest

.PHONY: all terraform deploy clean test travis
