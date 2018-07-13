include common.mk

all:
	@echo Looking good!
	@echo '`make deploy`' deploys the AWS Lambda functions
	@echo '`make terraform`' creates the necessary cloud infrastructure that the Lambda functions depend on

deploy:
	$(MAKE) -C lambdas

clean:
	for d in lambdas; do $(MAKE) -C $$d clean; done

test:
	make -C test testme

travis:
	make -C test travistest

.PHONY: all deploy clean test travis
