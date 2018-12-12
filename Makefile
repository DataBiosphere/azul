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
	python scripts/reindex.py --delete --prefix=2

clean:
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

tag:
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

.PHONY: all terraform deploy subscribe everything reindex clean test travis
