all: hello

include common.mk

hello:
	@echo Looking good!

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas

subscribe:
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py --shared; fi

reindex:
	python scripts/reindex.py --delete --partition-prefix-length=2

clean:
	rm -rf .cache .config
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

integration_test:
	python -m unittest -v local_integration_test

check_trufflehog:
	@hash trufflehog || ( echo 'Please install trufflehog using "pip install trufflehog"' ; false )

trufflehog: check_trufflehog
	trufflehog --regex --rules trufflehog-rules.json --entropy=False file:///$$azul_home

.PHONY: all hello terraform deploy subscribe everything reindex clean test travis integration_test trufflehog check_trufflehog
