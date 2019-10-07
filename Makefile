all: hello

include common.mk

hello:
	@echo Looking good!

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas

subscribe: check_branch
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py; fi

unsubscribe: check_branch
	python scripts/subscribe.py --unsubscribe

delete: check_branch
	python scripts/reindex.py --delete

index: check_branch
	python scripts/reindex.py --index --partition-prefix-length=2

reindex: check_branch
	python scripts/reindex.py --delete --index --purge --partition-prefix-length=2

clean:
	rm -rf .cache .config
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

absolute_sources = $(shell echo $(azul_home)/src \
                                $(azul_home)/scripts \
                                $(azul_home)/test \
                                $(azul_home)/lambdas/{indexer,service}/app.py \
                                $$(find $(azul_home)/terraform{,/gitlab} \
                                        $(azul_home)/lambdas/{indexer,service}{,/.chalice} \
                                        -maxdepth 1 \
                                        -name '*.template.py' \
                                        -type f ))

relative_sources = $(subst $(azul_home)/,,$(absolute_sources))

pep8:
	flake8 --max-line-length=120 $(absolute_sources)

# The container path resolution in the recipe below is needed on Gitlab where
# the build is already running in a container and the container below will be a
# sibling of the current container.

format:
	docker run \
	    --rm \
	    --volume $$(python scripts/resolve_container_path.py $(azul_home)):/home/developer/azul \
	    --workdir /home/developer/azul rycus86/pycharm:2019.2.3 \
	    /opt/pycharm/bin/format.sh -r -settings .pycharm.style.xml -mask '*.py' $(relative_sources)

test:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

integration_test: check_branch
	python -m unittest -v integration_test

check_trufflehog:
	@hash trufflehog || ( echo 'Please install trufflehog using "pip install trufflehog"' ; false )

trufflehog: check_trufflehog
	trufflehog --regex --rules .trufflehog.json --entropy=False file:///$$azul_home

check_clean:
	git diff --exit-code  && git diff --cached --exit-code

check_autosquash:
	@if [[ -z "$${TRAVIS_BRANCH}" || "$${TRAVIS_BRANCH}" == "develop" ]]; then \
	    _azul_merge_base=$$(git merge-base HEAD develop) \
	    ; if GIT_SEQUENCE_EDITOR=: git rebase -i --autosquash "$${_azul_merge_base}"; then \
	        git reset --hard ORIG_HEAD \
	        ; echo "The current branch is automatically squashable" \
	        ; true \
	    ; else \
	        git rebase --abort \
	        ; echo "The current branch doesn't appear to be automatically squashable" \
	        ; false \
	    ; fi \
	; else \
	    echo "Can only check squashability against default branch on Travis" \
	; fi

.PHONY: all hello \
        terraform deploy subscribe unsubscribe \
        delete index reindex \
        clean \
        tag \
        pep8 \
        test integration_test \
        check_trufflehog trufflehog \
        check_clean check_autosquash
