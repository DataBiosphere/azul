all: hello

include common.mk

hello:
	@echo Looking good!

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas

subscribe: check_branch
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py --shared; fi

unsubscribe:
	python scripts/subscribe.py --unsubscribe --shared

delete: check_branch
	python scripts/reindex.py --delete

index: check_branch
	python scripts/reindex.py --index --partition-prefix-length=2

reindex: check_branch
	python scripts/reindex.py --delete --index --purge --partition-prefix-length=2

clean:
	rm -rf .cache .config
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

test:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

integration_test: check_branch
	python -m unittest -v local_integration_test

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

.PHONY: all hello terraform deploy subscribe everything reindex clean test travis integration_test \
        trufflehog check_trufflehog delete check_clean check_autosquash
