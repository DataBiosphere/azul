.PHONY: all
all: hello

include common.mk

.PHONY: virtualenv
virtualenv: check_env
	@if test -s "$$VIRTUAL_ENV"; then echo -e "\nRun 'deactivate' first\n"; false; fi
	if test -e .venv; then rm -rf .venv/; fi
	python3.8 -m venv .venv
	@echo -e "\nRun 'source .venv/bin/activate' now!\n"

.PHONY: envhook
envhook: check_venv
	python scripts/envhook.py install

.PHONY: requirements_pip
requirements_pip: check_venv
	pip install -Ur requirements.pip.txt

.PHONY: requirements_runtime
requirements_runtime: check_venv requirements_pip
	pip install -Ur requirements.txt

.PHONY: requirements
requirements: check_venv requirements_pip
	pip install -Ur requirements.dev.txt

.PHONY: hello
hello: check_python
	@echo Looking good!

.PHONY: package
package: check_env
	$(MAKE) -C lambdas

.PHONY: deploy
deploy: check_env
	$(MAKE) -C terraform apply

.PHONY: auto_deploy
auto_deploy: check_env
	$(MAKE) -C terraform plan auto_apply

.PHONY: subscribe
subscribe: check_python check_branch
	if [[ $$AZUL_SUBSCRIBE_TO_DSS != 0 ]]; then python scripts/subscribe.py; fi

.PHONY: unsubscribe
unsubscribe: check_python check_branch
	python scripts/subscribe.py --unsubscribe

.PHONY: create
create: check_python check_branch
	python scripts/reindex.py --create

.PHONY: delete
delete: check_python check_branch
	python scripts/reindex.py --delete

.PHONY: index
index: check_python check_branch
	python scripts/reindex.py --index --partition-prefix-length=2

.PHONY: reindex
reindex: check_python check_branch
	python scripts/reindex.py --delete --index --purge --partition-prefix-length=2

.PHONY: clean
clean: check_env
	rm -rf .cache .config
	for d in lambdas terraform terraform/gitlab; do $(MAKE) -C $$d clean; done

absolute_sources = $(shell echo $(project_root)/src \
                                $(project_root)/scripts \
                                $(project_root)/test \
                                $(project_root)/lambdas/{indexer,service}/app.py \
                                $$(find $(project_root)/terraform{,/gitlab} \
                                        $(project_root)/lambdas/{indexer,service}{,/.chalice} \
                                        -maxdepth 1 \
                                        -name '*.template.py' \
                                        -type f ))

relative_sources = $(subst $(project_root)/,,$(absolute_sources))

.PHONY: pep8
pep8: check_python
	flake8 --max-line-length=120 $(absolute_sources)

# The container path resolution in the recipe below is needed on Gitlab where
# the build is already running in a container and the container below will be a
# sibling of the current container.

.PHONY: format
format: check_docker
	docker run \
	    --rm \
	    --volume $$(python scripts/resolve_container_path.py $(project_root)):/home/developer/azul \
	    --workdir /home/developer/azul rycus86/pycharm:2019.2.3 \
	    /opt/pycharm/bin/format.sh -r -settings .pycharm.style.xml -mask '*.py' $(relative_sources)

.PHONY: test
test: check_python
	PYTHONWARNINGS=ignore:ResourceWarning coverage run -m unittest discover test --verbose

.PHONY: tag
tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

.PHONY: integration_test
integration_test: check_python check_branch
	python -m unittest -v integration_test

.PHONY: check_clean
check_clean: check_env
	git diff --exit-code && git diff --cached --exit-code

.PHONY: check_autosquash
check_autosquash: check_env
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
