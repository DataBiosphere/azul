.PHONY: all
all: hello

include common.mk

DOCKER_IMAGE ?= azul
DOCKER_TAG ?= latest
CACHE_SEED ?=

.PHONY: virtualenv
virtualenv: check_env
	@if test -s "$$VIRTUAL_ENV"; then echo -e "\nRun 'deactivate' first\n"; false; fi
	if test -e .venv; then rm -rf .venv/; fi
	python3.9 -m venv .venv
	@echo -e "\nRun 'source .venv/bin/activate' now!\n"

.PHONY: envhook
envhook: check_venv
	python scripts/envhook.py install

define requirements
.PHONY: requirements$1
requirements$1: check_venv $2
	pip install $3 -Ur requirements$4.txt
endef

$(eval $(call requirements,_pip,,--no-deps,.pip))
$(eval $(call requirements,,requirements_pip,--no-deps,.dev))
$(eval $(call requirements,_runtime,requirements_pip,--no-deps,))
$(eval $(call requirements,_deps,requirements_pip,,.dev))
$(eval $(call requirements,_runtime_deps,requirements_pip,,))

define docker
.PHONY: docker$1
docker$1: check_docker
	docker build \
	       --build-arg PIP_DISABLE_PIP_VERSION_CHECK=$$(PIP_DISABLE_PIP_VERSION_CHECK) \
	       --build-arg make_target=requirements$2 \
	       --build-arg cache_seed=${CACHE_SEED} \
	       --tag $$(DOCKER_IMAGE)$3:$$(DOCKER_TAG) \
	       .

.PHONY: docker$1_push
docker$1_push: docker$1
	docker push $$(DOCKER_IMAGE)$3:$$(DOCKER_TAG)
endef

$(eval $(call docker,,_runtime,))  # runtime image w/o dependency resolution
$(eval $(call docker,_dev,,/dev))  # development image w/o dependency resolution
$(eval $(call docker,_deps,_runtime_deps,/deps))  # runtime image with automatic dependency resolution
$(eval $(call docker,_dev_deps,_deps,/dev-deps))  # development image with automatic dependency resolution

.gitlab.env:
	echo BUILD_IMAGE=$(DOCKER_IMAGE)/dev:$(DOCKER_TAG) > .gitlab.env


.PHONY: requirements_update
requirements_update: check_venv check_docker
#	Pull out transitive dependency pins so they can be recomputed. Instead
# 	of truncating the `.trans` file, we comment out every line in it such that
# 	a different .trans file produces a different "pulled out" .trans file and
# 	therefore a different image layer hash when the file is copied into the
# 	image. This makes the pin removal injective. If we truncated the file, we
# 	might inadvertently reuse a stale image layer despite the .trans file
# 	having been updated. Not using sed because Darwin's sed does not do -i.
	git restore requirements.trans.txt requirements.dev.trans.txt
	perl -i -p -e 's/^(?!#)/#/' requirements.trans.txt requirements.dev.trans.txt
	$(MAKE) docker_deps docker_dev_deps
	python scripts/manage_requirements.py \
	       --image=$(DOCKER_IMAGE)/deps:$(DOCKER_TAG) \
	       --build-image=$(DOCKER_IMAGE)/dev-deps:$(DOCKER_TAG)
	# Download wheels (source and binary) for the Lambda runtime
	pip download \
	    --platform=manylinux2014_x86_64 \
	    --no-deps \
	    -r requirements.txt \
	    --dest=${azul_chalice_bin}

.PHONY: requirements_update_force
requirements_update_force: check_venv check_docker
	CACHE_SEED=$$(python -c 'import uuid; print(uuid.uuid4())') $(MAKE) requirements_update

.PHONY: hello
hello: check_python
	@echo Looking good!

.PHONY: lambdas
lambdas: check_env
	$(MAKE) -C lambdas

define deploy
.PHONY: $(1)terraform
$(1)terraform: lambdas
	$(MAKE) -C terraform $(1)apply

.PHONY: $(1)deploy
$(1)deploy: check_python $(1)terraform
	python $(project_root)/scripts/post_deploy_sns.py
	python $(project_root)/scripts/post_deploy_tdr.py
endef

$(eval $(call deploy,))
$(eval $(call deploy,auto_))

.PHONY: destroy
destroy:
	$(MAKE) -C terraform destroy

.PHONY: create
create: check_python check_branch
	python scripts/reindex.py --create

.PHONY: delete
delete: check_python check_branch
	python scripts/reindex.py --delete

.PHONY: index
index: check_python check_branch
	python scripts/reindex.py --index

reindex_args = --delete --index --purge

.PHONY: reindex
reindex: check_python check_branch
	python scripts/reindex.py ${reindex_args}

.PHONY: reindex_no_slots
reindex_no_slots: check_python check_branch
	python scripts/reindex.py ${reindex_args} --no-slots

# By our own convention, a line starting with `##` in the top-level `.gitignore`
# file separates rules for build products from those for local configuration.
# Build products can be removed by the clean target, local configuration must
# not. The convention only applies to the top-level `.gitignore` file, in
# lower-level files, all rules are assumed to be for build products.
# 
# Implementation details: First, we run `git ls-files` to list *all* ignored
# files, and then run it again to list only files ignored by rules for local
# configuration. We use `comm` to subtract the two results, yielding a list of
# build products only, and remove them. We repeat the process for directories,
# passing `--directory` to `git ls-files` and `-r` to `rm`. Note that any files
# in matching directories have been already been removed in the first pass,
# rendering the directories empty. That's how we can avoid having to pass `-f`
# to `rm`. 
#
# We can't handle directories and files together because that would complicate
# the rules of subtraction: subtracting a directory could mean the removal of
# multiple files. If we do them separately, a simple set difference suffices.
#
# The temporary file has to be stored on disk (i.e. instead of using a redirect)
# due to https://github.com/DataBiosphere/azul/issues/4785
define clean
tmp=$$(mktemp) \
    ; sed -e '1,/^##/d' .gitignore >"$$tmp" \
    ; comm -23 \
        <(git ls-files --others --ignored --exclude-standard $1 \
            | sort) \
        <(git ls-files --others --ignored --exclude-from "$$tmp" $1 \
            | sort) \
        | xargs -r rm -v $2 \
    ; rm "$$tmp"
endef

.PHONY: clean
clean: check_env
	for d in lambdas terraform terraform/{gitlab,shared}; \
	    do $(MAKE) -C $$d clean; \
	done
	$(call clean,,)
	$(call clean,--directory,-r)


absolute_sources = $(shell echo $(project_root)/src \
                                $(project_root)/scripts \
                                $(project_root)/test \
                                $(project_root)/lambdas/{layer,indexer,service}/app.py \
                                $(project_root)/.flake8/azul_flake8.py \
                                $$(find $(project_root)/terraform{,/gitlab} \
                                        $(project_root)/lambdas/{indexer,service}{,/.chalice} \
                                        -maxdepth 1 \
                                        -name '*.template.py' \
                                        -type f ))

relative_sources = $(subst $(project_root)/,,$(absolute_sources))

.PHONY: pep8
pep8: check_python
	flake8 --config .flake8/conf $(absolute_sources)

# The container path resolution in the recipe below is needed on Gitlab where
# the build is already running in a container and the container below will be a
# sibling of the current container.

.PHONY: format
format: check_venv check_docker
	docker run \
	    --rm \
	    --volume $$(python scripts/resolve_container_path.py $(project_root)):/home/developer/azul \
	    --workdir /home/developer/azul rycus86/pycharm:2019.3.5 \
	    /opt/pycharm/bin/format.sh -r -settings .pycharm.style.xml -mask '*.py' $(relative_sources)

.PHONY: test
test: check_python
	coverage run -m unittest discover test --verbose

.PHONY: test_list
test_list: check_python
	python scripts/list_unit_tests.py test


.PHONY: tag
tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

.PHONY: integration_test
integration_test: check_python check_branch $(project_root)/lambdas/service/.chalice/config.json
	python -m unittest -v integration_test

.PHONY: check_clean
check_clean: check_env
	git diff --exit-code && git diff --cached --exit-code

.PHONY: openapi
openapi:
	python $(project_root)/scripts/generate_openapi_document.py
