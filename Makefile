.PHONY: all
all: hello

include common.mk

azul_image ?= docker.gitlab.$(AZUL_DOMAIN_NAME)/ucsc/azul
azul_image_tag ?= latest

.PHONY: hello
hello: check_python
	@echo Looking good!

.PHONY: virtualenv
virtualenv: check_env
	@if test -s "$$VIRTUAL_ENV"; then echo -e "\nRun 'deactivate' first\n"; false; fi
	uv venv --clear --force .venv
	@echo -e "\nRun 'source .venv/bin/activate' now!\n"

.PHONY: envhook
envhook: check_venv
	python scripts/envhook.py install

#	`--frozen` installs exactly what `uv.lock` specifies, without resolving
#	dependencies and without checking the lock against `pyproject.toml`. This
#	is what installing the pins from `requirements*.txt` with `--no-deps` used
#	to accomplish. Resolution is now a separate step, `uv lock`, so there is no
#	longer a variant of this target that resolves while installing. Note that
#	uv also *removes* any package the lock doesn't specify, leaving the virtual
#	environment matching the lock exactly.
#
.PHONY: requirements
requirements: check_venv
	uv sync --frozen

#	Fail if the lock file isn't consistent with `pyproject.toml`, say because a
#	dependency was added to the latter without updating the former. Newer
#	releases of a dependency don't affect this; `requirements_update` is what
#	picks those up.
#
.PHONY: check_requirements
check_requirements: check_env
	uv lock --check

.PHONY: check_transitive_requirements
check_transitive_requirements: check_python
	python scripts/check_transitive_requirements.py

.PHONY: docker_image
docker_image: check_docker
	docker build \
	       --build-arg azul_docker_registry=$(azul_docker_registry) \
	       --build-arg azul_python_image=$(azul_python_image) \
	       --build-arg azul_docker_version=$(azul_docker_version) \
	       --build-arg azul_terraform_version=$(azul_terraform_version) \
	       --build-arg azul_awscli_version=$(azul_awscli_version) \
	       --build-arg azul_ghcli_version=$(azul_ghcli_version) \
	       --build-arg azul_uv_version=$(azul_uv_version) \
	       --build-arg azul_pycharm_version=$(azul_pycharm_version) \
	       --tag $(azul_image):$(azul_image_tag) \
	       .

.PHONY: docker_push
docker_push: docker_image
	docker push $(azul_image):$(azul_image_tag)

.PHONY: requirements_update
requirements_update: check_env
	uv lock --upgrade

environment.boot: check_python
	python scripts/generate_environment_boot.py

gh_checksums: check_env
	curl --fail --silent --location -o bin/checksums/gh_checksums.txt \
	    https://github.com/cli/cli/releases/download/v$(azul_ghcli_version)/gh_$(azul_ghcli_version)_checksums.txt

#	Unlike the GitHub CLI, uv publishes one checksum file per release asset, so
#	we concatenate the ones we care about into the same format that `sha256sum
#	-c` expects.
#
uv_checksums: check_env
	rm -f bin/checksums/uv_checksums.txt
	for arch in x86_64 aarch64 ; do \
	    curl --fail --silent --location \
	        https://github.com/astral-sh/uv/releases/download/$(azul_uv_version)/uv-$$arch-unknown-linux-gnu.tar.gz.sha256 \
	        >> bin/checksums/uv_checksums.txt ; \
	done

pycharm_checksums: check_env
	rm -f bin/checksums/pycharm_checksums.txt
	for arch in "" -aarch64 ; do \
	    curl --fail --silent --location \
	        https://download.jetbrains.com/python/pycharm-community-$(azul_pycharm_version)$$arch.tar.gz.sha256 \
	        >> bin/checksums/pycharm_checksums.txt ; \
	done

.PHONY: lambdas
lambdas: check_env
	$(MAKE) -C lambdas

anvil_schema: check_python
	python scripts/download_anvil_schema.py

define deploy
.PHONY: $(1)terraform
$(1)terraform: lambdas
	$(MAKE) -C terraform $(1)apply

.PHONY: $(1)deploy
$(1)deploy: check_python $(1)terraform
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
# We can't use `sed … | git ls-files … --exclude-from /dev/stdin` because the
# --exclude-from option doesn't work with pipes. It calls `stat` to determine
# the file's size prior to reading the determined amount of data from the
# file. If the file is a pipe, there is a race with the writer, a race that,
# if lost, causes no or partial data to be read from the pipe. Instead we use
# sed to further massage the lines in .gitignore so that we can interpolate the
# result into the command line as repeats of the -x (--exclude) option.
#
define list_dirty
comm -23 \
    <(git ls-files --others --ignored \
        --exclude-standard \
        $1 \
        | sort) \
    <(git ls-files --others --ignored \
        $$(sed -e '1,/^##/d' \
               -e 's/#.*//' \
               -e '/^ *$$/d' \
               -e 's/.*/-x &/' \
               .gitignore) \
        $1 \
        | sort)
endef

.PHONY: list_dirty
list_dirty: check_env
	@$(call list_dirty,)
	@$(call list_dirty,--directory)

define clean
$(call list_dirty,$1) | xargs -r rm -v $2
endef

.PHONY: clean
clean: check_env
	for d in lambdas terraform terraform/{gitlab,shared}; \
	    do $(MAKE) -C $$d clean; \
	done
	@$(call clean,,)
	@$(call clean,--directory,-r)


absolute_sources = $(shell echo $(project_root)/src \
                                $(project_root)/scripts \
                                $(project_root)/test \
                                $(project_root)/lambdas/{indexer,service}/app.py \
                                $(project_root)/.flake8/azul_flake8.py \
                                $(project_root)/environment.py \
                                $(project_root)/deployments/*/environment.py \
                                $(project_root)/.github/workflows/*.py \
                                $$(find $(project_root)/terraform{,/gitlab,/shared,/browser} \
                                        $(project_root)/lambdas/{indexer,service}{,/.chalice} \
                                        $(project_root)/.github \
                                        $(project_root)/resources \
                                        -maxdepth 1 \
                                        -name '*.template.py' \
                                        -type f ))

relative_sources = $(subst $(project_root)/,,$(absolute_sources))

.PHONY: pep8
pep8: check_python
	python -m flake8 --config .flake8/conf $(absolute_sources)

# The formatter we use is part of PyCharm, which the Dockerfile at the project
# root installs into the development image. There are three targets for invoking
# the formatter, each assuming more than the one before it. `__format` assumes
# that the formatter is installed on the system it runs on, and is the target to
# use inside a container from that image. `_format` assumes only that the image
# exists; `format` assumes nothing, and builds the image first.
#
# Discarding stderr suppresses error output like stack traces. In order to
# reduce the number of vulnerabilities in the image, the Dockerfile retains only
# those parts of the IDE that the formatter needs. When the PyCharm process
# attempts to use one of the removed parts, an exception is raised. Fortunately
# this occurs in another thread, not affecting the main thread in which the code
# is being formatted. When diagnosing problems with the actual formatting,
# removing the redirection will reveal all output, potentially aiding in the
# diagnosis.
#
.PHONY: __format
__format: check_env
	/opt/pycharm/bin/format.sh \
	    -r -settings .pycharm.style.xml -mask '*.py' $(relative_sources) \
	    2>/dev/null

# The container path resolution in the recipe below is needed when `make
# _format` is invoked in a container. In that case the container started by the
# recipe will be a sibling of the invoking container. The Docker daemon resolves
# the source of every bind mount against the host's file system, so the path at
# which the project is mounted in the invoking container can't be used as the
# source of the mount for the sibling container. For the same reason we use
# --mount instead of --volume: the latter silently creates a missing source
# directory on the host instead of failing.
#
# Containers from the image run as root by default. If the one below did, the
# formatted files would end up being owned by root on the host, so it is run as
# the invoking user instead. That user has no entry in the image's /etc/passwd,
# so the JVM running PyCharm finds no home directory for it and falls back to
# $HOME, which is set to /tmp, the one directory in the image that any user can
# write to. Any caches and indexes PyCharm puts there stay in the container and
# are discarded with it.
#
.PHONY: _format
_format: check_venv
	container_root=/azul && \
	host_root=$$(python scripts/resolve_container_path.py $(project_root)) && \
	docker run \
	    --rm \
	    --user $$(id -u):$$(id -g) \
	    --env HOME=/tmp \
	    --env project_root=$$container_root \
	    --mount type=bind,source=$$host_root,target=$$container_root \
	    --workdir $$container_root \
	    $(azul_image):$(azul_image_tag) \
	    make __format

.PHONY: format
format: docker_image
	$(MAKE) _format

.PHONY: isort
isort: check_python
	isort --dont-order-by-type $(relative_sources)

test_args = -m unittest discover --verbose test

.PHONY: test
test: check_python
	coverage run $(test_args)

.PHONY: test_profile
test_profile: check_python
	uv run --frozen --no-sync --with pyinstrument \
	       python -m pyinstrument -r html -o test_profile.html $(test_args)

.PHONY: test_list
test_list: check_python
	python scripts/list_unit_tests.py test

.PHONY: tag
tag: check_branch
	@tag_name="$$(date '+deployed/$(AZUL_DEPLOYMENT_STAGE)/%Y-%m-%d__%H-%M')" ; \
	git tag $$tag_name && echo Run '"'git push origin tag $$tag_name'"' now to push the tag

.PHONY: integration_test
integration_test: check_python check_branch $(project_root)/lambdas/service/.chalice/config.json
	python -m unittest --verbose integration_test

.PHONY: check_clean
check_clean: check_env
	git diff --exit-code && git diff --cached --exit-code

docker_images.json: check_python
	python scripts/manage_images.py --update-manifests
