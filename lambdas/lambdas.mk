# Relative paths are based on the CWD, not the directory containing this file.
# project_root is not defined if the user forgot to source environment. This
# solution is based on comments in https://stackoverflow.com/questions/322936.
include $(abspath $(dir $(lastword $(MAKEFILE_LIST))))/../common.mk

# The compile target is used during packaging of lambdas. The target ensures
# that a .pyc file is present for every .py file in the package.
#
# One reason to compile before deploying is to reduce lambda start-up time. But
# more importantly, it ensures that the same files are always included in the
# Chalice deployment package. Having a consistent, deterministic deployment
# package allows Terraform to use the hash of the deployment package to easily
# decide if anything new is being deployed, and skip updating the lambdas
# otherwise.
#
# By default, Python embeds the modify timestamp of the source file into the
# .pyc and uses this to determine when to re-compile, but since Gitlab clones
# the repository each time it deploys, fresh timestamps prevented the deployment
# package from being deterministic. With the `--invalidation-mode checked-hash`
# option, Python embeds the hash of the source file embedded in the .pyc instead
# of the timestamp, which is consistent regardless of when the files were
# downloaded.
#
# The `-f` option forces recompilation. This is necessary because timestamp
# style .pycs may have already been created when other deployment scripts are
# run, and we need to overwrite them.
#
# Set literals will compile in a non-deterministic order unless PYTHONHASHSEED
# is set. For a full explanation see http://benno.id.au/blog/2013/01/15/python-determinism
#
# `compileall` ignores symlinks to directories during traversal, so we must
# explicitly list them as arguments to ensure all files in vendor/ are
# deterministically compiled.
#
.PHONY: compile
compile: check_python
	PYTHONHASHSEED=0 python -m compileall \
		-f -q --invalidation-mode checked-hash \
		vendor $(shell find -L $$(find vendor -maxdepth 1 -type l) -maxdepth 0 -type d)

.PHONY: config
config: .chalice/config.json

.PHONY: environ
environ: vendor/resources/environ.json

.PHONY: local
local: check_python config
	chalice local

.PHONY: clean
clean: git_clean_recursive

.PHONY: package
package: check_branch check_python check_aws config environ compile
	python -m azul.changelog vendor
	chalice package --stage $(AZUL_DEPLOYMENT_STAGE) --pkg-format terraform .chalice/terraform

.PHONY: openapi
openapi: check_python
	python $(project_root)/scripts/generate_openapi_document.py
