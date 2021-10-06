
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
.PHONY: compile
compile: check_env
	PYTHONHASHSEED=0 python -m compileall -f -q --invalidation-mode checked-hash vendor vendor/azul

.PHONY: config
config: .chalice/config.json

.PHONY: environ
environ: vendor/resources/environ.json

.PHONY: local
local: check_python config
	chalice local

.PHONY: clean
clean: check_env
	git clean -Xdf
