.PHONY: all
all: package

include ../../common.mk
include ../lambdas.mk

.PHONY: wheels
wheels: check_env
	rm -rf vendor/jsonobject*
	unzip $(project_root)/lambdas/.wheels/jsonobject-0.9.9-cp38-cp38-linux_x86_64.whl -d vendor

.PHONY: package
package: check_branch check_python check_aws config wheels
	python $(project_root)/scripts/stage_layer.py
