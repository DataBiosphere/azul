.PHONY: all
all: package

include ../../common.mk

.PHONY: config
config: .chalice/config.json

.PHONY: wheels
wheels: check_env
	rm -rf vendor/jsonobject*
	unzip $(azul_home)/lambdas/.wheels/jsonobject-0.9.9-cp38-cp38-linux_x86_64.whl -d vendor

.PHONY: package
package: check_branch check_python check_aws config wheels
	python $(azul_home)/scripts/stage_layer.py

.PHONY: clean
clean: check_env
	git clean -Xdf
