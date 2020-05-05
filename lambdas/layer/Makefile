.PHONY: all
all: deploy

include ../../common.mk

.PHONY: config
config: .chalice/config.json

.PHONY: deploy
deploy: check_branch check_python check_aws config
	python $(azul_home)/scripts/stage_layer.py

.PHONY: clean
clean: check_env
	git clean -Xdf
