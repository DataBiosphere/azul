all: deploy

include ../../common.mk

config: .chalice/config.json

deploy: config
	python $(azul_home)/scripts/stage_layer.py

clean:
	git clean -Xdf

.PHONY: all config deploy clean
