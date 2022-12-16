.PHONY: all
all: layer

include ../lambdas.mk

.PHONY: layer
layer: check_branch check_python check_aws config
	python $(project_root)/scripts/stage_layer.py
