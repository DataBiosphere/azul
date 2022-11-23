.PHONY: all
all: package

include ../lambdas.mk

.PHONY: package
package: check_branch check_python check_aws config
	python $(project_root)/scripts/stage_layer.py
