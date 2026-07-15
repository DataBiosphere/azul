.PHONY: all
all: layer registry indexer service

include ../common.mk

.PHONY: layer
layer: check_env
	$(MAKE) -C layer layer

.PHONY: registry
registry:
	$(MAKE) -C $(project_root)/terraform create_registry

.PHONY: indexer
indexer: check_env
	$(MAKE) -C indexer package

.PHONY: service
service: check_env
	$(MAKE) -C service package

.PHONY: clean
clean: check_env
	for d in indexer service layer; do $(MAKE) -C $$d clean || ! break; done

.PHONY: openapi
openapi: check_env
	for d in indexer service; do $(MAKE) -C $$d openapi || ! break; done
