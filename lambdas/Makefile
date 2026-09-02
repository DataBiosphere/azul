.PHONY: all
all: indexer service

include ../common.mk

.PHONY: indexer
indexer: check_env
	$(MAKE) -C indexer docker_push

.PHONY: service
service: check_env
	$(MAKE) -C service docker_push

.PHONY: clean
clean: check_env
	for d in indexer service; do $(MAKE) -C $$d clean || ! break; done

.PHONY: openapi
openapi: check_env
	for d in indexer service; do $(MAKE) -C $$d openapi || ! break; done
