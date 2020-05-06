.PHONY: all
all: layer indexer service

include ../common.mk

.PHONY: layer
layer: check_env
	$(MAKE) -C layer deploy

.PHONY: indexer
indexer: check_env
	$(MAKE) -C indexer deploy

.PHONY: service
service: check_env
	$(MAKE) -C service deploy

.PHONY: clean
clean: check_env
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean
