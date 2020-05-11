.PHONY: all
all: layer indexer service

include ../common.mk

.PHONY: layer
layer: check_env
	$(MAKE) -C layer package

.PHONY: indexer
indexer: check_env
	$(MAKE) -C indexer package

.PHONY: service
service: check_env
	$(MAKE) -C service package

.PHONY: clean
clean: check_env
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean
