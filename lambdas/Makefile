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
	for dir in indexer service layer; do $(MAKE) -C $$dir clean; done
