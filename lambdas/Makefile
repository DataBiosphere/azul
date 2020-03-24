all: layer indexer service

include ../common.mk

layer:
	$(MAKE) -C layer deploy

indexer:
	$(MAKE) -C indexer deploy

service:
	$(MAKE) -C service deploy

clean:
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean

.PHONY: all layer indexer service clean
