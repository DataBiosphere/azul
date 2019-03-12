all: indexer service

include ../common.mk

indexer:
	$(MAKE) -C indexer deploy

service:
	$(MAKE) -C service deploy

clean:
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean

.PHONY: all indexer service clean
