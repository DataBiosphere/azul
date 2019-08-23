all: indexer service

include ../common.mk

indexer:
	$(MAKE) -C indexer deploy

service:
	$(MAKE) -C service deploy

clean:
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean

delete:
	$(MAKE) -C indexer delete 
	$(MAKE) -C service delete

.PHONY: all indexer service clean delete
