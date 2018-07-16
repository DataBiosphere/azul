all: indexer service

indexer:
	$(MAKE) -C indexer deploy

service:
	$(MAKE) -C service deploy

clean:
	$(MAKE) -C indexer clean
	$(MAKE) -C service clean

.PHONY: indexer service clean
