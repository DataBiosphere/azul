include common.mk

all:
	@echo Looking good!

terraform:
	$(MAKE) -C terraform

deploy:
	$(MAKE) -C lambdas

clean:
	for d in lambdas terraform; do $(MAKE) -C $$d clean; done

.PHONY: all terraform deploy clean
