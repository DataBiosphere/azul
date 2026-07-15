.PHONY: all
all: package

include ../lambdas.mk

package: image_push

$(eval $(call image,indexer))
