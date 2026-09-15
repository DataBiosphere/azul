.PHONY: all
all: package

include ../lambdas.mk

$(eval $(call docker_image_macro,indexer))
