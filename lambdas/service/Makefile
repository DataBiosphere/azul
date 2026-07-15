.PHONY: all
all: package

include ../lambdas.mk

.PHONY: apat_public_key
apat_public_key: vendor/resources/apat_public_key.json

package: apat_public_key image_push

$(eval $(call image,service))
