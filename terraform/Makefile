include ../common.mk

templates=$(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

all: apply

config: $(templates)

init: config
	terraform init

validate: init
	terraform validate

plan: validate
	terraform plan

apply: validate
	terraform apply

clean:
	-rm *.tf.json

.PHONY: all init validate plan apply clean config
