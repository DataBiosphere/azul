include ../common.mk

all: apply

config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

init: config
	terraform init

validate: init
	terraform validate

plan: validate
	terraform plan

apply: validate
	terraform apply

destroy: validate
	terraform destroy

clean:
	-rm *.tf.json

.PHONY: all config init validate plan apply destroy clean
