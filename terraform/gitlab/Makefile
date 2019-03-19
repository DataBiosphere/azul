all: apply

include ../../common.mk

config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

init: check_branch config
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
	rm -f *.tf.json

.PHONY: all config init validate plan apply destroy clean
