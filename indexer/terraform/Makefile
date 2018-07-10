include ../common.mk

all: apply

init: elasticsearch.tf.json sqs.tf.json backend.tf.json providers.tf.json
	terraform init

validate: init
	terraform validate

plan: validate
	terraform plan

apply: validate
	terraform apply

clean:
	-rm *.tf.json

.PHONY: all init validate plan apply clean
