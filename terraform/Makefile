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

destroy_cloudwatch_alarms: validate
	terraform destroy $(terraform state list | grep '^aws_cloudwatch_metric_alarm' | sed 's/^/-target /')

clean:
	-rm *.tf.json

.PHONY: all config init validate plan apply destroy destroy_cloudwatch_alarms clean
