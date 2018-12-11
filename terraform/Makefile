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

gitlab-plan: validate
    terraform plan -out "planfile"

apply: validate
	terraform apply

gitlab-apply: validate
    terraform apply planfile

destroy: validate
	terraform destroy

destroy_cloudwatch_alarms: validate
	terraform destroy $(terraform state list | grep '^aws_cloudwatch_metric_alarm' | sed 's/^/-target /')

clean:
	-rm *.tf.json

.PHONY: all init validate plan apply clean config
