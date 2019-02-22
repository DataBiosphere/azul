all: grafana

include ../common.mk

config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

init: config
	terraform init

validate: init
	terraform validate

plan: validate
	terraform plan

apply: validate
	terraform apply

grafana: apply
	python $(azul_home)/scripts/update_grafana.py

destroy: validate
	terraform destroy

clean:
	-rm *.tf.json

.PHONY: all config init validate plan apply grafana destroy clean
