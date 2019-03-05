all: apply

include ../common.mk

config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

init: check_branch config
	terraform init

validate: init
	terraform validate

plan: validate
	terraform plan

apply: validate
	terraform apply
	$(MAKE) grafana

auto_apply: validate
	terraform apply -auto-approve
	$(MAKE) grafana

grafana:
	python $(azul_home)/scripts/update_grafana.py

destroy: validate
	terraform destroy

auto_destroy: validate
	@echo '!!! All resources will be deleted in 10s, hit Ctrl-C to cancel !!!'; sleep 10
	terraform destroy -auto-approve

clean:
	-rm *.tf.json

.PHONY: all config init validate plan apply auto_apply grafana destroy auto_destroy clean
