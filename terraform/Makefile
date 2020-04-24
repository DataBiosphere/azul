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
	$(MAKE) unlink_health_checks
	terraform apply
	$(MAKE) link_health_checks
	$(MAKE) grafana

auto_apply: validate
	$(MAKE) unlink_health_checks
	terraform apply -auto-approve
	$(MAKE) link_health_checks
	$(MAKE) grafana

link_health_checks:
	python $(azul_home)/scripts/manage_health_checks.py --link

unlink_health_checks:
	python $(azul_home)/scripts/manage_health_checks.py --unlink

grafana:
	python $(azul_home)/scripts/update_grafana.py

destroy: validate
	terraform destroy

auto_destroy: validate
	@echo '!!! All resources will be deleted in 10s, hit Ctrl-C to cancel !!!'; sleep 10
	terraform destroy -auto-approve

taint_dependencies_layer: init
	terraform taint -allow-missing aws_lambda_layer_version.dependencies

clean:
	rm -f *.tf.json

.PHONY: all config init validate plan apply auto_apply link_health_checks unlink_health_checks grafana \
        destroy auto_destroy clean
