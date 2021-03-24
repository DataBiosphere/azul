.PHONY: all
all: apply

include ../common.mk

.PHONY: providers
providers: check_terraform check_branch check_aws providers.tf.json backend.tf.json
	terraform init

.PHONY: config
config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py)) lambdas

.PHONY: init
init: providers config
	terraform init

.PHONY: rename_resources
rename_resources: init
	python $(project_root)/scripts/rename_resources.py

.PHONY: validate
validate: init
	terraform validate

.PHONY: plan
plan: validate
	terraform plan

.PHONY: apply
apply: validate rename_resources
	$(MAKE) unlink_health_checks
	terraform apply
	$(MAKE) link_health_checks
	$(MAKE) grafana

.PHONY: auto_apply
auto_apply: validate rename_resources
	$(MAKE) unlink_health_checks
	terraform apply -auto-approve
	$(MAKE) link_health_checks
	$(MAKE) grafana

.PHONY: link_health_checks
link_health_checks: check_env
	python $(project_root)/scripts/manage_health_checks.py --link

.PHONY: unlink_health_checks
unlink_health_checks: check_env
	python $(project_root)/scripts/manage_health_checks.py --unlink

.PHONY: grafana
grafana: check_env
	python $(project_root)/scripts/update_grafana.py

.PHONY: destroy
destroy: validate
	terraform destroy

.PHONY: auto_destroy
auto_destroy: validate
	@echo '!!! All resources will be deleted in 10s, hit Ctrl-C to cancel !!!'; sleep 10
	terraform destroy -auto-approve

.PHONY: taint_dependencies_layer
taint_dependencies_layer: init
	terraform taint -allow-missing aws_lambda_layer_version.dependencies

.PHONY: clean
clean: check_env
	rm -f *.tf.json

.PHONY: update_schema
update_schema: init
	python $(project_root)/scripts/terraform_schema.py update

.PHONY: check_schema
# Depend on init to ensure provider plugins are installed. Without it
# we wouldn't be able to check their versions against those in the schema.
check_schema: providers
	@if ! python $(project_root)/scripts/terraform_schema.py check; then \
		echo -e "\nRun '$(MAKE) update_schema'.\n"; \
		false; \
	fi

.PHONY: lambdas
lambdas:
	$(MAKE) -C $(project_root)/lambdas
