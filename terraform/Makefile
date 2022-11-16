.PHONY: all
all: apply

include ../common.mk

.PHONY: init
init: clean check_terraform check_branch check_aws providers.tf.json backend.tf.json
	terraform init -reconfigure

.PHONY: config
# FIXME: Lambda layers shouldn't be uploaded when `plan` target is run
#        https://github.com/DataBiosphere/azul/issues/3652
config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py)) lambdas

.PHONY: rename_resources
rename_resources: init config
	python $(project_root)/scripts/rename_resources.py
	# FIXME: Remove after all deployments have been upgraded to 1.3.x
	#        https://github.com/DataBiosphere/azul/issues/4744
	terraform state replace-provider -auto-approve registry.terraform.io/-/aws registry.terraform.io/hashicorp/aws
	terraform state replace-provider -auto-approve registry.terraform.io/-/external registry.terraform.io/hashicorp/external
	terraform state replace-provider -auto-approve registry.terraform.io/-/google registry.terraform.io/hashicorp/google
	terraform state replace-provider -auto-approve registry.terraform.io/-/null registry.terraform.io/hashicorp/null

.PHONY: validate
validate: init config
	terraform validate

.PHONY: plan
plan: validate
	terraform plan

.PHONY: apply
apply: validate rename_resources
ifeq ($(AZUL_PRIVATE_API),1)
	# For private API we need the VPC endpoints to be created first so that the
	# aws_lb_target_group_attachment can iterate over the network_interface_ids.
	terraform apply -target aws_vpc_endpoint.indexer -target aws_vpc_endpoint.service
endif
	terraform apply

.PHONY: auto_apply
auto_apply: validate rename_resources
ifeq ($(AZUL_PRIVATE_API),1)
	# For private API we need the VPC endpoints to be created first so that the
	# aws_lb_target_group_attachment can iterate over the network_interface_ids.
	terraform apply -auto-approve -target aws_vpc_endpoint.indexer -target aws_vpc_endpoint.service
endif
	terraform plan -no-color -out=plan.bin
	terraform show -json plan.bin > plan.json
	terraform apply -auto-approve plan.bin

.PHONY: destroy
destroy: validate
	terraform destroy

.PHONY: auto_destroy
auto_destroy: validate
	@echo '!!! All resources will be deleted in 10s, hit Ctrl-C to cancel !!!'; sleep 10
	terraform destroy -auto-approve

.PHONY: taint_dependencies_layer
taint_dependencies_layer: init config
	terraform taint -allow-missing aws_lambda_layer_version.dependencies

.PHONY: clean
clean: check_env
	rm -f *.tf.json

.PHONY: update_schema
update_schema: init config
	python $(project_root)/scripts/terraform_schema.py update

.PHONY: check_schema
# Depend on init to ensure provider plugins are installed. Without it
# we wouldn't be able to check their versions against those in the schema.
check_schema: init
	@if ! python $(project_root)/scripts/terraform_schema.py check; then \
		echo -e "\nRun '$(MAKE) update_schema'.\n"; \
		false; \
	fi

.PHONY: lambdas
lambdas:
	$(MAKE) -C $(project_root)/lambdas
