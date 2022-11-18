.PHONY: all
all: apply

include ../common.mk

.PHONY: providers
providers: check_terraform check_branch check_aws providers.tf.json backend.tf.json
	terraform init

.PHONY: config
# FIXME: Lambda layers shouldn't be uploaded when `plan` target is run
#        https://github.com/DataBiosphere/azul/issues/3652
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
taint_dependencies_layer: init
	terraform taint -allow-missing aws_lambda_layer_version.dependencies

.PHONY: clean
clean: check_env
	rm -f *.tf.json

.PHONY: update_schema
update_schema: init
	python $(project_root)/scripts/terraform_schema.py update

.PHONY: check_schema
check_schema: providers
	python $(project_root)/scripts/terraform_schema.py check \
	|| (echo "Schema is stale. Run 'make update_schema' and commit." ; false)

.PHONY: lambdas
lambdas:
	$(MAKE) -C $(project_root)/lambdas
