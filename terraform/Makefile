.PHONY: all
all: apply

include ../common.mk

.PHONY: clean
clean: check_env
	rm -rf *.tf.json service indexer $(TF_DATA_DIR)/modules

.PHONY: state
state: check_terraform check_branch check_aws clean
	@# FIXME: Remove after all deployments have been upgraded to 1.3.x
	@#        https://github.com/DataBiosphere/azul/issues/4744
	python $(project_root)/scripts/migrate_terraform_state.py

.PHONY: init
init: state providers.tf.json backend.tf.json
	terraform init -reconfigure

.PHONY: check_schema
check_schema: init
	python $(project_root)/scripts/terraform_schema.py check \
	|| (echo "Schema is stale. Run 'make update_schema' and commit." ; false)

.PHONY: update_schema
update_schema: init
	python $(project_root)/scripts/terraform_schema.py update

.PHONY: modules
modules: modules.tf.json
	$(MAKE) -C $(project_root)/lambdas
	terraform init  # to "install" modules

.PHONY: config
# FIXME: Lambda layers shouldn't be uploaded when `plan` target is run
#        https://github.com/DataBiosphere/azul/issues/3652
config: check_schema modules $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

.PHONY: validate
validate: config
	terraform validate

.PHONY: plan
plan: validate
	terraform plan

.PHONY: rename_resources
rename_resources: config
	python $(project_root)/scripts/rename_resources.py

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
	# See `apply` above
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
taint_dependencies_layer: config
	terraform taint -allow-missing aws_lambda_layer_version.dependencies
