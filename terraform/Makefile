.PHONY: all
all: apply

include ../common.mk

.PHONY: clean
clean: check_env check_branch git_clean

.PHONY: state
state: check_terraform check_aws

.PHONY: initable
initable: clean state providers.tf.json backend.tf.json

.PHONY: init
init: initable
	terraform init -reconfigure
	$(MAKE) check_providers_clean

.PHONY: check_schema
check_schema: init
	python $(project_root)/scripts/terraform_schema.py check \
	|| (echo "Schema is stale. Run 'make update_schema' and commit." ; false)

.PHONY: update_schema
update_schema: init
	python $(project_root)/scripts/terraform_schema.py update

.PHONY: config
config: check_schema $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

.PHONY: validate
validate: config
	terraform validate

.PHONY: rename_resources
rename_resources: config
	python $(project_root)/scripts/rename_resources.py

.PHONY: import_resources
import_resources: rename_resources

.PHONY: plan
plan: validate import_resources
	terraform plan

.PHONY: apply
apply: validate import_resources
ifeq ($(AZUL_PRIVATE_API),1)
	# For private API we need the VPC endpoints to be created first so that the
	# aws_lb_target_group_attachment can iterate over the network_interface_ids.
	terraform apply -target aws_vpc_endpoint.indexer -target aws_vpc_endpoint.service
endif
	terraform apply

.PHONY: auto_apply
auto_apply: validate import_resources
ifeq ($(AZUL_PRIVATE_API),1)
	# See `apply` above
	terraform apply -auto-approve -target aws_vpc_endpoint.indexer -target aws_vpc_endpoint.service
endif
	terraform plan -no-color -out=plan.bin
	terraform show -json plan.bin > plan.json
	terraform apply -auto-approve plan.bin

.PHONY: destroy
destroy: validate import_resources
	terraform destroy

.PHONY: auto_destroy
auto_destroy: validate import_resources
	@echo '!!! All resources will be deleted in 10s, hit Ctrl-C to cancel !!!'; sleep 10
	terraform destroy -auto-approve

.PHONY: provider_update
provider_update: initable
	terraform init -upgrade
	terraform providers lock -platform=linux_amd64 \
	                         -platform=linux_arm64 \
	                         -platform=darwin_amd64 \
	                         -platform=darwin_arm64

.PHONY: check_providers_clean
check_providers_clean:
	git diff --exit-code --name-only .terraform.lock.hcl \
	&& git diff --cached --exit-code --name-only .terraform.lock.hcl
