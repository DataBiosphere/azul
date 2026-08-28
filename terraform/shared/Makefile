.PHONY: all
all: apply

include ../../common.mk

.PHONY: clean
clean: check_env check_branch git_clean

.PHONY: state
state: check_terraform check_aws

.PHONY: initable
initable: clean state providers.tf.json backend.tf.json

.PHONY: init
init: initable
	terraform init -reconfigure -lockfile=readonly

.PHONY: config
config: init $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

.PHONY: validate
validate: config
	terraform validate

.PHONY: plan
plan: validate
	terraform plan

.PHONY: apply
apply: validate
	terraform apply

.PHONY: apply_keep_unused
apply_keep_unused:
	azul_terraform_keep_unused=1 $(MAKE) apply

.PHONY: auto_apply
auto_apply: validate
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
