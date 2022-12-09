.PHONY: all
all: apply

include ../../common.mk

.PHONY: clean
clean: git_clean

.PHONY: state
state: check_terraform check_branch check_aws

.PHONY: init
init: state providers.tf.json backend.tf.json
	terraform init -reconfigure

.PHONY: config
config: init $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

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
	terraform apply

.PHONY: destroy
destroy: validate
	terraform destroy
