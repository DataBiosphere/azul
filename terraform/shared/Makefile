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

.PHONY: import_resources
import_resources: rename_resources
	python $(project_root)/scripts/import_default_vpc.py
	# FIXME: https://github.com/DataBiosphere/azul/issues/4824
	#        Remove import of the GitLab default security group once completed
	python $(project_root)/scripts/import_gitlab_security_group.py

.PHONY: apply
apply: validate import_resources
	terraform apply
	# FIXME: Remove role once migration is done
	#        https://github.com/DataBiosphere/azul/issues/4918
	python $(project_root)/scripts/replicate_versioned_bucket.py

.PHONY: destroy
destroy: validate
	terraform destroy
