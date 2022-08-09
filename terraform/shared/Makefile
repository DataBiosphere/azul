.PHONY: all
 all: apply

include ../../common.mk

.PHONY: config
config: $(patsubst %.template.py,%,$(wildcard *.tf.json.template.py))

.PHONY: init
init: check_terraform check_branch check_aws config
	terraform init

.PHONY: validate
validate: init
	terraform validate

.PHONY: plan
plan: validate
	terraform plan

.PHONY: apply
apply: validate rename_resources
	terraform apply

.PHONY: destroy
destroy: validate
	terraform destroy

.PHONY: clean
clean: check_env
	rm -f *.tf.json

.PHONY: rename_resources
rename_resources: init
	python $(project_root)/scripts/rename_resources.py
