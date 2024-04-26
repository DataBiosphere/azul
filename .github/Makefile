.PHONY: all
all: pull_request_template.md $(addprefix PULL_REQUEST_TEMPLATE/,backport.md upgrade.md prod-hotfix.md prod-promotion.md anvilprod-hotfix.md anvilprod-promotion.md)

%.md: pull_request_template.md.template.py FORCE
	python $< $@

FORCE:
