.PHONY: all
all: pull_request_template.md $(addprefix PULL_REQUEST_TEMPLATE/,backport.md gitlab.md hotfix.md promotion.md)

%.md: pull_request_template.md.template.py FORCE
	python $< $@

FORCE:
