.PHONY: all
all: standalone.yml stub.yml

standalone.yml: pipeline.yml.template.py FORCE
	python $< $@

stub.yml: stub.yml.template.py FORCE
	python $< $@

FORCE:
