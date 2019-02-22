SHELL=/bin/bash

ifndef azul_home
$(error Please run "source environment" from the project root directory before running make commands)
endif

ifneq ($(shell python -c "import os; print('VIRTUAL_ENV' in os.environ)"),True)
$(error Looks like no virtualenv is active)
endif

ifneq ($(shell python -c "import sys; print(sys.version_info[0:2] == (3,6))"),True)
$(error Looks like Python 3.6 is not installed or active in the current virtualenv)
endif

ifneq ($(shell python -c "exec('try: import chalice\nexcept: print(False)\nelse: print(True)')"),True)
$(error Looks like some or all requirements is missing. Please run 'pip install -r requirements.dev.txt')
endif

ifneq ($(shell python -c "from chalice import chalice_version as v; \
                          from pkg_resources import parse_version as p; \
                          print(p(v) >= p('1.6.0'))"),True)
$(error Looks like chalice is out of date. Please run 'pip install -Ur requirements.dev.txt')
endif

ifeq ($(shell which terraform),)
$(warning Looks like TerraForm is not installed. This is ok as long as you're not trying to create a new deployment. \
          Deploying new lambdas is still possible with `make deploy` but `make terraform` will not work.)
endif

ifneq ($(shell python -c "import wheel as w; \
                          from pkg_resources import parse_version as p; \
                          print(p(w.__version__) >= p('0.32.3'))"),True)
$(error Looks like the `wheel` package is outdated or missing. See README for instructions on how to fix this.)
endif

check_branch:
	python $(azul_home)/scripts/check_branch.py

.PHONY: check_branch

%: %.template.py .FORCE
	python $< $@
.FORCE:

# The template output file depends on the template file, of course, as well as the environment. To be safe we force the
# template creation. This is what the fake .FORCE target does. It still is necessary to declare a target's dependency on
# a template to ensure correct ordering.
