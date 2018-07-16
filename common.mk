SHELL=/bin/bash

ifndef AZUL_HOME
$(error Please run "source environment" from the project root directory before running make commands)
endif

ifneq ($(shell python -c "import sys; print(hasattr(sys, 'real_prefix'))"),True)
$(error Looks like no virtualenv is active)
endif

ifneq ($(shell python -c "import sys; print(sys.version_info >= (3,6))"),True)
$(error Looks like Python 3.6 is not installed or active in the current virtualenv)
endif

ifneq ($(shell python -c "exec('try: import chalice\nexcept: print(False)\nelse: print(True)')"),True)
$(error Looks like some or all requirements is missing. Please run 'pip install -r requirements.dev.txt')
endif

ifeq ($(shell which terraform),)
$(warning Looks like TerraForm is not installed. This is ok as long as you're not trying to create a new deployment. \
          Deploying new lambdas is still possible with `make deploy` but `make terraform` will not work.)
endif

# FIXME: remove conditional once projects are merged

%: %.template.py .FORCE
ifeq (,$(wildcard $(AZUL_HOME)/src))
	PYTHONPATH=$(AZUL_HOME) python $<
else
	PYTHONPATH=$(AZUL_HOME)/src python $<
endif
.FORCE:

# The template output file depends on the template file, of course, as well as the environment. To be safe we force the
# template creation. This is what the fake .FORCE target does. It still is necessary to declare a target's dependency on
# a template to ensure correct ordering.
