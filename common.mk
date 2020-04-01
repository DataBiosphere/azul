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

# This check should not occur within CI environments, where AWS Credentials might not be supplied
ifneq ($(shell python -c "import os, sys; \
                          import boto3 as b; \
                          print(os.environ.get('CI') == 'true' or \
                          b.client('sts').get_caller_identity()['Account'] == os.environ['AZUL_AWS_ACCOUNT_ID'])\
                          "),True)
$(error Looks like there is a mismatch between the AWS account you have configured, and what the deployment expects. \
        Compare output from 'aws sts get-caller-identity' and the deployment environment file)
endif

check_branch:
	python $(azul_home)/scripts/check_branch.py

check_branch_personal:
	python $(azul_home)/scripts/check_branch.py --personal

ifeq ($(shell git push --dry-run 2> /dev/null && echo yes),yes)
ifeq ($(shell git secrets --list | grep -- --aws-provider),)
$(error Please install and configure git-secrets. See README.md for details)
endif
ifneq ($(shell grep -Fo 'git secrets' `git rev-parse --git-dir`/hooks/pre-commit),git secrets)
$(error Looks like the git-secrets hooks are not installed. Please run 'git secrets --install')
endif
endif

.PHONY: check_branch check_branch_personal

%: %.template.py .FORCE
	python $< $@
.FORCE:

# The template output file depends on the template file, of course, as well as the environment. To be safe we force the
# template creation. This is what the fake .FORCE target does. It still is necessary to declare a target's dependency on
# a template to ensure correct ordering.
