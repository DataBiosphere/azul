Upgrading
---------

This file documents any upgrade procedure that must be performed. Because we
don't use a semantic version, a change that requires explicit steps to upgrade
a is referenced by its Github issue number. After checking out a branch that
contains a commit resolving an issue listed here, the steps listed underneath
the issue need to be performed. When switching away from that branch, to a
branch that does not have the listed changes, the steps would need to be
reverted. This is all fairly informal and loosely defined. Hopefully we won't
have too many entries in this file.

#1673 Ensure Lambda package hash is deterministic
=================================================

#. If you haven't yet, install Python 3.8.

#. Recreate your virtual environment::

    make virtualenv
    make requirements
    make envhook  # if you use PyCharm


#1645 Rethink template config variable mechanism
================================================

The format of environment variable 'AZUL_SUBDOMAIN_TEMPLATE' has been changed
and will need to be updated in personal deployment's 'environment.py' file.

Change ::

    'AZUL_SUBDOMAIN_TEMPLATE': '{{lambda_name}}.{AZUL_DEPLOYMENT_STAGE}',

to ::

    'AZUL_SUBDOMAIN_TEMPLATE': '*.{AZUL_DEPLOYMENT_STAGE}',


#1272 Use Lambda layers to speed up ``make deploy``
===================================================

Upgrading with these changes should work as expected.

If downgrading, however, you may encounter a Terraform cycle. This can be
resolved by running ::

    cd terraform
    make init
    terraform destroy -target aws_lambda_layer_version.dependencies_layer


#1577 Switch all deployments to DSS ``prod``
============================================

Please switch your personal deployments to point at the production instance of
the DSS. See the example configuration files in ``deployments/.example.local``
for the necessary configuration changes.


#556 Deploying lambdas with Terraform
=====================================

To deploy lambdas with Terraform you will need to remove the currently deployed
lambda resources using Chalice. Checkout the most recent commit *before* these
changes and run ::

    cd terraform
    make init
    terraform destroy $(terraform state list | grep aws_api_gateway_base_path_mapping | sed 's/^/-target /')
    cd ..
    make -C lambdas delete

If the last command fails with a TooManyRequests error, wait 1min and rerun it.

Switch back to your branch that includes these changes. Now use Chalice to
generate the new Terraform config. Run ::

    make deploy

And finally ::

    make terraform

If that command fails with a ResourceNotFoundException, wait 1min and rerun it.

In the unlikely case that you need to downgrade, perform the steps below.

Switch to the new branch you want to deploy. Run ::

    cd terraform
    rm -r indexer/ service/
    make init
    terraform destroy $(terraform state list | grep aws_api_gateway_base_path_mapping | sed 's/^/-target /')
    cd ..
    make terraform

This will remove the Lambda resources provisioned by Terraform. Now run ::

    make deploy

to set up the Lambdas again, and finally ::

    make terraform

To complete the API Gateway domain mappings, etc.

Run ::

    make deploy

a final time to work around a bug with OpenAPI spec generation.


#1637 Refactor handling of environment for easier reuse
=======================================================

1. Run ::

      python scripts/convert_environment.py deployments/foo.local/environment{,.local}

   where ``foo.local`` is the name of your personal deployment. This should
   create ``environment.py`` and possibly ``environment.local.py`` with
   essentially the same settings, but in Python syntax.

2. Close the shell, start a new one and activate your venv

3. Run ``source environment``

4. Run ``_select foo.local``

5. If you use ``envhook.py``

   i)   Reinstall it ::

          python scripts/envhook.py remove
          python scripts/envhook.py install

   ii)  Confirm that PyCharm picks up the new files via ``envhook.py`` by starting a Python console inside PyCharm or
        running a unit test

   iii) Confirm that running ``python`` from a shell picks up the new files via
        ``envhook.py``

6. Confirm that ``make deploy`` and ``make terraform`` still work
