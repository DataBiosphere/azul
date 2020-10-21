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


#2332 Version of pip used by build ignores wheel for gevent
===========================================================

Run ``make requirements``.


#1921 Incorporate symlink fix from Chalice upstream
===================================================

Run ``make requirements``.


#2318 Switch dcp2 catalog to optimusb snapshot
==============================================

Update ``$AZUL_TDR_SOURCE`` in personal deployments.


#1764 Adapt /dss/files proxy to work with v2 and TDR
====================================================

Run ``make requirements``.


#1398 Locust script is stale
============================

Run ``pip uninstall locustio && make requirements``.


#2313 Fix `make requirements_update` assertion failure
======================================================

Run ``make requirements``.


#2269 Fix: `make requirements_update` fails in `sed` on macOS
=============================================================

Run ``make requirements``.


#2261 Fix: `make requirements_update` may use stale docker image layer
======================================================================

Run ``make requirements``.


#2149 Update DCP2 catalog to `hca_ucsc_files___20200909` snapshot
=================================================================

Change ``AZUL_TDR_SOURCE`` in personal deployments to refer to the snapshot
mentioned in the title above.


#2025 Register indexer SA with Broad's SAM during deployment
============================================================

This PR introduces two new deployment-specific environment variables,
``AZUL_TDR_SERVICE_URL`` and ``AZUL_SAM_SERVICE_URL``. Copy the settings for
these variables from the example deployment to your personal deployment.

Service accounts must be registered and authorized with SAM for integration
tests to pass. See `section 3.2.1`_ of the README for registration instructions.

.. _section 3.2.1: ./README.md#321-tdr-and-sam


#2069 Upgrade PyJWT to 1.7.1
============================

The PyJWT dependency has been pinned from v1.6.4 to v1.7.1. Update by doing
`make requirements`.


#2112 Upgrade Chalice version to 1.14.0+5
=========================================

The Chalice dependency was updated. Run ::

    make requirements


#2149 Switch to TDR snapshot hca_dev_20200817_dssPrimaryOnly
============================================================

Change ``AZUL_TDR_SOURCE`` in personal deployments to refer to the snapshot
mentioned in the title above.


#2071 Separate ES domain for sandbox and personal deployments
=============================================================

1. Before upgrading to this commit, and for every one of your personal
   deployments, run ::

     python scripts/reindex.py --delete --catalogs it1 it2 dcp1 dcp2

   to delete any indices that deployment may have used on the ``dev`` ES domain.

2. Upgrade to this commit or a later one.

3. For each personal deployment:

   a. Configure it to share an ES domain with the sandbox deployment. See
      example deployment for details.

   b. Run ``make package``

   c. Run ``make deploy``

   d. Run ``make create``

   e. Run ``make reindex``


#2015 Change DRS URLs to Broad resolver
=======================================

Rename `AZUL_TDR_TARGET` to `AZUL_TDR_SOURCE` in `environment.py` files for
personal deployments.


#2025 Register indexer SA with Broad's SAM during deployment
============================================================

This PR introduces two new deployment-specific environment variables,
``AZUL_TDR_SERVICE_URL`` and ``AZUL_SAM_SERVICE_URL``. Copy the settings for
these variables from the sandbox deployment to your personal deployment.


#2011 Always provision indexer service account
==============================================

The indexer service account is provisioned, even if ``AZUL_SUBSCRIBE_TO_DSS`` is
0. Make sure that ``GOOGLE_APPLICATION_CREDENTIALS`` is set in
``environment.local.py`` for all deployments that you use.


#1644 Replace `azul_home` with `project_root`
=============================================

Replace references to ``azul_home`` with ``project_root`` in personal deployment
files (``environment.local.py`` and
``deployments/*.local/environment{,.local}.py``).


#1719 Upgrade Elasticsearch version to 6.8
==========================================

The personal deployments that share an ES domain with ``dev`` need to be
redeployed and reindexed::

    make package
    make deploy
    make reindex


#1770 Move `json-object` wheel from lambda packages to layer package
====================================================================

Run ::

    rm -r lambdas/service/vendor/jsonobject* lambdas/indexer/vendor/jsonobject*

To ensure ``json-object`` is only deployed via the dependencies layer.


#1673 Ensure Lambda package hash is deterministic
=================================================

#. If you haven't yet, install Python 3.8.

#. Recreate your virtual environment::

    make virtualenv
    make requirements
    make envhook  # if you use PyCharm

#. If you use PyCharm, update your interpreter settings by going to
   ``Settings > Project: azul > Project Interpreter``. From the drop down,
   select ``Show All``. Use the minus sign to remove the Python 3.6 entry
   at ``azul/.venv/bin/python``. Then use the plus sign to add the newly
   generated Python 3.8 interpreter, located at the same path as the one you
   just removed.


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
