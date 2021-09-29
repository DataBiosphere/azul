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


#3448 Make BQ slot location configurable
========================================

A new configuration variable has been added, ``AZUL_TDR_SOURCE_LOCATION``.
Set the variable to the storage location of the snapshots the deployment is
configured to index. Concurrently indexing snapshots with inconsistent locations
is no longer supported. As always, use the sandbox deployment's
``environment.py`` as a model when upgrading personal deployments.


#2750 Add partition_prefix_length to sources
============================================

The syntax of the ``AZUL_TDR_SOURCES`` and ``AZUL_TDR_…_SOURCES`` environment
variables was modified to include a partition prefix length. To specify a
partition prefix length within a source, append a slash delimiter ``/`` followed
by a partition length (e.g., ``/2``) to the source entry in the
deployment's ``environment.py`` . If the partition prefix length is not
specified in one of the above variables, the default value from
``AZUL_PARTITION_PREFIX_LENGTH`` will be used.
As always, use the sandbox deployment's ``environment.py`` as a template.


#2865 Allow catalog.internal to be configurable
===============================================

The definition of the ``AZUL_CATALOGS`` environment variable now requires
the ``internal`` property. All IT catalogs must have the ``internal`` property
set to ``True``, while for non-IT catalogs it must be set to ``False``.  As
always, use the sandbox deployment's ``environment.py`` as a model when
upgrading personal deployments.


#2495 Convert AZUL_CATALOGS to JSON
===================================

The definition of the ``AZUL_CATALOGS`` environment variable has been changed to
contain a JSON string. Personal deployments must be upgraded to reflect this
change in format. For details, refer to the specification within the
``environment.py`` file in the project root. As always, use the sandbox
deployment's ``environment.py`` as a model when upgrading personal deployments.


#3137 Increase lambda concurrency and BigQuery slots in prod
============================================================

If you set the variable `AZUL_INDEXER_CONCURRENCY` in your personal deployment,
replace the setting with two separate settings for
`AZUL_CONTRIBUTION_CONCURRENCY` and `AZUL_AGGREGATION_CONCURRENCY`. Also note
that you can now set different concurrencies for the retry lambdas.


#3080  Provision separate OAuth Client IDs for lower deployments
================================================================

1. Follow the instructions in section 3.2.2 of the README. For step 8, replace
   the previously configured Client ID with the one you just created in your
   `environment.py` file.

2. From the hca-dev Google Cloud console, navigate to *APIs & Services* ->
   *Credentials*

3. Select the `azul-dev` Client ID and click the pencil icon to edit

4. Delete the URL's corresponding to your deployment under
   *Authorized JavaScript origins* and *Authorized redirect URIs*

5. CLick *SAVE*

6. `_refresh`


#2978 Use public snapshots for unauthenticated service requests
===============================================================

A second Google service account, ``AZUL_GOOGLE_SERVICE_ACCOUNT_PUBLIC``, has
been added and needs to be registered and authorized with SAM. Run `_refresh`
and `make deploy` to create the service account and register it with SAM.

You can obtain the full email address of the public service account by running:
::

    python3 -c 'from azul.terra import TDRClient; print(TDRClient.with_public_service_account_credentials().credentials.service_account_email)'

This email must then be manually added to the group `azul-public-dev` by a team
member with administrator access (currently Hannes or Noah).


#2951 Add OAuth 2.0 authentication and log user IDs (#2951)
===========================================================

Follow the instructions in section 3.2.2 of the README


#2650 Add prefix to sources
===========================

Remove the ``azul_dss_query_prefix`` variable from any ``environment.py``
files for personal deployments in which ``AZUL_DSS_ENDPOINT`` is set to
``None``. For personal deployments in which that is not the case, rename the
variable to ``AZUL_DSS_QUERY_PREFIX``.

The syntax of ``AZUL_TDR_SOURCES`` and ``AZUL_TDR_…_SOURCES`` environment
variables was modified to include a UUID prefix. To upgrade a
deployment, append every source entry in the deployment's ``environment.py``
with a colon delimiter ``:`` followed by a valid hexadecimal prefix e.g.,
``:42``. For IT catalogs within a personal deployment set the source prefix to
an empty string. Failure to do so may cause IT errors. As always, use the
sandbox deployment's ``environment.py`` as a template.


#2950 Move auth and cart service to attic
=========================================

1. Before upgrading to this commit, run ::

      source environment
      _select foo
      (cd terraform && make validate && terraform destroy \
         -target=module.chalice_service.aws_api_gateway_rest_api.rest_api \
         -target=module.chalice_service.aws_api_gateway_deployment.rest_api )

2. Upgrade to this commit or a later one and run ::

      _refresh
      make deploy


#2755 Change AZUL_TDR_SOURCE to AZUL_TDR_SOURCES
================================================

Rename ``AZUL_TDR_SOURCE`` to ``AZUL_TDR_SOURCES`` and ``AZUL_TDR_…_SOURCE`` to
``AZUL_TDR_…_SOURCES``. Wrap the value of these entries in ``','.join([…,])``.
Yes, trailing comma after the entry, diverging from our guidelines, but these
entries will soon have multiple items and we want to start minimizing the
diffs from the onset.  If you have multiple ``AZUL_TDR_…_SOURCES`` entries of
the same value, consider interpolating a dictionary comprehension to eliminate
the duplication. As always, use the sandbox deployment's ``environment.py`` as
a template.


#2399 Reduce portal DB IT concurrency
=====================================

Reset the integrations portal database to its default state to ensure that no
pollution persists from previous IT failures ::

    python3 scripts/reset_portal_db.py


#2066 Add means for determining which catalogs are available
============================================================

The syntax of the value of the AZUL_CATALOGS environment variable was modified
to include an atlas name. In the future catalogs from other atlases will be
added, but at the moment all catalogs belong to the HCA atlas. To upgrade a
deployment, prepend every catalog entry in that variable with ``hca:``.


#2445 Example deployment is stale
=================================

This change does not modify any environment variables, it just streamlines
where and how they are set. Personal deployments most resemble the sandbox so it
makes sense to use the sandbox as a template instead of a dedicated example
deployment.

1.  Remove all ``environment.local`` files you may have lying around in your
    working copy. This commit removes the ``.gitignore`` rule for them so they
    should show up as new files. Before deleting such a file, check if you want
    to port any settings from it to the corresponding ``environment.local.py``.

2.  Synchronize ``deployments/sandbox/environment.py`` with the corresponding
    file in each of your personal deployments. You want the personal
    deployment's file to look structurally the same as the one for the sandbox
    while retaining any meaningful differences between your personal
    deployment and the sandbox. This will make it easier in the future to keep
    your personal deployment up-to date with the sandbox. I used PyCharm's
    diff editor for this but you could also copy the sandbox files and apply
    any differences as if it were the first time you created the deployment.

3.  Check your ``environment.local.py`` files for redundant or misplaced
    variables. Use the corresponding ``.example.environment.local.py`` files as
    a guide.


#2494 Move lower deployments to ``platform-hca-dev``
====================================================

1.  Before upgrading to this commit run ::

      source environment
      _select yourname.local
      _preauth
      ( cd terraform && make validate && terraform destroy \
          -target google_service_account.azul \
          -target google_project_iam_custom_role.azul \
          -target google_project_iam_member.azul )

2.  Upgrade to this commit or a later one

3.  Make sure that your individual Google account and you burner account are
    owners of the Google project ``platform-hca-dev``. Create a personal service
    account and obtain its private key. Be sure to set the environment variable
    ``GOOGLE_APPLICATION_CREDENTIALS`` to the new key.

4.  Ask to have your burner added as an admin of the ``azul-dev`` SAM group
    (`README sections 2.3.2 and 2.3.3`_).

5.  For your personal deployment, set ``GOOGLE_PROJECT`` to ``platform-hca-dev``
    and run ::

      _refresh && _preauth
      make package deploy

6.  When that fails to verify TDR access (it should, and the error message will
    contain the service account name), add your personal deployment's service
    account to the ``azul-dev`` SAM group (`README sections 2.3.2 and 2.3.3`_)
    and run ``make deploy`` again.

.. _README sections 2.3.2 and 2.3.3: ./README.md#232-google-cloud-credentials


#2658 Disable DSS plugin in all deployments
===========================================

In your personal deployment configuration,

* Remove any ``AZUL_CATALOGS`` entries that contain ``repository/dss``

* Unset any environment variables starting in ``AZUL_DSS_``

Use the `sandbox` deployment's configuration as a guide.


#2246 Add deployment incarnation counter
========================================

See instructions for #2143 below.


#2143 Merge service accounts for indexer and service
====================================================

1. Before upgrading to this commit, run ::

      source environment
      _select foo
      _preauth
      (cd terraform && make validate && terraform destroy -target=google_service_account.indexer)


2. Upgrade to this commit or a later one and run ::

      _refresh
      _preauth
      make package deploy

3. If this fails—it should—with

      azul.RequirementError: Google service account
      azul-ucsc-0-foo@human-cell-atlas-travis-test.iam.gserviceaccount.com is
      not authorized to access the TDR BigQuery tables. Make sure that the SA
      is registered with SAM and has been granted repository read access for
      datasets and snapshots.

   let someone who can administer the SAM group that controls access to TDR
   know of the renamed service account via Slack. The administrator will need
   to replace the old service account email with the new one. For example, 
   ask them to replace
   
   ``azul-ucsc-indexer-foo@human-cell-atlas-travis-test.iam.gserviceaccount.com``
   
   with 

   ``azul-ucsc-0-foo@human-cell-atlas-travis-test.iam.gserviceaccount.com``

4. Run ::

      make -C terraform sam

   which should now succeed.


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
