Upgrading
---------

.. |deprecated| raw:: html

   <strike>

.. |end_deprecated| raw:: html

   </strike>


This file documents any upgrade procedure that must be performed. Because we
don't use a semantic version, a change that requires explicit steps to upgrade a
is referenced by its Github issue number. After checking out a branch that
contains a commit resolving an issue listed here, the steps listed underneath
the issue need to be performed. When switching away from that branch, to a
branch that does not have the listed changes, the steps would need to be
reverted. This is all fairly informal and loosely defined. Hopefully we won't
have too many entries in this file.


#5246 Route SNS notifications through a Lambda function
=======================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.

After the merge commit builds on GitLab, follow the instructions in the
README.md, for section 3.2.3 titled `Transition Amazon SES resource out of sandbox`.
This needs to be done for all main deployments, except for ``dev``.


#5703 Consolidate dependency updates into single bi-weekly issue
================================================================

Operator
~~~~~~~~

Run ``make -C terraform/gitlab/runner`` with the ``gitlab`` component of every
main deployment selected just before pushing the PR branch to the GitLab
instance in that deployment. If the PR has to be sent back, checkout ``develop``
and run that command again in all deployments where it was run with the PR
branch checked out.

Deploy the ``shared`` component of any main deployment just before pushing the
PR branch to the GitLab instance in that deployment. Do so with the PR branch
checked out. You will need to use the ``CI_COMMIT_REF_NAME=develop`` override
for that. Notify team members that their local development work will be impacted
until they rebase their branches to the PR branch or until this PR is merged and
they rebase their branches onto ``develop``. If the PR has to be sent back,
checkout ``develop`` and deploy the ``shared`` component again in any deployment
where it was deployed with the PR branch checked out, and notify the developers
to rebase their branches on ``develop`` again.

Deploy the ``gitlab`` component of any main deployment just before pushing the
merge commit to the GitLab instance in that deployment.


#5617 False positive AWS Inspector findings after GitLab deploy
===============================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5612 AWS Inspector fails to post findings to SNS topic
=======================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


DataBiosphere/azul-private#110 Reduce predictability of manifest keys
=====================================================================

Operator
~~~~~~~~

Before pushing the PR branch to the ``sandbox``, ``anvilbox``, or ``hammerbox``
deployments, manually deploy the ``gitlab`` component of the corresponding main
deployment. You will likely need assistance from the system administrator
because this particular change modifies the boundary policy. If the PR fails
during testing and is not merged, roll back the changes made to the main
deployments by deploying the ``gitlab`` component from the ``develop`` branch.

When deploying to ``prod``, manually deploy ``prod.gitlab`` just before
pushing the merge commit to the GitLab instance.


#4982 Update to Python 3.11.x
=============================

Everyone
~~~~~~~~

Update Python on your developer machines to version 3.11.5. In your working
copy, run ``make virtualenv`` and ``make requirements envhook``.

Operator
~~~~~~~~

Before pushing the PR branch to the ``sandbox``, ``anvilbox``, or ``hammerbox``
deployments, manually deploy the ``shared`` component of the corresponding main
deployment. If the PR fails during testing and is not merged, roll back the
changes made to the main deployments by deploying the ``shared`` component from
the ``develop`` branch.

When deploying to ``prod``, manually deploy ``prod.gitlab`` just before
pushing the merge commit to the GitLab instance.

#5518 GitLab updates cause false positive insufficient_data alarms
==================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


DataBiosphere/azul-private#108 Resolve vulnerabilities in docker image
======================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` & ``gitlab`` components (in that order) of any
main deployment, and with the ``gitlab`` component selected, run ``make -C
terraform/gitlab/runner`` just before pushing the merge commit to the GitLab
instance in that deployment.


DataBiosphere/azul-private#103 Resolve vulnerabilities in azul-pycharm
======================================================================

Operator
~~~~~~~~

Before pushing the PR branch to the ``sandbox``, ``anvilbox``, or ``hammerbox``
deployments, manually deploy the ``shared`` component of the corresponding main
deployment. If the PR fails during testing and is not merged, roll back the
changes made to the main deployments by deploying the ``shared`` component from
the ``develop`` branch.

When deploying to ``prod``, manually deploy ``prod.gitlab`` just before
pushing the merge commit to the GitLab instance.


DataBiosphere/azul-private#93 Resolve vulnerabilities in azul-elasticsearch
===========================================================================

Operator
~~~~~~~~

Before pushing the PR branch to the ``sandbox``, ``anvilbox``, or ``hammerbox``
deployments, manually deploy the ``shared`` component of the corresponding main
deployment. If the PR fails during testing and is not merged, roll back the
changes made to the main deployments by deploying the ``shared`` component from
the ``develop`` branch.

When deploying to ``prod``, manually deploy ``prod.shared`` just before
pushing the merge commit to the GitLab instance.


DataBiosphere/azul-private#94 Resolve vulnerabilities in azul-pycharm
=====================================================================

Operator
~~~~~~~~

Before pushing the PR branch to the ``sandbox``, ``anvilbox``, or ``hammerbox``
deployments, manually deploy the ``shared`` component of the corresponding main
deployment. If the PR fails during testing and is not merged, roll back the
changes made to the main deployments by deploying the ``shared`` component from
the ``develop`` branch.

When deploying to ``prod``, manually deploy ``prod.gitlab`` just before
pushing the merge commit to the GitLab instance.


#5301 Alarm on detection of new vulnerabilities by Inspector
============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` and ``gitlab`` component (in that order) of any
main deployment just before pushing the merge commit to the GitLab instance in
that deployment.


#5518 GitLab updates cause false positive insufficient_data alarms
==================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5552 Increase retention of non-current object versions in shared bucket
========================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


DataBiosphere/azul-private#15 Insecure Transportation Security Protocol Supported (TLS 1.0)
===========================================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5189 Delete unused Docker images from ECR
==========================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment. Retain a
terminal transcript for each deployment so that the author can diagnose any
issues that may come up.


#4468 Logs by different containers are hard to distinguish
==========================================================

Manually deploy the ``gitlab`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5408 Prepare for vacation
==========================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment. Only the
``prod.gitlab`` deployment should actually have a non-empty plan.


DataBiosphere/azul-private#95 Resolve vulnerabilities in AMI for GitLab
=======================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5301 Alarm on detection of new vulnerabilities by Inspector
============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5363 Noisy alarm from EC2 for CreateNetworkInterface during initial deploy
===========================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5408 Prepare for vacation
==========================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5139 CloudWatch metrics and alarms for GitLab EC2 instance
===========================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5155 Update AnVIL catalogs in `anvilprod` with replacement snapshots
=====================================================================

Update the snapshots for any personal deployments that share an Elasticsearch
domain with ``hammerbox``, using that deployment's ``environment.py`` as a
template.


#5413 Make anvildev and anvilbox public
=======================================

Operator
~~~~~~~~

The ``deploy`` job will fail for ``anvildev`` when building the merge commit on
the ``develop`` branch. It may also fail for ``anvilbox`` when building the feature
branch. The expected failure produces the following output::

   ╷
   │ Error: updating REST API (1yxdxpa3db): BadRequestException: Cannot update endpoint from PRIVATE to EDGE
   │
   │   with aws_api_gateway_rest_api.indexer,
   │   on api_gateway.tf.json line 862, in resource[6].aws_api_gateway_rest_api[0].indexer:
   │  862:                     }
   │
   ╵
   ╷
   │ Error: updating REST API (pmmwi1i8la): BadRequestException: Cannot update endpoint from PRIVATE to EDGE
   │
   │   with aws_api_gateway_rest_api.service,
   │   on api_gateway.tf.json line 1467, in resource[24].aws_api_gateway_rest_api[0].service:
   │ 1467:                     }
   │
   ╵

To work around this, check out the respective branch and perform the commands
below. If you have the feature branch checked out, you will need to prefix the
``make`` invocations with ``CI_COMMIT_REF_NAME=develop``. ::

   make lambdas
   cd terraform
   make validate
   terraform taint aws_api_gateway_rest_api.indexer
   terraform taint aws_api_gateway_rest_api.service

Retry the ``deploy`` job on GitLab. It should succeed now. If the subsequent
``integration_test`` job fails with 403 or 503 errors returned by the service or
indexer, simply retry it. It appears that the edge distribution process in AWS
is subject to several minutes of latency aka eventual consistency.


#5292 Update/harden docker.elastic.co/elasticsearch/elasticsearch
=================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5407 False positive for unauthorized alarm from MandoService
=============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5298 Keep docker Docker images updated
=======================================

Operator
~~~~~~~~

Manually deploy the ``shared`` & ``gitlab`` components (in that order) of any
main deployment, and with the ``gitlab`` component selected, run ``make -C
terraform/gitlab/runner`` just before pushing the merge commit to the GitLab
instance in that deployment.


#5400 Make anvilprod public
===========================

Operator
~~~~~~~~

The ``deploy`` job will fail for ``anvilprod`` when building the merge commit on
the ``develop`` branch. It may also fail for ``hammerbox`` when building the feature
branch. The expected failure produces the following output::

   ╷
   │ Error: updating REST API (1yxdxpa3db): BadRequestException: Cannot update endpoint from PRIVATE to EDGE
   │
   │   with aws_api_gateway_rest_api.indexer,
   │   on api_gateway.tf.json line 862, in resource[6].aws_api_gateway_rest_api[0].indexer:
   │  862:                     }
   │
   ╵
   ╷
   │ Error: updating REST API (pmmwi1i8la): BadRequestException: Cannot update endpoint from PRIVATE to EDGE
   │
   │   with aws_api_gateway_rest_api.service,
   │   on api_gateway.tf.json line 1467, in resource[24].aws_api_gateway_rest_api[0].service:
   │ 1467:                     }
   │
   ╵

To work around this, check out the respective branch perform the commands below.
If you have the feature branch checked out, you will need to prefix the ``make``
invocations with ``CI_COMMIT_REF_NAME=develop``. ::

   make lambdas
   cd terraform
   make validate
   terraform taint aws_api_gateway_rest_api.indexer
   terraform taint aws_api_gateway_rest_api.service

Retry the ``deploy`` job on GitLab. It should succeed now. If the subsequent
``integration_test`` job fails with 403 or 503 errors returned by the service or
indexer, simply retry it. It appears that the edge distribution process in AWS
is subject to several minutes of latency aka eventual consistency.


#5189 Delete unused Docker images from ECR
==========================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5291 Suppress unauthorized alarms for visiting Inspector console
=================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5299 Keep Python updated
=========================

Everyone
~~~~~~~~

Update Python on your developer machines to version 3.9.17.

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5289 Fix: _select doesn't validate its argument
================================================

Set the environment variable ``azul_google_user`` in all deployments to your
``…@ucsc.edu`` email address. The easiest way to do that is in an
``environment.local.py`` at the project root.

Many of the shell functions defined in ``environment`` have been renamed. To
avoid stale copies of these functions lingering around under their old names,
exit all shells in which you sourced that file.


#5325 Exclude noisy events from api_unauthorized alarm
======================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5280 Enable FIPS mode on GitLab instance
=========================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, just before
pushing the merge commit to the GitLab instance in that deployment.


#5283: Swap anvilprod and anvildev
==================================

Update any personal deployments you own in AWS account ``platform-anvil-dev`` to
mirror the configuration of the ``anvilbox`` deployment. Specifically, you will
need to update the list of sources for the ``anvil`` catalog and the TDR and SAM
endpoints. You will also need to ask the system administrator to move the Terra
group memebership of the indexer service account of any such personal deployment
from ``azul-anvil-prod`` in Terra production to ``azul-anvil-dev`` in TDR
development. Redeploy and reindex those deployments after updating their
configuration.

All indices in the Elasticsearch domains for ``anvildev`` and ``anvilbox`` have
been deleted, including the indices of personal deployments that share an
Elasticsearch domain with ``anvilbox``,  regardless of whether these indices
contained managed-access or public snapshots. In order to recover from the loss
of these indices in your personal deployment, you will need to reindex that
deployment.


#5260 Fix: Inconsistent bucket names and CloudFront origin IDs in anvildev
==========================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of the ``anvildev`` deployment just
before pushing the merge commit to the GitLab instance in that deployment. When
the ``deploy_browser`` job of the ``deploy`` stage fails on GitLab, manually
empty and delete the S3 buckets ``anvil.explorer.gi.ucsc.edu`` and
``anvil.gi.ucsc.edu`` in ``platform-anvil-dev`` . Retry the job.


#5226 Sporadic DNS resolution errors on GitLab
==============================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5232 Fix: Operators should have SSH access to anvildev and anvilprod
=====================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment, except
``prod``, just before pushing the merge commit to the GitLab instance in that
deployment.


#5015 Prepare platform-anvil-prod for compliance assessment
===========================================================

Everyone
~~~~~~~~

Update Python on your developer machines to version 3.9.16.

Create a `personal access token`_ on every GitLab instance you have access to
and specify that token as the value of the ``azul_gitlab_access_token`` in your
``environment.local.py`` for the main deployment collocated with that instance.
See the documentation of that variable in the top-level ``environment.py`` for
the set of scopes (permissions) to be assigned to the token. Refresh the
environment and run ``_preauth``.

.. _personal access token: https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html

Operator
~~~~~~~~

Follow the steps for everyone listed above.

Just before pushing the feature branch to a GitLab instance, locally merge the
feature branch into ``develop`` — without pushing the resultimg merge commit —
and deploy the merge commit to the ``shared`` & ``gitlab`` components (in that
order) of the main deployment for that GitLab instance. When the PR cannot be
merged for any reason, undo the merge locally by resetting the ``develop``
branch to the prior commit and manually deploy the ``develop`` branch to
``shared`` & ``gitlab`` components (in that order) of the main deployment for
that GitLab instance.

If deploying the ``gitlab`` component results in an ``OptInRequired`` error,
login to the AWS Console using credentials for the AWS account that contains the
GitLab instance and visit the URL that is included in the error message. This
will enable the required AWS Marketplace subscription for the CIS-hardened
image.

With the ``gitlab`` component selected, run ``make -C terraform/gitlab/runner``.

#3894 Send GitLab host logs to CloudWatch
=========================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5207 Fix: Partition sizing ignores supplementary bundles
=========================================================

Subgraph counts have been updated for `anvildev` and `anvilbox`. If you have any
personal deployments that index these snapshots, update the subgraph counts
accordingly.


#4022 Encrypt GitLab data and root volume and snapshots
=======================================================

Operator
~~~~~~~~

Prior to pushing the merge commit to a GitLab instance, login to the AWS
Console and navigate to `EC2` -> `Instances` -> select the GitLab instance ->
`Storage` to confirm that root volume is encrypted.

If the root volume is not encrypted, manually deploy the ``gitlab`` component of
a deployment just before pushing the merge commit to the GitLab instance in that
deployment.


#5043 S3 server access logs are inherently incomplete
=====================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5133 Trigger an alarm on absence of logs
=========================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5110 Update GitLab IAM policy for FedRAMP inventory
====================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4218 Configure WAF with rules
==============================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment immediately
before the first time this change is pushed to the GitLab instance for that
main deployment, regardless of whether the changes come as part of a feature
branch, a merge commit or in a promotion.


#3911 Disallow ``||`` joiners in metadata
=========================================

A new catalog ``dcp3`` has been added to ``dev`` and ``sandbox`` deployments.
Add the ``dcp3`` catalog to your personal deployments using the sandbox
deployment's ``environment.py`` as a model.


#5116 Enable NIST 800.53 conformance pack for AWS Config
========================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4713 S3 Block Public Access setting should be enabled
======================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#5071 s3_access_log_bucket_policy includes redundant condition on source account
================================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4960 S3 server access logging for shared bucket
================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4189 Scan GitLab EC2 instance with Amazon Inspector
====================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just *before*
pushing the merge commit to the GitLab instance in that deployment. The
Terraform code that enables Amazon Inspector is currently unreliable. Check
the Amazon Inspector console to see if it is enabled. If you see a *Get
started …* button, it is not, and you need to repeat this step.


#5019 Index public & mock-MA snapshots in anvilprod
===================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just *before*
pushing the merge commit to the GitLab instance in that deployment.


#3634 Automate creation of a FedRAMP Integrated Inventory Workbook
==================================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment just *before*
pushing the merge commit to the GitLab instance in that deployment.

Afterwards, edit the existing schedule in the Azul project on that GitLab
instance. Its description is ``Sell unused BigQuery slot commitments``. You may
need to ask a system administrator to perform make these changes on your behalf.

1) Set the Cron timezone to ``Pacific Time (US & Canada)``

2) Set the variable ``azul_gitlab_schedule`` to ``sell_unused_slots``


Add another schedule:

1) Set the description to ``Prepare FedRAMP inventory``

2) Set the interval pattern to ``0 4 * * *``

3) Set the Cron timezone to ``Pacific Time (US & Canada)``

4) Set the variable ``azul_gitlab_schedule`` to ``fedramp_inventory``


#5004 Enable access logging on AWS Config bucket
================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4176 Enable VPC flow logs
==========================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` and  ``shared`` components of any main deployment
just before pushing the merge commit to the GitLab instance in that deployment.


#4918 Rename shared (aka versioned aka config) bucket (PR 2 of 2)
=================================================================

This change removes the old shared (aka versioned aka config) bucket and
switches all deployments to the replacement.

Everyone
~~~~~~~~

When requested by the operator, remove the ``AZUL_VERSIONED_BUCKET`` variable
from all of your personal deployments, then deploy this change to all of them.
Notify the operator when done.

Operator
~~~~~~~~

1. After pushing the merge commit for this change to ``develop`` on GitHub,
   request that team members upgrade their personal deployments. Request that
   team members report back when done.

2. Manually deploy the ``gitlab`` component of any main deployment just *before*
   pushing the merge commit to the GitLab instance in that deployment.

3. Manually deploy the ``shared`` component of any main deployment just *after*
   this change was deployed to all collocated deployments, both personal and
   shared ones.

Promote this change separately from the previous one, and when promoting it,
follow steps 2 and 3 above.


#4918 Rename shared (aka versioned aka config) bucket (PR 1 of 2)
=================================================================

This change creates the new bucket with the correct name, sets up replication
between the old and the new bucket so that future object versions are copied,
and runs a batch migration of prior and current objects versions. The next PR
will actually switch all deployments to using the new bucket.

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4966 Chatbot role policy is too restrictive and causes persistent alarms
=========================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4958 Storage bucket is still being removed from TF state
=========================================================

Everyone
~~~~~~~~

PR #4926 for issue #4646 left in place code to remove the S3 storage bucket
from the Terraform state. We'll refer to the changes from that PR as *broken*
and the changes for #4958 described here as *this fix*. The broken upgrading
instructions have been deprecated. When you follow these instructions, be
sure you have this fix checked out, or a commit that includes it.

There are three possible cases to consider when upgrading a deployment. Pick
the one applicable to the deployment being upgraded and only follow the steps
listed under that case:

Case A:
   If you have already deployed the broken changes once, and have not yet
   attempted to deploy again, verify that ::

      (cd terraform && make init && terraform state show aws_s3_bucket.storage)

   produces output that includes the following lines::

      # aws_s3_bucket.storage:
      resource "aws_s3_bucket" "storage" {

   Then deploy this fix.

Case B:
   If you have already deployed the broken changes, and then attempted to
   deploy them again, the affected deployment needs to be repaired. A symptom
   of the breakage is that the command ::

      (cd terraform && make init && terraform state show aws_s3_bucket.storage)

   fails with the message *No instance found for the given address*.

   To repair the deployment, run ::

      (cd terraform && make validate && terraform import aws_s3_bucket.storage $AZUL_S3_BUCKET)

   Then deploy this fix. Afterwards, confirm that ::

      (cd terraform && make init && terraform state show aws_s3_bucket.storage)

   produces no error but instead output that includes the following lines::

      # aws_s3_bucket.storage:
      resource "aws_s3_bucket" "storage" {

Case C:
   If you have *not* yet deployed the broken changes, first run the following
   command::

      (cd terraform && make init && terraform state rm aws_s3_bucket.storage)

   This will cause Terraform to leave the old bucket in place when you
   deploy this fix, and create a new one alongside it.

   Next, in personal deployments only, specify a name for the new bucket by
   changing the value of ``AZUL_S3_BUCKET`` in ``environment.py`` to ::

      "edu-ucsc-gi-{account}-storage-{AZUL_DEPLOYMENT_STAGE}.{AWS_DEFAULT_REGION}"

   where ``{account}`` is the name of the AWS account hosting the deployment,
   e.g., ``"platform-hca-dev"``. As always, use the sandbox deployment's
   ``environment.py`` as a model when upgrading personal deployments.

   For main deployments, the update to ``AZUL_S3_BUCKET`` has already been
   made.

   Then deploy this fix. **Afterwards, manually delete the old storage bucket
   for the deployment.** 

   Finally, verify that ::

      (cd terraform && make init && terraform state show aws_s3_bucket.storage)

   produces output that includes the following lines ::

      # aws_s3_bucket.storage:
      resource "aws_s3_bucket" "storage" {

Operator
~~~~~~~~

Follow the instructions in case A above for ``sandbox``, ``dev``,
``anvilbox``, and ``anvildev``. As part of the now deprecated upgrading steps
for #4646, the old storage buckets for these deployments should already have
been removed. Confirm that this is still the case.

Announce for other developers to upgrade their personal deployments.

When promoting this fix to ``prod``, follow the instructions in case C above.


#4646 Rename Azul storage buckets
=================================

This section has been deprecated. If you've already followed the steps
included here, please read the section for #4958 above.

|deprecated|

After these changes are successfully merged to ``develop``, manually delete the
old storage buckets for ``sandbox``, ``dev``, ``anvilbox``, and ``anvildev``.
Then announce for all other developers to follow the instructions in the section
below.

After these changes are successfully merged to ``prod``, manually delete the old
storage bucket for ``prod``.

Everyone
~~~~~~~~

For each of your personal deployments, change the value of ``AZUL_S3_BUCKET`` in
``environment.py`` to ::

    "edu-ucsc-gi-{account}-storage-{AZUL_DEPLOYMENT_STAGE}.{AWS_DEFAULT_REGION}"

Where ``{account}`` is the name of the AWS account hosting the deployment, e.g.,
``"platform-hca-dev"``. As always, use the sandbox deployment's
``environment.py`` as a model when upgrading personal deployments.

After the changes are deployed to a given personal deployment, manually delete
the old storage bucket for that deployment.

|end_deprecated|


#4011 Integrate monitoring SNS topic with Slack
===============================================

Operator
~~~~~~~~

Before pushing a merge commit with these changes to a GitLab instance, `set up
AWS Chatbot <./README.md#313-aws-chatbot-integration-with-slack>`_ in the AWS
account hosting that instance. AWS Chatbot has already been set up in the
``platform-hca-dev`` account. Once AWS Chatbot is set up, manually deploy the
``shared`` component of the main deployment collocated with the GitLab instance
you will be pushing to.


#4673 Eliminate burner accounts
===============================

Operator
~~~~~~~~

Complete the steps in the next section. Then announce on `#team-boardwalk` for
other developers to do the same.

Everyone
~~~~~~~~

When notified by the operator, complete the following steps:

#. Remove your burner account from the Google Cloud project:

   #. Go to the Google Cloud console, select the `platform-hca-dev` project,
      and navigate to ``IAM & Admin`` -> ``IAM``

   #. Select your burner; it includes the string "…ucsc.edu@gmail.com"

   #. Click ``REMOVE ACCESS`` -> ``CONFIRM``

#. Close your burner Google account:

   #. Sign in to Google using your burner email account. Click on the icon with
      your burner's name initial (upper right-hand of the page), click the
      ``Manage your Google Account`` button, and navigate to ``Data & Privacy``

   #. At the bottom of the page, under ``More options``, click on the
      ``Delete your Google Account`` button. Complete Google's requisites and
      terminate your burner account by clicking on ``Delete Account``

#. Make sure to register your UCSC account with SAM as `described
   <./README.md#234-google-cloud-tdr-and-sam>`_ in the README.


#4907 CIS 2.6 (S3 access logging on CloudTrail bucket) still flagged in dev
===========================================================================

Operator
~~~~~~~~

Manually deploy the ``dev.shared`` component just before pushing the merge
commit to GitLab ``dev``.


#4880 Alarms for CIS recommendations treat missing data as OK
=============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4832 Disable original CloudTrail trail
=======================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment. This
deployment is expected not to change any resources; everything should be handled
by the ``rename_resources`` script. Do not proceed with the deployment if the
plan shows any changes to the resources.


#4794 Ensure log metric filters and alarms exist for CIS recommendations
========================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4807 Move monitoring SNS topic to shared component
===================================================

Operator
~~~~~~~~

Manually deploy the ``gitlab`` component of any main deployment immediately
before the first time this change is pushed to the GitLab instance for that
main deployment, regardless of whether the changes come as part of a feature
branch, a merge commit or in a promotion. This is to ensure that the GitLab
instance has sufficient permissions to deploy these changes.

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment. Expect to
confirm the SNS subscription for each deployment while doing so.


#4792 Ensure S3 bucket access logging is enabled on the CloudTrail S3 bucket
============================================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4831 Move CloudTrail trail to default region
=============================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4764 Ensure security contact information is registered
=======================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4692 Ensure IAM password policies have strong configurations
=============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4793 Create support role to manage incidents with AWS support
==============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4196 Enable sending of CloudTrail events to CloudWatch logs
============================================================

Operator
~~~~~~~~

Manually deploy the ``shared`` component of any main deployment just before
pushing the merge commit to the GitLab instance in that deployment.


#4224 Eliminate personal service accounts
=========================================

When this PR lands in the main deployment in a given Google cloud project, the
operator should perform the following steps *in that project*, and then announce
for the other developers to do the same *in that project*.

#. Delete your personal Google service account:

   #. Go to the Google Cloud console, select the appropriate project, and
      navigate to ``IAM & Admin`` -> ``Service Accounts``

   #. Select your personal service account. This is the one where the part
      before the ``@`` symbol exactly matches your email address; it does not
      include the string "azul").

   #. Click ``DISABLE SERVICE ACCOUNT`` -> ``DISABLE``.

   #. Click ``DELETE SERVICE ACCOUNT`` -> ``DELETE``.

#. Delete the local file containing the private key of the service account that
   you deleted during step 1. Such files are usually stored in ``~/.gcp/``.

#. Remove the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable from
   ``environment.local.py`` for all Azul deployments (including non-personal
   deployments) where that variable references the key file that you deleted in
   step 2.

#. For clarity's sake, remove comments referencing the
   ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable from
   ``environment.py`` for all personal deployments that were changed during step
   3. As always, use the sandbox deployment's ``environment.py`` as a model when
   upgrading personal deployments.


#4752 On replacement, Terraform creates ES domain before deleting it
====================================================================

Note: The ``apply`` and ``auto_apply`` targets in ``terraform/Makefile`` do not
recurse into the sibling ``lambdas`` directory anymore. The only way to get a
proper deployment is to run ``make deploy`` or ``make auto_deploy`` in the
project root. This change speeds up the ``apply`` and ``auto_apply`` targets
for those who know what they are doing™.

Note: The ``post_deploy`` target is gone. The ``deploy`` target has been renamed
to ``terraform``. The new ``deploy`` target depends on the ``terraform`` target
and invokes the post-deplot scripts directly. The same goes for ``auto_deploy``
and ``auto_terraform`` respectively.

Ensure that the ``comm`` utility is installed. The `clean` target in most
Makefiles depends on it.

This is a complicated change that involves renaming lots of resources, both in
TF config and in state. If a deployment is stale or borked, upgrading to this
change is just going to make things worse. Before upgrading any deployment to
this commit, or more precisely, the merge commit that introduces this change,
first check out the previous merge commit, and deploy while following any
upgrade instructions up to that commit. Then run ``make clean``, check out this
commit and run ``make deploy``.


#4688 Fix: Elasticsearch domains should be in a VPC
===================================================

Everyone
~~~~~~~~

Perform the steps listed below for all personal deployments that don't share an
ES domain with a shared deployment. The deletion of the ES domain will cascade
to many other resources that depend on it. Once the deletion is complete, it is
necessary to re-deploy the missing resources and perform a reindex to repopulate
the newly created ES domain::

    (cd terraform && make validate && terraform destroy -target aws_elasticsearch_domain.index)
    make deploy
    make reindex

Operator
~~~~~~~~

Before pushing the PR branch to ``sandbox`` or ``anvilbox``, notify the team
that personal deployments sharing the Elasticsearch domain with that deployment
will lose their indices.

For any shared deployment, perform the first of the above steps after the
GitLab ``deploy`` job fails in that deployment. Then retry the ``deploy`` job.
When that succeeds, start the ``reindex`` or ``early_reindex`` job.

When reindexing completes in the ``sandbox`` or ``anvilbox`` deployments,
request that team members re-deploy and reindex all personal deployments that
share the Elasticsearch domain with that deployment.


#4334 Upgrade Terraform CLI to 1.3.4
====================================

Before upgrading personal deployments, install Terraform 1.3.4 as `described
<./README.md#21-development-prerequisites>`_ in our README. Then run ``make
deploy``.


#4690 Fix: EC2 instances should use Instance Metadata Service Version 2 (IMDSv2)
================================================================================

Operator
~~~~~~~~

The steps below have already been performed on ``anvildev.gitlab``, but need to
be run for ``dev.gitlab`` and ``prod.gitlab``, run::

    _select dev.gitlab
    make -C terraform/gitlab


#4691 Fix: S3 Block Public Access setting should be enabled at the bucket-level
===============================================================================

This change blocks public access for all S3 buckets in the shared component and
in all deployments.

Everyone
~~~~~~~~

Run `make deploy` to update personal deployments as soon as your are notified on
Slack by the operator.

Operator
~~~~~~~~

Follow these steps to deploy for ``dev.shared``, ``anvildev.shared``, and
``prod.shared``::

    _select dev.shared
    make -C $project_root/terraform/shared apply


#4625 Disable URL shortener
===========================

Everyone
~~~~~~~~

In personal deployments, remove ``AZUL_URL_REDIRECT_BASE_DOMAIN_NAME`` and
``AZUL_URL_REDIRECT_FULL_DOMAIN_NAME``. As always, use the sandbox deployment's
``environment.py`` as a model when upgrading personal deployments.

Operator
~~~~~~~~

After this change lands in ``dev``, follow these instructions for the AWS
account ``platform-hca-dev``:

#. Ask everyone to upgrade their personal deployments in that account.

#. In the AWS console, navigate to *Route53 service* → *Hosted zones*.

#. Open the hosted zone ``dev.url.singlecell.gi.ucsc.edu`` and check for
   records of type ``CNAME``. If there are any, contact the owner of the
   corresponding deployment. Their deployment wasn't upgraded properly. As a
   last resort, remove the CNAME record. If there are records for the
   ``sandbox`` or ``dev`` deployments, contact the lead. Ultimately, there
   should only be SOA and NS records left.

#. Delete the hosted zone ``dev.url.singlecell.gi.ucsc.edu``.

#. Delete the hosted zone ``url.singlecell.gi.ucsc.edu``.

#. In the ``singlecell.gi.ucsc.edu`` zone, delete the record for
   ``url.singlecell.gi.ucsc.edu``.

After this change lands in ``anvildev``, follow these instructions for the AWS
account ``platform-anvil-dev``:

#. Ask everyone to bring their personal deployments in that account
   up to date with ``develop``.

#. In the AWS console, navigate to *Route53 service* → *Hosted zones*.

#. Select ``anvil.gi.ucsc.edu`` and check for records beginning with ``url.``.
   If there are any, contact the owner of the corresponding deployment. Their
   deployment wasn't upgraded properly. If there are records for the
   ``anvilbox`` or ``anvildev`` deployments, contact the lead. As a last
   resort, remove the record.

After completing the above two sections, ask the lead to deploy the
``dev.gitlab``, and ``anvildev.gitlab`` components. Nothing needs to be done
for ``prod.gitlab``.

After this change lands in ``prod``, follow these instructions for AWS account
``platform-hca-prod``:

#. In the AWS console, navigate to *Route53 service* → *Hosted zones*.

#. Open the hosted zone ``azul.data.humancellatlas.org`` and check for a
   record called ``url.azul.data.humancellatlas.org`` record. There should be
   none. If there is, contact the lead. 

#. In the ``data.humancellatlas.org`` zone, delete the record for
   ``url.data.humancellatlas.org``.



#4648 Move GitLab ALB access logs to shared bucket
==================================================

A new bucket in the ``shared`` component will reveived the GitLab ALB access
logs previously hosted in a dedicated bucket in the ``gitlab`` component. The
steps below have already been performed on ``dev`` and ``anvildev`` but need to
be run for ``prod`` before pushing the merge commit::

    _select prod.shared
    cd terraform/shared
    make
    cd ../gitlab
    _select prod.gitlab
    make

This will fail to destroy the non-empty bucket. Move the contents of the old
bucket to the new one::

    aws s3 sync s3://edu-ucsc-gi-singlecell-azul-gitlab-prod-us-east-1/logs/alb s3://edu-ucsc-gi-platform-hca-prod-logs.us-east-1/alb/access/prod/gitlab/
    aws s3 rm --recursive s3://edu-ucsc-gi-singlecell-azul-gitlab-prod-us-east-1/logs/alb
    make

If this fails with an error message about a non-empty state for an orphaned
bucket resource, the following will fix that::

    terraform state rm aws_s3_bucket.gitlab
    make


#4174 Enable GuardDuty and SecurityHub
======================================

This change enables the AWS Config, GuardDuty, and SecurityHub services,
deployed as part of the ``shared`` Terraform component. Prior to deploy, the
operator must ensure these services are currently not active and disable/remove
any that are. Use the AWS CLI's _list_ and _describe_ functionality to obtain
the status of each service, and the CLI's _delete_ and _disable_ functionality
to remove the ones that are active ::

    _select dev.shared

    aws configservice describe-configuration-recorders
    aws configservice delete-configuration-recorder --configuration-recorder-name <value>

    aws configservice describe-delivery-channels
    aws configservice delete-delivery-channel --delivery-channel-name <value>

    aws guardduty list-detectors
    aws guardduty delete-detector --detector-id <value>

    aws securityhub get-enabled-standards
    aws securityhub batch-disable-standards --standards-subscription-arns <value>

    aws securityhub describe-hub
    aws securityhub disable-security-hub

After ensuring the services are disabled, follow these steps to deploy for the
``dev.shared``, ``anvildev.shared``, and ``prod.shared`` deployments ::

    _select dev.shared
    cd $project_root/terraform/shared
    make apply


#4190 Create SNS topic for monitoring and security notifications
================================================================

A new environment variable called ``AZUL_MONITORING_EMAIL`` has been added. In
personal deployments, set this variable to ``'{AZUL_OWNER}'``. As always, use
the sandbox deployment's ``environment.py`` as a model when upgrading personal
deployments.

Note: The SNS topic and email subscription will only be created for deployments
that have ``AZUL_ENABLE_MONITORING`` enabled, which is typically the case in
main deployments only.

**IMPORTANT**: The SNS topic subscription will be created with a status of
"pending confirmation". Instead of simply clicking the link in the "Subscription
Confirmation" email, you should follow the instructions given during the
``make deploy`` process to confirm the subscription.


#4122 Create AnVIL deployments of Azul and Data Browser
=======================================================

Everyone
~~~~~~~~

In personal deployments dedicated to AnVIL, set ``AZUL_BILLING`` to ``'anvil'``,
set it to ``'hca'`` in all other personal deployments.

In personal deployments, set ``AZUL_VERSIONED_BUCKET`` and ``AZUL_S3_BUCKET`` to
the same value as in the ``sandbox`` deployment.

In personal deployments, remove ``AZUL_URL_REDIRECT_FULL_DOMAIN_NAME`` if its
value is (``'{AZUL_DEPLOYMENT_STAGE}.{AZUL_URL_REDIRECT_BASE_DOMAIN_NAME}'``.

In ``environment.py`` for personal deployments, initialize the ``is_sandbox``
variable to ``False``, replacing the dynamic initializer, and copy the
definition of the ``AZUL_IS_SANDBOX`` environment variable from sandbox'
``environment.py``. This will make it easier in the future to synchronize your
deployments' ``environment.py`` with that of the sandbox.

Operator
~~~~~~~~

Run ::

    _select dev.shared # or prod.shared
    cd terraform/shared
    make validate
    terraform import aws_s3_bucket.versioned $AZUL_VERSIONED_BUCKET
    terraform import aws_s3_bucket_versioning.versioned $AZUL_VERSIONED_BUCKET
    terraform import aws_s3_bucket_lifecycle_configuration.versioned $AZUL_VERSIONED_BUCKET
    terraform import aws_api_gateway_account.shared api-gateway-account
    terraform import aws_iam_role.api_gateway azul-api_gateway

Repeat for ``shared.prod``.

Redeploy the ``shared.dev`, ``gitlab.dev``, ``shared.prod`, and ``gitlab.prod``
components to apply the needed changes to any resources.


#4224 Index ENCODE snapshot as PoC
==================================

Replace ``'tdr'`` with ``'tdr_hca'`` in the repository plugin configuration for
the ``AZUL_CATALOGS`` variable in your personal deployments. As always, use the
sandbox deployment's ``environment.py`` as a model when upgrading personal
deployments.


#4197 Manage CloudTrail trail in 'shared' TF component
======================================================

This change adds a ``shared`` terraform component to allow Terraform to manage
the existing CloudTrail resources on `develop` and `prod`. To import these
resources into Terraform, the operator must run the following steps after the
change has been merged into the respective branches.

For `develop` ::

    git checkout develop
    _select dev.shared
    cd $project_root/terraform/shared
    make config
    terraform import aws_s3_bucket.cloudtrail_shared "edu-ucsc-gi-platform-hca-dev-cloudtrail"
    terraform import aws_s3_bucket_policy.cloudtrail_shared "edu-ucsc-gi-platform-hca-dev-cloudtrail"
    aws cloudtrail delete-trail --name Default
    make apply

For `prod` ::

    git checkout prod
    _select prod.shared
    cd $project_root/terraform/shared
    make config
    terraform import aws_s3_bucket.cloudtrail_shared "edu-ucsc-gi-platform-hca-prod-cloudtrail"
    terraform import aws_s3_bucket_policy.cloudtrail_shared "edu-ucsc-gi-platform-hca-prod-cloudtrail"
    aws cloudtrail delete-trail --name platform-hca-cloudtrail
    make apply


#4001 Put API Gateway behind GitLab VPC
=======================================

A new configuration variable has been added, ``AZUL_PRIVATE_API``. Set this
variable's value to ``1`` to place the deployment's API Gateway in the
GitLab VPC, thus requiring use of a VPN connection to access to the deployment.

Note that when changing the variable's value from ``0`` to ``1`` or vice versa,
the deployment must first be destroyed (``make -C terraform destroy``), and
``AZUL_DEPLOYMENT_INCARNATION`` incremented before the change can be deployed.
Refer to the `Private API` section of the README for more information.


#4170 Update Python to 3.9.x
============================

Update your local Python installation to 3.9.12. In your working copy, run
``make virtualenv`` and ``make requirements envhook``.

Reconcile the import section in your personal deployments' ``environment.py``
with that in the sandbox's copy of that file. Some of the imports from the
``typing`` module have been removed or replaced with imports from other modules,
like ``collections.abc``.


#3530 Remove AZUL_PARTITION_PREFIX_LENGTH
=========================================

The environment variable ``AZUL_PARTITION_PREFIX_LENGTH`` has been removed.
Ensure that all configured sources specify their own partition prefix length.
As always, use the sandbox deployment's ``environment.py`` as a model when
upgrading personal deployments.


#4048 Remove JsonObject
=======================

Run ``make clean`` to remove any left-over unpacked wheel distributions.

Run ``pip uninstall jsonobject`` to deinstall JsonObject. If that gives you
trouble, run ::

    deactivate ; make virtualenv && source .venv/bin/activate && make requirements envhook

instead.


#3073 Move parsing of prefix to SourceSpec
==========================================

The ``AZUL_DSS_ENDPOINT`` environment variable has been replaced with
``AZUL_DSS_SOURCE``. If a deployment needs to be updated, refer to the root
``environment.py`` file for the updated EBNF syntax.


#3605 Place GitLab behind VPN
=============================

Follow the instructions in the README on `requesting VPN access to GitLab`_ for
both ``dev.gitlab`` and ``prod.gitlab``.

.. _requesting VPN access to GitLab: ./README.md#911-requesting-access

Upgrade to Terraform 0.12.31 and run ``make deploy`` in every personal
deployment.


#3796 Fix: Can't easily override AZUL_DEBUG for all deployments locally
=======================================================================

This changes the precedence of ``environment.py`` and ``environment.local.py``
files. Previously, the precedence was as follows (from high to low, with
``dev.gitlab`` selected as an example):

1) deployments/dev.gitlab/environment.py.local
2) deployments/dev.gitlab/environment.py
3) deployments/dev/environment.py.local
4) deployments/dev/environment.py
5) environment.py.local
6) environment.py

The new order of precedence is

1) deployments/dev.gitlab/environment.py.local
2) deployments/dev/environment.py.local
3) environment.py.local
4) deployments/dev.gitlab/environment.py
5) deployments/dev/environment.py
6) environment.py

Before this change, it wasn't possible to override, say, ``AZUL_DEBUG`` for all
deployments using a ``environment.py.local`` in the project root because the
setting of that variable in ``deployments/*/environment.py`` would have taken
precedence. One would have had to specify an override in every
``deployments/*/environment.local.py``.

You may need to adjust your personal deployment's ``environment.py`` file
and/or any ``environment.local.py`` you may have created.


#3006 Upgrade to ElasticSearch 7.10
===================================

This will destroy and recreate the ES domain for all main deployments, including
``sandbox`` which hosts the ES indices for typical personal deployments. If your
personal deployment shares the ES instance with the ``sandbox`` deployment, you
will need to run ``make reindex`` to repopulate your indices on the new ES
domain. In the uncommon case that your personal deployment uses its own ES
domain, update ``AZUL_ES_INSTANCE_TYPE`` and ``AZUL_ES_VOLUME_SIZE`` to be
consistent with what the ``sandbox`` deployment uses. Then run ``make deploy``
and ``make reindex``.

For main deployments, the operator needs to manually delete the deployement's
existing Elasticsearch domain before initiating the GitLab build.


#3561 Fix: Listing bundles for a snapshot gives zero bundles
============================================================

The definition of the ``mksrc`` function and the source configuration for the
``dcp2`` catalog have been updated. As always, use the sandbox deployment's
``environment.py`` as a model when upgrading personal deployments.


#3113 IT catalog names are inconsistent
=======================================

The format of IT catalog name has been updated. IT catalog names are composed by
appending ``-it`` to the end of a primary catalog name. (e.g. dcp2, dcp2-it).
The regular expression that validates an IT catalog name can be found at
``azul.Config.Catalog._it_catalog_re``. As always, use the sandbox deployment's
``environment.py`` as a model when upgrading personal deployments.


#3515 Reduce number of shards for IT catalogs
=============================================

The configuration will take effect in the next IT run after deleting the old
indices. To delete them run::

    python scripts/reindex.py --catalogs it it2 --delete --index


#3439 Upgrade Python runtime to 3.8.12
======================================

Update Python to 3.8.12


#3552 Index updated snapshot into dcp2 on dev
=============================================

A snapshot was updated in ``dcp2_sources``. As always, use the sandbox
deployment's ``environment.py`` as a model when upgrading personal deployments.


#3114 Define sources within catalog JSON
========================================

The ``AZUL_TDR_SOURCES`` and ``AZUL_…_SOURCES`` environment variables have been
removed. Sources must be defined within the catalog configuration as a list of
sources. As always, use the sandbox deployment's ``environment.py`` as a model
when upgrading personal deployments.


HumanCellAtlas/dcp2#17 TDR dev dataset is stale
===============================================

Before upgrading to this commit, run::

    python scripts/reindex.py --delete --catalogs dcp2ebi it2ebi lungmap it3lungmap


#3196 Cover can_bundle.py in integration tests
==============================================

Follow instructions in section 2.3.1 of the README.


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
