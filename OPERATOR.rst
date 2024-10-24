.. contents::

Getting started as operator
---------------------------

* Read the entire document

* It is **strongly recommend** that you install `SmartGit`_

.. _SmartGit: https://www.syntevo.com/smartgit/download/

* Ask the lead via Slack to:

  - add you to the ``Azul Operators`` GitHub group on DataBiosphere

  - give you Maintainer access to the GitLab ``dev``, ``anvildev``,
    ``anvilprod`` and ``prod`` instances

  - assign to you the ``Editor`` role on the Google Cloud
    projects ``platform-hca-prod`` and ``platform-hca-anvilprod``

  - remove the ``Editor`` role in those projects from the previous operator

* Ask Erich Weiler (weiler@soe.ucsc.edu) via email (cc Ben and Hannes) to:

  - grant you developer access to AWS accounts ``platform-hca-prod`` and ``platform-anvil-prod`

  - revoke that access from the previous operator (mention them by name)

* Confirm access to GitLab:

  #. Add your SSH key to your user account on GitLab under the "Settings/SSH
     Keys" panel

  #. Confirm SSH access to the GitLab instance::

         ssh -T git@ssh.gitlab.dev.singlecell.gi.ucsc.edu
         Welcome to GitLab, @amarjandu!

  #. Add the gitlab instances to the local working copy's ``.git/config`` file
     using::

         [remote "gitlab.dcp2.dev"]
             url = git@ssh.gitlab.dev.singlecell.gi.ucsc.edu:ucsc/azul
             fetch = +refs/heads/*:refs/remotes/gitlab.dcp2.dev/*
         [remote "gitlab.dcp2.prod"]
             url = git@ssh.gitlab.azul.data.humancellatlas.org:ucsc/azul.git
             fetch = +refs/heads/*:refs/remotes/gitlab.dcp2.prod/*
         [remote "gitlab.anvil.dev"]
             url = git@ssh.gitlab.anvil.gi.ucsc.edu:ucsc/azul.git
             fetch = +refs/heads/*:refs/remotes/gitlab.anvil.dev/*

  #. Confirm access to fetch branches::

         git fetch -v gitlab.dcp2.dev
         From ssh.gitlab.dev.singlecell.gi.ucsc.edu:ucsc/azul
         = [up to date]        develop                    -> gitlab.dcp2.dev/develop
         = [up to date]        issues/amar/2653-es-2-slow -> gitlab.dcp2.dev/issues/amar/2653-es-2-slow`

* Standardize remote repository names. If the name of the remote repository on
  GitHub is set to ``origin`` rename the remote repository to ``github``. Run::

    git remote rename origin github

Operator jobs
-------------

First order of business: add a calendar event for the next scheduled operator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As soon as your shift begins and before performing any other actions as an
operator create the following Google Calendar event in the `Team Boardwalk
calendar`_.

Create an all-day calendar event for the two weeks after your current stint,
using the title ``Azul Operator: <name>`` with the name of the operator who will
be serving next.

If you are aware of any schedule irregularities, such as one operator performing
more than one consecutive stints, create events for those as well.

.. _`Team Boardwalk calendar`: https://calendar.google.com/calendar/u/0/r?cid=dWNzYy5lZHVfMDRuZ3J1NXQzNDB0aWd0cW5qYWQ5Nm5jOWtAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ

Check weekly for Amazon OpenSearch Service updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator checks weekly for notifications about service software updates to
Amazon OpenSearch Service domains for all Azul deployments. Note that service
software updates are distinct from updates to the upstream version of
ElasticSearch (or Amazon's OpenSearch fork) in use on an ES domain. While the
latter are discretional and applied via a change to TerraForm configuration,
some of the latter are mandatory.

Unless we intervene, AWS will automatically force the installation of any update
about which we receive a ``High`` severity notification, typically two weeks
after the notification was sent. Read `Amazon notification severities`_ for more
information.  The operator must prevent the automatic installation of such
updates. It would be disastrous if an update were to be applied during a reindex
in ``prod``. Instead, the operator must apply the update manually as part of an
operator ticket in GitHub, as soon as possible, and well before Amazon would
apply it automatically.

To check for, and apply, if necessary, any pending service software updates, the
operator performs the following steps daily.

1. In *Amazon OpenSearch Service Console* select the *Notifications* pane and
   identify notifications with subject ``Service Software Update``.

2. Record the severity, date and the ES domain name of these notifications.
   Collect this information for all ES domain in both the ``prod`` and ``dev``
   AWS accounts. If there are no notifications, you are done.

3. Open a new ticket in GitHub and title it ``Apply Amazon OpenSearch (ES)
   Software Update (before {date})``. Include ``(before {date})`` in the title
   if any notification is of ``High`` severity, representing a forced update.
   Replace ``{date}`` with the anticipated date of the forced installation. If
   there already is an open ticket for pending updates, reuse that ticket and
   adjust it accordingly.

4. If title contains a date, pin the ticket as *High Priority* in ZenHub.

5. The description of the ticket should include a checklist item for each ES
   domain recorded in step 2. The checklist should include items for notifying
   the team members about any disruptions to their personal deployments, say,
   when the ``sandbox`` domain is being updated.

   Use this template for the checklist::

      - [ ] Update `azul-index-dev`
      - [ ] Update `azul-index-anvildev`
      - [ ] Update `azul-index-anvilprod`
      - [ ] Confirm with Azul devs that their personal deployments are idle
      - [ ] Update `azul-index-sandbox`
      - [ ] Update `azul-index-anvilbox`
      - [ ] Update `azul-index-hammerbox`
      - [ ] Update `azul-index-prod`
      - [ ] Confirm snapshots are disabled on all domains
        - `aws opensearch describe-domains --domain-name <NAME> | jq '.DomainStatusList[].SnapshotOptions'`
        - Value of `AutomatedSnapshotStartHour` should be `-1`

   Note that, somewhat counterintuitively, main deployments are updated before
   their respective ``sandbox``. If, during step 3, updates or domains were
   added to an existing ticket, the entire process may have to be restarted and
   certain checklist items may need to be reset.

6. To update an ES domain, select it the Amazon OpenSearch Service console.
   Under *General information*, the *Service software version* should have an
   *Update available* hyperlink. Click on it and follow the subsequent
   instructions.

7. Once the upgrade process is completed for the ``dev`` or ``prod`` ES domain,
   perform a smoke test using the respective Data Browser instance.

.. _`Amazon notification severities`: https://docs.aws.amazon.com/opensearch-service/latest/developerguide/managedomains-notifications.html#managedomains-notifications-severities

Review counts
^^^^^^^^^^^^^

When verifying accuracy of the ``review count`` label, search for the string
``hannes-ucsc requested`` on the PR page. Make sure to check for comments that
indicate if a review count was not bumped.

Testing a PR in the ``sandbox``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator sets ``sandbox`` label on a PR before pushing the PR branch to
GitLab. If the resulting sandbox build passes, the PR is merged and the label
stays on. If the build fails, the label is removed. Only one un-merged PR should
have the label.

If the tests fail while running a sandbox PR, an operator should do minor
failure triage.

Triaging ``sandbox`` failures
"""""""""""""""""""""""""""""

* If the PR fails because of out-of-date requirements on a PR with the ``[R]``
  tag the operator should rerun ``make requirements_update``, `committing the
  changes separately`_ with a title like ``[R] Update requirements``. It is not
  necessary to re-request a review after doing so.

* For integration test failures, check if the PR has the ``reindex`` tag. If so,
  running an early reindex may resolve the failure.

* Determine if the failure could have been caused by the changes in the PR. If
  so, there is no need to open up a new ticket. Bounce the PR back to the "In
  progress" column and notify the author of the failure. Ideally provide a link.

* All other build failures need to be tracked in tickets. If there is an
  existing ticket, comment on it with a link to the failed job and move the
  ticket to Triage. If there is no existing ticket resembling the failed build,
  create a new one, with a link to the failed build, a transcript of any
  relevant error messages and stack traces from the build output, and any
  relevant log entries from CloudWatch.

Triaging GitLab build failures on ``dev`` and ``prod``
""""""""""""""""""""""""""""""""""""""""""""""""""""""

If a GitLab build fails on a main deployment, the operator must evaluate the
impact of that failure. This evaluation should include visiting the Data Browser
to verify it isn't broken.

To restore the deployment to a known working state, the operator should rerun
the deploy job of previous passing pipeline for that deployment. This can be
done without pushing anything and only takes a couple of minutes. The branch for
that deployment must then be reverted to the previously passing commit.

.. _committing the changes separately: https://github.com/DataBiosphere/azul/issues/2899#issuecomment-804508017

Reindexing
^^^^^^^^^^

During reindexing, watch the ES domain for unassigned shards, using the AWS
console. The ``azul-prod`` CloudWatch dashboard has a graph for the shard count.
It is OK to have unassigned shards for a while but if the same unassigned shards
persist for over an hour, they are probably permanently unassigned. Follow the
procedure outlined in `this AWS support article`_, using either Kibana or
Cerebro. Cerebro has a dedicated form field for the index setting referenced in
that article. In the past, unassigned shards have been caused by AWS attempting
to make snapshots of the indices that are currently being written to under high
load during reindexing. Make sure that ``GET _cat/snapshots/cs-automated``
returns nothing. Make sure that the *Start Hour* under *Snapshots* on the
*Cluster confguration* tab of the ES domain page in the AWS console is shown as
``0-1:00 UTC``. If either of these checks fails, file a support ticket with AWS
urgently requesting snapshots to be disabled.

.. _this AWS support article: https://aws.amazon.com/premiumsupport/knowledge-center/opensearch-in-memory-shard-lock/

The operator must check the status of the queues after every reindex for
failures. Use ``python scripts/manage_queues.py`` to identify any failed
messages. If failed messages are found, use ``python scripts/manage_queues.py``
to

- dump the failed notifications to JSON file(s), using ``--delete`` to
  simultaneously clear the ``notifications_fail`` queue

- force-feed the failed notifications back into the ``notifications_retry``
  queue. We feed directly into the retry queue, not the primary queue, to save
  time if/when the messages fail again.

This may cause the previously failed messages to succeed. Repeat this procedure
until the set of failed notifications stabilizes, i.e., the
``notifications_fail`` queue is empty or no previously failed notifications
succeeded.

Next, repeat the dump/delete/force-feed steps with the failed tallies, feeding
them into ``tallies_retry`` queue (again, **NOT** the primary queue) until the
set of failed tallies stabilizes.

If at this point the fail queues are not empty, all remaining failures must be
tracked in tickets:

- document the failures within the PR that added the changes

- triage against expected failures from existing issues

- create new issues for unexpected failures

- link each failure you document to their respective issue

- ping people on the Slack channel ``#dcp2`` about those issues, and finally

- clear the fail queues so they are empty for the next reindexing

For an example of how to document failures within a PR `click here`_.

.. _click here: https://github.com/DataBiosphere/azul/pull/3050#issuecomment-840033931

Reindexing a specific catalog or sources in GitLab
""""""""""""""""""""""""""""""""""""""""""""""""""

From the GitLab web app, select the ``reindex`` or ``early_reindex`` job for
the pipeline that needs reindexing of a specific catalog. From there, you
should see an option for defining the key and value of additional variables to
parameterize the job with.

To specify a catalog to be reindexed, set ``Key`` to ``azul_current_catalog``
and ``Value`` to the name of the catalog, for example, ``dcp3``. To specify the
sources to be reindexed, set ``Key`` to ``azul_current_sources`` and
``Value`` to a space-separated list of sources globs, e.g.
``*:hca_dev_* *:lungmap_dev_*``. Check the inputs you just
made. Start the ``reindex`` job by clicking on ``Run job``. Wait until the job
has completed.

Repeat these steps to reindex any additional catalogs.


Updating the AMI for GitLab instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once a month, operators must check for updates to the AMI for the root volume of
the EC2 instance running GitLab. We use a hardened — to the requirements of the
CIS Amazon Linux 2 benchmark — variant of Amazon's Linux 2 AMI. The license to
use the AMI for an EC2 instance is sold by CIS as a subscription on the AWS
Marketplace:

https://aws.amazon.com/marketplace/pp/prodview-5ihz572adcm7i

The license costs $0.02 per instance/hour. Every AWS account must subscribe
separately.

There are ways to dynamically determine the latest AMI released by CIS under the
subscription but in the spirit of reproducible builds, we would rather pin the
AMI ID and adopt updates at our own discretion to avoid unexpected failures. To
obtain the latest compatible AMI ID, select the desired ``….gitlab`` component,
say, ``_select dev.gitlab`` and run

::

    aws ec2 describe-images \
            --owners aws-marketplace \
            --filters="Name=name,Values=*4c096026-c6b0-440c-bd2f-6d34904e4fc6*" \
        | jq -r '.Images[] | .CreationDate+"\t"+.ImageId+"\t"+.Name' \
        | sort \
        | tail -1

This prints the date, ID and name of the latest CIS-hardened AMI. Update the
``ami_id`` variable in ``terraform/gitlab/gitlab.tf.json.template.py`` to refer
to the AMI ID. Update the image name in the comment right above the variable so
that we know which semantic product version the AMI represents. AMIs are
specific to a region so the variable holds a dictionary with one entry per
region. If there are ``….gitlab`` components in more than one AWS region (which
is uncommon), you need to select at least one ``….gitlab`` component in each of
these regions, rerun the command above for each such component, and add or
update the ``ami_id`` entry for the respective region. Instead of selecting a
``….gitlab`` component, you can just specify the region of the component using
the ``--region`` option to ``aws ec2 describe-images``.

Upgrading GitLab & ClamAV
^^^^^^^^^^^^^^^^^^^^^^^^^

Operators check for updates to the Docker images for GitLab and ClamAV as part
of the biweekly upgrade process, and whenever a GitLab security releases
requires it. An email notification is sent to ``azul-group@ucsc.edu`` when a
GitLab security release is available. Discuss with the lead the **Table of
Fixes** referenced in the release blog post to determine the urgency of the
update. When updating the GitLab version, either as part of the regular update
or when necessary, check if there are applicable updates to the `GitLab runner
image`_ as well. Use the latest runner image whose major and minor version match
that of the GitLab image. When upgrading across multiple GitLab versions, follow
the prescribed GitLab `upgrade path`_. You will likely only be able to perform
a step on that path per biweekly upgrade PR.

.. _upgrade path: https://docs.gitlab.com/ee/update/index.html#upgrade-paths

Before upgrading the GitLab version, create a backup of the GitLab volume. See
`Backup GitLab volumes`_ for help.

Increase GitLab data volume size
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the CloudWatch alarm for high disk usage on the GitLab data volume goes
off, you must attach a new, larger volume to the instance. Run the command below
to create both a snapshot of the current data volume and a new data volume with
the specified size restored from that snapshot.

Discuss the desired new size with the system administrator before running the
command::

    python scripts/create_gitlab_snapshot.py --new-size [new_size]

When this command finishes, it will leave the instance in a stopped state. Take
note of the command logged by the script. You'll use it to delete the old data
volume after confirming that GitLab is up and running with the new volume
attached.

Next, deploy the ``gitlab`` TF component in order to attach the new data volume.
The only resource with changes in the resulting plan should be
``aws_instance.gitlab``. Once the ``gitlab`` TF component has been deployed,
start the GitLab instance again by running::

    python scripts/create_gitlab_snapshot.py --start-only

Finally, SSH into the instance to complete the setup of new data volume. Use the
``df`` command to confirm the size and mount point of the device, and
``resize2fs`` to grow the size of the mounted file system so that it matches
that of the volume. Run::

    df # Verify device /dev/nvme1n1 is mounted on /mnt/gitlab, note available size
    sudo resize2fs /dev/nvme1n1
    df # Verify the new available size is larger

The output of the last ``df`` command should inform of the success of these
operations. A larger available size compared to the first run indicates that
the resizing operation was successful. You can now delete the old data volume by
running the deletion command you noted earlier.

Backup GitLab volumes
^^^^^^^^^^^^^^^^^^^^^

Use the ``create_gitlab_snapshot.py`` script to back up the EBS data volume
attached to each of our GitLab instances. The script will stop the instance,
create a snapshot of the GitLab EBS volume, tag the snapshot and finally restart
the instance::

	python scripts/create_gitlab_snapshot.py

For GitLab or ClamAV updates, use the ``--no-restart`` flag in order to leave
the instance stopped after the snapshot has been created. There is no point in
starting the instance only to have the update terminate it again.

Updating software packages on GitLab instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once a week, operators must update all Linux packages installed on the root
volume of each GitLab instance. SSH access to the instances is necessary to
perform these instructions but on production instances this access is
unavailable, even to operators. In these cases the operator must request the
help of the system administrator via Slack to perform these steps.

SSH into the instance, and run ``sudo yum update`` followed by ``sudo reboot``.
Wait for the GitLab web application to become available again and perform a
``git fetch`` from one of the Git repositories hosted on that instance.

Export AWS Inspector findings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#. ``_select anvilprod``

#. Run ``python scripts/export_inspector_findings.py`` to generate a CSV file

#. Open the `Anvilprod Inspector Findings spreadsheet`_

#. Select ``File`` > ``Import`` to import the generated CSV, and on the ``Import
   file`` dialog use these options:

    - Import location: Insert new sheet(s)

    - Convert text to numbers, dates, and formulas: Checked

#. Rename the new tab using ``YYYY-MM-DD`` with the date of the upgrade issue,
   and move it to the front of the stack

#. Apply visual formatting (e.g. column width) to the sheet using a previous
   sheet as a guide

.. _Anvilprod Inspector Findings spreadsheet: https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8/edit#gid=1657352747

Adding snapshots to ``dev``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

When adding a new snapshot to ``dev``, ``anvildev``, the operator should also
add the snapshot to ``sandbox`` or ``anvilbox``, respectively.

The ``post_deploy_tdr.py`` script will fail if the computed common prefix
contains an unacceptable number of subgraphs. If the script reports that the
common prefix is too long, truncate it by 1 character. If it's too short, append
1 arbitrary hexadecimal character. Pass the updated prefix as a keyword argument
to the ``mksrc`` function for the affected source(s), including a partition
prefix length of 1. Then refresh the environment and re-attempt the deployment.

Adding snapshots to ``prod``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We decide on a case-by-case basis whether PRs which update or add new snapshots
to ``prod`` should be filed against the ``prod`` branch instead of ``develop``.
When deciding whether to perform snapshot changes directly to ``prod`` or
include them in a routine promotion, the system admin considers the scope of
changes to be promoted. It would be a mistake to promote large changes in
combination with snapshots because that would make it difficult to diagnose
whether indexing failures are caused by the changes or the snapshots.

Removing catalogs from ``prod`` and setting a new default
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PRs which remove catalogs or set a new default for ``prod`` should be filed
against the ``prod`` branch instead of ``develop``.

When setting a new default catalog in ``prod``, the operator shall also delete
the old default catalog unless the ticket explicitly specifies not to delete the
old catalog.

Add a checklist item at the end of the PR checklist to file a back-merge PR from
``prod`` to ``develop``.

Add another checklist item instructing the operator to manually delete the old
catalog.

Promoting to ``prod``
^^^^^^^^^^^^^^^^^^^^^

Promotions to ``prod`` should happen weekly on Wednesdays, at 3pm. We promote
earlier in the week in order to triage any potential issues during reindexing.
We promote at 3pm to give a cushion of time in case anything goes wrong.

To do a promotion:

#. Decide together with lead up to which commit to promote. This commit will be
   the HEAD of the promotions branch.

#. Create a new GitHub issue with the title ``Promotion yyyy-mm-dd``

#. Make sure your ``prod`` branch is up to date with the remote.

#. Create a branch at the commit chosen above. Name the branch correctly. See
   `promotion PR template`_ for what the correct branch name is.

#. File a PR on GitHub from the new promotion branch and connect it to the
   issue. The PR must target ``prod``. Use the `promotion PR template`_.

#. Request a review from the primary reviewer.

#. Once PR is approved, announce in the `#team-boardwalk Slack channel`_ that
   you plan to promote to ``prod``

#. Search for and follow any special ``[u]`` upgrading instructions that were
   added.

#. When merging, follow the checklist and making sure to carry over any commit
   title tags (``[u r R]`` for example) into the default merge commit title
   e.g., ``[u r R] Merge branch 'promotions/2022-02-22' into prod``. Don't
   rebase the promotion branch and don't push the promotion branch to GitLab.
   Merge the promotion branch into ``prod`` and push the merge commit on the
   ``prod`` branch first to GitHub and then to the ``prod`` instance of GitLab.

.. _promotion PR template: /.github/PULL_REQUEST_TEMPLATE/promotion.md

Backporting from ``prod`` to ``develop``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There should only ever be one open backport PR against ``develop``. If more
commits accumulate on ``prod``, waiting to be backported, close the existing
backport PR first. The new PR will include the changes from the old one.

#. Make a branch from ``prod`` at the most recent commit being backported. Name
   the branch following this pattern::

       backports/<7-digit SHA1 of most recent backported commit>

#. Open a PR from your branch, targeting ``develop``. The PR title should be

   ::

       Backport: <7-digit SHA1 of most recent backported commit> (#<Issue number(s)>, PR #<PR number>)

   Repeat this pattern for each of the older backported commits, if there are
   any. An example commit title would be

   ::

       Backport 32c55d7 (#3383, PR #3384) and d574f91 (#3327, PR #3328)

   Be sure to use the PR template for backports by appending
   ``&template=backport.md`` to the URL in your browser's address bar.

#. Assign and request review from the primary reviewer. The PR should only be
   assigned to one person at a time, either the reviewer or the operator.

#. Perform the merge. The commit title should match the PR title ::

       git merge prod --no-ff

#. Push the merge commit to ``develop``. It is normal for the branch history to
   look very ugly following the merge.

.. _#team-boardwalk Slack channel: https://ucsc-gi.slack.com/archives/C705Y6G9Z


Deploying the Data Browser
^^^^^^^^^^^^^^^^^^^^^^^^^^

The Data Browser is deployed two steps. The first step is building the
``ucsc/data-browser`` project on GitLab. This is initiated by pushing a branch
whose name matches ``ucsc/*/*`` to one of our GitLab instances. The resulting
pipeline produces a tarball stored in the package registry on that GitLab
instance. The second step is running the ``deploy_browser`` job of the
``ucsc/azul`` project pipeline on that same instance. This job creates or
updates the necessary cloud infrastructure (CloudFront, S3, ACM, Route 53),
downloads the tarball from the package registry and unpacks that tarball to the
S3 bucket backing the Data Browser's CloudFront distribution.

Typically, CC requests the deployment of a Data Browser instance on Slack,
specifying the commit they wish to be deployed. After the system administrator
approves that request, the operator merges the specified commit into one of the
``ucsc/{atlas}/{deployment}`` branches and then pushes that branch to the
``DataBiosphere/data-browser`` project on GitHub, and the ``ucsc/data-browser``
project on the GitLab instance for the Azul ``{deployment}`` that backs the Data
Browser instance to be deployed. For the merge commit title, SmartGit's default
can be used, as long as the title reflects the commit (branch, tag, or sha1)
specified by CC.

The ``{atlas}`` placeholder can be ``hca``, ``anvil`` or ``lungmap``. Not all
combinations of ``{atlas}`` and ``{deployment}`` are valid. Valid combinations
are ``ucsc/anvil/anvildev``, ``ucsc/anvil/anvilprod``, ``ucsc/hca/dev``,
``ucsc/hca/prod``, ``ucsc/lungmap/dev`` or ``ucsc/lungmap/prod``, for example.
The ``ucsc/data-browser`` pipeline on GitLab blindly builds any branch, but
Azul's ``deploy_browser`` job is configured to only use the tarball from exactly
one branch (see ``deployments/*.browser/environment.py``) and it will always use
the tarball from the most recent pipeline on that branch.


Troubleshooting
---------------

Credentials expire in the middle of a long-running operation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some instances, deploying a Terraform component can take a long time. While
``_login`` now makes sure that there are four hours left on the current
credentials, it can't do that if you don't call it before such an operation.
Note that ``_select`` also calls ``_login``. The following is a list of
operations which you should expect to take an hour or longer:

- the first time deploying any component

- deploying a plan that creates or replaces an Elasticsearch domain

- deploying a plan that involves ACM certificates

- deploying a ``shared`` component after modifying
  ``azul_docker_images`` in ``environment.py``, especially on a slow uplink

To make things worse, if the credentials expire while Terraform is updating
resources, it will not be able to write the partially updated state back to the
shared bucket. A subsequent retry will therefore likely report conflicts due to
already existing resources. The rememdy is to import those existing resources
into the Terraform state using ``terraform import``.

Push errors
^^^^^^^^^^^

If an error occurs when pushing to the develop branch, ensure that the branch
you would like to merge in is rebased on develop and has completed its CI
pipeline. If there is only one approval (from the primary reviewer) an operator
may approve a PR that does not belong to them. If the PR has no approvals (for
example, it belongs to the primary reviewer), the  operator may approve the PR
and seek out another team member to perform the second needed review. When
making such a pro-forma review, indicate this within the review summary
(`example`_).

.. _example: https://github.com/DataBiosphere/azul/pull/2646#pullrequestreview-572818767

PR Closed automatically and can't be reopened
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This can happen when a PR is chained on another PR and the base PR is merged and
its branch deleted. To solve this, first restore the base PR branch. The
operator should have a copy of the branch locally that they can push. If not,
then the PR's original author should.

Once the base branch is restored, the ``Reopen PR`` button should again be
clickable on the chained PR.

Integration test time out
^^^^^^^^^^^^^^^^^^^^^^^^^

This can happen on the rare occasion that the IT's random selection of bundles
happens to pick predominantly large bundles that need to be partitioned before
they can be indexed. This process can divide bundles into partitions, and divide
partitions into sub-partitions, since technically bundles are partitions with an
empty prefix.

In the AWS console, run the CloudWatch Insights query below with the indexer log
groups selected to see how many divisions have occurred::

    fields @timestamp, @log, @message
    | filter @message like 'Dividing partition'
    | parse 'Dividing partition * of bundle *, version *, with * entities into * sub-partitions.' as partition, bundle, version, enities, subpartitions
    | display partition, bundle, version, enities, subpartitions
    | stats count(@requestId) as total_count by bundle, partition
    | sort total_count desc
    | sort @timestamp desc
    | limit 1000

Note that when bundles are being partitioned, errors of exceeded rate & quota
limits should be expected::

    [ERROR] TransportError: TransportError(429, '429 Too Many Requests /azul_v2_prod_dcp17-it_cell_suspensions/_search')

    [ERROR] Forbidden: 403 GET https://bigquery.googleapis.com/bigquery/v2/projects/...: Quota exceeded: Your project:XXXXXXXXXXXX exceeded quota for tabledata.list bytes per second per project. For more information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas


Follow these steps to retry the IT job:

#. Cancel the ongoing IT job (if in progress)

#. Comment on `issue #4299`_ with a link to the failed job

#. Purge the queues::

    python scripts/manage_queues.py purge_all

#. Rerun the IT job

.. _`issue #4299`: https://github.com/DataBiosphere/azul/issues/4299

GitHub bot account
------------------

Continuous integration environments (GitLab, Travis) may need a GitHub token to
access GitHub's API. To avoid using a personal access token tied to any
particular developer's account, we created a Google Group called
``azul-group@ucsc.edu`` of which Hannes is the owner. We then used that group
email to register a bot account in GitHub. Apparently that's ok:

    User accounts are intended for humans, but you can give one to a robot, such as a continuous integration bot, if necessary.

    (https://docs.github.com/en/github/getting-started-with-github/types-of-github-accounts#personal-user-accounts)

Only Hannes knows the GitHub password of the bot account but any member of the
group can request the password to be reset. All members will receive the
password reset email. Hannes knows the 2FA recovery codes.

Handing over operator duties
----------------------------

#. Old operator must finish any merges in progress. The sandbox should be empty.
   The new operator should inherit a clean slate. This should be done before the
   first working day of the new operator's shift.

#. Old operator must re-assign `all tickets in the approved column`_ to the new
   operator.

#. Old operator must re-assign expected indexing failure tickets to the new
   operator, along with ticket that tracks operator duties.

#. New operator must request the necessary permissions, as specified in `Getting
   started as operator`_.

.. _all tickets in the approved column: https://github.com/DataBiosphere/azul/pulls?q=is%3Apr+is%3Aopen+reviewed-by%3Ahannes-ucsc+review%3Aapproved
