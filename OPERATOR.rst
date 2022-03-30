**Note:** Edits to this document can be merged by the operator with one approving peer review.
An issue is not necessary.

----

.. contents::

Getting started as operator
---------------------------

* It is **strongly recommend** that you install `smartgit`_

.. _smartgit: https://www.syntevo.com/smartgit/download/

* Ask the lead via Slack to:

  - add you to the ``Azul Operators`` GitHub group on DataBiosphere

  - give you Maintainer access to the Gitlab ``dev`` and ``prod`` instances

  - assign you the ``Owner`` role on the ``platform-hca-prod`` Google Cloud project

* Ask Erich Weiler (weiler@soe.ucsc.edu) via email (cc Trevor and Hannes) to give you developer access to the ``platform-hca-prod`` AWS account

* Confirm access to Gitlab:

  #. Add your SSH key to your user account on Gitlab under the "Settings/SSH Keys" panel

  #. Confirm SSH access to the gitlab instance::

         ssh -T git@ssh.gitlab.dev.singlecell.gi.ucsc.edu
         Welcome to GitLab, @amarjandu!

  #. Add the gitlab instances to the local working copy's ``.git/config`` file using::

         [remote "gitlab.dcp2.dev"]
             url = git@ssh.gitlab.dev.singlecell.gi.ucsc.edu:ucsc/azul
             fetch = +refs/heads/*:refs/remotes/gitlab.dcp2.dev/*
         [remote "gitlab.dcp2.prod"]
             url = git@ssh.gitlab.azul.data.humancellatlas.org:ucsc/azul.git
             fetch = +refs/heads/*:refs/remotes/gitlab.dcp2.prod/*

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

Review counts
^^^^^^^^^^^^^

When verifying accuracy of the ``review count`` label, search for the string
``hannes-ucsc requested`` on the PR page. Make sure to check for comments that
indicate if a review count was not bumped.

Testing a PR in the ``sandbox``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator sets ``sandbox`` label on a PR before pushing the PR branch to
Gitlab. If the resulting sandbox build passes, the PR is merged and the label
stays on. If the build fails, the label is removed. Only one un-merged PR should
have the label.

If the tests fail while running a sandbox PR, an operator should do minor
failure triage.

Triaging ``sandbox`` failures
"""""""""""""""""""""""""""""

* If the PR fails because of out-of-date requirements on a PR with the ``[R]``
  tag the operator should rerun ``make requirements_update``,
  `committing the changes separately`_ with a title like ``[R] Update requirements``.
  It is not necessary to re-request a review after doing so.

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
done without pushing anything and only takes a couple of minutes. The branch
for that deployment must then be reverted to the previously passing commit.


.. _committing the changes separately: https://github.com/DataBiosphere/azul/issues/2899#issuecomment-804508017

Reindexing
^^^^^^^^^^

During reindexing, watch the ES domain for unassigned shards, using the AWS
console. The `azul-prod` CloudWatch dashboard has a graph for the shard count.
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

Chain shortening
^^^^^^^^^^^^^^^^

For the blocked PR, change the target branch to develop. Remove the chain
label from blocking PR. Last, remove the blocking relationship.

Upgrading GitLab
^^^^^^^^^^^^^^^^

Occasionally it falls on the operator to upgrade the Azul GitLab instance. If
the current major version is ``n`` and the latest available major version is
greater than ``n+1`` (i.e. upgrading directly to the latest version would skip
one or more major versions) then multiple successive upgrades must be made, such
that no upgrade skips a major version. For example, if the current version is
13.x.y and the latest available version is 15.x.y, then one would first upgrade
to 14.x.y and then repeat the process to upgrade to 15.x.y.

Before any changes are applied, stop the instance (do not terminate) and create
a snapshot of its EBS volume. Edit ``terraform/gitlab/gitlab.tf.json.template.py``,
updating the versions of the docker images for ``gitlab-ce`` and
``gitlab-runner``. Then run::

    _select dev.gitlab
    cd terraform/gitlab
    make apply

It may be necessary to set ``CI_COMMIT_REF_NAME=develop`` to work around
``check_branch``.

The GitLab instance should be online again in 10 minutes or so. If it takes
substantially longer, contact the lead.

Adding snapshots to ``dev``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

When adding a new snapshot to dev, the operator should also add the snapshot to sandbox, but with
an appropriate prefix.

To determine the prefix:

#. Go to `TDR dev in the Google Cloud Console`_. Authenticate with your burner account.

#. Run queries such as ::

       SELECT COUNT(*) FROM `<TDR_PROJECT_NAME>.<SNAPSHOT_NAME>.links` where starts_with(links_id, '4')

   in order to find the shortest prefix that yields 64 or more links (the amount
   required by the integration test). By convention, prefixes start with 42.

.. _TDR dev in the Google Cloud Console: https://console.cloud.google.com/bigquery?project=platform-hca-dev

Adding snapshots to ``prod``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PRs which update or add new snapshots to ``prod`` should be filed against the
``prod`` branch instead of ``develop``.

Add new or updated snapshots on an ad hoc basis, when requested. Do not sync
with regular promotions.

Add a checklist item at the end of the operator's PR checklist to file a
back-merge PR from ``prod`` to ``develop``.

Removing catalogs from ``prod`` and setting a new default
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

PRs which remove catalogs or set a new default for ``prod`` should be filed
against the ``prod`` branch instead of ``develop``.

When setting a new default catalog in ``prod``, the operator shall also delete
the old default catalog unless the ticket explicitly specifies not to delete the
old catalog.

Add a checklist item at the end of the PR checklist to file a back-merge
PR from ``prod`` to ``develop``.

Add another checklist item instructing the operator to manually delete the old
catalog.

Promoting to ``prod``
^^^^^^^^^^^^^^^^^^^^^

Promotions to ``prod`` should happen weekly on Wednesdays, at 3pm. We promote
earlier in the week in order to triage any potential issues during reindexing.
We promote at 3pm to give a cushion of time in case anything goes wrong.

To do a promotion:

#. Create a new GitHub issue with the title ``Promotion yyyy-mm-dd``

#. Announce in the `#team-boardwalk Slack channel`_ that you plan to promote to ``prod``

#. Make sure your ``develop`` and ``prod`` branches are up to date. Run::

	git checkout develop
	git pull -ff-only
	git checkout prod
	git pull -ff-only

#. Then run::

      git checkout -b promotions/yyyy-mm-dd develop
      git push github --set-upstream promotions/yyyy-mm-dd

#. File a PR on GitHub from the new promotion branch and connect it to the issue.
   The PR must target ``prod``.

#. Request a review from the primary reviewer.

#. Search for and follow any special ``[u]`` upgrading instructions that were added.

#. When merging, follow the checklist and making sure to carry over any commit
   title tags (``[u r R]`` for example) into the default merge commit title
   e.g., ``[u r R] Merge branch 'promotions/2022-02-22' into prod``. Don't
   rebase the promotion branch and don't push the promotion branch to GitLab.
   Merge the promotion branch into ``prod`` and push the merge commit on the
   ``prod`` branch first to GitHub and then to the ``prod`` instance of GitLab.

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

Troubleshooting
---------------

Push errors
^^^^^^^^^^^

If an error occurs when pushing to the develop branch, ensure that the branch
you would like to merge in is rebased on develop and has completed its CI
pipeline. If there is only one approval (from the primary reviewer) an operator
may approve a PR that does not belong to them. If the PR has no approvals (for
example, it belongs to the primary reviewer), the  operator may approve the PR
and seek out another team member to perform the second needed review. When
making such a pro-forma review, indicate this within the review summary (`example`_).

.. _example: https://github.com/DataBiosphere/azul/pull/2646#pullrequestreview-572818767

PR Closed automatically and can't be reopened
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This can happen when a PR is chained on another PR and the base PR is
merged and its branch deleted. To solve this, first restore the base PR branch.
The operator should have a copy of the branch locally that they can push. If
not, then the PR's original author should.

Once the base branch is restored, the ``Reopen PR`` button should again be
clickable on the chained PR.

GitHub bot account
------------------

Continuous integration environments (Gitlab, Travis) may need a GitHub token to
access GitHub's API. To avoid using a personal access token tied to any
particular developer's account, we created a Google Group called
``azul-group@ucsc.edu`` of which Hannes and Trevor are owners. We then used that
group email to register a bot account in GitHub. Apparently that's ok:

    User accounts are intended for humans, but you can give one to a robot, such as a continuous integration bot, if necessary.

    (https://docs.github.com/en/github/getting-started-with-github/types-of-github-accounts#personal-user-accounts)

Only Hannes knows the GitHub password of the bot account but any member of the
group can request the password to be reset. All members will receive the
password reset email. Hannes and Trevor know the 2FA recovery codes. Hannes sent
them to Trevor via Slack on 05/11/2021.

Handing over operator duties
----------------------------

#. Old operator must finish any merges in progress. The sandbox should be empty. The new operator should inherit a clean slate. This should be done before the first working day of the new operator's shift.

#. Old operator must re-assign `all tickets in the approved column`_ to the new operator.

#. Old operator must re-assign expected indexing failure tickets to the new operator, along with
   ticket that tracks operator duties.

#. New operator must request the necessary permissions, as specified in `Getting started as operator`_.

.. _all tickets in the approved column: https://github.com/DataBiosphere/azul/pulls?q=is%3Apr+is%3Aopen+reviewed-by%3Ahannes-ucsc+review%3Aapproved
