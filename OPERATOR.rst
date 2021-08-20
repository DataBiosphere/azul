**Note:** Edits to this document can be merged by the operator with one approving peer review.
An issue is not necessary.

----

.. contents::

Getting started as operator
---------------------------

* It is **strongly recommend** that you install `smartgit`_

.. _smartgit: https://www.syntevo.com/smartgit/download/

* Ask the lead via Slack to add you to the ``Azul Operators`` Github group on DataBiosphere

* Ask the lead via Slack to give you Maintainer access to the Gitlab `dev` and `prod` instances

* Ask the lead via Slack to give assign you the Owner role on the `hca-platform-prod` Google Cloud project

* Ask Erich Weiler (weiler@soe.ucsc.edu) via email (cc Trevor and Hannes) to give you developer access to the `hca-platform-prod` AWS account

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

* For integration test failures, check if the PR has the ``reindex`` tag. If so,
  running an early reindex may resolve the failure.

* Determine if the failure could have been caused by the changes in the PR. If
  so, there is no need to open up a new ticket. Bounce the PR back to the "In
  progress" column and notify the author of the failure. Ideally provide a link.

* When the failure is something uncommon or irregular, open a new ticket
  describing the failure. These types of infrequent failures tend to solve
  themselves on the second/third run of the IT job. While the the operator has
  discretion on whether or not to open a new ticket, it is important to record
  unusual failures to determine their eventual frequency.

.. _committing the changes separately: https://github.com/DataBiosphere/azul/issues/2899#issuecomment-804508017

Reindexing
^^^^^^^^^^

The operator must check the status of the queues after every reindex for
failures. Use ``python scripts/manage_queues.py`` to identify any failed
messages. If failed messages are found

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

Promoting to ``prod``
---------------------

Promotions to ``prod`` should happen weekly on Wednesdays, at 3pm. We promote
earlier in the week in order to triage any potential issues during reindexing.
We promote at 3pm to give a cushion of time in case anything goes wrong.

To do a promotion:

#. Announce in the `#team-boardwalk Slack channel`_ that you plan to promote to ``prod``

#. Make sure your ``develop`` and ``prod`` branches are up to date.

#. Check the ``prod`` branch for hotfixes. If there are changes on ``dev`` that
   permanently solve the issues, revert the hotfix on ``prod``. Check the
   contributing guide for specifics on procedure.

#. ::

      git checkout prod

#. ::

      git merge develop --no-ff

   Use the default merge commit message, which should be ``Merge branch 'develop' into prod``.

#. Examine the changes to ``prod`` since the last promotion to determine if a reindex
   is necessary.

#. Search for and follow any special ``[u]`` upgrading instructions that were added.

#. ::

       git push github

#. If a reindex is necessary, preemptively cancel the integration test before it
   runs, and run ``early_reindex``. This is to prevent prod from going down
   longer than necessary.

#. ::

       git push gitlab.dcp2.prod

#. Monitor reindex and check / triage any failures.

#. If the integration test was cancelled because of reindexing, run it now.

#. On the Zenhub board, move the issues that were merged from the "dev" column to "prod".

Backporting from ``prod`` to ``develop``
----------------------------------------

#. Open a PR from the GitHub UI

#. Trim PR checklist to the section ``Primary reviewer``

#. Get approval from a peer

#. Push a merge commit titled ``Merge branch 'prod' to develop`` to the ``develop`` branch.

**Note that the HEAD from the merge commit needs to be the same as the HEAD commit on the PR branch.**

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

Github bot account
------------------

Continuous integration environments (Gitlab, Travis) may need a Github token to
access Github's API. To avoid using a personal access token tied to any
particular developer's account, we created a Google Group called
``azul-group@ucsc.edu`` of which Hannes and Trevor are owners. We then used that
group email to register a bot account in Github. Apparently that's ok:

    User accounts are intended for humans, but you can give one to a robot, such as a continuous integration bot, if necessary.

    (https://docs.github.com/en/github/getting-started-with-github/types-of-github-accounts#personal-user-accounts)

Only Hannes knows the Github password of the bot account but any member of the
group can request the password to be reset. All members will receive the
password reset email. Hannes and Trevor know the 2FA recovery codes. Hannes sent
them to Trevor via Slack on 05/11/2021.

Handing over operator duties
----------------------------

#. Old operator must finish any merges in progress. The sandbox should be empty. The new operator should inherit a clean slate. This should be done before the first working day of the new operator's shift.

#. Old operator must re-assign `all tickets in the approved column`_ to the new operator.

#. Old operator must re-assign expected indexing failure tickets to the new operator, along with
   ticket that tracks operator duties.

#. New operator must ask to join the ``Azul Operators`` Github permissions group on DataBiosphere, and be given ``Maintainer`` access on GitLab. It's easiest to ask the tech lead via Slack.

.. _all tickets in the approved column: https://github.com/DataBiosphere/azul/pulls?q=is%3Apr+is%3Aopen+reviewed-by%3Ahannes-ucsc+review%3Aapproved
