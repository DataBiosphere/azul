Software development policy & procedures
****************************************


Introduction
============

Changes to the system are implemented through a workflow process that involves
multiple team members and covers a range of steps including the
conceptualization, prioritization, implementation, testing, deployment, and
verification of changes on the production environment. Multiple software
products and services are utilized during this process including `git`_ for
version control, `GitHub`_ for issue tracking, pull request (PR) management and
continuous integration (CI), `Terraform`_ for deployment of infrastructure as
code (IaC), `GitLab`_ for CI/CD, and the cloud providers `AWS`_ and `Google
Cloud`_ for cloud resources.

.. _git: https://git-scm.com/
.. _GitHub: https://github.com/
.. _Terraform: https://www.terraform.io/
.. _GitLab: https://about.gitlab.com/
.. _AWS: https://aws.amazon.com/
.. _Google Cloud: https://cloud.google.com/

During this process of managing changes, several separate `deployments`_ are
used. The `dev` deployment receives changes that were merged into the `develop`
branch, and allows verification in a scaled down production-like environment
prior to changes being merged into the `prod` branch and deployed to the `prod`
deployment. For the `AnVIL project`_, this setup is duplicated with both an
`anvildev` and `anvilprod` deployment. The two dev deployments `dev` and
`anvildev` are updated together from the same `develop` branch, however they
differ from each other in their deployment configuration which specifies the set
of TDR snapshots the deployment will index, and which metadata and repository
plugins the deployment will use. Similar to the dev deployments, the two prod
deployments `prod` and `anvilprod` are kept in sync with updates from the `prod`
branch.

.. _deployments: https://docs.google.com/document/d/1Kg0dMZmCw6gtkvabD2jYWPZO2Mx_wsC8BJPKdeKTfg0/edit#bookmark=id.3zefi1arki7p
.. _AnVIL project: https://anvilproject.org/


Issue Management
================

All change requests, including requests for new features, changes to existing
features, or changes that address defects, start as GitHub issues in one of the
GitHub repositories used by the system. The GitHub issue includes a description
of the desired change and, if the issue constitutes a defect, the steps needed
to reproduce it.

The `project manager`_ and `system administrator`_ triage the GitHub issue,
determine its priority relative to existing issues, and assign it to a developer
for implementation.

.. _project manager: https://docs.google.com/document/d/1Kg0dMZmCw6gtkvabD2jYWPZO2Mx_wsC8BJPKdeKTfg0/edit#heading=h.jk936f4i59y8
.. _system administrator: https://docs.google.com/document/d/1Kg0dMZmCw6gtkvabD2jYWPZO2Mx_wsC8BJPKdeKTfg0/edit#heading=h.o3qbvwbucpqo


Code Development & Peer Review
==============================

When working on a GitHub issue, the assigned developer will create a feature
branch using the latest commit from the `develop` branch as a base. The
`develop` branch contains the latest development code, and is updated by the
`operator`_ through the merging of tested, reviewed, and approved feature
branches.

.. _operator: https://docs.google.com/document/d/1Kg0dMZmCw6gtkvabD2jYWPZO2Mx_wsC8BJPKdeKTfg0/edit#heading=h.1rxjx57g24fq

Following the guidance provided in the contribution guide (`CONTRIBUTING.rst`_)
and project README (`README.md`_), the developer implements the requested change
in the feature branch and deploys the branch to their personal deployment for
testing. A developer's personal deployment is similar in configuration to the
`dev` deployment, with the main difference being the scale of the infrastructure
(such as the size of the ElasticSearch domain) and the set of snapshots (frozen
sets of metadata documents) configured to be indexed on each deployment.

.. _CONTRIBUTING.rst: https://github.com/DataBiosphere/azul/blob/develop/CONTRIBUTING.rst
.. _README.md: https://github.com/DataBiosphere/azul/blob/develop/README.md

When the developer’s feature branch is ready, it is pushed to GitHub where a PR
is created and connected to the respective GitHub issue. GitHub performs CI
checks against the branch including running unit tests, checking for
vulnerabilities with `CodeQL`_, checking test coverage with `CodeCov`_ &
`Coveralls`_, and security scanning with `Snyk`_. The unit tests have no
dependencies on the cloud infrastructure in any deployments within the system.
If a unit test covers code that relies on a cloud resource, that resource is
mocked by the test. Only PRs from developers (team members) kick off unit tests.
This is because running unit tests consumes resources and requires credentials
for uploading test coverage results to CodeCov & Coveralls.

.. _CodeQL: https://codeql.github.com/
.. _CodeCov: https://about.codecov.io/
.. _Coveralls: https://github.com/marketplace/coveralls
.. _Snyk: https://snyk.io/

The developer then follows the `checklist`_ included in every PR to ensure that
the PR has been properly set up and is ready for review. When ready, the
developer requests a review from a peer. If the peer has review feedback and/or
requests changes, ownership of the PR goes back to the developer for updates and
the review process repeats.

.. _checklist: https://github.com/DataBiosphere/azul/blob/develop/.github/pull_request_template.md


Change Approval
===============

Once the PR is approved by the peer, a review is requested from the system
administrator. If the system administrator has review feedback and/or requests
changes, ownership of the PR goes back to the developer for updates. After
completing the updates, the developer requests another review from the system
administrator and the review process is repeated until the PR is approved. When
approving a PR, the system administrator decides what procedures (if any) are
needed to demonstrate the resolution of the issue, adds these demo expectations
to the GitHub issue (or marks the issue “no demo”), approves the PR, and assigns
the PR to the operator for further validation and merging the PR's feature
branch into the `develop` branch.


Deployment to dev environment
=============================

To facilitate CI/CD to the various deployments, multiple separate GitLab
instances are used. One GitLab instance is used to manage both the `dev` and
`sandbox` deployments, and another GitLab instance is used solely for the `prod`
deployment. This setup is mirrored for the AnVIL project, with one GitLab
instance to manage both the `anvildev` and `anvilbox` deployments, and another
GitLab instance for the `anvilprod` deployment.

The `sandbox` deployment is similar in configuration to the `dev` deployment,
the main difference being the scale of the infrastructure. The `sandbox` and
`dev` deployments share the same set of snapshots, although `sandbox` only
indexes a subset of each snapshot. The operator follows the PR checklist to
validate the feature branch in the `sandbox` and `anvilbox` deployments prior to
merging into the `develop` branch. The exception to this is when a PR is labeled
`no sandbox`, which indicates that the system administrator has deemed it not
necessary to test the PR in the sandbox, for instance when the change is
specific only to the `dev` deployment.

This process of testing a PR in the `sandbox` deployment starts with the feature
branch being rebased on the latest commit in `develop` and the squashing of any
fixup commits. The operator then pushes the feature branch to GitHub, followed
by `GitLab dev`_ and `GitLab anvildev`_. On GitLab, the branch is run through a
CI/CD pipeline to build, test, deploy the branch to the `sandbox` and `anvilbox`
deployments, and run integration tests against the deployments. The difference
between unit and integration tests is that unit tests are specific to individual
components of the system and will mock components that are outside the focus of
the test, while integration tests follow a holistic approach to verify the
interconnection between the components of the system as a whole.

.. _GitLab dev: https://gitlab.dev.singlecell.gi.ucsc.edu/
.. _GitLab anvildev: https://gitlab.anvil.gi.ucsc.edu/

A reindex is performed on the `sandbox` deployment if the feature branch
includes an update to the set of snapshots indexed by the deployment or changes
the behavior of the indexer in a way that affects the shape of documents in the
ElasticSearch index.

Some PRs require the operator to perform special procedures beyond the standard
deploy/test/reindex cycle. Common examples of this include deploying to the
`shared` components (which manage infrastructure shared between deployments in
the same AWS account, e.g., `dev.shared` and `prod.shared`) and updating the
GitLab instances. These special procedures are referred to as upgrading
instructions and are cumulatively documented in `UPGRADING.rst`_.

.. _UPGRADING.rst: https://github.com/DataBiosphere/azul/blob/develop/UPGRADING.rst

After the CI/CD pipeline in GitLab completes without error, the operator merges
the feature branch into `develop`. The operator then pushes the updated
`develop` branch to GitHub, followed by GitLab `dev` and `anvildev`. On GitLab,
the `develop` branch is run through a CI/CD pipeline again to build, test,
deploy the merged changes, but this time to the `dev` (or `anvildev`)
deployment, and run integration tests against that deployment. A reindex is
performed on the deployment if the feature branch includes an update to the set
of snapshots indexed by the deployment or changes the behavior of the indexer.


Deployment to production environment
====================================

Once a week, the system administrator and operator review the recent changes to
the `develop` branch and decide which changes are ready to be promoted to the
`prod` and `anvilprod` deployments. The decision as to what changes to include
in a promotion considers a number of factors: For one, changes should usually
mature on the develop branch for one week, before they are promoted to `prod`,
so that they can be validated interactively, and more subtle defects like memory
leaks have time to emerge. If the changes affect a REST API in a way that
requires changes to the UI code, a second PR must add those changes to the UI
component. Only after both PRs have been deployed to `dev`, can they be promoted
to `prod`. The operator creates a GitHub issue for the promotion, creates a
branch from the agreed commit in the `develop` branch, pushes the branch to
GitHub, and creates a promotion PR. The promotion PR contains a `promotion
checklist`_ of tasks for the operator to complete to ensure the PR is properly
set up and ready for review. The operator requests a review from the system
administrator, and after approval the PR is assigned back to the operator.

.. _promotion checklist: https://github.com/DataBiosphere/azul/blob/develop/.github/PULL_REQUEST_TEMPLATE/promotion.md

At this time the operator announces the promotion via Slack. The promotion
branch is merged into the `prod` branch, then the updated `prod` branch is
pushed to GitHub, followed by `GitLab prod`_ and `GitLab anvilprod`_. On GitLab,
the `prod` branch is run through a CI/CD pipeline to build, test, deploy to the
`prod` deployment, and run integration tests. A reindex is performed on the
`prod` deployment if the promotion PR includes an update to the set of snapshots
indexed by the deployment or changes to the indexer. The operator also performs
all accumulated upgrading instructions from the changes included in the
promotion. When the operator finishes with the updates, the promoted GitHub
issues are marked as merged, and the promotion PR checklist is completed with
the operator unassigning themself from the promotion PR.

.. _GitLab prod: https://gitlab.azul.data.humancellatlas.org/ucsc/azul
.. _GitLab anvilprod: https://prod.anvil.gi.ucsc.edu

As a final step in the process, a meeting is held once a week for developers to
demonstrate to the team the changes they’ve implemented. Following the demo
expectations provided by the system administrator at the time of approval, a
developer demonstrates the resolution of the GitHub issue to the team, and if
successful the issue is then closed. Issues marked “no demo” are also closed at
this time. In the event that a demonstration shows that the issue has not been
successfully resolved, the original issue will be put back in the developer’s
sprint for additional work, or a new follow-up issue will be created.


Hotfixes and backports
======================

An exception to the procedure of change management and deployment detailed above
is in the case of a `hotfix`_. A hotfix is a change made directly to, or that is
merged into, the `prod` branch without first being merged into the `develop`
branch. The system administrator may determine that a hotfix is necessary when a
defect is discovered following an update to the production environment and there
is need for urgent remediation. Using the checklist included in the `hotfix
PR`_, the change is created, reviewed, and deployed to the production
environment. After a hotfix has been deployed, a `backport PR`_ is created to
backport the change from the `prod` branch to `develop`.

.. _hotfix: https://github.com/DataBiosphere/azul/blob/develop/CONTRIBUTING.rst#hotfixes
.. _hotfix PR: https://github.com/DataBiosphere/azul/blob/develop/.github/PULL_REQUEST_TEMPLATE/hotfix.md
.. _backport PR: https://github.com/DataBiosphere/azul/blob/develop/.github/PULL_REQUEST_TEMPLATE/backport.md


GitLab updates
==============

The GitLab instances used by the system for CI/CD are self-managed, created from
`GitLab Docker images`_, and are routinely updated by the operator as security
release updates and new versions of GitLab become available. When an update to
GitLab is available, the operator reviews the list of changes in the update with
the system administrator. If the update is approved, the operator will first
create a backup of the storage volumes attached to the `dev` and `anvildev`
GitLab instances. The operator then creates a feature branch to update the
version of the `GitLab Docker image`_ and/or `GitLab runner image`_ used by the
system, and deploys this change to the `dev` and `anvildev` deployments. Once
the new GitLab instances have been created and are active, the same change is
deployed to update the GitLab instances used by the production (`prod` and
`anvilprod`) deployments. Once GitLab has been updated on all deployments, a PR
is created from the feature branch, and the PR checklist is followed to get the
PR reviewed, approved, and merged.

.. _GitLab Docker images: https://docs.gitlab.com/ee/install/docker.html
.. _GitLab Docker image: https://hub.docker.com/r/gitlab/gitlab-ce/tags
.. _GitLab runner image: https://hub.docker.com/r/gitlab/gitlab-runner/tags
