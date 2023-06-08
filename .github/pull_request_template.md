<!--
This is the PR template for regular PRs against `develop`. Edit the URL in your
browser's location bar, appending either `&template=promotion.md`,
`&template=hotfix.md`, `&template=backport.md` or `&template=gitlab.md` to
switch the template.
-->

Connected issues: #0000


## Checklist


### Author

- [ ] PR is a draft
- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `issues/<GitHub handle of author>/<issue#>-<slug>`
- [ ] PR title references all connected issues
- [ ] PR title matches<sup>1</sup> that of a connected issue <sub>or comment in PR explains why they're different</sub>
- [ ] For each connected issue, there is at least one commit whose title references that issue
- [ ] PR is connected to all connected issues via ZenHub
- [ ] PR description links to connected issues
- [ ] Added `partial` label to PR <sub>or this PR completely resolves all connected issues</sub>

<sup>1</sup> when the issue title describes a problem, the corresponding PR
title is `Fix: ` followed by the issue title


### Author (reindex, API changes)

- [ ] Added `r` tag to commit title <sub>or this PR does not require reindexing</sub>
- [ ] Added `reindex` label to PR <sub>or this PR does not require reindexing</sub>
- [ ] Added `a` (compatible changes) or `A` (incompatible ones) tag to commit title <sub>or this PR does not modify the Azul service API</sub>
- [ ] Added `API` label to connected issues <sub>or this PR does not modify the Azul service API</sub>


### Author (chains)

- [ ] This PR is blocked by previous PR in the chain <sub>or this PR is not chained to another PR</sub>
- [ ] Added `base` label to the blocking PR <sub>or this PR is not chained to another PR</sub>
- [ ] Added `chained` label to this PR <sub>or this PR is not chained to another PR</sub>


### Author (upgrading)

- [ ] Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading</sub>
- [ ] Added `u` tag to commit title <sub>or this PR does not require upgrading</sub>
- [ ] Added `upgrade` label to PR <sub>or this PR does not require upgrading</sub>


### Author (operator tasks)

- [ ] Added checklist items for additional operator tasks <sub>or this PR does not require additional tasks</sub>


### Author (hotfixes)

- [ ] Added `F` tag to main commit title <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] Reverted the temporary hotfixes for any connected issues <sub>or the `prod` branch has no temporary hotfixes for any connected issues</sub>


### Author (before every review)

- [ ] Rebased PR branch on `develop`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR <sub>or this PR does not touch requirements*.txt</sub>
- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not touch functionality that could break the IT</sub>


### Peer reviewer (after requesting changes)

Uncheck the *Author (before every review)* checklists.


### Peer reviewer (after approval)

- [ ] PR is not a draft
- [ ] Ticket is in *Review requested* column
- [ ] Requested review from primary reviewer
- [ ] Assigned PR to primary reviewer


### Primary reviewer (after requesting changes)

Uncheck the *before every review* checklists. Update the `N reviews` label.


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Labeled connected issues as `demo` or `no demo`
- [ ] Commented on connected issues about demo expectations <sub>or all connected issues are labeled `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Checked `reindex` label and `r` commit title tag
- [ ] Checked that demo expectations are clear <sub>or all connected issues are labeled `no demo`</sub>
- [ ] PR has checklist items for upgrading instructions <sub>or PR is not labeled `upgrade`</sub>
- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Pushed PR branch to GitLab `dev` and added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Deleted unreferenced indices in `sandbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Deleted unreferenced indices in `anvilbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Started reindex in `sandbox` <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Started reindex in `anvilbox` <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR does not require reindexing `sandbox`</sub>
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Added commit title tags to merge commit title
- [ ] Moved connected issues to Merged column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (chain shortening)

- [ ] Changed the target branch of the blocked PR to `develop` <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the `chained` label from the blocked PR <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the blocking relationship from the blocked PR <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the `base` label from this PR <sub>or this PR is not labeled `base`</sub>


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed merge commit to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed merge commit to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes on GitLab `dev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `dev`<sup>1</sup>
- [ ] Build passes on GitLab `anvildev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`<sup>1</sup>
- [ ] Build passes on GitLab `anvilprod`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`<sup>1</sup>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Deleted PR branch from GitLab `anvilprod`

<sup>1</sup> When pushing the merge commit is skipped due to the PR being
labelled `no sandbox`, the next build triggered by a PR whose merge commit *is*
pushed determines this checklist item.


### Operator (reindex)

- [ ] Deleted unreferenced indices in `dev` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Deleted unreferenced indices in `anvildev` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Deleted unreferenced indices in `anvilprod` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Started reindex in `dev` <sub>or this PR does not require reindexing</sub>
- [ ] Started reindex in `anvildev` <sub>or this PR does not require reindexing</sub>
- [ ] Started reindex in `anvilprod` <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures in `dev` <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures in `anvildev` <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures in `anvilprod` <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in `dev` deployment <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in `anvildev` deployment <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in `anvilprod` deployment <sub>or this PR does not require reindexing</sub>


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
