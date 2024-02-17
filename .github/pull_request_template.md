<!--
This is the PR template for regular PRs against `develop`. Edit the URL in your
browser's location bar, appending either `&template=promotion.md`,
`&template=hotfix.md`, `&template=backport.md` or `&template=upgrade.md` to
switch the template.
-->

Connected issues: #0000


## Checklist


### Author

- [ ] PR is a draft
- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `issues/<GitHub handle of author>/<issue#>-<slug>`
- [ ] On ZenHub, PR is connected to all issues it (partially) resolves
- [ ] PR description links to connected issues
- [ ] PR title matches<sup>1</sup> that of a connected issue <sub>or comment in PR explains why they're different</sub>
- [ ] PR title references all connected issues
- [ ] For each connected issue, there is at least one commit whose title references that issue


### Author (partiality)

- [ ] Added `p` tag to titles of partial commits
- [ ] This PR is labeled `partial` <sub>or completely resolves all connected issues</sub>
- [ ] This PR partially resolves each of the connected issues <sub>or does not have the `partial` label</sub>

<sup>1</sup> when the issue title describes a problem, the corresponding PR
title is `Fix: ` followed by the issue title


### Author (chains)

- [ ] This PR is blocked by previous PR in the chain <sub>or is not chained to another PR</sub>
- [ ] The blocking PR is labeled `base` <sub>or this PR is not chained to another PR</sub>
- [ ] This PR is labeled `chained` <sub>or is not chained to another PR</sub>


### Author (reindex, API changes)

- [ ] Added `r` tag to commit title <sub>or this PR does not require reindexing</sub>
- [ ] This PR is labeled `reindex:dev` <sub>or does not require reindexing `dev`</sub>
- [ ] This PR is labeled `reindex:anvildev` <sub>or does not require reindexing `anvildev`</sub>
- [ ] This PR is labeled `reindex:anvilprod` <sub>or does not require reindexing `anvilprod`</sub>
- [ ] This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full reindex or carries none of the labels `reindex:dev`, `reindex:anvildev`, `reindex:anvilprod` and `reindex:prod`</sub>
- [ ] This PR and its connected issues are labeled `API` <sub>or this PR does not modify a REST API</sub>
- [ ] Added `a` (`A`) tag to commit title for backwards (in)compatible changes <sub>or this PR does not modify a REST API</sub>
- [ ] Updated REST API version number in `app.py` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] Ran `make image_manifests.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] This PR is labeled `deploy:shared` <sub>or does not modify `image_manifests.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (operator tasks)

- [ ] Added checklist items for additional operator tasks <sub>or this PR does not require additional tasks</sub>


### Author (hotfixes)

- [ ] Added `F` tag to main commit title <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] Reverted the temporary hotfixes for any connected issues <sub>or the `prod` branch has no temporary hotfixes for any connected issues</sub>


### Author (before every review)

- [ ] Rebased PR branch on `develop`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `requirements*.txt`, `common.mk`, `Makefile` and `Dockerfile`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `requirements*.txt`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `requirements*.txt`</sub>
- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>


### Peer reviewer (after requesting changes)

Uncheck the *Author (before every review)* checklists.


### Peer reviewer (after approval)

- [ ] PR is not a draft
- [ ] Ticket is in *Review requested* column
- [ ] Requested review from system administrator
- [ ] PR is assigned to system administrator


### System administrator (after requesting changes)

Uncheck the *before every review* checklists. Update the `N reviews` label.


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled connected issues as `demo` or `no demo`
- [ ] Commented on connected issues about demo expectations <sub>or all connected issues are labeled `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] A comment to this PR details the completed security design review <sub>or this PR is a promotion or a backport</sub>
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Checked `reindex:…` labels and `r` commit title tag
- [ ] Checked that demo expectations are clear <sub>or all connected issues are labeled `no demo`</sub>
- [ ] PR has checklist items for upgrading instructions <sub>or PR is not labeled `upgrade`</sub>
- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Ran `_select anvilprod.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvilprod.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] Assigned system administrator <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator

- [ ] Background migrations for `dev.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Background migrations for `anvildev.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Background migrations for `anvilprod.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to operator


### Operator (before pushing merge the commit)

- [ ] Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Ran `_select anvilprod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Deleted unreferenced indices in `sandbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices in `dev`</sub>
- [ ] Deleted unreferenced indices in `anvilbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices in `anvildev`</sub>
- [ ] Deleted unreferenced indices in `hammerbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices in `anvilprod`</sub>
- [ ] Started reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Started reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] Started reindex in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] Checked for failures in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but only include `p` if the PR is labeled `partial`</sub>
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
- [ ] Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvilprod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Deleted PR branch from GitLab `anvilprod`

<sup>1</sup> When pushing the merge commit is skipped due to the PR being
labelled `no sandbox`, the next build triggered by a PR whose merge commit *is*
pushed determines this checklist item.


### Operator (reindex)

- [ ] Deindexed all unreferenced catalogs in `dev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:dev`</sub>
- [ ] Deindexed all unreferenced catalogs in `anvildev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvildev`</sub>
- [ ] Deindexed all unreferenced catalogs in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Deindexed specific sources in `dev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:dev`</sub>
- [ ] Deindexed specific sources in `anvildev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvildev`</sub>
- [ ] Deindexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Indexed specific sources in `dev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:dev`</sub>
- [ ] Indexed specific sources in `anvildev` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvildev`</sub>
- [ ] Indexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Started reindex in `dev` <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Started reindex in `anvildev` <sub>or this PR does not require reindexing `anvildev`</sub>
- [ ] Started reindex in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `dev` <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvildev` <sub>or this PR does not require reindexing `anvildev`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Emptied fail queues in `dev` <sub>or this PR does not require reindexing `dev`</sub>
- [ ] Emptied fail queues in `anvildev` <sub>or this PR does not require reindexing `anvildev`</sub>
- [ ] Emptied fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>


### Operator

- [ ] Propagated the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial` and `reindex:prod` labels to the next promotion PR <sub>or this PR carries none of these labels</sub>
- [ ] Propagated any specific instructions related to the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial` and `reindex:prod` labels from the description of this PR to that of the next promotion PR <sub>or this PR carries none of these labels</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
