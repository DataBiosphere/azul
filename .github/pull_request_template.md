<!--
This is the PR template for regular PRs against `develop`. Edit the URL in your
browser's location bar, appending either `&template=anvilprod-promotion.md`,
`&template=prod-promotion.md`, `&template=anvilprod-hotfix.md`, `&template=prod-
hotfix.md`, `&template=backport.md` or `&template=upgrade.md` to switch the
template.
-->

Linked issues: #0000


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] PR is a draft
- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `issues/<GitHub handle of author>/<issue#>-<slug>`
- [ ] PR is linked to all issues it (partially) resolves
- [ ] Status of linked issues is *In progress*
- [ ] PR description links to linked issues
- [ ] PR title matches<sup>1</sup> that of a linked issue <sub>or comment in PR explains why they're different</sub>
- [ ] PR title references all linked issues
- [ ] For each linked issue, there is at least one commit whose title references that issue

<sup>1</sup> when the issue title describes a problem, the corresponding PR
title is `Fix: ` followed by the issue title


### Author (partiality)

- [ ] Added `p` tag to titles of partial commits
- [ ] This PR is labeled `partial` <sub>or completely resolves all linked issues</sub>
- [ ] This PR partially resolves each of the linked issues <sub>or does not have the `partial` label</sub>


### Author (reindex)

- [ ] Added `r` tag to commit title <sub>or the changes introduced by this PR will not require reindexing of any deployment</sub>
- [ ] This PR is labeled `reindex:dev` <sub>or the changes introduced by it will not require reindexing of `dev`</sub>
- [ ] This PR is labeled `reindex:anvildev` <sub>or the changes introduced by it will not require reindexing of `anvildev`</sub>
- [ ] This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] This PR is labeled `reindex:prod` <sub>or the changes introduced by it will not require reindexing of `prod`</sub>
- [ ] This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full reindex or carries none of the labels `reindex:dev`, `reindex:anvildev`, `reindex:anvilprod` and `reindex:prod`</sub>


### Author (mirror)

- [ ] This PR is labeled `mirror:dev` <sub>or the changes introduced by it will not require mirroring of `dev`</sub>
- [ ] This PR is labeled `mirror:anvildev` <sub>or the changes introduced by it will not require mirroring of `anvildev`</sub>
- [ ] This PR is labeled `mirror:anvilprod` <sub>or the changes introduced by it will not require mirroring of `anvilprod`</sub>
- [ ] This PR is labeled `mirror:prod` <sub>or the changes introduced by it will not require mirroring of `prod`</sub>
- [ ] This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full mirroring or carries none of the labels `mirror:dev`, `mirror:anvildev`, `mirror:anvilprod` and `mirror:prod`</sub>


### Author (API changes)

- [ ] This PR and its linked issues are labeled `API` <sub>or this PR does not modify a REST API</sub>
- [ ] Added `a` (`A`) tag to commit title for backwards (in)compatible changes <sub>or this PR does not modify a REST API</sub>
- [ ] Updated REST API version number in `app.py` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (hotfixes)

- [ ] Added `F` tag to main commit title <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] Reverted the temporary hotfixes for any linked issues <sub>or the none of the stable branches (`anvilprod` and `prod`) have temporary hotfixes for any of the issues linked to this PR</sub>


### Author (before every review)

- [ ] Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] PR is awaiting requested review from a peer
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the peer and the author


### Peer reviewer (after approval)

Note that after requesting changes, the PR must be assigned to only the author.

- [ ] Actually approved the PR
- [ ] PR is not a draft
- [ ] PR is awaiting requested review from system administrator
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled linked issues as `demo` or `no demo`
- [ ] Commented on linked issues about demo expectations <sub>or all linked issues are labeled `no demo`</sub>
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Checked `reindex:…` labels and `r` commit title tag
- [ ] Checked `mirror:…` labels
- [ ] Checked that demo expectations are clear <sub>or all linked issues are labeled `no demo`</sub>
- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>
- [ ] In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but only included `p` if the PR is also labeled `partial`</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged lower*
- [ ] Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issues</sub>


### Operator (main build)

- [ ] Pushed merge commit to GitLab `dev`
- [ ] Pushed merge commit to GitLab `anvildev`
- [ ] Build passes on GitLab `dev`
- [ ] Reviewed build logs for anomalies on GitLab `dev`
- [ ] Build passes on GitLab `anvildev`
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Status of linked issues is *Lower*, or *Triage*, if PR is partial


### Operator (reindex)

- [ ] In `dev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvildev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `dev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvildev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `dev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvildev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `dev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvildev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] Started full reindex in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] Started full reindex in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] Emptied fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] Emptied fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/hca/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fdev) on GitLab in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/lungmap/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fdev) on GitLab in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Restarted `deploy_browser` job in the GitLab pipeline for this PR in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/anvil/anvildev branch](https://gitlab.anvil.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvildev) on GitLab in `anvildev` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvildev` <sub>or this PR is not labeled `reindex:anvildev`</sub>


### Operator (mirroring)

- [ ] Started mirroring in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] Started mirroring in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] Checked for, triaged and possibly requeued messages in mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] Checked for, triaged and possibly requeued messages in mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] Emptied mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] Emptied mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>


### Operator

- [ ] Propagated the `upgrade` and `API` labels to the next promotion PRs <sub>or this PR carries neither of these labels</sub>
- [ ] Propagated the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] Propagated any specific instructions related to the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
