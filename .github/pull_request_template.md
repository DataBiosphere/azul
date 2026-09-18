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

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` PR is a draft
- [ ] `004` Target branch is `develop`
- [ ] `005` Name of PR branch matches `issues/<GitHub handle of author>/<issue#>-<slug>`
- [ ] `006` PR is linked to all issues it (partially) resolves
- [ ] `007` Status of linked issues is *In progress*
- [ ] `008` PR description links to linked issues
- [ ] `009` PR title matches<sup>1</sup> that of a linked issue <sub>or comment in PR explains why they're different</sub>
- [ ] `010` PR title references all linked issues
- [ ] `011` For each linked issue, there is at least one commit whose title references that issue

<sup>1</sup> when the issue title describes a problem, the corresponding PR
title is `Fix: ` followed by the issue title


### Author (partiality)

- [ ] `012` Added `p` tag to titles of partial commits
- [ ] `013` This PR is labeled `partial` <sub>or completely resolves all linked issues</sub>
- [ ] `014` This PR partially resolves each of the linked issues <sub>or does not have the `partial` label</sub>


### Author (reindex)

- [ ] `015` Added `r` tag to commit title <sub>or the changes introduced by this PR will not require reindexing of any deployment</sub>
- [ ] `016` This PR is labeled `reindex:dev` <sub>or the changes introduced by it will not require reindexing of `dev`</sub>
- [ ] `017` This PR is labeled `reindex:anvildev` <sub>or the changes introduced by it will not require reindexing of `anvildev`</sub>
- [ ] `018` This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] `019` This PR is labeled `reindex:prod` <sub>or the changes introduced by it will not require reindexing of `prod`</sub>
- [ ] `020` This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full reindex or carries none of the labels `reindex:dev`, `reindex:anvildev`, `reindex:anvilprod` and `reindex:prod`</sub>


### Author (mirror)

- [ ] `021` This PR is labeled `mirror:dev` <sub>or the changes introduced by it will not require mirroring of `dev`</sub>
- [ ] `022` This PR is labeled `mirror:anvildev` <sub>or the changes introduced by it will not require mirroring of `anvildev`</sub>
- [ ] `023` This PR is labeled `mirror:anvilprod` <sub>or the changes introduced by it will not require mirroring of `anvilprod`</sub>
- [ ] `024` This PR is labeled `mirror:prod` <sub>or the changes introduced by it will not require mirroring of `prod`</sub>
- [ ] `025` This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full mirroring or carries none of the labels `mirror:dev`, `mirror:anvildev`, `mirror:anvilprod` and `mirror:prod`</sub>


### Author (API changes)

- [ ] `026` This PR and its linked issues are labeled `API` <sub>or this PR does not modify a REST API</sub>
- [ ] `027` Added `a` (`A`) tag to commit title for backwards (in)compatible changes <sub>or this PR does not modify a REST API</sub>
- [ ] `028` Updated REST API version number in `app.py` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] `029` Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] `030` Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] `031` Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] `032` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `033` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `034` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `035` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (hotfixes)

- [ ] `036` Added `F` tag to main commit title <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] `037` Reverted the temporary hotfixes for any linked issues <sub>or the none of the stable branches (`anvilprod` and `prod`) have temporary hotfixes for any of the issues linked to this PR</sub>


### Author (before every review)

- [ ] `038` Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] `039` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `040` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `041` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `042` `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] `043` PR is awaiting requested review from a peer
- [ ] `044` Status of PR is *Review requested*
- [ ] `045` PR is assigned to only the peer and the author


### Peer reviewer (after approval)

Note that after requesting changes, the PR must be assigned to only the author.

- [ ] `046` Actually approved the PR
- [ ] `047` PR is not a draft
- [ ] `048` PR is awaiting requested review from system administrator
- [ ] `049` Status of PR is *Review requested*
- [ ] `050` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `051` Actually approved the PR
- [ ] `052` Labeled linked issues as `demo` or `no demo`
- [ ] `053` Commented on linked issues about demo expectations <sub>or all linked issues are labeled `no demo`</sub>
- [ ] `054` Decided if PR can be labeled `no sandbox`
- [ ] `055` A comment to this PR details the completed security design review
- [ ] `056` PR title is appropriate as title of merge commit
- [ ] `057` `N reviews` label is accurate
- [ ] `058` Status of PR is *Approved*
- [ ] `059` PR is assigned to only the operator and the author


### Operator

- [ ] `060` Checked `reindex:…` labels and `r` commit title tag
- [ ] `061` Checked `mirror:…` labels
- [ ] `062` Checked that demo expectations are clear <sub>or all linked issues are labeled `no demo`</sub>
- [ ] `063` Squashed PR branch and rebased onto `develop`
- [ ] `064` Sanity-checked history
- [ ] `065` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `066` Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `067` Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `068` Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `069` Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `070` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `071` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `072` Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `073` Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `074` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `075` Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] `076` Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] `077` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `078` Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `079` Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `080` Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `081` Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `082` Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `083` Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `084` Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] `085` Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>
- [ ] `086` In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `087` In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `088` In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `089` In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `090` In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `091` In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `092` In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `093` In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `094` Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `095` Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `096` Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `097` Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `098` Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `099` Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] `100` Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `101` Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] `102` All status checks passed and the PR is mergeable
- [ ] `103` The title of the merge commit starts with the title of this PR
- [ ] `104` Added PR # reference to merge commit title
- [ ] `105` Collected commit title tags in merge commit title <sub>but only included `p` if the PR is also labeled `partial`</sub>
- [ ] `106` Pushed merge commit to GitHub
- [ ] `107` Status of PR is *Merged lower*
- [ ] `108` Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issues</sub>


### Operator (main build)

- [ ] `109` Pushed merge commit to GitLab `dev`
- [ ] `110` Pushed merge commit to GitLab `anvildev`
- [ ] `111` Build passes on GitLab `dev`
- [ ] `112` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `113` Build passes on GitLab `anvildev`
- [ ] `114` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `115` Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] `116` Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] `117` Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] `118` Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `119` Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `120` Deleted PR branch from GitHub
- [ ] `121` PR is assigned to only the operator
- [ ] `122` Deleted PR branch from GitLab `dev`
- [ ] `123` Deleted PR branch from GitLab `anvildev`
- [ ] `124` Status of linked issues is *Lower*, or *Triage*, if PR is partial


### Operator (reindex)

- [ ] `125` In `dev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `126` In `anvildev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `127` In `dev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `128` In `anvildev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `129` In `dev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `130` In `anvildev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `131` In `dev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `132` In `anvildev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `133` Started full reindex in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `134` Started full reindex in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `135` Checked for, triaged and possibly requeued messages in both fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `136` Checked for, triaged and possibly requeued messages in both fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `137` Emptied fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `138` Emptied fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `139` Restarted the Data Browser pipeline for the [ucsc/hca/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fdev) on GitLab in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `140` Restarted the Data Browser pipeline for the [ucsc/lungmap/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fdev) on GitLab in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `141` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `dev` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `142` Restarted the Data Browser pipeline for the [ucsc/anvil/anvildev branch](https://gitlab.anvil.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvildev) on GitLab in `anvildev` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `143` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvildev` <sub>or this PR is not labeled `reindex:anvildev`</sub>


### Operator (mirroring)

- [ ] `144` Started mirroring in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `145` Started mirroring in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] `146` Checked for, triaged and possibly requeued messages in mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `147` Checked for, triaged and possibly requeued messages in mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] `148` Emptied mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `149` Emptied mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>


### Operator

- [ ] `150` Propagated the `upgrade` and `API` labels to the next promotion PRs <sub>or this PR carries neither of these labels</sub>
- [ ] `151` Propagated the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `152` Propagated any specific instructions related to the `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `153` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
