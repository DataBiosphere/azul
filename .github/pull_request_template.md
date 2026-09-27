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

- [ ] `A01` PR is assigned to the author
- [ ] `A02` Status of PR is *In progress*
- [ ] `A03` PR is a draft
- [ ] `A04` Target branch is `develop`
- [ ] `A05` Name of PR branch matches `issues/<GitHub handle of author>/<issue#>-<slug>`
- [ ] `A06` PR is linked to all issues it (partially) resolves
- [ ] `A07` Status of linked issues is *In progress*
- [ ] `A08` PR description links to linked issues
- [ ] `A09` PR title matches<sup>1</sup> that of a linked issue <sub>or comment in PR explains why they're different</sub>
- [ ] `A10` PR title references all linked issues
- [ ] `A11` For each linked issue, there is at least one commit whose title references that issue

<sup>1</sup> when the issue title describes a problem, the corresponding PR
title is `Fix: ` followed by the issue title


### Author (partiality)

- [ ] `B01` Added `p` tag to titles of partial commits
- [ ] `B02` This PR is labeled `partial` <sub>or completely resolves all linked issues</sub>
- [ ] `B03` This PR partially resolves each of the linked issues <sub>or does not have the `partial` label</sub>


### Author (reindex)

- [ ] `C01` Added `r` tag to commit title <sub>or the changes introduced by this PR will not require reindexing of any deployment</sub>
- [ ] `C02` This PR is labeled `reindex:dev` <sub>or the changes introduced by it will not require reindexing of `dev`</sub>
- [ ] `C03` This PR is labeled `reindex:anvildev` <sub>or the changes introduced by it will not require reindexing of `anvildev`</sub>
- [ ] `C04` This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] `C05` This PR is labeled `reindex:prod` <sub>or the changes introduced by it will not require reindexing of `prod`</sub>
- [ ] `C06` This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full reindex or carries none of the labels `reindex:dev`, `reindex:anvildev`, `reindex:anvilprod` and `reindex:prod`</sub>


### Author (mirror)

- [ ] `D01` This PR is labeled `mirror:dev` <sub>or the changes introduced by it will not require mirroring of `dev`</sub>
- [ ] `D02` This PR is labeled `mirror:anvildev` <sub>or the changes introduced by it will not require mirroring of `anvildev`</sub>
- [ ] `D03` This PR is labeled `mirror:anvilprod` <sub>or the changes introduced by it will not require mirroring of `anvilprod`</sub>
- [ ] `D04` This PR is labeled `mirror:prod` <sub>or the changes introduced by it will not require mirroring of `prod`</sub>
- [ ] `D05` This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `dev`, `anvildev`, `anvilprod` and `prod` <sub>or requires a full mirroring or carries none of the labels `mirror:dev`, `mirror:anvildev`, `mirror:anvilprod` and `mirror:prod`</sub>


### Author (API changes)

- [ ] `E01` This PR and its linked issues are labeled `API` <sub>or this PR does not modify a REST API</sub>
- [ ] `E02` Added `a` (`A`) tag to commit title for backwards (in)compatible changes <sub>or this PR does not modify a REST API</sub>
- [ ] `E03` Updated REST API version number in `app.py` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] `F01` Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] `F02` Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] `F03` Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] `F04` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `F05` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `F06` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `F07` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (hotfixes)

- [ ] `G01` Added `F` tag to main commit title <sub>or this PR does not include permanent fix for a temporary hotfix</sub>
- [ ] `G02` Reverted the temporary hotfixes for any linked issues <sub>or the none of the stable branches (`anvilprod` and `prod`) have temporary hotfixes for any of the issues linked to this PR</sub>


### Author (before every review)

- [ ] `H01` Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] `H02` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `H03` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `H04` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `H05` `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] `H06` PR is awaiting requested review from a peer
- [ ] `H07` Status of PR is *Review requested*
- [ ] `H08` PR is assigned to only the peer and the author


### Peer reviewer (after approval)

Note that after requesting changes, the PR must be assigned to only the author.

- [ ] `J01` Actually approved the PR
- [ ] `J02` PR is not a draft
- [ ] `J03` PR is awaiting requested review from system administrator
- [ ] `J04` Status of PR is *Review requested*
- [ ] `J05` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `K01` Actually approved the PR
- [ ] `K02` Labeled linked issues as `demo` or `no demo`
- [ ] `K03` Commented on linked issues about demo expectations <sub>or all linked issues are labeled `no demo`</sub>
- [ ] `K04` Decided if PR can be labeled `no sandbox`
- [ ] `K05` A comment to this PR details the completed security design review
- [ ] `K06` PR title is appropriate as title of merge commit
- [ ] `K07` `N reviews` label is accurate
- [ ] `K08` Status of PR is *Approved*
- [ ] `K09` PR is assigned to only the operator and the author


### Operator

- [ ] `L01` Checked `reindex:…` labels and `r` commit title tag
- [ ] `L02` Checked `mirror:…` labels
- [ ] `L03` Checked that demo expectations are clear <sub>or all linked issues are labeled `no demo`</sub>
- [ ] `L04` Squashed PR branch and rebased onto `develop`
- [ ] `L05` Sanity-checked history
- [ ] `L06` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `M01` Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `M02` Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `M03` Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `M04` Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `M05` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `M06` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `N01` Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `N02` Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `N03` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `P01` Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] `P02` Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] `Q01` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q02` Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q03` Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q04` Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q05` Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q06` Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q07` Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q08` Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] `Q09` Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>
- [ ] `Q10` In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q11` In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q12` In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q13` In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q14` In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q15` In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q16` In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q17` In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q18` Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `Q19` Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `Q20` Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `Q21` Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `Q22` Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `Q23` Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] `Q24` Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `Q25` Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] `R01` All status checks passed and the PR is mergeable
- [ ] `R02` The title of the merge commit starts with the title of this PR
- [ ] `R03` Added PR # reference to merge commit title
- [ ] `R04` Collected commit title tags in merge commit title <sub>but only included `p` if the PR is also labeled `partial`</sub>
- [ ] `R05` Pushed merge commit to GitHub
- [ ] `R06` Status of PR is *Merged lower*
- [ ] `R07` Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issues</sub>


### Operator (main build)

- [ ] `S01` Pushed merge commit to GitLab `dev`
- [ ] `S02` Pushed merge commit to GitLab `anvildev`
- [ ] `S03` Build passes on GitLab `dev`
- [ ] `S04` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `S05` Build passes on GitLab `anvildev`
- [ ] `S06` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `S07` Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] `S08` Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] `S09` Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] `S10` Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `S11` Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `S12` Deleted PR branch from GitHub
- [ ] `S13` PR is assigned to only the operator
- [ ] `S14` Deleted PR branch from GitLab `dev`
- [ ] `S15` Deleted PR branch from GitLab `anvildev`
- [ ] `S16` Status of linked issues is *Lower*, or *Triage*, if PR is partial


### Operator (reindex)

- [ ] `T01` In `dev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `T02` In `anvildev`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `T03` In `dev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `T04` In `anvildev`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `T05` In `dev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `T06` In `anvildev`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `T07` In `dev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `T08` In `anvildev`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `T09` Started full reindex in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `T10` Started full reindex in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `T11` Checked for, triaged and possibly requeued messages in both fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `T12` Checked for, triaged and possibly requeued messages in both fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `T13` Emptied fail queues in `dev` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `T14` Emptied fail queues in `anvildev` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `T15` Restarted the Data Browser pipeline for the [ucsc/hca/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fdev) on GitLab in `dev`, and it succeeded <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `T16` Restarted the Data Browser pipeline for the [ucsc/lungmap/dev branch](https://gitlab.dev.singlecell.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fdev) on GitLab in `dev`, and it succeeded <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `T17` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `dev`, and it succeeded <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `T18` Restarted the Data Browser pipeline for the [ucsc/anvil/anvildev branch](https://gitlab.anvil.gi.ucsc.edu/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvildev) on GitLab in `anvildev`, and it succeeded <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `T19` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvildev`, and it succeeded <sub>or this PR is not labeled `reindex:anvildev`</sub>


### Operator (mirroring)

- [ ] `U01` Started mirroring in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `U02` Started mirroring in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] `U03` Checked for, triaged and possibly requeued messages in mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `U04` Checked for, triaged and possibly requeued messages in mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>
- [ ] `U05` Emptied mirror fail queue in `dev` <sub>or this PR is not labelled `mirror:dev`</sub>
- [ ] `U06` Emptied mirror fail queue in `anvildev` <sub>or this PR is not labelled `mirror:anvildev`</sub>


### Operator

- [ ] `V01` Propagated the `upgrade`, `API`, `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `V02` Propagated any specific instructions related to the `upgrade`, `API`, `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `reindex:partial`, `reindex:anvilprod`, `reindex:prod`, `mirror:partial`, `mirror:anvilprod` and `mirror:prod` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `V03` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
