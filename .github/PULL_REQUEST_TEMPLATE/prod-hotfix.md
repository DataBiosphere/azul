<!--
This is the PR template for hotfix PRs against `prod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `A01` PR is assigned to the author
- [ ] `A02` Status of PR is *In progress*
- [ ] `A03` Target branch is `prod`
- [ ] `A04` Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-prod`
- [ ] `A05` PR is linked to the issue it hotfixes
- [ ] `A06` Status of linked issue is *In progress*
- [ ] `A07` PR description links to linked issue
- [ ] `A08` PR title is `Hotfix prod: ` followed by title of linked issue
- [ ] `A09` PR title references the linked issue


### Author (hotfixes)

- [ ] `G01` Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] `G02` Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] `G03` Added `hotfix` label to PR
- [ ] `G04` This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>
- [ ] `G05` PR carries all applicable `reindex:…` , `mirror:…` and `deploy:…` labels of the preceding incomplete promotion or hotfix PR
- [ ] `G06` PR description contains all applicable notes from the preceding incomplete promotion or hotfix PR


### Author (before every review)

- [ ] `H01` Rebased PR branch on `prod`, squashed fixups from prior reviews
- [ ] `H02` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `H03` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `H04` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `H05` PR is not a draft
- [ ] `H06` PR is awaiting requested review from system administrator
- [ ] `H07` Status of PR is *Review requested*
- [ ] `H08` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `K01` Actually approved the PR
- [ ] `K02` Labeled PR as `no sandbox`
- [ ] `K03` A comment to this PR details the completed security design review
- [ ] `K04` PR title is appropriate as title of merge commit
- [ ] `K05` `N reviews` label is accurate
- [ ] `K06` Status of PR is *Approved*
- [ ] `K07` PR is assigned to only the operator and the author


### Operator

- [ ] `L01` Squashed PR branch and rebased onto `prod`
- [ ] `L02` Sanity-checked history
- [ ] `L03` Pushed PR branch to GitHub


### Operator (merge the branch)

- [ ] `R01` All status checks passed and the PR is mergeable
- [ ] `R02` The title of the merge commit starts with the title of this PR
- [ ] `R03` Added PR # reference to merge commit title
- [ ] `R04` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `R05` Pushed merge commit to GitHub
- [ ] `R06` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `S01` Pushed merge commit to GitLab `prod`
- [ ] `S02` Build passes on GitLab `prod`
- [ ] `S03` Reviewed build logs for anomalies on GitLab `prod`
- [ ] `S04` Deleted PR branch from GitHub
- [ ] `S05` PR is assigned to only the operator
- [ ] `S06` Status of linked issue is *Stable*


### Operator (reindex)

- [ ] `T01` In `prod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T02` In `prod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T03` In `prod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T04` In `prod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T05` Started full reindex in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T06` Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T07` Emptied fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T08` Restarted the Data Browser pipeline for the [ucsc/hca/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fprod) on GitLab in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `T09` Restarted the Data Browser pipeline for the [ucsc/lungmap/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fprod) on GitLab in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `T10` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `T11` Created backport PR and linked to it in a comment on this PR


### Operator (mirroring)

- [ ] `U01` Started mirroring in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `U02` Checked for, triaged and possibly requeued messages in mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `U03` Emptied mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>


### Operator

- [ ] `V01` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
