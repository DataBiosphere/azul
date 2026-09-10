<!--
This is the PR template for hotfix PRs against `prod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] Target branch is `prod`
- [ ] Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-prod`
- [ ] PR is linked to the issue it hotfixes
- [ ] Status of linked issue is *In progress*
- [ ] PR description links to linked issue
- [ ] PR title is `Hotfix prod: ` followed by title of linked issue
- [ ] PR title references the linked issue


### Author (hotfixes)

- [ ] Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR
- [ ] This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>
- [ ] PR carries all applicable `reindex:…` , `mirror:…` and `deploy:…` labels of the preceding incomplete promotion or hotfix PR
- [ ] PR description contains all applicable notes from the preceding incomplete promotion or hotfix PR


### Author (before every review)

- [ ] Rebased PR branch on `prod`, squashed fixups from prior reviews
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] PR is not a draft
- [ ] PR is awaiting requested review from system administrator
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Squashed PR branch and rebased onto `prod`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged stable*


### Operator (main build)

- [ ] Pushed merge commit to GitLab `prod`
- [ ] Build passes on GitLab `prod`
- [ ] Reviewed build logs for anomalies on GitLab `prod`
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Status of linked issue is *Stable*


### Operator (reindex)

- [ ] In `prod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] In `prod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] In `prod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] In `prod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] Started full reindex in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] Emptied fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/hca/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/lungmap/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] Restarted `deploy_browser` job in the GitLab pipeline for this PR in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] Created backport PR and linked to it in a comment on this PR


### Operator (mirroring)

- [ ] Started mirroring in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] Emptied mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
