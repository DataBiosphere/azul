<!--
This is the PR template for hotfix PRs against `anvilprod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] Target branch is `anvilprod`
- [ ] Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-anvilprod`
- [ ] PR is linked to the issue it hotfixes
- [ ] Status of linked issue is *In progress*
- [ ] PR description links to linked issue
- [ ] PR title is `Hotfix anvilprod: ` followed by title of linked issue
- [ ] PR title references the linked issue


### Author (hotfixes)

- [ ] Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR
- [ ] This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>
- [ ] PR carries all applicable `reindex:…` , `mirror:…` and `deploy:…` labels of the preceding incomplete promotion or hotfix PR
- [ ] PR description contains all applicable notes from the preceding incomplete promotion or hotfix PR


### Author (before every review)

- [ ] Rebased PR branch on `anvilprod`, squashed fixups from prior reviews
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `Dockerfile`, `environment`, `requirements*.txt`, `common.mk`, `Makefile` or `environment.boot`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `requirements*.txt`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `requirements*.txt`</sub>
- [ ] PR is not a draft
- [ ] PR is awaiting requested review from system administrator
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Squashed PR branch and rebased onto `anvilprod`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub


### Operator (sandbox build)

- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] In `hammerbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `hammerbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `hammerbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `hammerbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] Started full reindex in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] Checked for failures in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Started mirroring in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>
- [ ] Checked for failures in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged stable*


### Operator (main build)

- [ ] Pushed merge commit to GitLab `anvilprod`
- [ ] Build passes on GitLab `anvilprod`
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Deleted PR branch from GitLab `anvilprod`
- [ ] Status of linked issue is *Stable*


### Operator (reindex)

- [ ] In `anvilprod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `anvilprod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `anvilprod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] In `anvilprod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] Started full reindex in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] Emptied fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/anvil/anvilprod branch](https://gitlab.explore.anvilproject.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvilprod) on GitLab in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Created backport PR and linked to it in a comment on this PR


### Operator (mirroring)

- [ ] Started mirroring in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] Emptied mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
