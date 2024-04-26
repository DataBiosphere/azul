<!--
This is the PR template for hotfix PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `prod`
- [ ] Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-prod`
- [ ] On ZenHub, PR is connected to the issue it hotfixes
- [ ] PR description links to connected issue
- [ ] PR title is `Hotfix prod: ` followed by title of connected issue
- [ ] PR title references the connected issue


### Author (hotfixes)

- [ ] Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR
- [ ] This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>


### Author (before every review)

- [ ] Rebased PR branch on `prod`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `requirements*.txt`, `common.mk`, `Makefile` and `Dockerfile`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `requirements*.txt`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `requirements*.txt`</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `prod`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Moved connected issue to *Merged stable* column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `prod`
- [ ] Build passes on GitLab `prod`
- [ ] Reviewed build logs for anomalies on GitLab `prod`
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `prod`


### Operator (reindex)

- [ ] Deindexed all unreferenced catalogs in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Deindexed specific sources in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Indexed specific sources in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Started reindex in `prod` <sub>or neither this PR nor a failed, prior promotion requires it</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or neither this PR nor a failed, prior promotion requires it</sub>
- [ ] Emptied fail queues in `prod` <sub>or neither this PR nor a failed, prior promotion requires it</sub>
- [ ] Created backport PR and linked to it in a comment on this PR


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
