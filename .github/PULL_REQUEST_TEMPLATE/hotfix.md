<!--
This is the PR template for hotfix PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `prod`
- [ ] Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>`
- [ ] On ZenHub, PR is connected to the issue it hotfixes
- [ ] PR description links to connected issue
- [ ] PR title is `Hotfix: ` followed by title of connected issue
- [ ] PR title references the connected issue


### Author (hotfixes)

- [ ] Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] Added `hotfix` label to PR
- [ ] Added `partial` label to PR <sub>or this PR is a permanent hotfix</sub>


### Author (before every review)

- [ ] Rebased PR branch on `prod`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR <sub>or this PR does not touch requirements*.txt</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `prod`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but exclude any `p` tags</sub>
- [ ] Moved connected issue to *Merged prod* column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `prod`
- [ ] Build passes on GitLab `prod`
- [ ] Reviewed build logs for anomalies on GitLab `prod`
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `prod`


### Operator (reindex)

- [ ] Deleted unreferenced indices in `prod` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices in `prod`</sub>
- [ ] Considered deindexing individual sources in `prod` <sub>or this PR does not remove individual sources from existing catalogs in `prod`</sub>
- [ ] Considered indexing individual sources in `prod` <sub>or this PR does not merely add individual sources to existing catalogs in `prod`</sub>
- [ ] Started reindex in `prod` <sub>or neither this PR nor a prior failed promotion requires it</sub>
- [ ] Checked for and triaged indexing failures in `prod` <sub>or neither this PR nor a prior failed promotion requires it</sub>
- [ ] Emptied fail queues in `prod` deployment <sub>or neither this PR nor a prior failed promotion requires it</sub>
- [ ] Created backport PR and linked to it in a comment on this PR


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
