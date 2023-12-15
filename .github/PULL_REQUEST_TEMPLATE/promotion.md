<!--
This is the PR template for promotion PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `prod`
- [ ] Name of PR branch matches `promotions/yyyy-mm-dd`
- [ ] On ZenHub, PR is connected to the promotion issue it resolves
- [ ] PR description links to connected issue
- [ ] Title of connected issue matches `Promotion yyyy-mm-dd`
- [ ] PR title starts with title of connected issue
- [ ] PR title references the connected issue


### Author (reindex, API changes)

- [ ] Added `reindex` label to PR <sub>or this PR does not require reindexing</sub>
- [ ] PR and connected issue are labeled `API` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading)

- [ ] Added `upgrade` label to PR <sub>or this PR does not require upgrading</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Pushed PR branch to GitHub
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but exclude any `p` tags</sub>
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `prod`
- [ ] Build passes on GitLab `prod`
- [ ] Reviewed build logs for anomalies on GitLab `prod`
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `prod`
- [ ] Moved connected issue to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *Merged* to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *dev* to *prod* column on ZenHub


### Operator (reindex)

- [ ] Deleted unreferenced indices in `prod` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Started reindex in `prod` <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures in `prod` <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in `prod` deployment <sub>or this PR does not require reindexing</sub>


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
