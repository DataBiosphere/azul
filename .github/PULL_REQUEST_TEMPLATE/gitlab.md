<!--
This is the PR template for upgrading the GitLab instance.
-->

Connected issue: #4014


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `gitlab/yyyy-mm-dd/<GitLab version>`
- [ ] On Zenhub, no other PRs are connected to #4014
- [ ] On ZenHub, this PR is connected to issue #4014
- [ ] PR title matches `Update GitLab to <GitLab version> (#4014)`


### Author (deploy)

- [ ] Deployed changes to `dev.gitlab`
- [ ] Deployed changes to `anvildev.gitlab`
- [ ] Deployed changes to `anvilprod.gitlab`
- [ ] Deployed changes to `prod.gitlab`


### System administrator (after approval)

- [ ] Verified background migrations for `dev.gitlab` are complete
- [ ] Verified background migrations for `anvildev.gitlab` are complete
- [ ] Verified background migrations for `anvilprod.gitlab` are complete
- [ ] Verified background migrations for `prod.gitlab` are complete
- [ ] Actually approved the PR
- [ ] Labeled connected issue as `no demo`
- [ ] Labeled PR as `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but exclude any `p` tags</sub>
- [ ] Moved connected issue to Merged column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (chain shortening)

- [ ] Changed the target branch of the blocked PR to `develop` <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the `chained` label from the blocked PR <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the blocking relationship from the blocked PR <sub>or this PR is not labeled `base`</sub>
- [ ] Removed the `base` label from this PR <sub>or this PR is not labeled `base`</sub>


### Operator (after pushing the merge commit)

- [ ] Deleted PR branch from GitHub


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
