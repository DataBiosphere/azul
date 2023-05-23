<!--
This is the PR template for upgrading the GitLab instance.
-->

Connected issue: #4014


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `gitlab/yyyy-mm-dd/<GitLab version>`
- [ ] PR title matches `Update GitLab to <GitLab version> (#4014)`
- [ ] Disconnected any other PRs currently connected to #4014 via ZenHub
- [ ] PR is connected to issue #4014 via ZenHub


### Author (deploy)

- [ ] Deployed changes to `dev.gitlab`
- [ ] Deployed changes to `anvildev.gitlab`
- [ ] Deployed changes to `prod.gitlab`


### Primary reviewer (after approval)

- [ ] Verified background migrations for `dev.gitlab` are complete
- [ ] Verified background migrations for `anvildev.gitlab` are complete
- [ ] Verified background migrations for `prod.gitlab` are complete
- [ ] Actually approved the PR
- [ ] Labeled connected issue as `no demo`
- [ ] Labeled PR as `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Added commit title tags to merge commit title
- [ ] Moved connected issue to Merged column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Shortened the PR chain <sub>or this PR is not labeled `base`</sub>
- [ ] Deleted PR branch from GitHub


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
