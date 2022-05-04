<!-- 
This is the PR template for upgrading the GitLab instance.
-->

Connected issue: #4014


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Source branch matches `gitlab/yyyy-mm-dd`
- [ ] PR title is `Update GitLab to X.Y.Z (#4014)`
- [ ] Disconnected any other PRs currently connected to #4014 via ZenHub
- [ ] PR is connected to issue #4014 via ZenHub


### Author (deploy)

- [ ] Deployed changes to `dev.gitlab`
- [ ] Deployed changes to `prod.gitlab`


### Primary reviewer (after approval)

- [ ] Verified background migrations for `dev.gitlab` are complete
- [ ] Verified background migrations for `prod.gitlab` are complete
- [ ] Actually approved the PR
- [ ] Labeled connected issue as `no demo`
- [ ] Labeled connected PR as `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Rebased and squashed branch
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title
- [ ] Moved connected issue to Merged column
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Shortened the PR chain                                        <sub>or this PR is not labeled `chain`</sub>
- [ ] Deleted PR branch from GitHub


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem