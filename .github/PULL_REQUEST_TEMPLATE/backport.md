<!--
This is the PR template for backport PRs against `develop`.
-->


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `backports/<7-digit SHA1 of most recent backported commit>`
- [ ] PR title contains the 7-digit SHA1 of the backported commits
- [ ] PR title references the issues relating to the backported commits
- [ ] PR title references the PRs that introduced the backported commits


### Author (before every review)

- [ ] Merged `develop` into PR branch to integrate upstream changes
- [ ] Ran `make requirements_update` <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR <sub>or this PR does not touch requirements*.txt</sub>


### Primary reviewer (after requesting changes)

Uncheck the *before every review* checklists. Update the `N reviews` label.


### Primary reviewer (after approval)

- [ ] Actually approved the PR
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] Assigned PR to current operator


### Operator (before pushing merge the commit)

- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Pushed PR branch to GitLab `dev` and added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference (this PR) to merge commit title
- [ ] Added commit title tags to merge commit title
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed merge commit to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed merge commit to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes on GitLab `dev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `dev`<sup>1</sup>
- [ ] Build passes on GitLab `anvildev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`<sup>1</sup>
- [ ] Build passes on GitLab `anvilprod`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`<sup>1</sup>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Deleted PR branch from GitLab `anvilprod`

<sup>1</sup> When pushing the merge commit is skipped due to the PR being
labelled `no sandbox`, the next build triggered by a PR whose merge commit *is*
pushed determines this checklist item.


### Operator

- [ ] Unassigned PR


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
