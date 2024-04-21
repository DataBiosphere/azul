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
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `requirements*.txt`, `common.mk`, `Makefile` and `Dockerfile`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `requirements*.txt`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `requirements*.txt`</sub>


### System administrator (after requesting changes)

Uncheck the *before every review* checklists. Update the `N reviews` label.


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] A comment to this PR details the completed security design review <sub>or this PR is a promotion or a backport</sub>
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference (to this PR) to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed merge commit to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes on GitLab `dev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `dev`<sup>1</sup>
- [ ] Build passes on GitLab `anvildev`<sup>1</sup>
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`<sup>1</sup>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`

<sup>1</sup> When pushing the merge commit is skipped due to the PR being
labelled `no sandbox`, the next build triggered by a PR whose merge commit *is*
pushed determines this checklist item.


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
