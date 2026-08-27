<!--
This is the PR template for backport PRs against `develop`.
-->


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `backports/<7-digit SHA1 of most recent backported commit>`
- [ ] Status of linked issue is *Stable*
- [ ] PR title contains the 7-digit SHA1 of the backported commits
- [ ] PR title references the issues relating to the backported commits
- [ ] PR title references the PRs that introduced the backported commits


### Author (before every review)

- [ ] PR branch is up to date (if not, merge `develop` into PR branch to integrate upstream changes)
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
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub


### Operator (sandbox build)

- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference (to this PR) to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged lower*


### Operator (main build)

- [ ] Pushed merge commit to GitLab `dev`
- [ ] Pushed merge commit to GitLab `anvildev`
- [ ] Build passes on GitLab `dev`
- [ ] Reviewed build logs for anomalies on GitLab `dev`
- [ ] Build passes on GitLab `anvildev`
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Status of linked issue is *Stable*


### Operator

- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
