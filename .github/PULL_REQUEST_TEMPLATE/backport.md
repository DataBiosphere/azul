<!--
This is the PR template for backport PRs against `develop`.
-->

Linked issues: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `develop`
- [ ] `004` Name of PR branch matches `backports/<7-digit SHA1 of most recent backported commit>`
- [ ] `005` PR is linked to the issues it backports
- [ ] `006` Status of linked issues is *Stable*
- [ ] `007` PR title contains the 7-digit SHA1 of the backported commits
- [ ] `008` PR title references the issues relating to the backported commits
- [ ] `009` PR title references the PRs that introduced the backported commits


### Author (before every review)

- [ ] `010` PR branch is up to date (if not, merge `develop` into PR branch to integrate upstream changes)
- [ ] `011` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `012` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `013` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `014` PR is not a draft
- [ ] `015` PR is awaiting requested review from system administrator
- [ ] `016` Status of PR is *Review requested*
- [ ] `017` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `018` Actually approved the PR
- [ ] `019` Decided if PR can be labeled `no sandbox`
- [ ] `020` PR title is appropriate as title of merge commit
- [ ] `021` `N reviews` label is accurate
- [ ] `022` Status of PR is *Approved*
- [ ] `023` PR is assigned to only the operator and the author


### Operator

- [ ] `024` Sanity-checked history
- [ ] `025` Pushed PR branch to GitHub


### Operator (sandbox build)

- [ ] `026` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `027` Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `028` Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `029` Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `030` Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `031` Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `032` Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `033` In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `034` In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `035` In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `036` In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `037` In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `038` In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `039` In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `040` In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `041` Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `042` Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `043` Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `044` Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `045` Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `046` Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] `047` Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `048` Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] `049` All status checks passed and the PR is mergeable
- [ ] `050` The title of the merge commit starts with the title of this PR
- [ ] `051` Added PR # reference (to this PR) to merge commit title
- [ ] `052` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `053` Pushed merge commit to GitHub
- [ ] `054` Status of PR is *Merged lower*


### Operator (main build)

- [ ] `055` Pushed merge commit to GitLab `dev`
- [ ] `056` Pushed merge commit to GitLab `anvildev`
- [ ] `057` Build passes on GitLab `dev`
- [ ] `058` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `059` Build passes on GitLab `anvildev`
- [ ] `060` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `061` Deleted PR branch from GitHub
- [ ] `062` PR is assigned to only the operator
- [ ] `063` Deleted PR branch from GitLab `dev`
- [ ] `064` Deleted PR branch from GitLab `anvildev`
- [ ] `065` Status of linked issues is *Stable*


### Operator

- [ ] `066` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
