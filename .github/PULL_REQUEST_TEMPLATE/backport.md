<!--
This is the PR template for backport PRs against `develop`.
-->

Linked issues: #0000


## Checklist


### Author

- [ ] `A01` PR is assigned to the author
- [ ] `A02` Status of PR is *In progress*
- [ ] `A03` Target branch is `develop`
- [ ] `A04` Name of PR branch matches `backports/<7-digit SHA1 of most recent backported commit>`
- [ ] `A05` PR is linked to the issues it backports
- [ ] `A06` Status of linked issues is *Stable*
- [ ] `A07` PR title contains the 7-digit SHA1 of the backported commits
- [ ] `A08` PR title references the issues relating to the backported commits
- [ ] `A09` PR title references the PRs that introduced the backported commits


### Author (before every review)

- [ ] `H01` PR branch is up to date (if not, merge `develop` into PR branch to integrate upstream changes)
- [ ] `H02` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `H03` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `H04` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `H05` PR is not a draft
- [ ] `H06` PR is awaiting requested review from system administrator
- [ ] `H07` Status of PR is *Review requested*
- [ ] `H08` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `K01` Actually approved the PR
- [ ] `K02` Decided if PR can be labeled `no sandbox`
- [ ] `K03` PR title is appropriate as title of merge commit
- [ ] `K04` `N reviews` label is accurate
- [ ] `K05` Status of PR is *Approved*
- [ ] `K06` PR is assigned to only the operator and the author


### Operator

- [ ] `L01` Sanity-checked history
- [ ] `L02` Pushed PR branch to GitHub


### Operator (sandbox build)

- [ ] `Q01` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q02` Pushed PR branch to GitLab `dev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q03` Pushed PR branch to GitLab `anvildev` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q04` Build passes in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q05` Build passes in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q06` Reviewed build logs for anomalies in `sandbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q07` Reviewed build logs for anomalies in `anvilbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `Q08` In `sandbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q09` In `anvilbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q10` In `sandbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q11` In `anvilbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q12` In `sandbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q13` In `anvilbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q14` In `sandbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:dev` label, or both</sub>
- [ ] `Q15` In `anvilbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvildev` label, or both</sub>
- [ ] `Q16` Started full reindex in `sandbox` <sub>or this PR is not labeled `reindex:dev` or it is labeled reindex:partial</sub>
- [ ] `Q17` Started full reindex in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev` or it is labeled reindex:partial</sub>
- [ ] `Q18` Checked for failures in `sandbox` <sub>or this PR is not labeled `reindex:dev`</sub>
- [ ] `Q19` Checked for failures in `anvilbox` <sub>or this PR is not labeled `reindex:anvildev`</sub>
- [ ] `Q20` Started mirroring in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `Q21` Started mirroring in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>
- [ ] `Q22` Checked for failures in `sandbox` <sub>or this PR is not labeled `mirror:dev`</sub>
- [ ] `Q23` Checked for failures in `anvilbox` <sub>or this PR is not labeled `mirror:anvildev`</sub>


### Operator (merge the branch)

- [ ] `R01` All status checks passed and the PR is mergeable
- [ ] `R02` The title of the merge commit starts with the title of this PR
- [ ] `R03` Added PR # reference (to this PR) to merge commit title
- [ ] `R04` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `R05` Pushed merge commit to GitHub
- [ ] `R06` Status of PR is *Merged lower*


### Operator (main build)

- [ ] `S01` Pushed merge commit to GitLab `dev`
- [ ] `S02` Pushed merge commit to GitLab `anvildev`
- [ ] `S03` Build passes on GitLab `dev`
- [ ] `S04` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `S05` Build passes on GitLab `anvildev`
- [ ] `S06` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `S07` Deleted PR branch from GitHub
- [ ] `S08` PR is assigned to only the operator
- [ ] `S09` Deleted PR branch from GitLab `dev`
- [ ] `S10` Deleted PR branch from GitLab `anvildev`
- [ ] `S11` Status of linked issues is *Stable*


### Operator

- [ ] `V01` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
