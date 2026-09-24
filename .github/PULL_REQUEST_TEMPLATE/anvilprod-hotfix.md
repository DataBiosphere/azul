<!--
This is the PR template for hotfix PRs against `anvilprod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `anvilprod`
- [ ] `004` Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-anvilprod`
- [ ] `005` PR is linked to the issue it hotfixes
- [ ] `006` Status of linked issue is *In progress*
- [ ] `007` PR description links to linked issue
- [ ] `008` PR title is `Hotfix anvilprod: ` followed by title of linked issue
- [ ] `009` PR title references the linked issue


### Author (hotfixes)

- [ ] `010` Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] `011` Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] `012` Added `hotfix` label to PR
- [ ] `013` This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>
- [ ] `014` PR carries all applicable `reindex:…` , `mirror:…` and `deploy:…` labels of the preceding incomplete promotion or hotfix PR
- [ ] `015` PR description contains all applicable notes from the preceding incomplete promotion or hotfix PR


### Author (before every review)

- [ ] `016` Rebased PR branch on `anvilprod`, squashed fixups from prior reviews
- [ ] `017` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `018` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `019` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `020` PR is not a draft
- [ ] `021` PR is awaiting requested review from system administrator
- [ ] `022` Status of PR is *Review requested*
- [ ] `023` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `024` Actually approved the PR
- [ ] `025` Decided if PR can be labeled `no sandbox`
- [ ] `026` A comment to this PR details the completed security design review
- [ ] `027` PR title is appropriate as title of merge commit
- [ ] `028` `N reviews` label is accurate
- [ ] `029` Status of PR is *Approved*
- [ ] `030` PR is assigned to only the operator and the author


### Operator

- [ ] `031` Squashed PR branch and rebased onto `anvilprod`
- [ ] `032` Sanity-checked history
- [ ] `033` Pushed PR branch to GitHub


### Operator (sandbox build)

- [ ] `034` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `035` Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `036` Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `037` Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `038` In `hammerbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `039` In `hammerbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `040` In `hammerbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `041` In `hammerbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `042` Started full reindex in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `043` Checked for failures in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] `044` Started mirroring in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>
- [ ] `045` Checked for failures in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>


### Operator (merge the branch)

- [ ] `046` All status checks passed and the PR is mergeable
- [ ] `047` The title of the merge commit starts with the title of this PR
- [ ] `048` Added PR # reference to merge commit title
- [ ] `049` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `050` Pushed merge commit to GitHub
- [ ] `051` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `052` Pushed merge commit to GitLab `anvilprod`
- [ ] `053` Build passes on GitLab `anvilprod`
- [ ] `054` Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] `055` Deleted PR branch from GitHub
- [ ] `056` PR is assigned to only the operator
- [ ] `057` Deleted PR branch from GitLab `anvilprod`
- [ ] `058` Status of linked issue is *Stable*


### Operator (reindex)

- [ ] `059` In `anvilprod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `060` In `anvilprod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `061` In `anvilprod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `062` In `anvilprod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `063` Started full reindex in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `064` Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `065` Emptied fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `066` Restarted the Data Browser pipeline for the [ucsc/anvil/anvilprod branch](https://gitlab.explore.anvilproject.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvilprod) on GitLab in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] `067` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] `068` Created backport PR and linked to it in a comment on this PR


### Operator (mirroring)

- [ ] `069` Started mirroring in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] `070` Checked for, triaged and possibly requeued messages in mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] `071` Emptied mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>


### Operator

- [ ] `072` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
