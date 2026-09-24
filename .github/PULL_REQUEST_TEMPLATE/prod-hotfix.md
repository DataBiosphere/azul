<!--
This is the PR template for hotfix PRs against `prod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `prod`
- [ ] `004` Name of PR branch matches `hotfixes/<GitHub handle of author>/<issue#>-<slug>-prod`
- [ ] `005` PR is linked to the issue it hotfixes
- [ ] `006` Status of linked issue is *In progress*
- [ ] `007` PR description links to linked issue
- [ ] `008` PR title is `Hotfix prod: ` followed by title of linked issue
- [ ] `009` PR title references the linked issue


### Author (hotfixes)

- [ ] `010` Added `h` tag to commit title <sub>or this PR does not include a temporary hotfix</sub>
- [ ] `011` Added `H` tag to commit title <sub>or this PR does not include a permanent hotfix</sub>
- [ ] `012` Added `hotfix` label to PR
- [ ] `013` This PR is labeled `partial` <sub>or represents a permanent hotfix</sub>
- [ ] `014` PR carries all applicable `reindex:…` , `mirror:…` and `deploy:…` labels of the preceding incomplete promotion or hotfix PR
- [ ] `015` PR description contains all applicable notes from the preceding incomplete promotion or hotfix PR


### Author (before every review)

- [ ] `016` Rebased PR branch on `prod`, squashed fixups from prior reviews
- [ ] `017` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `018` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `019` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `020` PR is not a draft
- [ ] `021` PR is awaiting requested review from system administrator
- [ ] `022` Status of PR is *Review requested*
- [ ] `023` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `024` Actually approved the PR
- [ ] `025` Labeled PR as `no sandbox`
- [ ] `026` A comment to this PR details the completed security design review
- [ ] `027` PR title is appropriate as title of merge commit
- [ ] `028` `N reviews` label is accurate
- [ ] `029` Status of PR is *Approved*
- [ ] `030` PR is assigned to only the operator and the author


### Operator

- [ ] `031` Squashed PR branch and rebased onto `prod`
- [ ] `032` Sanity-checked history
- [ ] `033` Pushed PR branch to GitHub


### Operator (merge the branch)

- [ ] `034` All status checks passed and the PR is mergeable
- [ ] `035` The title of the merge commit starts with the title of this PR
- [ ] `036` Added PR # reference to merge commit title
- [ ] `037` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `038` Pushed merge commit to GitHub
- [ ] `039` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `040` Pushed merge commit to GitLab `prod`
- [ ] `041` Build passes on GitLab `prod`
- [ ] `042` Reviewed build logs for anomalies on GitLab `prod`
- [ ] `043` Deleted PR branch from GitHub
- [ ] `044` PR is assigned to only the operator
- [ ] `045` Status of linked issue is *Stable*


### Operator (reindex)

- [ ] `046` In `prod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `047` In `prod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `048` In `prod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `049` In `prod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `050` Started full reindex in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `051` Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `052` Emptied fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `053` Restarted the Data Browser pipeline for the [ucsc/hca/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `054` Restarted the Data Browser pipeline for the [ucsc/lungmap/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `055` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `056` Created backport PR and linked to it in a comment on this PR


### Operator (mirroring)

- [ ] `057` Started mirroring in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `058` Checked for, triaged and possibly requeued messages in mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `059` Emptied mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>


### Operator

- [ ] `060` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
