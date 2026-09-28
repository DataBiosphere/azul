<!--
This is the PR template for a promotion PR against `prod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `A01` PR is assigned to the author
- [ ] `A02` Status of PR is *In progress*
- [ ] `A03` Target branch is `prod`
- [ ] `A04` Name of PR branch matches `promotions/yyyy-mm-dd-prod`
- [ ] `A05` PR is linked to the promotion issue it resolves
- [ ] `A06` Status of linked issue is *In progress*
- [ ] `A07` PR description links to linked issue
- [ ] `A08` Title of linked issue matches `Promotion yyyy-mm-dd`
- [ ] `A09` PR title starts with title of linked issue followed by ` prod`
- [ ] `A10` PR title references the linked issue
- [ ] `A11` Propagated the `upgrade`, `API`, `deploy:shared`, `deploy:gitlab`, `deploy:runner`, `backup:gitlab`, `reindex:partial`, `reindex:prod`, `mirror:partial` and `mirror:prod` labels and associated notes from the PRs included in this promotion <sub>or none of the promoted PRs include any of these labels</sub>


### Author (reindex)

- [ ] `C01` This PR is labeled `reindex:prod` <sub>or the changes introduced by it will not require reindexing of `prod`</sub>
- [ ] `C02` This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `prod` <sub>or requires a full reindex or is not labeled`reindex:prod`</sub>


### Author (mirror)

- [ ] `D01` This PR is labeled `mirror:prod` <sub>or the changes introduced by it will not require mirroring of `prod`</sub>
- [ ] `D02` This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `prod` <sub>or requires a full mirroring or is not labeled`mirror:prod`</sub>


### Author (upgrading deployments)

- [ ] `F01` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `F02` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `F03` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `F04` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] `H01` PR branch is up to date (if not, merge `prod` into PR branch to integrate upstream changes)
- [ ] `H02` PR is not a draft
- [ ] `H03` PR is awaiting requested review from system administrator
- [ ] `H04` Status of PR is *Review requested*
- [ ] `H05` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `K01` Actually approved the PR
- [ ] `K02` Labeled PR as `no sandbox`
- [ ] `K03` `N reviews` label is accurate
- [ ] `K04` Status of PR is *Approved*
- [ ] `K05` PR is assigned to only the operator and the author


### Operator

- [ ] `L01` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `M01` Ran `_select prod.shared && CI_COMMIT_REF_NAME=prod make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `M02` Ran `_select prod.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `M03` Ran `_select prod.gitlab && CI_COMMIT_REF_NAME=prod make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `M04` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `M05` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `N01` Background migrations for [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `N02` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `P01` Ran `_select prod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (merge the branch)

- [ ] `R01` All status checks passed and the PR is mergeable
- [ ] `R02` The title of the merge commit starts with the title of this PR
- [ ] `R03` Added PR # reference to merge commit title
- [ ] `R04` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `R05` Pushed merge commit to GitHub
- [ ] `R06` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `S01` Pushed merge commit to GitLab `prod`
- [ ] `S02` Build passes on GitLab `prod`
- [ ] `S03` Reviewed build logs for anomalies on GitLab `prod`
- [ ] `S04` Applied upgrade instructions from UPGRADING.rst to `prod` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `prod`</sub>
- [ ] `S05` Ran `_select prod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `S06` Deleted PR branch from GitHub
- [ ] `S07` PR is assigned to only the operator
- [ ] `S08` Status of linked issue is *Stable*
- [ ] `S09` Status of promoted<sup>1</sup> PRs is *Merged stable*
- [ ] `S10` Status of promoted<sup>1</sup> issues is *Stable*

<sup>1</sup> Promoted issues and PRs are referenced in the titles of the commits
that the promotion branch introduces to the stable branch. Prior to the
promotion, the status of promoted issues (PRs) is *Lower* (*Merged lower*).
Promoted PRs in status *Done* do not need to be moved.


### Operator (reindex)

- [ ] `T01` In `prod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T02` In `prod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T03` In `prod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T04` In `prod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `T05` Started full reindex in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T06` Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T07` Emptied fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `T08` Restarted the Data Browser pipeline for the [ucsc/hca/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fprod) on GitLab in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `T09` Restarted the Data Browser pipeline for the [ucsc/lungmap/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fprod) on GitLab in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `T10` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `prod`, and it succeeded <sub>or this PR is not labeled `reindex:prod`</sub>


### Operator (mirroring)

- [ ] `U01` Started mirroring in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `U02` Checked for, triaged and possibly requeued messages in mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `U03` Emptied mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>


### Operator

- [ ] `V01` PR is assigned to only the system administrator


### System administrator

- [ ] `W01` Removed unused image tags from [pycharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `W02` Removed unused image tags from [bigquery_emulator image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `W03` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
