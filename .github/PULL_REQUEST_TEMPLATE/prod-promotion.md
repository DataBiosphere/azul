<!--
This is the PR template for a promotion PR against `prod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `prod`
- [ ] `004` Name of PR branch matches `promotions/yyyy-mm-dd-prod`
- [ ] `005` PR is linked to the promotion issue it resolves
- [ ] `006` Status of linked issue is *In progress*
- [ ] `007` PR description links to linked issue
- [ ] `008` Title of linked issue matches `Promotion yyyy-mm-dd`
- [ ] `009` PR title starts with title of linked issue followed by ` prod`
- [ ] `010` PR title references the linked issue


### Author (reindex)

- [ ] `011` This PR is labeled `reindex:prod` <sub>or the changes introduced by it will not require reindexing of `prod`</sub>
- [ ] `012` This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `prod` <sub>or requires a full reindex or is not labeled`reindex:prod`</sub>


### Author (mirror)

- [ ] `013` This PR is labeled `mirror:prod` <sub>or the changes introduced by it will not require mirroring of `prod`</sub>
- [ ] `014` This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `prod` <sub>or requires a full mirroring or is not labeled`mirror:prod`</sub>


### Author (upgrading deployments)

- [ ] `015` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `016` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `017` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `018` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] `019` PR branch is up to date (if not, merge `prod` into PR branch to integrate upstream changes)
- [ ] `020` PR is not a draft
- [ ] `021` PR is awaiting requested review from system administrator
- [ ] `022` Status of PR is *Review requested*
- [ ] `023` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `024` Actually approved the PR
- [ ] `025` Labeled PR as `no sandbox`
- [ ] `026` `N reviews` label is accurate
- [ ] `027` Status of PR is *Approved*
- [ ] `028` PR is assigned to only the operator and the author


### Operator

- [ ] `029` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `030` Ran `_select prod.shared && CI_COMMIT_REF_NAME=prod make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `031` Ran `_select prod.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `032` Ran `_select prod.gitlab && CI_COMMIT_REF_NAME=prod make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `033` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `034` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `035` Background migrations for [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `036` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `037` Ran `_select prod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (merge the branch)

- [ ] `038` All status checks passed and the PR is mergeable
- [ ] `039` The title of the merge commit starts with the title of this PR
- [ ] `040` Added PR # reference to merge commit title
- [ ] `041` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `042` Pushed merge commit to GitHub
- [ ] `043` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `044` Pushed merge commit to GitLab `prod`
- [ ] `045` Build passes on GitLab `prod`
- [ ] `046` Reviewed build logs for anomalies on GitLab `prod`
- [ ] `047` Applied upgrade instructions from UPGRADING.rst to `prod` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `prod`</sub>
- [ ] `048` Ran `_select prod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `049` Deleted PR branch from GitHub
- [ ] `050` PR is assigned to only the operator
- [ ] `051` Status of linked issue is *Stable*
- [ ] `052` Status of promoted<sup>1</sup> PRs is *Merged stable*
- [ ] `053` Status of promoted<sup>1</sup> issues is *Stable*

<sup>1</sup> Promoted issues and PRs are referenced in the titles of the commits
that the promotion branch introduces to the stable branch. Prior to the
promotion, the status of promoted issues (PRs) is *Lower* (*Merged lower*).
Promoted PRs in status *Done* do not need to be moved.


### Operator (reindex)

- [ ] `054` In `prod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `055` In `prod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `056` In `prod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `057` In `prod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:prod` label, or both</sub>
- [ ] `058` Started full reindex in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `059` Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `060` Emptied fail queues in `prod` <sub>or this PR is not labeled `reindex:prod` or it is labeled reindex:partial</sub>
- [ ] `061` Restarted the Data Browser pipeline for the [ucsc/hca/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fhca%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `062` Restarted the Data Browser pipeline for the [ucsc/lungmap/prod branch](https://gitlab.azul.data.humancellatlas.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Flungmap%2Fprod) on GitLab in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>
- [ ] `063` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `prod` <sub>or this PR is not labeled `reindex:prod`</sub>


### Operator (mirroring)

- [ ] `064` Started mirroring in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `065` Checked for, triaged and possibly requeued messages in mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>
- [ ] `066` Emptied mirror fail queue in `prod` <sub>or this PR is not labelled `mirror:prod`</sub>


### Operator

- [ ] `067` PR is assigned to only the system administrator


### System administrator

- [ ] `068` Removed unused image tags from [pycharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `069` Removed unused image tags from [bigquery_emulator image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `070` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
