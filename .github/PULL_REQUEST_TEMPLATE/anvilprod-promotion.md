<!--
This is the PR template for a promotion PR against `anvilprod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `anvilprod`
- [ ] `004` Name of PR branch matches `promotions/yyyy-mm-dd-anvilprod`
- [ ] `005` PR is linked to the promotion issue it resolves
- [ ] `006` Status of linked issue is *In progress*
- [ ] `007` PR description links to linked issue
- [ ] `008` Title of linked issue matches `Promotion yyyy-mm-dd`
- [ ] `009` PR title starts with title of linked issue followed by ` anvilprod`
- [ ] `010` PR title references the linked issue


### Author (reindex)

- [ ] `011` This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] `012` This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `anvilprod` <sub>or requires a full reindex or is not labeled`reindex:anvilprod`</sub>


### Author (mirror)

- [ ] `013` This PR is labeled `mirror:anvilprod` <sub>or the changes introduced by it will not require mirroring of `anvilprod`</sub>
- [ ] `014` This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `anvilprod` <sub>or requires a full mirroring or is not labeled`mirror:anvilprod`</sub>


### Author (upgrading deployments)

- [ ] `015` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `016` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `017` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `018` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] `019` PR branch is up to date (if not, merge `anvilprod` into PR branch to integrate upstream changes)
- [ ] `020` PR is not a draft
- [ ] `021` PR is awaiting requested review from system administrator
- [ ] `022` Status of PR is *Review requested*
- [ ] `023` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `024` Actually approved the PR
- [ ] `025` Decided if PR can be labeled `no sandbox`
- [ ] `026` `N reviews` label is accurate
- [ ] `027` Status of PR is *Approved*
- [ ] `028` PR is assigned to only the operator and the author


### Operator

- [ ] `029` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `030` Ran `_select anvilprod.shared && CI_COMMIT_REF_NAME=anvilprod make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `031` Ran `_select anvilprod.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `032` Ran `_select anvilprod.gitlab && CI_COMMIT_REF_NAME=anvilprod make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `033` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `034` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `035` Background migrations for [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `036` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `037` Ran `_select anvilprod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] `038` Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] `039` Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] `040` Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `041` Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] `042` Applied upgrade instructions from UPGRADING.rst to `hammerbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `hammerbox`</sub>
- [ ] `043` In `hammerbox`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `044` In `hammerbox`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `045` In `hammerbox`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `046` In `hammerbox`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `047` Started full reindex in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `048` Checked for failures in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] `049` Started mirroring in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>
- [ ] `050` Checked for failures in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>


### Operator (merge the branch)

- [ ] `051` All status checks passed and the PR is mergeable
- [ ] `052` The title of the merge commit starts with the title of this PR
- [ ] `053` Added PR # reference to merge commit title
- [ ] `054` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `055` Pushed merge commit to GitHub
- [ ] `056` Status of PR is *Merged stable*


### Operator (main build)

- [ ] `057` Pushed merge commit to GitLab `anvilprod`
- [ ] `058` Build passes on GitLab `anvilprod`
- [ ] `059` Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] `060` Applied upgrade instructions from UPGRADING.rst to `anvilprod` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilprod`</sub>
- [ ] `061` Ran `_select anvilprod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `062` Deleted PR branch from GitHub
- [ ] `063` PR is assigned to only the operator
- [ ] `064` Deleted PR branch from GitLab `anvilprod`
- [ ] `065` Status of linked issue is *Stable*
- [ ] `066` Status of promoted<sup>1</sup> PRs is *Merged stable*
- [ ] `067` Status of promoted<sup>1</sup> issues is *Stable*

<sup>1</sup> Promoted issues and PRs are referenced in the titles of the commits
that the promotion branch introduces to the stable branch. Prior to the
promotion, the status of promoted issues (PRs) is *Lower* (*Merged lower*).
Promoted PRs in status *Done* do not need to be moved.


### Operator (reindex)

- [ ] `068` In `anvilprod`, deleted the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `069` In `anvilprod`, deindexed the sources sepcified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `070` In `anvilprod`, indexed the sources specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `071` In `anvilprod`, indexed the catalogs specified in the notes <sub>or this PR is missing either the `reindex:partial` or the `reindex:anvilprod` label, or both</sub>
- [ ] `072` Started full reindex in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `073` Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `074` Emptied fail queues in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod` or it is labeled reindex:partial</sub>
- [ ] `075` Restarted the Data Browser pipeline for the [ucsc/anvil/anvilprod branch](https://gitlab.explore.anvilproject.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvilprod) on GitLab in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] `076` Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvilprod` <sub>or this PR is not labeled `reindex:anvilprod`</sub>


### Operator (mirroring)

- [ ] `077` Started mirroring in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] `078` Checked for, triaged and possibly requeued messages in mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] `079` Emptied mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>


### Operator

- [ ] `080` PR is assigned to only the system administrator


### System administrator

- [ ] `081` Removed unused image tags from [pycharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `082` Removed unused image tags from [bigquery_emulator image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] `083` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
