<!--
This is the PR template for a promotion PR against `anvilprod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `anvilprod`
- [ ] Name of PR branch matches `promotions/yyyy-mm-dd-anvilprod`
- [ ] On ZenHub, PR is connected to the promotion issue it resolves
- [ ] PR description links to connected issue
- [ ] Title of connected issue matches `Promotion yyyy-mm-dd anvilprod`
- [ ] PR title starts with title of connected issue
- [ ] PR title references the connected issue


### Author (reindex, API changes)

- [ ] This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `anvilprod` <sub>or requires a full reindex or is not labeled`reindex:anvilprod`</sub>
- [ ] This PR and its connected issues are labeled `API` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] This PR is labeled `deploy:shared` <sub>or does not modify `image_manifests.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Pushed PR branch to GitHub
- [ ] Ran `_select anvilprod.shared && CI_COMMIT_REF_NAME=anvilprod make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Made a backup of the GitLab data volume in `anvilprod` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select anvilprod.gitlab && CI_COMMIT_REF_NAME=anvilprod make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the system administrator <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator

- [ ] Background migrations for `anvilprod.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Ran `_select anvilprod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `anvilprod`
- [ ] Build passes on GitLab `anvilprod`
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] Ran `_select anvilprod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] Moved connected issue to *Merged stable* column on ZenHub
- [ ] Moved promoted issues from *Merged lower* to *Merged stable* column on ZenHub
- [ ] Moved promoted issues from *Lower* to *Stable* column on ZenHub


### Operator (reindex)

- [ ] Deindexed all unreferenced catalogs in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Deindexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Indexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Started reindex in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Emptied fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>


### Operator

- [ ] PR is assigned to only the system administrator


### System administrator

- [ ] Removed unused image tags from [pycharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm) <sub>or this promotion does not alter references to that image</sub>
- [ ] Removed unused image tags from [elasticsearch image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch) <sub>or this promotion does not alter references to that image</sub>
- [ ] Removed unused image tags from [bigquery_emulator image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator) <sub>or this promotion does not alter references to that image</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
