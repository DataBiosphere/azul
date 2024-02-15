<!--
This is the PR template for promotion PRs against `prod`.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `prod`
- [ ] Name of PR branch matches `promotions/yyyy-mm-dd`
- [ ] On ZenHub, PR is connected to the promotion issue it resolves
- [ ] PR description links to connected issue
- [ ] Title of connected issue matches `Promotion yyyy-mm-dd`
- [ ] PR title starts with title of connected issue
- [ ] PR title references the connected issue


### Author (reindex, API changes)

- [ ] PR is labeled `reindex:prod` <sub>or this PR does not require reindexing `prod`</sub>
- [ ] This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `prod` <sub>or requires a full reindex or is not labeled`reindex:prod`</sub>
- [ ] PR and connected issue are labeled `API` <sub>or this PR does not modify a REST API</sub>


### Author (upgrading deployments)

- [ ] Added `upgrade` label to PR <sub>or this PR does not require upgrading deployments</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled PR as `no sandbox`
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Pushed PR branch to GitHub
- [ ] Selected `prod.shared` and ran `CI_COMMIT_REF_NAME=prod make -C terraform/shared apply` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `prod.gitlab` and ran `CI_COMMIT_REF_NAME=prod make -C terraform/gitlab apply` <sub>or this PR does not include any changes to files in terraform/gitlab</sub>
- [ ] Assigned system administrator <sub>or this PR does not include any changes to files in terraform/gitlab</sub>
- [ ] Checked the items in the next section <sub>or this PR includes changes to files in terraform/gitlab</sub>


### System administrator

- [ ] Background migrations for `prod.gitlab` are complete <sub>or this PR does not include any changes to files in terraform/gitlab</sub>
- [ ] PR is assigned to operator


### Operator (before pushing merge the commit)

- [ ] Selected `prod.gitlab` and ran `make -C terraform/gitlab/runner` <sub>or this PR does not change `azul_docker_version`</sub>
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but exclude any `p` tags</sub>
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `prod`
- [ ] Build passes on GitLab `prod`
- [ ] Reviewed build logs for anomalies on GitLab `prod`
- [ ] Deleted PR branch from GitHub
- [ ] Moved connected issue to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *Merged* to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *dev* to *prod* column on ZenHub


### Operator (reindex)

- [ ] Deindexed all unreferenced catalogs in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Deindexed specific sources in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Indexed specific sources in `prod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:prod`</sub>
- [ ] Started reindex in `prod` <sub>or this PR does not require reindexing `prod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `prod` <sub>or this PR does not require reindexing `prod`</sub>
- [ ] Emptied fail queues in `prod` <sub>or this PR does not require reindexing `prod`</sub>


### Operator

- [ ] PR is assigned to system administrator


### System administrator

- [ ] Removed unused image tags from [Elasticsearch image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch) <sub>or this promotion does not alter references to that image`</sub>
- [ ] Removed unused image tags from [PyCharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm) <sub>or this promotion does not alter references to that image`</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
