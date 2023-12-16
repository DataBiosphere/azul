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

- [ ] Added `reindex` label to PR <sub>or this PR does not require reindexing</sub>
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
- [ ] Selected `prod.shared` and ran `make -C terraform/shared apply` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `prod.gitlab` and ran `make -C terraform/gitlab apply` <sub>or this PR does not change the GitLab version</sub>
- [ ] Assigned system administrator <sub>or this PR does not change the GitLab version</sub>
- [ ] Checked the items in the next section <sub>or this PR changes the GitLab version</sub>


### System administrator

- [ ] Background migrations for `prod.gitlab` are complete <sub>or this PR does not change the GitLab version</sub>
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
- [ ] Deleted PR branch from GitLab `prod`
- [ ] Moved connected issue to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *Merged* to *Merged prod* column on ZenHub
- [ ] Moved promoted issues from *dev* to *prod* column on ZenHub


### Operator (reindex)

- [ ] Deleted unreferenced indices in `prod` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices </sub>
- [ ] Started reindex in `prod` <sub>or this PR does not require reindexing</sub>
- [ ] Checked for and triaged indexing failures in `prod` <sub>or this PR does not require reindexing</sub>
- [ ] Emptied fail queues in `prod` deployment <sub>or this PR does not require reindexing</sub>


### Operator

- [ ] PR is assigned to system administrator


### System administrator

- [ ] Removed unused image tags from (Elasticsearch image on DockerHub)[https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch] <sub>or this promotion does not include changes to `azul_docker_elasticsearch_version`</sub>
- [ ] Removed unused image tags from (PyCharm image on DockerHub)[https://hub.docker.com/repository/docker/ucscgi/azul-pycharm] <sub>or this promotion does not include changes to `azul_docker_pycharm_version`</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
