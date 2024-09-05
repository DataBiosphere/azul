<!--
This is the PR template for upgrading Azul dependencies.
-->

Connected issue: #0000


## Checklist


### Author

- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `upgrades/yyyy-mm-dd`
- [ ] On ZenHub, PR is connected to the upgrade issue it resolves
- [ ] PR title matches `Upgrade dependencies yyyy-mm-dd`
- [ ] PR title references the connected issue


### Author (upgrading deployments)

- [ ] Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] This PR is labeled `backup:gitlab`
- [ ] This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] Rebased PR branch on `develop`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `requirements*.txt`, `common.mk`, `Makefile` and `Dockerfile`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `requirements*.txt`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `requirements*.txt`</sub>
- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled connected issue as `no demo`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved connected issue to *Approved* column
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Made a backup of the GitLab data volume in `dev` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Made a backup of the GitLab data volume in `anvildev` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the system administrator <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator

- [ ] Background migrations for `dev.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Background migrations for `anvildev.gitlab` are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the operator


### Operator (before pushing merge the commit)

- [ ] Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Added `sandbox` label
- [ ] Pushed PR branch to GitLab `dev`
- [ ] Pushed PR branch to GitLab `anvildev`
- [ ] Build passes in `sandbox` deployment
- [ ] Build passes in `anvilbox` deployment
- [ ] Reviewed build logs for anomalies in `sandbox` deployment
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Moved connected issue to *Merged lower* column in ZenHub
- [ ] Closed related Dependabot PRs with a comment referencing the corresponding commit in this PR <sub>or this PR does not include any such commits</sub>
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `dev`
- [ ] Pushed merge commit to GitLab `anvildev`
- [ ] Build passes on GitLab `dev`
- [ ] Reviewed build logs for anomalies on GitLab `dev`
- [ ] Build passes on GitLab `anvildev`
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`


### Operator

- [ ] At least one hour has passed since `anvildev.shared` was last deployed
- [ ] Ran `script/export_inspector_findings.py` against `anvildev`, imported results to [Google Sheet](https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of relevant<sup>1</sup> findings as a comment on the connected issue.
- [ ] Propagated the `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] Propagated any specific instructions related to the `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] PR is assigned to only the system administrator

<sup>1</sup>A relevant finding is a high or critical vulnerability in an image
that is used within the security boundary. Images not used within the boundary
are tracked in `azul.docker_images` under a key starting with `_`.


### System administrator

- [ ] No currently reported vulnerability requires immediate attention
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
