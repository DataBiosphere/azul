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

- [ ] Added `upgrade` label to PR <sub>or this PR does not require upgrading deployments</sub>


### Author (before every review)

- [ ] Rebased PR branch on `develop`, squashed old fixups
- [ ] Ran `make requirements_update` <sub>or this PR does not touch requirements*.txt, common.mk, Makefile and Dockerfile</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Added `reqs` label to PR <sub>or this PR does not touch requirements*.txt</sub>
- [ ] Selected `dev.shared` and ran `CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `anvildev.shared` and ran `CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `anvilprod.shared` and ran `CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR does not change any Docker image versions</sub>


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled connected issue as `no demo`
- [ ] PR title is appropriate as title of merge commit
- [ ] Moved ticket to *Approved* column
- [ ] PR is assigned to current operator


### Operator (before pushing merge the commit)

- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub
- [ ] Selected `dev.gitlab` and ran `make -C terraform/gitlab apply` <sub>or this PR does not change the GitLab version</sub>
- [ ] Selected `anvildev.gitlab` and ran `make -C terraform/gitlab apply` <sub>or this PR does not change the GitLab version</sub>
- [ ] Selected `anvilprod.gitlab` and ran `make -C terraform/gitlab apply` <sub>or this PR does not change the GitLab version</sub>
- [ ] Assigned system administrator <sub>or this PR does not change the GitLab version</sub>
- [ ] Checked the items in the next section <sub>or this PR changes the GitLab version</sub>


### System administrator

- [ ] Background migrations for `dev.gitlab` are complete <sub>or this PR does not change the GitLab version</sub>
- [ ] Background migrations for `anvildev.gitlab` are complete <sub>or this PR does not change the GitLab version</sub>
- [ ] Background migrations for `anvilprod.gitlab` are complete <sub>or this PR does not change the GitLab version</sub>
- [ ] PR is assigned to operator


### Operator (before pushing merge the commit)

- [ ] Selected `dev.gitlab` and ran `make -C terraform/gitlab/runner` <sub>or this PR does not change `azul_docker_version`</sub>
- [ ] Selected `anvildev.gitlab` and ran `make -C terraform/gitlab/runner` <sub>or this PR does not change `azul_docker_version`</sub>
- [ ] Selected `anvilprod.gitlab` and ran `make -C terraform/gitlab/runner` <sub>or this PR does not change `azul_docker_version`</sub>
- [ ] Pushed PR branch to GitLab `dev` and added `sandbox` label
- [ ] Pushed PR branch to GitLab `anvildev`
- [ ] Pushed PR branch to GitLab `anvilprod`
- [ ] Build passes in `sandbox` deployment
- [ ] Build passes in `anvilbox` deployment
- [ ] Build passes in `hammerbox` deployment
- [ ] Reviewed build logs for anomalies in `sandbox` deployment
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment
- [ ] Reviewed build logs for anomalies in `hammerbox` deployment
- [ ] Title of merge commit starts with title from this PR
- [ ] Added PR reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but exclude any `p` tags</sub>
- [ ] Moved connected issue to Merged column in ZenHub
- [ ] Pushed merge commit to GitHub


### Operator (after pushing the merge commit)

- [ ] Pushed merge commit to GitLab `dev`
- [ ] Pushed merge commit to GitLab `anvildev`
- [ ] Pushed merge commit to GitLab `anvilprod`
- [ ] Build passes on GitLab `dev`
- [ ] Reviewed build logs for anomalies on GitLab `dev`
- [ ] Build passes on GitLab `anvildev`
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] Build passes on GitLab `anvilprod`
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] Selected `dev.shared` and ran `make -C terraform/shared apply` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `anvildev.shared` and ran `make -C terraform/shared apply` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Selected `anvilprod.shared` and ran `make -C terraform/shared apply` <sub>or this PR does not change any Docker image versions</sub>
- [ ] Deleted PR branch from GitHub
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Deleted PR branch from GitLab `anvilprod`


### Operator

- [ ] Ran `script/export_inspector_findings.py` against `anvilprod`, imported results to [Google Sheet](https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of relevant<sup>1</sup> findings as a comment on the connected issue.
- [ ] PR is assigned to system administrator

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
