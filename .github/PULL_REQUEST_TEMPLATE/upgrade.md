<!--
This is the PR template for upgrading Azul dependencies.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] Target branch is `develop`
- [ ] Name of PR branch matches `upgrades/yyyy-mm-dd`
- [ ] PR is linked to the upgrade issue it resolves
- [ ] Status of linked issue is *In progress*
- [ ] PR description links to linked issue
- [ ] PR title matches `Upgrade software dependencies yyyy-mm-dd`
- [ ] PR title references the linked issue


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

- [ ] Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] Updated the `AL2023_release` variable in [gitlab.tf.json.template.py](../blob/develop/terraform/gitlab/gitlab.tf.json.template.py) to the most recent [AL2023 release](../blob/develop/OPERATOR.rst#updating-software-packages-via-release-version-upgrade-in-al2023-instances) <sub>or no update is available</sub>
- [ ] `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] PR is not a draft
- [ ] PR is awaiting requested review from system administrator
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Labeled linked issue as `no demo`
- [ ] A comment to this PR details the completed security design review
- [ ] PR title is appropriate as title of merge commit
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Squashed PR branch and rebased onto `develop`
- [ ] Sanity-checked history
- [ ] Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select dev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] Added `sandbox` label
- [ ] Pushed PR branch to GitLab `dev`
- [ ] Pushed PR branch to GitLab `anvildev`
- [ ] Build passes in `sandbox` deployment
- [ ] Build passes in `anvilbox` deployment
- [ ] Reviewed build logs for anomalies in `sandbox` deployment
- [ ] Reviewed build logs for anomalies in `anvilbox` deployment
- [ ] Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Closed related Dependabot PRs with a comment referencing the corresponding commit in this PR <sub>or this PR does not include any such commits</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged lower*
- [ ] Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issue</sub>


### Operator (main build)

- [ ] Pushed merge commit to GitLab `dev`
- [ ] Pushed merge commit to GitLab `anvildev`
- [ ] Build passes on GitLab `dev`
- [ ] Reviewed build logs for anomalies on GitLab `dev`
- [ ] Build passes on GitLab `anvildev`
- [ ] Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Deleted PR branch from GitLab `dev`
- [ ] Deleted PR branch from GitLab `anvildev`
- [ ] Status of linked issue is *Lower*


### Operator

- [ ] At least 24 hours have passed since `anvildev.shared` was last deployed
- [ ] Ran `scripts/export_inspector_findings.py` against `anvildev`, imported results to [Google Sheet](https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of relevant<sup>1</sup> findings as a comment on the linked issue.
- [ ] Propagated the `upgrade` and `API` labels to the next promotion PRs <sub>or this PR carries neither of these labels</sub>
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
