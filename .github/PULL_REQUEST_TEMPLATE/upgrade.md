<!--
This is the PR template for upgrading Azul dependencies.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `001` PR is assigned to the author
- [ ] `002` Status of PR is *In progress*
- [ ] `003` Target branch is `develop`
- [ ] `004` Name of PR branch matches `upgrades/yyyy-mm-dd`
- [ ] `005` PR is linked to the upgrade issue it resolves
- [ ] `006` Status of linked issue is *In progress*
- [ ] `007` PR description links to linked issue
- [ ] `008` PR title matches `Upgrade software dependencies yyyy-mm-dd`
- [ ] `009` PR title references the linked issue


### Author (upgrading deployments)

- [ ] `010` Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] `011` Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] `012` Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] `013` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `014` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `015` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `016` This PR is labeled `backup:gitlab`
- [ ] `017` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] `018` Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] `019` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `020` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `021` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `022` Updated the `AL2023_release` variable in [gitlab.tf.json.template.py](../blob/develop/terraform/gitlab/gitlab.tf.json.template.py) to the most recent [AL2023 release](../blob/develop/OPERATOR.rst#updating-software-packages-via-release-version-upgrade-in-al2023-instances) <sub>or no update is available</sub>
- [ ] `023` `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] `024` PR is not a draft
- [ ] `025` PR is awaiting requested review from system administrator
- [ ] `026` Status of PR is *Review requested*
- [ ] `027` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `028` Actually approved the PR
- [ ] `029` Labeled linked issue as `no demo`
- [ ] `030` A comment to this PR details the completed security design review
- [ ] `031` PR title is appropriate as title of merge commit
- [ ] `032` `N reviews` label is accurate
- [ ] `033` Status of PR is *Approved*
- [ ] `034` PR is assigned to only the operator and the author


### Operator

- [ ] `035` Squashed PR branch and rebased onto `develop`
- [ ] `036` Sanity-checked history
- [ ] `037` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `038` Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `039` Ran `_select dev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `040` Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `041` Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `042` Ran `_select anvildev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `043` Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `044` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `045` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `046` Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `047` Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `048` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `049` Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] `050` Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] `051` Added `sandbox` label
- [ ] `052` Pushed PR branch to GitLab `dev`
- [ ] `053` Pushed PR branch to GitLab `anvildev`
- [ ] `054` Build passes in `sandbox` deployment
- [ ] `055` Build passes in `anvilbox` deployment
- [ ] `056` Reviewed build logs for anomalies in `sandbox` deployment
- [ ] `057` Reviewed build logs for anomalies in `anvilbox` deployment
- [ ] `058` Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] `059` Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>


### Operator (merge the branch)

- [ ] `060` All status checks passed and the PR is mergeable
- [ ] `061` The title of the merge commit starts with the title of this PR
- [ ] `062` Added PR # reference to merge commit title
- [ ] `063` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `064` Closed related Dependabot PRs with a comment referencing the corresponding commit in this PR <sub>or this PR does not include any such commits</sub>
- [ ] `065` Pushed merge commit to GitHub
- [ ] `066` Status of PR is *Merged lower*
- [ ] `067` Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issue</sub>


### Operator (main build)

- [ ] `068` Pushed merge commit to GitLab `dev`
- [ ] `069` Pushed merge commit to GitLab `anvildev`
- [ ] `070` Build passes on GitLab `dev`
- [ ] `071` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `072` Build passes on GitLab `anvildev`
- [ ] `073` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `074` Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] `075` Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] `076` Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] `077` Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `078` Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `079` Deleted PR branch from GitHub
- [ ] `080` PR is assigned to only the operator
- [ ] `081` Deleted PR branch from GitLab `dev`
- [ ] `082` Deleted PR branch from GitLab `anvildev`
- [ ] `083` Status of linked issue is *Lower*


### Operator

- [ ] `084` At least 24 hours have passed since `anvildev.shared` was last deployed
- [ ] `085` Ran `scripts/export_inspector_findings.py` against `anvildev`, imported results to [Google Sheet](https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of relevant<sup>1</sup> findings as a comment on the linked issue.
- [ ] `086` Propagated the `upgrade` and `API` labels to the next promotion PRs <sub>or this PR carries neither of these labels</sub>
- [ ] `087` Propagated the `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `088` Propagated any specific instructions related to the `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `089` PR is assigned to only the system administrator

<sup>1</sup>A relevant finding is a high or critical vulnerability in an image
that is used within the security boundary. Images not used within the boundary
are tracked in `azul.docker_images` under a key starting with `_`.


### System administrator

- [ ] `090` No currently reported vulnerability requires immediate attention
- [ ] `091` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
