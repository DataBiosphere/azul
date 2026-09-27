<!--
This is the PR template for upgrading Azul dependencies.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] `A01` PR is assigned to the author
- [ ] `A02` Status of PR is *In progress*
- [ ] `A03` Target branch is `develop`
- [ ] `A04` Name of PR branch matches `upgrades/yyyy-mm-dd`
- [ ] `A05` PR is linked to the upgrade issue it resolves
- [ ] `A06` Status of linked issue is *In progress*
- [ ] `A07` PR description links to linked issue
- [ ] `A08` PR title matches `Upgrade software dependencies yyyy-mm-dd`
- [ ] `A09` PR title references the linked issue


### Author (upgrading deployments)

- [ ] `F01` Ran `make docker_images.json` and committed the resulting changes <sub>or this PR does not modify `azul_docker_images`, or any other variables referenced in the definition of that variable</sub>
- [ ] `F02` Documented upgrading of deployments in UPGRADING.rst <sub>or this PR does not require upgrading deployments</sub>
- [ ] `F03` Added `u` tag to commit title <sub>or this PR does not require upgrading deployments</sub>
- [ ] `F04` This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] `F05` This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] `F06` This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] `F07` This PR is labeled `backup:gitlab`
- [ ] `F08` This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] `H01` Rebased PR branch on `develop`, squashed fixups from prior reviews
- [ ] `H02` Ran `make requirements_update` <sub>or this PR does not modify `pyproject.toml`</sub>
- [ ] `H03` Added `R` tag to commit title <sub>or this PR does not modify `uv.lock`</sub>
- [ ] `H04` This PR is labeled `reqs` <sub>or does not modify `uv.lock`</sub>
- [ ] `H05` Updated the `AL2023_release` variable in [gitlab.tf.json.template.py](../blob/develop/terraform/gitlab/gitlab.tf.json.template.py) to the most recent [AL2023 release](../blob/develop/OPERATOR.rst#updating-software-packages-via-release-version-upgrade-in-al2023-instances) <sub>or no update is available</sub>
- [ ] `H06` `make integration_test` passes in personal deployment <sub>or this PR does not modify functionality that could affect the IT outcome</sub>
- [ ] `H07` PR is not a draft
- [ ] `H08` PR is awaiting requested review from system administrator
- [ ] `H09` Status of PR is *Review requested*
- [ ] `H10` PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] `K01` Actually approved the PR
- [ ] `K02` Labeled linked issue as `no demo`
- [ ] `K03` A comment to this PR details the completed security design review
- [ ] `K04` PR title is appropriate as title of merge commit
- [ ] `K05` `N reviews` label is accurate
- [ ] `K06` Status of PR is *Approved*
- [ ] `K07` PR is assigned to only the operator and the author


### Operator

- [ ] `L01` Squashed PR branch and rebased onto `develop`
- [ ] `L02` Sanity-checked history
- [ ] `L03` Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] `M01` Ran `_select dev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `M02` Ran `_select dev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `M03` Ran `_select dev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `M04` Ran `_select anvildev.shared && CI_COMMIT_REF_NAME=develop make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `M05` Ran `_select anvildev.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] `M06` Ran `_select anvildev.gitlab && CI_COMMIT_REF_NAME=develop make -C terraform/gitlab apply`(an error from _login_docker_gitlab is benign if the instance was stopped for backup) <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `M07` Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] `M08` PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] `N01` Background migrations for [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `N02` Background migrations for [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] `N03` PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] `P01` Ran `_select dev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>
- [ ] `P02` Ran `_select anvildev.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] `Q01` Added `sandbox` label
- [ ] `Q02` Pushed PR branch to GitLab `dev`
- [ ] `Q03` Pushed PR branch to GitLab `anvildev`
- [ ] `Q04` Build passes in `sandbox` deployment
- [ ] `Q05` Build passes in `anvilbox` deployment
- [ ] `Q06` Reviewed build logs for anomalies in `sandbox` deployment
- [ ] `Q07` Reviewed build logs for anomalies in `anvilbox` deployment
- [ ] `Q08` Applied upgrade instructions from UPGRADING.rst to `sandbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `sandbox`</sub>
- [ ] `Q09` Applied upgrade instructions from UPGRADING.rst to `anvilbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilbox`</sub>


### Operator (merge the branch)

- [ ] `R01` All status checks passed and the PR is mergeable
- [ ] `R02` The title of the merge commit starts with the title of this PR
- [ ] `R03` Added PR # reference to merge commit title
- [ ] `R04` Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] `R05` Closed related Dependabot PRs with a comment referencing the corresponding commit in this PR <sub>or this PR does not include any such commits</sub>
- [ ] `R06` Pushed merge commit to GitHub
- [ ] `R07` Status of PR is *Merged lower*
- [ ] `R08` Status of blocked issues is *Triage* <sub>or no issues are blocked on the linked issue</sub>


### Operator (main build)

- [ ] `S01` Pushed merge commit to GitLab `dev`
- [ ] `S02` Pushed merge commit to GitLab `anvildev`
- [ ] `S03` Build passes on GitLab `dev`
- [ ] `S04` Reviewed build logs for anomalies on GitLab `dev`
- [ ] `S05` Build passes on GitLab `anvildev`
- [ ] `S06` Reviewed build logs for anomalies on GitLab `anvildev`
- [ ] `S07` Applied upgrade instructions from UPGRADING.rst to `dev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `dev`</sub>
- [ ] `S08` Applied upgrade instructions from UPGRADING.rst to `anvildev` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvildev`</sub>
- [ ] `S09` Notified developers to apply upgrade instructions from UPGRADING.rst to their personal deployments <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to personal deployments</sub>
- [ ] `S10` Ran `_select dev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `S11` Ran `_select anvildev.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] `S12` Deleted PR branch from GitHub
- [ ] `S13` PR is assigned to only the operator
- [ ] `S14` Deleted PR branch from GitLab `dev`
- [ ] `S15` Deleted PR branch from GitLab `anvildev`
- [ ] `S16` Status of linked issue is *Lower*


### Operator

- [ ] `V01` At least 24 hours have passed since `anvildev.shared` was last deployed
- [ ] `V02` Ran `scripts/export_inspector_findings.py` against `anvildev`, imported results to [Google Sheet](https://docs.google.com/spreadsheets/d/1RWF7g5wRKWPGovLw4jpJGX_XMi8aWLXLOvvE5rxqgH8) and posted screenshot of relevant<sup>1</sup> findings as a comment on the linked issue.
- [ ] `V03` Propagated the `upgrade`, `API`, `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels to the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `V04` Propagated any specific instructions related to the `upgrade`, `API`, `deploy:shared`, `deploy:gitlab`, `deploy:runner` and `backup:gitlab` labels, from the description of this PR to that of the next promotion PRs <sub>or this PR carries none of these labels</sub>
- [ ] `V05` PR is assigned to only the system administrator

<sup>1</sup>A relevant finding is a high or critical vulnerability in an image
that is used within the security boundary. Images not used within the boundary
are tracked in `azul.docker_images` under a key starting with `_`.


### System administrator

- [ ] `W01` No currently reported vulnerability requires immediate attention
- [ ] `W02` PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
