<!--
This is the PR template for a promotion PR against `anvilprod`.
-->

Linked issue: #0000


## Checklist


### Author

- [ ] PR is assigned to the author
- [ ] Status of PR is *In progress*
- [ ] Target branch is `anvilprod`
- [ ] Name of PR branch matches `promotions/yyyy-mm-dd-anvilprod`
- [ ] PR is linked to the promotion issue it resolves
- [ ] Status of linked issue is *In progress*
- [ ] PR description links to linked issue
- [ ] Title of linked issue matches `Promotion yyyy-mm-dd`
- [ ] PR title starts with title of linked issue followed by ` anvilprod`
- [ ] PR title references the linked issue


### Author (reindex)

- [ ] This PR is labeled `reindex:anvilprod` <sub>or the changes introduced by it will not require reindexing of `anvilprod`</sub>
- [ ] This PR is labeled `reindex:partial` and its description documents the specific reindexing procedure for `anvilprod` <sub>or requires a full reindex or is not labeled`reindex:anvilprod`</sub>


### Author (mirror)

- [ ] This PR is labeled `mirror:anvilprod` <sub>or the changes introduced by it will not require mirroring of `anvilprod`</sub>
- [ ] This PR is labeled `mirror:partial` and its description documents the specific mirroring procedure for `anvilprod` <sub>or requires a full mirroring or is not labeled`mirror:anvilprod`</sub>


### Author (upgrading deployments)

- [ ] This PR is labeled `upgrade` <sub>or does not require upgrading deployments</sub>
- [ ] This PR is labeled `deploy:shared` <sub>or does not modify `docker_images.json`, and does not require deploying the `shared` component for any other reason</sub>
- [ ] This PR is labeled `deploy:gitlab` <sub>or does not require deploying the `gitlab` component</sub>
- [ ] This PR is labeled `deploy:runner` <sub>or does not require deploying the `runner` image</sub>


### Author (before every review)

- [ ] PR branch is up to date (if not, merge `anvilprod` into PR branch to integrate upstream changes)
- [ ] PR is not a draft
- [ ] PR is awaiting requested review from system administrator
- [ ] Status of PR is *Review requested*
- [ ] PR is assigned to only the system administrator and the author


### System administrator (after approval)

- [ ] Actually approved the PR
- [ ] Decided if PR can be labeled `no sandbox`
- [ ] `N reviews` label is accurate
- [ ] Status of PR is *Approved*
- [ ] PR is assigned to only the operator and the author


### Operator

- [ ] Pushed PR branch to GitHub


### Operator (deploy `.shared` and `.gitlab` components)

- [ ] Ran `_select anvilprod.shared && CI_COMMIT_REF_NAME=anvilprod make -C terraform/shared apply_keep_unused` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Ran `_select anvilprod.gitlab && python scripts/create_gitlab_snapshot.py --no-restart` (see [operator manual](../blob/develop/OPERATOR.rst#backup-gitlab-volumes) for details) <sub>or this PR is not labeled `backup:gitlab`</sub>
- [ ] Ran `_select anvilprod.gitlab && CI_COMMIT_REF_NAME=anvilprod make -C terraform/gitlab apply` <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] Checked the items in the next section <sub>or this PR is labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the system administrator and the author <sub>or this PR is not labeled `deploy:gitlab`</sub>


### System administrator (post-deploy of `.gitlab` component)

- [ ] Background migrations for [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/admin/background_migrations) are complete <sub>or this PR is not labeled `deploy:gitlab`</sub>
- [ ] PR is assigned to only the operator and the author


### Operator (deploy runner image)

- [ ] Ran `_select anvilprod.gitlab && make -C terraform/gitlab/runner` <sub>or this PR is not labeled `deploy:runner`</sub>


### Operator (sandbox build)

- [ ] Added `sandbox` label <sub>or PR is labeled `no sandbox`</sub>
- [ ] Pushed PR branch to GitLab `anvilprod` <sub>or PR is labeled `no sandbox`</sub>
- [ ] Build passes in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Reviewed build logs for anomalies in `hammerbox` deployment <sub>or PR is labeled `no sandbox`</sub>
- [ ] Applied upgrade instructions from UPGRADING.rst to `hammerbox` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `hammerbox`</sub>
- [ ] Deleted unreferenced indices in `hammerbox` <sub>or this PR does not remove catalogs or otherwise causes unreferenced indices in `hammerbox`</sub>
- [ ] Started reindex in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Checked for failures in `hammerbox` <sub>or this PR is not labeled `reindex:anvilprod`</sub>
- [ ] Started mirroring in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>
- [ ] Checked for failures in `hammerbox` <sub>or this PR is not labeled `mirror:anvilprod`</sub>


### Operator (merge the branch)

- [ ] All status checks passed and the PR is mergeable
- [ ] The title of the merge commit starts with the title of this PR
- [ ] Added PR # reference to merge commit title
- [ ] Collected commit title tags in merge commit title <sub>but excluded any `p` tags</sub>
- [ ] Pushed merge commit to GitHub
- [ ] Status of PR is *Merged stable*


### Operator (main build)

- [ ] Pushed merge commit to GitLab `anvilprod`
- [ ] Build passes on GitLab `anvilprod`
- [ ] Reviewed build logs for anomalies on GitLab `anvilprod`
- [ ] Applied upgrade instructions from UPGRADING.rst to `anvilprod` <sub>or this PR is not labeled `upgrade`, or upgrade instructions do not apply to `anvilprod`</sub>
- [ ] Ran `_select anvilprod.shared && make -C terraform/shared apply` <sub>or this PR is not labeled `deploy:shared`</sub>
- [ ] Deleted PR branch from GitHub
- [ ] PR is assigned to only the operator
- [ ] Deleted PR branch from GitLab `anvilprod`
- [ ] Status of linked issue is *Stable*
- [ ] Status of promoted<sup>1</sup> PRs is *Merged stable*
- [ ] Status of promoted<sup>1</sup> issues is *Stable*

<sup>1</sup> Promoted issues and PRs are referenced in the titles of the commits
that the promotion branch introduces to the stable branch. Prior to the
promotion, the status of promoted issues (PRs) is *Lower* (*Merged lower*).
Promoted PRs in status *Done* do not need to be moved.


### Operator (reindex)

- [ ] Deindexed all unreferenced catalogs in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Deindexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Indexed specific sources in `anvilprod` <sub>or this PR is neither labeled `reindex:partial` nor `reindex:anvilprod`</sub>
- [ ] Started reindex in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in both fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Emptied fail queues in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Restarted the Data Browser pipeline for the [ucsc/anvil/anvilprod branch](https://gitlab.explore.anvilproject.org/ucsc/data-browser/-/pipelines/new?ref=ucsc%2Fanvil%2Fanvilprod) on GitLab in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>
- [ ] Restarted `deploy_browser` job in the GitLab pipeline for this PR in `anvilprod` <sub>or this PR does not require reindexing `anvilprod`</sub>


### Operator (mirroring)

- [ ] Started mirroring in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] Checked for, triaged and possibly requeued messages in mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>
- [ ] Emptied mirror fail queue in `anvilprod` <sub>or this PR is not labelled `mirror:anvilprod`</sub>


### Operator

- [ ] PR is assigned to only the system administrator


### System administrator

- [ ] Removed unused image tags from [pycharm image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] Removed unused image tags from [bigquery_emulator image on DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags) <sub>or this promotion does not alter references to that image</sub>
- [ ] PR is assigned to no one


## Shorthand for review comments

- `L` line is too long
- `W` line wrapping is wrong
- `Q` bad quotes
- `F` other formatting problem
