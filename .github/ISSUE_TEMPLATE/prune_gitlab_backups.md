---
name: Prune GitLab data volume backups
about: Issue template for the quarterly pruning of GitLab data volume snapshots
title: Prune GitLab data volume backups
labels: infra,operator
type: Chore
_priority: Medium
_start: 2025-04-01T09:00
_period: 3 months
---
In each deployment, use `scripts/prune_gitlab_snapshots.py` to prune the snapshots (see [operator manual](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#prune-gitlab-volume-backups) for details).

- [ ] The `dev.gitlab` data volume snapshots have been pruned
- [ ] The `anvildev.gitlab` data volume snapshots have been pruned
- [ ] The `anvilprod.gitlab` data volume snapshots have been pruned
- [ ] The `prod.gitlab` data volume snapshots have been pruned
