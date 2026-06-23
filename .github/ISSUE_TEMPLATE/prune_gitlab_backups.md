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
In each deployment, keep 
- every backup for the last 6 months (roughly 12 backups),
- one backup per month for the 12 months before that (12 backups), 
- one backup per quarter for the 12 months before that (4 backups) 
- and one per year for older backups

- [ ] The `dev.gitlab` data volume snapshots have been pruned
- [ ] The `anvildev.gitlab` data volume snapshots have been pruned
- [ ] The `anvilprod.gitlab` data volume snapshots have been pruned
- [ ] The `anvilprod.gitlab` data volume snapshots have been pruned
