---
name: GitLab data volume backups
about: 'Issue template for operator tasks to backup GitLab data volumes'
title: Apply GitLab data volume backups
labels: -,infra,no demo,operator,orange,task
assignees: ''
_start: 2024-02-26T09:00
_period: 14 days
---

- [ ] Ran GitLab data-volume backup script for `dev.gitlab`
- [ ] Updated software packages and rebooted `dev.gitlab` <sub>or this is an unecessary step</sub> 
- [ ] Ran GitLab data-volume backup script for `anvildev.gitlab`
- [ ] Updated software packages and rebooted `anvildev.gitlab` <sub>or this is an unecessary step</sub>
- [ ] Ran GitLab data-volume backup script for `anvilprod.gitlab`
- [ ] Updated software packages and rebooted `anvilprod.gitlab` <sub>or this is an unecessary step</sub>
- [ ] Ran GitLab data-volume backup script for `prod.gitlab`
- [ ] Updated software packages and rebooted `prod.gitlab` <sub>or this is an unecessary step</sub>
