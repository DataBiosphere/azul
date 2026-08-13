---
name: Promotion
about: Issue template for promoting changes to stable deployments on a weekly basis
title: Promotion
labels: infra,no demo,operator
type: Chore
_priority: Medium
_start: 2024-02-27T09:00
_period: 7 days
---
- [ ] The title of this issue matches `Promotion yyyy-mm-dd`
- [ ] For `prod`
  - [ ] System administrator and operator determined the commit to be promoted
  - [ ] Operator created the promotion PR
- [ ] For `anvilprod`
  - [ ] System administrator and operator determined the commit to be promoted
  - [ ] Operator created the promotion PR
- [ ] This issue is part of the sprint that includes the date from the issue title
