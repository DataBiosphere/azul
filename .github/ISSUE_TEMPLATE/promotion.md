---
name: Promotion pull request
about: 'Issue template for promoting changes to stable deployments'
title: Promotion
labels: -,infra,no demo,operator,orange,task
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
