---
name: Promotion pull request
about: 'Issue template for the current operator to perform a scheduled promotion'
title: Promotion
labels: -,infra,no demo,operator,orange,task
assignees: ''
---
- [ ] Confirmed with tech lead the commit that will be promoted
- [ ] Confirmed this issues maches the appropriate title convention `Promotion yyyy-mm-dd`
- [ ] Confirmed local `prod` branch is upto date with remote
- [ ] Promotion feature branch follows appropriate name convention `promotions/yyyy-mm-dd`
- [ ] Created promotion PR and completed the Author sections of the promotion checklist
- [ ] Confirmed all pending checklist items from other PRs as upgrading instructions are accounted for
