---
name: Apply Amazon OpenSearch software updates
about: Issue template for operator to update the service software version on all OpenSearch domains 
title: Apply Amazon OpenSearch software updates
labels: infra,no demo,operator
type: Chore
_priority: Medium
_start: 2024-02-26T09:00
_period: 14 days
---
- [ ] Update `azul-index-dev` <sub>or `azul-index-dev` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-dev`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-dev`
- [ ] Update `azul-index-anvildev` <sub>or `azul-index-anvildev` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-anvildev`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-anvildev`
- [ ] Team members confirmed personal deployments collocated with `dev` are idle <sub>or `azul-index-sandbox` is on the latest version</sub>
- [ ] Update `azul-index-sandbox` <sub>or `azul-index-sandbox` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-sandbox`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-sandbox`
- [ ] Team members confirmed personal deployments collocated with `anvildev` are idle <sub>or `azul-index-anvilbox` is on the latest version</sub>
- [ ] Update `azul-index-anvilbox` <sub>or `azul-index-anvilbox` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-anvilbox`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-anvilbox`
- [ ] Update `azul-index-anvilprod` <sub>or `azul-index-anvilprod` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-anvilprod`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-anvilprod`
- [ ] Update `azul-index-prod` <sub>or `azul-index-prod` is on the latest version</sub>
- [ ] There are no outstanding notifications in `azul-index-prod`
- [ ] Automated snapshots are disabled<sup>1</sup> on `azul-index-prod`

<sup>1</sup> *Automated Snapshots Enabled* is *No* under *Snapshot* on the *Cluster configuration* tab of the domain in the AWS console
