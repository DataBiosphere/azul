---
name: Temporary SA handover
about: Issue template for handing over system administrator permissions to a temporary system administrator
title: Temporary SA handover
labels: infra,no demo,operator
type: Chore
---


## Notes


### Abbreviations

SA = permanent system administrator
TSA = temporary system administrator
OA = organization administrator


### Scope

Temporary hand-overs do not involve …

- Handing over the PKI for the VPNs
- Removal of any permissions from the SA (except for code ownership on GitHub)
- Modifying the permissions or ownership of azul-group@ucsc.edu
- Other organizations on GitHub, e.g., HumanCellAtlas
- Changing GitLab root user credentials
- Adding/removing SSH keys to/from GitLab instances. This implies that the TSA is in the operator rotation; special action may be necessary if they are not


## Hand-over tasks for SA

- [ ] Add TSA to [`Azul Admins`](https://github.com/orgs/DataBiosphere/teams/azul-admins) team in DataBiosphere on GitHub
- [ ] Add TSA as a maintainer of the [`Azul Operators`](https://github.com/orgs/DataBiosphere/teams/azul-operators) team in DataBiosphere on GitHub
- [ ] Make TSA an admin on the Admin UI of [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/users)
- [ ] Make TSA an admin on the Admin UI of [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/users)
- [ ] Make TSA an admin on the Admin UI of [`tempdev.gitlab`](https://gitlab.temp.gi.ucsc.edu/admin/users) <sub>or `tempdev.gitlab` is not deployed</sub>
- [ ] Make TSA an admin on the Admin UI of [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/admin/users)
- [ ] Make TSA an admin on the Admin UI of [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/admin/users)
- [ ] Make TSA owner of the `ucsc` group on [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/groups/ucsc/-/group_members)
- [ ] Make TSA owner of the `ucsc` group on [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/groups/ucsc/-/group_members)
- [ ] Make TSA owner of the `ucsc` group on [`tempdev.gitlab`](https://gitlab.temp.gi.ucsc.edu/groups/ucsc/-/group_members) <sub>or `tempdev.gitlab` is not deployed</sub>
- [ ] Make TSA owner of the `ucsc` group on [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/groups/ucsc/-/group_members)
- [ ] Make TSA owner of the `ucsc` group on [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/groups/ucsc/-/group_members)
- [ ] Assign `Owner` role to TSA in GCP project [`platform-hca-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-hca-dev)
- [ ] Assign `Owner` role to TSA in GCP project [`platform-anvil-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-anvil-dev)
- [ ] Assign `Owner` role to TSA in GCP project [`platform-temp-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-temp-dev)
- [ ] Assign `Owner` role to TSA in GCP project [`platform-hca-prod`](https://console.cloud.google.com/iam-admin/iam?project=platform-hca-prod)
- [ ] Assign `Owner` role to TSA in GCP project [`platform-anvil-prod`](https://console.cloud.google.com/iam-admin/iam?project=platform-anvil-prod)
- [ ] Assign `Owner` role to TSA in [UCSCGI](https://app.docker.com/accounts/ucscgi/members) org on DockerHub
- [ ] Make TSA an admin of Terra group [`azul-dev`](https://bvdp-saturn-dev.appspot.com/#groups/azul-dev)
- [ ] Make TSA an admin of Terra group [`azul-anvil-dev`](https://bvdp-saturn-dev.appspot.com/#groups/azul-anvil-dev)
- [ ] Make TSA an admin of Terra group [`azul-prod`](https://app.terra.bio/#groups/azul-prod)
- [ ] Make TSA an admin of Terra group [`azul-anvil-prod`](https://app.terra.bio/#groups/azul-anvil-prod)
- [ ] Issue is assigned to only the TSA
- [ ] Make TSA code owner in [.github/CODEOWNERS](https://github.com/DataBiosphere/azul/blob/develop/.github/CODEOWNERS)


## Hand-over tasks for TSA

- [ ] Request OA to add TSA to the `platform-hca-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to add TSA to the `platform-anvil-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to add TSA to the `platform-temp-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to add TSA to the `platform-hca-prod-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to add TSA to the `platform-anvil-prod-administrator` IAM group in the `gi-gateway` AWS account


## Reversal tasks for TSA

- [ ] Request OA to remove TSA from the `platform-hca-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to remove TSA from the `platform-anvil-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to remove TSA from the `platform-temp-dev-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to remove TSA from the `platform-hca-prod-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Request OA to remove TSA from the `platform-anvil-prod-administrator` IAM group in the `gi-gateway` AWS account
- [ ] Issue is assigned to only the SA


## Reversal tasks for SA

- [ ] Remove TSA from [`Azul Admins`](https://github.com/orgs/DataBiosphere/teams/azul-admins) team in DataBiosphere on GitHub
- [ ] Remove TSA from [`Azul Operators`](https://github.com/orgs/DataBiosphere/teams/azul-operators) team in DataBiosphere on GitHub
- [ ] Remove TSA's admin role on the Admin UI of [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/users)
- [ ] Remove TSA's admin role on the Admin UI of [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/admin/users)
- [ ] Remove TSA's admin role on the Admin UI of [`tempdev.gitlab`](https://gitlab.temp.gi.ucsc.edu/admin/users) <sub>or `tempdev.gitlab` is not deployed</sub>
- [ ] Remove TSA's admin role on the Admin UI of [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/admin/users)
- [ ] Remove TSA's admin role on the Admin UI of [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/admin/users)
- [ ] Remove TSA as owner of the `ucsc` group on [`dev.gitlab`](https://gitlab.dev.singlecell.gi.ucsc.edu/groups/ucsc/-/group_members)
- [ ] Remove TSA as owner of the `ucsc` group on [`anvildev.gitlab`](https://gitlab.anvil.gi.ucsc.edu/groups/ucsc/-/group_members)
- [ ] Remove TSA as owner of the `ucsc` group on [`tempdev.gitlab`](https://gitlab.temp.gi.ucsc.edu/groups/ucsc/-/group_members) <sub>or `tempdev.gitlab` is not deployed</sub>
- [ ] Remove TSA as owner of the `ucsc` group on [`prod.gitlab`](https://gitlab.azul.data.humancellatlas.org/groups/ucsc/-/group_members)
- [ ] Remove TSA as owner of the `ucsc` group on [`anvilprod.gitlab`](https://gitlab.explore.anvilproject.org/groups/ucsc/-/group_members)
- [ ] Remove `Owner` role from TSA in GCP project [`platform-hca-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-hca-dev)
- [ ] Remove `Owner` role from TSA in GCP project [`platform-anvil-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-anvil-dev)
- [ ] Remove `Owner` role from TSA in GCP project [`platform-temp-dev`](https://console.cloud.google.com/iam-admin/iam?project=platform-temp-dev)
- [ ] Remove `Owner` role from TSA in GCP project [`platform-hca-prod`](https://console.cloud.google.com/iam-admin/iam?project=platform-hca-prod)
- [ ] Remove `Owner` role from TSA in GCP project [`platform-anvil-prod`](https://console.cloud.google.com/iam-admin/iam?project=platform-anvil-prod)
- [ ] Remove `Owner` role from TSA in [UCSCGI](https://app.docker.com/accounts/ucscgi/members) org on DockerHub
- [ ] Remove TSA as admin of Terra group [`azul-dev`](https://bvdp-saturn-dev.appspot.com/#groups/azul-dev)
- [ ] Remove TSA as admin of Terra group [`azul-anvil-dev`](https://bvdp-saturn-dev.appspot.com/#groups/azul-anvil-dev)
- [ ] Remove TSA as admin of Terra group [`azul-prod`](https://app.terra.bio/#groups/azul-prod)
- [ ] Remove TSA as admin of Terra group [`azul-anvil-prod`](https://app.terra.bio/#groups/azul-anvil-prod)
- [ ] Remove TSA as code owner from [.github/CODEOWNERS](https://github.com/DataBiosphere/azul/blob/develop/.github/CODEOWNERS)
