---
name: Enable GitLab cleanup policy
about: 'Issue template to remind the system administrator to enable the GitLab cleanup policy'
title: Enable the GitLab cleanup policy setting in all deployments
labels: orange,infra,enh,debt 
_start: 2024-07-29T09:00
_period: 28 days
---
- [ ] Cleanup policy enabled for [GitLab dev](https://gitlab.dev.singlecell.gi.ucsc.edu/admin/application_settings/ci_cd#application_setting_container_registry_expiration_policies_worker_capacity) <sub>or not applicable</sub>
  - [ ] Set the number of clean-up workers to four
  - [ ] At least one hour has passed since the number of clean-up workers was set
  - [ ] Set the number of clean-up workers back to zero
- [ ] Cleanup policy enabled for [GitLab anvildev](https://gitlab.anvil.gi.ucsc.edu/admin/application_settings/ci_cd#application_setting_container_registry_expiration_policies_worker_capacity) <sub>or not applicable</sub>
  - [ ] Set the number of clean-up workers to four
  - [ ] At least one hour has passed since the number of clean-up workers was set
  - [ ] Set the number of clean-up workers back to zero
- [ ] Cleanup policy enabled for [GitLab anvilprod](https://gitlab.explore.anvilproject.org/admin/application_settings/ci_cd#application_setting_container_registry_expiration_policies_worker_capacity) <sub>or not applicable</sub>
  - [ ] Set the number of clean-up workers to four
  - [ ] At least one hour has passed since the number of clean-up workers was set
  - [ ] Set the number of clean-up workers back to zero
- [ ] Cleanup policy enabled for [GitLab prod](https://gitlab.azul.data.humancellatlas.org/admin/application_settings/ci_cd#application_setting_container_registry_expiration_policies_worker_capacity) <sub>or not applicable</sub>
  - [ ] Set the number of clean-up workers to four
  - [ ] At least one hour has passed since the number of clean-up workers was set
  - [ ] Set the number of clean-up workers back to zero
