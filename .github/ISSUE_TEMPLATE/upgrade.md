---
name: Upgrade software dependencies
about: Issue template for the bi-weekly upgrade of Azul's software dependencies
title: Upgrade software dependencies
labels: operator,infra
type: Chore
_priority: \-
_start: 2023-11-27T09:00
_period: 14 days
---
- [ ] Update [PyCharm image](https://github.com/DataBiosphere/azul-docker-pycharm)
  - [ ] Bump [base image](https://hub.docker.com/_/debian/tags?name=trixie) `-slim` tag (only same Debian release), if possible
  - [ ] Bump upstream version, if possible
  - [ ] Bump internal version
  - [ ] Remove unused dependencies with high or critical CVEs
  - [ ] Push commit to GitHub (directly to `master` branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags) 
- [ ] Update [BigQuery Emulator image](https://github.com/DataBiosphere/azul-bigquery-emulator)
  - [ ] Bump [base image](https://hub.docker.com/_/debian/tags?name=trixie) `-slim` tag, if possible
  - [ ] Bump internal version 
  - [ ] Push commit to GitHub (directly to `azul` branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags) 
- [ ] Create Azul PR, linked to this issue, with … 
    - [ ] … changes to `requirements*.txt` from open Dependabot PRs, one commit per PR
    - [ ] … upgrade direct Python dependencies, [reference the operator manual](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#upgrade-direct-python-dependencies) for instructions <sub>or not applicable</sub>
    - [ ] … update to [Python](https://hub.docker.com/_/python/tags) (only patch versions) <sub>or no update available</sub>
    - [ ] … update to [Terraform](https://hub.docker.com/r/hashicorp/terraform/tags) (only patch versions) <sub>or no update available</sub>
    - [ ] … update to Terraform provider (only minor and patch versions) …
        - [ ] … [hashicorp/aws](https://registry.terraform.io/providers/hashicorp/aws/latest) <sub>or no update available</sub>
        - [ ] … [hashicorp/external](https://registry.terraform.io/providers/hashicorp/external/latest) <sub>or no update available</sub>
        - [ ] … [hashicorp/google](https://registry.terraform.io/providers/hashicorp/google/latest) <sub>or no update available</sub>
        - [ ] … [hashicorp/null](https://registry.terraform.io/providers/hashicorp/null/latest) <sub>or no update available</sub>
    - [ ] … new [PyCharm image](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm/tags)
    - [ ] … new [BigQuery Emulator image](https://hub.docker.com/repository/docker/ucscgi/azul-bigquery-emulator/tags)
    - [ ] … update to [OpenSearch image](https://hub.docker.com/r/opensearchproject/opensearch/tags) (only minor and patch versions) <sub>or no update available</sub>
    - [ ] … update to [Docker images](https://hub.docker.com/_/docker/tags) (only minor and patch versions) <sub>or no update available</sub>
    - [ ] … update to [GitLab](https://hub.docker.com/r/gitlab/gitlab-ce/tags) & [GitLab runner images](https://hub.docker.com/r/gitlab/gitlab-runner/tags) <sub>or no update available</sub>
    - [ ] … update to [ClamAV image](https://hub.docker.com/r/clamav/clamav/tags) <sub>or no update available</sub>
    - [ ] … update to [GitLab AMI](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#updating-the-ami-for-gitlab-instances) <sub>or no update available</sub>
    - [ ] … update to [AL2023 release](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#updating-software-packages-via-release-version-upgrade-in-al2023-instances) <sub>or no update available</sub>
    - [ ] … update to [Swagger UI](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#updating-the-swagger-ui) <sub>or no update available</sub>
    - [ ] … update to [AWS CLI v2](https://github.com/aws/aws-cli/blob/v2/CHANGELOG.rst) <sub>or no update available</sub>
- [ ] Created issues for any deferred updates to …
  - [ ] … the next major or minor Python version <sub>or such an issue already exists</sub>
  - [ ] … the next major Docker version <sub>or such an issue already exists</sub>
  - [ ] … the next major or minor Terraform version <sub>or such an issue already exists</sub>
  - [ ] … the next major version of any Terraform provider used by Azul <sub>or such issues already exist</sub>
  - [ ] … the next major OpenSearch version <sub>or such an issue already exists</sub>
