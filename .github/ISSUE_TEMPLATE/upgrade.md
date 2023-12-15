---
name: Dependency upgrades
about: 'Issue template for bi-weekly dependency upgrades'
title: Upgrade dependencies
labels: orange,operator,infra,enh,debt 
assignees: ''
---
- [ ] Update [PyCharm image](https://github.com/DataBiosphere/azul-docker-pycharm)
  - [ ] Bump [base image](https://hub.docker.com/_/debian/tags?name=bullseye) tag (only same Debian release), if possible
  - [ ] Bump upstream version, if possible
  - [ ] Bump internal version
  - [ ] Build and test new image locally with Azul's `make format`
  - [ ] Remove unused dependencies with high or critical CVEs
  - [ ] Push commit to GitHub (directly to master branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm) 
- [ ] Update [Elasticsearch image](https://github.com/DataBiosphere/azul-docker-elasticsearch)
  - [ ] Bump [base image](https://hub.docker.com/_/elasticsearch/tags) tag (only minor and patch versions), if possible
  - [ ] Bump internal version 
  - [ ] Build and test new image locally with Azul's `make test`
  - [ ] Remove unused dependencies with high or critical CVEs
  - [ ] Push commit to GitHub (directly to master branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch) 
- [ ] Create Azul PR, connected to this issue, with … 
  - [ ] … changes to `requirements*.txt` from open Dependabot PRs, one commit per PR
  - [ ] … update to [Python](https://hub.docker.com/_/python/tags) (only patch versions) <sub>or no update available</sub>
  - [ ] … update to [Terraform](https://hub.docker.com/r/hashicorp/terraform/tags) (only patch versions) <sub>or no update available</sub>
  - [ ] … new [PyCharm image](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm)
  - [ ] … new [Elasticsearch image](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch)
  - [ ] … update to [Docker images](https://hub.docker.com/_/docker/tags) (only minor and patch versions) <sub>or no update available</sub>
  - [ ] … update to [GitLab](https://hub.docker.com/r/gitlab/gitlab-ce/tags) & [GitLab runner images](https://hub.docker.com/r/gitlab/gitlab-runner/tags) <sub>or no update available</sub>
  - [ ] … update to [ClamAV image](https://hub.docker.com/r/clamav/clamav/tags) <sub>or no update available</sub>
  - [ ] … update to [GitLab AMI](https://github.com/DataBiosphere/azul/blob/develop/OPERATOR.rst#updating-the-ami-for-gitlab-instances) <sub>or no update available</sub>
- [ ] Delete obsolete image tags from DockerHub (but consider that `prod` may not use the latest image) … 
  - [ ] … for [PyCharm image](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm)
  - [ ] … for [Elasticsearch image](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch)
- [ ] Created tickets for any deferred updates to …
  - [ ] … to next major or minor Python version <sub>or such ticket already exists</sub>
  - [ ] … to next major Docker version <sub>or such ticket already exists</sub>
  - [ ] … to next major or minor Terraform version <sub>or such ticket already exists</sub>
