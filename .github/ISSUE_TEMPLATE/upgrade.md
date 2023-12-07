---
name: Dependency upgrades
about: 'Issue template for bi-weekly dependency upgrades'
title: Upgrade dependencies
labels: orange,operator,infra,enh,debt 
assignees: ''
---
- [ ] Update [PyCharm image](https://github.com/DataBiosphere/azul-docker-pycharm)
  - [ ] Bump base image tag, if possible
  - [ ] Bump upstream version, if possible
  - [ ] Bump internal version
  - [ ] Remove unused dependencies with high or critical CVEs
  - [ ] Build and test new image locally with Azul's `make format`
  - [ ] Push commit to GitHub (directly to master branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm) 
- [ ] Update [Elasticsearch image](https://github.com/DataBiosphere/azul-docker-elasticsearch)
  - [ ] Bump base image tag (only minor and patch version)
  - [ ] Bump internal version 
  - [ ] Remove unused dependencies with high or critical CVEs
  - [ ] Build and test new image locally with Azul's `make test`
  - [ ] Push commit to GitHub (directly to master branch, no PR needed)
  - [ ] GH Action workflow succeeded
  - [ ] Image is available on [DockerHub](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch) 
- [ ] Create Azul PR, connected to this issue, with … 
  - [ ] … changes to `requirements*.txt` from open Dependabot PRs, one commit per PR
  - [ ] … update to Python (only patch versions) 
  - [ ] … Updates to Terraform (only patch versions)
  - [ ] … new [PyCharm image](https://github.com/DataBiosphere/azul-docker-pycharm)
  - [ ] … new [Elasticsearch image](https://github.com/DataBiosphere/azul-docker-elasticsearch)
  - [ ] … update to Docker images (only minor and patch versions)
  - [ ] … update to GitLab images 
  - [ ] … update to ClamAV image
  - [ ] … update to GitLab AMI
- [ ] Delete obsolete image tags from DockerHub (but consider that `prod` may not use the latest image) … 
  - [ ] … for [PyCharm image](https://hub.docker.com/repository/docker/ucscgi/azul-pycharm)
  - [ ] … for [Elasticsearch image](https://hub.docker.com/repository/docker/ucscgi/azul-elasticsearch)
- [ ] Created tickets for any deferred updates to …
  - [ ] … to next major or minor Python version
  - [ ] … to next major Docker version
  - [ ] … to next major or minor Terraform version
- [ ] Post vulnerability report for `anvilprod` on this issue
