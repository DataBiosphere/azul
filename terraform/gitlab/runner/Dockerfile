ARG azul_docker_registry
FROM ${azul_docker_registry}docker.io/library/docker:24.0.6

# Increment the value of this variable to ensure that all installed OS packages
# are upgraded.
#
ENV azul_docker_executor_image_version=1
RUN apk -U upgrade \
    && apk add make bash docker-credential-ecr-login

ARG azul_docker_registry
ENV azul_docker_registry=${azul_docker_registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${azul_docker_registry%/}" \
    > "${HOME}/.docker/config.json"

# This is the image for the Gitlab runner for Azul builds. It needs to be
# built locally and pushed to the Gitlab-integrated Docker registry on the
# Gitlab instance hosting the Azul project before that project can be built on
# Gitlab. This should happen once after the EBS volume attached to the
# instance is initially provisioned. Additionally, the Gitlab runner on that
# instance needs to be configured to use it. The major version in the tag of
# the base image should match that of the docker-dind image used on the
# instance (look for the EC2 instance's cloud-config in the TF config
# template). To build and push the image, and configure the runner to use it,
# perform the following steps:
#
# Login to the Gitlab instance's web UI and perform the following two steps:
#
# 1) Create a group called `UCSC` and a project called Azul underneath it. This
#    will ensure that the `ucsc/azul` image repository is created underneath in
#    Gitlab's image registry.
#
# 2) Go to your user profile settings and generate a personal access token. For
#    details see the documentation on the `azul_gitlab_access_token` environment
#    variable. Set that variable to the token. Set the `azul_gitlab_user`
#    variable to your GitLab user name.
#
# Run
#
# $ _login
#
# You should now be logged into the GitLab docker registry.
#
# From the directory containing this file, build the image:
#
# $ make docker
#
# Push the image:
#
# $ make docker_push
#
# SSH into Gitlab instance and edit `/mnt/gitlab/runner/config/config.toml`.
# Make sure that the runner(s) reserved for Azul refer to this image.
