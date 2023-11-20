ARG azul_docker_registry
ARG azul_docker_image
FROM --platform=${TARGETPLATFORM} ${azul_docker_registry}${azul_docker_image}

# Increment the value of this variable to ensure that all installed OS packages
# are upgraded.
#
ARG azul_executor_image_version=1
RUN apk -U upgrade \
    && apk add make bash docker-credential-ecr-login

ARG azul_docker_registry
ENV azul_docker_registry=${azul_docker_registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${azul_docker_registry%/}" \
    > "${HOME}/.docker/config.json"

# This is the image for the GitLab executor for Azul builds. It needs to be
# built locally and pushed to the Docker registry on each GitLab instance before
# Azul can be built there. This should happen once after the EBS data volume
# attached to the instance is initially provisioned, and everytime this file is
# modified. Additionally, the GitLab runner on that instance needs to be
# configured to use it. To build and push the image to a GitLab instance, login
# to the GitLab instance's web UI and perform the following two steps:
#
# 1) Create a group called `UCSC` and a project called Azul underneath it. This
#    will ensure that the `ucsc/azul` image repository is created underneath in
#    GitLab's image registry. Skip this step if the group and project already
#    exist.
#
# 2) Go to your user profile settings and generate a personal access token. For
#    details see the documentation on the `azul_gitlab_access_token` environment
#    variable. Set that variable to the token. Set the `azul_gitlab_user`
#    variable to your GitLab user name. You only need to perform this step once
#    per instance.
#
# Run
#
# $ _login
#
# You should now be logged into the GitLab instance's Docker registry.
#
# From the directory containing this file, build the image:
#
# $ make docker
#
# Push the image:
#
# $ make docker_push
#
# To configure the Azul runners on the GitLab instance to use the executor
# image, SSH into the instance and edit `/mnt/gitlab/runner/config/config.toml`.
# Make sure that the runner(s) reserved for Azul refer to this image. This only
# needs to be done once per GitLab data volume, after the volume was newly
# provisioned per data volume.
