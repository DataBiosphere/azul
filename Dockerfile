ARG registry
FROM ${registry}docker.io/library/python:3.11.5-bullseye

# Increment the value of this variable to ensure that all installed OS packages
# are updated.
#
ENV azul_docker_image_version=1
RUN apt-get update \
    && apt-get upgrade -y

RUN curl -o /usr/bin/docker-credential-ecr-login \
    https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/0.7.0/linux-amd64/docker-credential-ecr-login \
    && printf 'c978912da7f54eb3bccf4a3f990c91cc758e1494a8af7a60f3faf77271b565db /usr/bin/docker-credential-ecr-login\n' | sha256sum -c \
    && chmod +x /usr/bin/docker-credential-ecr-login

ARG registry
ENV azul_docker_registry=${registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${registry%/}" \
    > "${HOME}/.docker/config.json"

SHELL ["/bin/bash", "-c"]

RUN mkdir /build

WORKDIR /build

RUN mkdir terraform \
    && (cd terraform \
        && wget --quiet https://releases.hashicorp.com/terraform/1.3.4/terraform_1.3.4_linux_amd64.zip \
        && unzip terraform_1.3.4_linux_amd64.zip \
        && mv terraform /usr/local/bin) \
    && rm -rf terraform

# Install `docker` client binary. Installing from distribution packages (.deb)
# is too much of a hassle. The version should roughly match that of the docker
# version installed on the Gitlab instance.
#
RUN curl -s https://download.docker.com/linux/static/stable/x86_64/docker-20.10.18.tgz \
        | tar -xvzf - --strip-components=1 docker/docker \
    && install -g root -o root -m 755 docker /usr/bin \
    && rm docker

ENV project_root /build

COPY requirements*.txt common.mk Makefile ./

ARG PIP_DISABLE_PIP_VERSION_CHECK
ENV PIP_DISABLE_PIP_VERSION_CHECK=${PIP_DISABLE_PIP_VERSION_CHECK}

ARG make_target

RUN make virtualenv \
    && source .venv/bin/activate \
    && make $make_target \
    && rm requirements*.txt common.mk Makefile
