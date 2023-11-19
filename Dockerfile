ARG azul_docker_registry
ARG azul_python_image
FROM ${azul_docker_registry}${azul_python_image}

ARG TARGETARCH

# Increment the value of this variable to ensure that all installed OS packages
# are updated.
#
ENV azul_image_version=1
RUN apt-get update \
    && apt-get upgrade -y

RUN curl -o /usr/bin/docker-credential-ecr-login \
    https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/0.7.0/linux-amd64/docker-credential-ecr-login \
    && printf 'c978912da7f54eb3bccf4a3f990c91cc758e1494a8af7a60f3faf77271b565db /usr/bin/docker-credential-ecr-login\n' | sha256sum -c \
    && chmod +x /usr/bin/docker-credential-ecr-login

ARG azul_docker_registry
ENV azul_docker_registry=${azul_docker_registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${azul_docker_registry%/}" \
    > "${HOME}/.docker/config.json"

SHELL ["/bin/bash", "-c"]

RUN mkdir /build

WORKDIR /build

RUN mkdir terraform \
    && (set -o pipefail \
        && cd terraform \
        && curl -s -o terraform.zip \
           https://releases.hashicorp.com/terraform/1.3.4/terraform_1.3.4_linux_${TARGETARCH}.zip \
        && unzip terraform.zip \
        && mv terraform /usr/local/bin) \
    && rm -rf terraform

# Install `docker` client binary. Installing from distribution packages (.deb)
# is too much of a hassle. The version should match that of the docker version
# installed on the Gitlab instance.
#
RUN set -o pipefail \
    && export docker_arch=$(python3 -c "print(dict(amd64='x86_64',arm64='aarch64')['${TARGETARCH}'])") \
    && curl -s https://download.docker.com/linux/static/stable/${docker_arch}/docker-24.0.6.tgz \
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
