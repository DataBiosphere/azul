ARG azul_docker_registry
ARG azul_python_image
FROM ${azul_docker_registry}${azul_python_image}

ARG TARGETARCH

# Increment the value of this variable to ensure that all installed OS packages
# are updated.
#
ARG azul_image_version=1
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

ARG azul_terraform_version
RUN mkdir terraform \
    && (set -o pipefail \
        && cd terraform \
        && curl -s -o terraform.zip \
           https://releases.hashicorp.com/terraform/${azul_terraform_version}/terraform_${azul_terraform_version}_linux_${TARGETARCH}.zip \
        && unzip terraform.zip \
        && mv terraform /usr/local/bin) \
    && rm -rf terraform

# Install Docker from apt repository. The statically linked binaries don't
# include buildx or buildkit.
#
# https://docs.docker.com/engine/install/debian/#install-using-the-repository
#
RUN install -m 0755 -d /etc/apt/keyrings
COPY --chmod=0644 bin/keys/docker-apt-keyring.pgp /etc/apt/keyrings/docker.gpg
ARG azul_docker_version
RUN set -o pipefail \
    && ( \
      echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" \
      | tee /etc/apt/sources.list.d/docker.list \
    ) \
    && apt-get update \
    && version=$(apt-cache madison docker-ce | awk '{ print $3 }' | grep -P "^5:\Q${azul_docker_version}\E" | head -1) \
    && test -n "$version" \
    && apt-get -y install docker-ce=$version docker-ce-cli=$version docker-buildx-plugin

ENV project_root /build

COPY requirements*.txt common.mk Makefile ./

ARG PIP_DISABLE_PIP_VERSION_CHECK
ENV PIP_DISABLE_PIP_VERSION_CHECK=${PIP_DISABLE_PIP_VERSION_CHECK}

ARG make_target

RUN make virtualenv \
    && source .venv/bin/activate \
    && make $make_target \
    && rm requirements*.txt common.mk Makefile
