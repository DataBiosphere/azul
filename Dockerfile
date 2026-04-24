ARG azul_docker_registry
ARG azul_python_image
FROM --platform=${TARGETPLATFORM} ${azul_docker_registry}${azul_python_image}

ARG TARGETARCH

SHELL ["/bin/bash", "-c"]

# Increment the value of this argument to ensure that all installed OS packages
# are updated.
#
ARG azul_image_version=2
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get -y install build-essential curl gnupg unzip

# Install helper for access to ECR with credendtials from EC2 metadata service
#
RUN curl -o /usr/bin/docker-credential-ecr-login \
    https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/0.7.0/linux-amd64/docker-credential-ecr-login \
    && printf 'c978912da7f54eb3bccf4a3f990c91cc758e1494a8af7a60f3faf77271b565db /usr/bin/docker-credential-ecr-login\n' | sha256sum -c \
    && chmod +x /usr/bin/docker-credential-ecr-login
ARG azul_docker_registry
ENV azul_docker_registry=${azul_docker_registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${azul_docker_registry%/}" \
    > "${HOME}/.docker/config.json"

# Install Terraform
#
ARG azul_terraform_version
RUN mkdir terraform \
    && (set -o pipefail \
        && cd terraform \
        && curl -s -o terraform.zip \
           https://releases.hashicorp.com/terraform/${azul_terraform_version}/terraform_${azul_terraform_version}_linux_${TARGETARCH}.zip \
        && unzip terraform.zip \
        && mv terraform /usr/local/bin) \
    && rm -rf terraform

# Install AWS CLI v2
#
COPY bin/keys/awscli-public-key.asc /tmp/awscli-public-key.asc
ARG azul_awscli_version
RUN gpg --import /tmp/awscli-public-key.asc \
    && rm /tmp/awscli-public-key.asc \
    && case "$TARGETARCH" in \
           amd64) arch=x86_64 ;; \
           arm64) arch=aarch64 ;; \
           *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && curl -s -o awscliv2.zip \
       https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${azul_awscli_version}.zip \
    && curl -s -o awscliv2.sig \
       https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${azul_awscli_version}.zip.sig \
    && gpg --verify awscliv2.sig awscliv2.zip \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip awscliv2.sig aws

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

# Prepare working directory for builds
#
RUN mkdir /build
WORKDIR /build

# Install Azul dependencies
#
ARG PIP_DISABLE_PIP_VERSION_CHECK
ENV PIP_DISABLE_PIP_VERSION_CHECK=${PIP_DISABLE_PIP_VERSION_CHECK}
COPY environment requirements*.txt common.mk Makefile ./
ARG make_target
RUN source environment \
    && make virtualenv \
    && source .venv/bin/activate \
    && make $make_target \
    && rm requirements*.txt common.mk Makefile
