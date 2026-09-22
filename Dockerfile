ARG azul_docker_registry
ARG azul_python_image
FROM --platform=${TARGETPLATFORM} ${azul_docker_registry}${azul_python_image}

ARG TARGETARCH

SHELL ["/bin/bash", "-c"]

# Configure Docker's apt repository. Docker itself is installed further below
# but the repository is configured here, ahead of the package index being
# fetched, so that we only need to fetch once.
#
# https://docs.docker.com/engine/install/debian/#install-using-the-repository
#
RUN install -m 0755 -d /etc/apt/keyrings
COPY --chmod=0644 bin/keys/docker-apt-keyring.pgp /etc/apt/keyrings/docker.gpg
RUN set -o pipefail \
    && ( \
      echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" \
      | tee /etc/apt/sources.list.d/docker.list \
    )

# Fetch the package index. Every package installed below comes from it. The
# packages the base image ships are not upgraded, because that image is pinned
# to a digest that is bumped every other week. Leaving them at the versions that
# digest ships makes the content of this image a function of that digest.
#
# Increment the value of this argument to fetch a new index and to reinstall the
# packages below from it. That is necessary when a build fails because a version
# in the cached index is no longer available from the repository.
#
ARG azul_image_version=3
RUN apt-get update

RUN apt-get -y install curl gnupg unzip

# We pass --ignore-missing to sha256sum so that one checksum file can cover all
# platforms without forcing us to download a file for each one of them. With
# this flag, at least one of the files listed in the checksum file must exist
# and every listed file that does exist must match the listed checksum.

# Install helper for access to ECR with credendtials from EC2 metadata service
#
RUN case "$TARGETARCH" in \
        amd64) sha=c978912da7f54eb3bccf4a3f990c91cc758e1494a8af7a60f3faf77271b565db ;; \
        arm64) sha=ff14a4da40d28a2d2d81a12a7c9c36294ddf8e6439780c4ccbc96622991f3714 ;; \
        *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl --fail --no-progress-meter -o /usr/bin/docker-credential-ecr-login \
    https://amazon-ecr-credential-helper-releases.s3.us-east-2.amazonaws.com/0.7.0/linux-${TARGETARCH}/docker-credential-ecr-login \
    && printf '%s /usr/bin/docker-credential-ecr-login\n' "$sha" | sha256sum -c \
    && chmod +x /usr/bin/docker-credential-ecr-login
ARG azul_docker_registry
ENV azul_docker_registry=${azul_docker_registry}
RUN mkdir -p ${HOME}/.docker \
    && printf '{"credHelpers": {"%s": "ecr-login"}}\n' "${azul_docker_registry%/}" \
    > "${HOME}/.docker/config.json"

# Install Terraform
#
ARG azul_terraform_version
RUN archive=terraform_${azul_terraform_version}_linux_${TARGETARCH}.zip \
    && curl --fail --no-progress-meter --location -o /tmp/${archive} \
       https://releases.hashicorp.com/terraform/${azul_terraform_version}/${archive} \
    && unzip -q -d /usr/local/bin /tmp/${archive} terraform \
    && rm /tmp/${archive}

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
    && curl --fail --no-progress-meter -o awscliv2.zip \
       https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${azul_awscli_version}.zip \
    && curl --fail --no-progress-meter -o awscliv2.sig \
       https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${azul_awscli_version}.zip.sig \
    && gpg --verify awscliv2.sig awscliv2.zip \
    && unzip -q awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip awscliv2.sig \
    && rm -rf aws

# Install GitHub CLI
#
ARG azul_ghcli_version
COPY bin/checksums/gh_checksums.txt /tmp/gh_checksums.txt
RUN tarball=gh_${azul_ghcli_version}_linux_${TARGETARCH}.tar.gz \
    && curl --fail --no-progress-meter --location -o /tmp/${tarball} \
       https://github.com/cli/cli/releases/download/v${azul_ghcli_version}/${tarball} \
    && cd /tmp && sha256sum --ignore-missing -c gh_checksums.txt \
    && tar -xzf /tmp/${tarball} -C /usr/local/bin --strip-components=2 --wildcards "*/bin/gh" --occurrence=1 \
    && rm /tmp/${tarball} /tmp/gh_checksums.txt

# Install uv
#
ARG azul_uv_version
COPY bin/checksums/uv_checksums.txt /tmp/uv_checksums.txt
RUN case "$TARGETARCH" in \
           amd64) arch=x86_64 ;; \
           arm64) arch=aarch64 ;; \
           *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && tarball=uv-${arch}-unknown-linux-gnu.tar.gz \
    && curl --fail --no-progress-meter --location -o /tmp/${tarball} \
       https://github.com/astral-sh/uv/releases/download/${azul_uv_version}/${tarball} \
    && cd /tmp && sha256sum --ignore-missing -c uv_checksums.txt \
    && tar -xzf /tmp/${tarball} -C /usr/local/bin --strip-components=1 --wildcards "*/uv" \
    && rm /tmp/${tarball} /tmp/uv_checksums.txt

# Install Docker using the Apt repository configured above. We can't use the
# statically linked binaries because they lack buildx and buildkit.
#
# Only the client and buildx are installed. Containers from this image don't run
# a daemon, they use the one that `DOCKER_HOST` refers to, so the engine and the
# packages it pulls in, containerd among them, would only add to the size of
# this image.
#
ARG azul_docker_version
RUN set -o pipefail \
    && version=$(apt-cache madison docker-ce-cli | awk '{ print $3 }' | grep -P "^5:\Q${azul_docker_version}\E" | head -1) \
    && test -n "$version" \
    && apt-get -y install --no-install-recommends docker-ce-cli=$version docker-buildx-plugin

# Set UV_PROJECT_ENVIRONMENT to point at the image's own Python installation,
# and install Azul's dependencies there. A container typically has no need for
# the isolation a virtual environment provides.
#
# A few of the dependencies are only published as source distributions, so a
# compiler is needed to install them. It is installed, used and removed in one
# instruction, because an image only ever shrinks within the instruction that
# creates the layer, never in a later one.
#
# `git` and `make` are needed for the Azul build. The latter is also a
# dependency of build-essential but we list it explicitly so that purging
# build-essential does not remove it again.
#
ENV UV_PROJECT_ENVIRONMENT=/usr/local
COPY pyproject.toml uv.lock /azul/
RUN apt-get -y install build-essential git make \
    && cd /azul \
    && uv sync --frozen \
    && apt-get -y purge build-essential \
    && apt-get -y autoremove \
    && rm -r /azul /tmp/uv-*.lock
