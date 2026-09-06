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
ARG azul_image_version=2
RUN apt-get update

RUN apt-get -y install build-essential curl gnupg unzip

# Install helper for access to ECR with credendtials from EC2 metadata service
#
RUN case "$TARGETARCH" in \
        amd64) sha=c978912da7f54eb3bccf4a3f990c91cc758e1494a8af7a60f3faf77271b565db ;; \
        arm64) sha=ff14a4da40d28a2d2d81a12a7c9c36294ddf8e6439780c4ccbc96622991f3714 ;; \
        *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && curl -o /usr/bin/docker-credential-ecr-login \
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
    && rm awscliv2.zip awscliv2.sig \
    && rm -rf aws

# Install GitHub CLI
#
ARG azul_ghcli_version
COPY bin/checksums/gh_checksums.txt /tmp/gh_checksums.txt
RUN tarball=gh_${azul_ghcli_version}_linux_${TARGETARCH}.tar.gz \
    && curl --fail --silent --location -o /tmp/${tarball} \
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
    && curl --fail --silent --location -o /tmp/${tarball} \
       https://github.com/astral-sh/uv/releases/download/${azul_uv_version}/${tarball} \
    && cd /tmp && sha256sum --ignore-missing -c uv_checksums.txt \
    && tar -xzf /tmp/${tarball} -C /usr/local/bin --strip-components=1 --wildcards "*/uv" \
    && rm /tmp/${tarball} /tmp/uv_checksums.txt

# Install Docker using the Apt repository configured above. We can't use the
# statically linked binaries because they lack buildx and buildkit.
#
ARG azul_docker_version
RUN set -o pipefail \
    && version=$(apt-cache madison docker-ce | awk '{ print $3 }' | grep -P "^5:\Q${azul_docker_version}\E" | head -1) \
    && test -n "$version" \
    && apt-get -y install docker-ce=$version docker-ce-cli=$version docker-buildx-plugin

# Install the Python formatter, which is part of PyCharm. See the `format`
# target in the Makefile.
#
# PyCharm bundles its own JRE, the JetBrains Runtime. We install the
# distribution's JRE instead, which is patched whenever the pin of the base
# image is bumped, and don't extract the bundled one. Its major version matches
# the one of the bundled runtime.
#
# We only extract what the formatter needs: the platform, the launchers, and the
# five plugins that PyCharm considers essential. We omit the bundled runtime,
# four dozen other plugins and the helper scripts of the Python plugin, together
# around two thirds of the distribution.
#
# The archive member selection names the plugins individually rather than as a
# directory because `plugins/plugin-classpath.txt` must not be extracted. That
# file is a precomputed index of the JARs of all bundled plugins, and with it in
# place the platform would not start on this image. Without it, the platform
# discovers the plugins by scanning the directory, tolerating the absence of the
# ones left behind.
#
# If a future version of PyCharm needs more than what is extracted here, the
# `format` and `check_clean` targets in the GitLab build will fail. Consult the
# `pycharm-upgrade` skill before changing the version.
#
ARG azul_pycharm_version
COPY bin/checksums/pycharm_checksums.txt /tmp/pycharm_checksums.txt
RUN apt-get -y install --no-install-recommends openjdk-21-jre-headless \
    && case "$TARGETARCH" in \
           amd64) arch= ;; \
           arm64) arch=-aarch64 ;; \
           *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && tarball=pycharm-community-${azul_pycharm_version}${arch}.tar.gz \
    && curl --fail --silent --location -o /tmp/${tarball} \
       https://download.jetbrains.com/python/${tarball} \
    && cd /tmp && sha256sum --ignore-missing -c pycharm_checksums.txt \
    && mkdir /opt/pycharm \
    && tar -xzf /tmp/${tarball} -C /opt/pycharm \
           --strip-components=1 \
           --wildcards \
           --no-wildcards-match-slash \
           --anchored \
           # The first * matches a top-level directory named after the release \
           --exclude '*/plugins/python-ce/helpers' \
           # The remaining arguments are inclusions \
           '*/bin' \
           '*/lib' \
           '*/license' \
           '*/modules' \
           '*/product-info.json' \
           '*/plugins/json' \
           '*/plugins/pycharm-community-customization' \
           '*/plugins/pycharm-community-customization-shared' \
           '*/plugins/python-ce' \
           '*/plugins/toml' \
    && rm /tmp/${tarball} /tmp/pycharm_checksums.txt \
    && rm -r /tmp/hsperfdata_root

# Prepare working directory for builds
#
RUN mkdir /build
WORKDIR /build

# Install Azul dependencies
#
COPY pyproject.toml uv.lock common.mk Makefile ./
# We don't source `environment` here. It loads the environment by running
# `scripts/export_environment.py`, and neither that script nor the
# `environment.py` files it reads are part of this image. The only variable the
# targets below need is `project_root`, which `environment` assigns itself,
# without involving that script.
#
RUN export project_root="$PWD" \
    && make virtualenv \
    && source .venv/bin/activate \
    && make requirements \
    && rm pyproject.toml uv.lock common.mk Makefile /tmp/uv-*.lock
