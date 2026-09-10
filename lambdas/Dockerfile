ARG azul_docker_registry
ARG azul_python_image

# The dependencies are installed in a separate stage so that neither uv, nor the
# tools needed to fetch it, end up in the image we deploy.
#
FROM ${azul_docker_registry}${azul_python_image} AS deps

ARG TARGETARCH

SHELL ["/bin/bash", "-c"]

# The base image has neither `curl` nor `wget`, and installing one would mean
# paying for an `apt-get update`. The tarball is fetched with the interpreter
# that is already present instead. Note that `urlretrieve` follows redirects,
# which the download URL below relies on, and raises on any error status.
#
ARG azul_uv_version
COPY bin/checksums/uv_checksums.txt /tmp/uv_checksums.txt
RUN set -o pipefail \
    && case "$TARGETARCH" in \
           amd64) arch=x86_64 ;; \
           arm64) arch=aarch64 ;; \
           *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
       esac \
    && tarball=uv-${arch}-unknown-linux-gnu.tar.gz \
    && python -c 'import sys, urllib.request; urllib.request.urlretrieve(*sys.argv[1:])' \
       https://github.com/astral-sh/uv/releases/download/${azul_uv_version}/${tarball} \
       /tmp/${tarball} \
    && cd /tmp && grep "${tarball}" uv_checksums.txt | sha256sum -c \
    && tar -xzf /tmp/${tarball} -C /usr/local/bin --strip-components=1 --wildcards "*/uv" \
    && rm /tmp/${tarball} /tmp/uv_checksums.txt

# Install the run-time dependencies, and the Lambda runtime interface client,
# into the image's own Python installation instead of a virtual environment,
# which is what `UV_PROJECT_ENVIRONMENT` selects. The client is declared in the
# `lambda` dependency group, which only this image installs.
#
COPY pyproject.toml uv.lock /build/
ENV UV_PROJECT_ENVIRONMENT=/usr/local
RUN cd /build \
    && uv sync --frozen --no-default-groups --group lambda \
    && rm -rf /build /usr/local/bin/uv

FROM ${azul_docker_registry}${azul_python_image}

ARG APP

COPY --from=deps /usr/local /usr/local

COPY lambdas/${APP}/.chalice/terraform/deployment.zip /tmp/
RUN python -m zipfile -e /tmp/deployment.zip /var/task \
    && rm /tmp/deployment.zip \
    && find /var/task -name __pycache__ -exec rm -rf {} + \
    && python -m compileall -q /var/task

WORKDIR /var/task

ENTRYPOINT ["/usr/local/bin/python", "-m", "awslambdaric"]
