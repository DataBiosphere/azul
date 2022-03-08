FROM python:3.8.12-buster

SHELL ["/bin/bash", "-c"]

RUN mkdir /build

WORKDIR /build

RUN mkdir terraform \
    && (cd terraform \
        && wget --quiet https://releases.hashicorp.com/terraform/0.12.31/terraform_0.12.31_linux_amd64.zip \
        && unzip terraform_0.12.31_linux_amd64.zip \
        && mv terraform /usr/local/bin/ \
        && rm -rf terraform)

# Install `docker` client binary. Installing from distribution packages (.deb)
# is too much of a hassle. The version should roughly match that of the docker
# version installed on the Gitlab instance.
#
RUN curl -s https://download.docker.com/linux/static/stable/x86_64/docker-18.03.1-ce.tgz \
        | tar -xvzf - --strip-components=1 docker/docker \
    && install -g root -o root -m 755 docker /usr/bin

ENV project_root /build

COPY requirements*.txt common.mk Makefile ./

ARG make_target

ARG cache_seed

RUN make virtualenv \
    && source .venv/bin/activate \
    && make $make_target \
    && rm requirements*.txt common.mk Makefile
