FROM python:3.6.8-stretch

SHELL ["/bin/bash", "-c"]

RUN mkdir /build

WORKDIR /build

RUN mkdir terraform \
    && (cd terraform \
        && wget --quiet https://releases.hashicorp.com/terraform/0.12.24/terraform_0.12.24_linux_amd64.zip \
        && unzip terraform_0.12.24_linux_amd64.zip \
        && mv terraform /usr/local/bin/) \
    ; rm -rf terraform

COPY requirements.txt .
COPY requirements.dev.txt .
RUN python3.6 -m venv .venv \
    && source .venv/bin/activate \
    && pip install -U pip==10.0.1 setuptools==40.1.0 wheel==0.32.3 \
    && pip install -Ur requirements.dev.txt \
    ; rm requirements.txt requirements.dev.txt

# Install `docker` client binary. Installing from distribution packages (.deb)
# is too much of a hassle. The version should roughly match that of the docker
# version installed on the Gitlab instance.
#
RUN curl -s https://download.docker.com/linux/static/stable/x86_64/docker-18.03.1-ce.tgz \
    | tar -xvzf - --strip-components=1 docker/docker \
    && install -g root -o root -m 755 docker /usr/bin
