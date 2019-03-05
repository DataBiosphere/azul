FROM python:3.6.8-stretch

SHELL ["/bin/bash", "-c"]

RUN mkdir /build

WORKDIR /build

RUN mkdir terraform \
    && (cd terraform \
        && wget --quiet https://releases.hashicorp.com/terraform/0.11.11/terraform_0.11.11_linux_amd64.zip \
        && unzip terraform_0.11.11_linux_amd64.zip \
        && mv terraform /usr/local/bin/) \
    ; rm -rf terraform

# FIXME: the mention of pip==18.1 is inconsistent with .travis.yml and README

COPY requirements.txt .
COPY requirements.dev.txt .
RUN python3.6 -m venv .venv \
    && source .venv/bin/activate \
    && pip install -U pip==18.1 setuptools==40.1.0 wheel==0.32.3 \
    && pip install -r requirements.dev.txt \
    ; rm requirements.txt requirements.dev.txt
