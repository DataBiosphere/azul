FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    libssl-dev \
    libffi-dev \
    libxml2-dev \
    libxslt-dev \
    python3-dev \
    curl \
    python3-pip \
    rsync

RUN pip3 install --upgrade pip

# FIXME: Explain in a comment why these two requirements aren't part of
# requirements.txt

RUN pip3 install faker_schema
RUN pip3 install --upgrade cffi
ADD webservice/requirements.txt /app/requirements.txt
RUN pip install --ignore-installed -r /app/requirements.txt

# Add app code. Note that this expects the Docker build context to be set to the
# project root, i.e. the parent of the test directory.

COPY webservice /app
# Docker copies symlinks as symlinks, so to prevent a dangling symlink we need to copy the referee
COPY indexer/utils /app/src/utils
WORKDIR /app
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV PYTHONPATH=/app/src
CMD ["test/go.sh"]
