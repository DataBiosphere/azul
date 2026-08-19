ARG azul_docker_registry
ARG azul_python_image
FROM ${azul_docker_registry}${azul_python_image}

ARG APP

RUN pip install --no-cache-dir awslambdaric

COPY requirements.txt requirements.trans.txt /tmp/
RUN pip install --no-cache-dir --no-deps -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt /tmp/requirements.trans.txt

COPY lambdas/${APP}/.chalice/terraform/deployment.zip /tmp/
RUN python -m zipfile -e /tmp/deployment.zip /var/task \
    && rm /tmp/deployment.zip

WORKDIR /var/task

ENTRYPOINT ["/usr/local/bin/python", "-m", "awslambdaric"]
