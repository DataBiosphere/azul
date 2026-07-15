FROM public.ecr.aws/lambda/python:3.14.2026.06.16.17

ARG APP

COPY bin/wheels/runtime/ /tmp/wheels/
COPY lambdas/layer/requirements.txt lambdas/layer/requirements.trans.txt /tmp/
RUN sed -e '/^--find-links/d' -e '/^chalice==/d' /tmp/requirements.txt > /tmp/requirements.clean.txt \
    && pip install --no-index --find-links /tmp/wheels -r /tmp/requirements.clean.txt \
    && pip install --no-deps --no-index --find-links /tmp/wheels chalice==1.32.0+21 \
    && rm -rf /tmp/wheels /tmp/requirements*.txt

COPY src/azul ${LAMBDA_TASK_ROOT}/azul
COPY src/humancellatlas ${LAMBDA_TASK_ROOT}/humancellatlas
COPY lambdas/${APP}/vendor/resources ${LAMBDA_TASK_ROOT}/resources
RUN rm -rf ${LAMBDA_TASK_ROOT}/resources/static
COPY resources/static ${LAMBDA_TASK_ROOT}/resources/static
RUN chmod -R a+r ${LAMBDA_TASK_ROOT}/resources
COPY lambdas/${APP}/app.py ${LAMBDA_TASK_ROOT}/app.py

CMD ["app.app"]
