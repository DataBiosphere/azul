from contextlib import (
    contextmanager,
)
import logging
from typing import (
    Optional,
)

import attr
from more_itertools import (
    one,
)

import azul
from azul import (
    config,
)
from azul.chalice import (
    AzulChaliceApp,
)
from azul.strings import (
    trunc_ellipses,
)


@attr.s(frozen=False, kw_only=False, auto_attribs=True)
class LambdaLogFilter(logging.Filter):
    app: Optional[AzulChaliceApp] = None

    def filter(self, record):
        if self.app is None or self.app.lambda_context is None:
            record.aws_request_id = '00010ca1-b0ba-466f-8c58-dabbad000000'
        else:
            record.aws_request_id = self.app.lambda_context.aws_request_id
        return True


lambda_log_format = '\t'.join([
    '[%(levelname)s]',
    '%(asctime)s.%(msecs)03dZ',
    '%(aws_request_id)s',
    '%(name)s',
    '%(message)s'
])
lambda_log_date_format = '%Y-%m-%dT%H:%M:%S'


def configure_app_logging(app: AzulChaliceApp, *loggers):
    _configure_log_levels(app.log, *loggers)
    if not app.unit_test:
        # Environment is not unit test
        root_logger = logging.getLogger()
        if root_logger.hasHandlers():
            # If a handler is already present, assume we're running in AWS Lambda. The
            # handler is setup by AWS Lambda's bootstrap.py, around line 443. That
            # module can be found on GitHub, in the repository linked below. Note
            # that one must extract the image tarball to get to the module.
            #
            # https://github.com/aws/aws-lambda-base-images/tree/python3.11
            #
            handler = one(root_logger.handlers)
            root_formatter = logging.Formatter(lambda_log_format, lambda_log_date_format)
            handler.setFormatter(root_formatter)
            root_logger.addHandler(handler)
        else:
            # Otherwise, we're running `chalice local`
            handler = logging.StreamHandler()
            logging.basicConfig(format=lambda_log_format, datefmt=lambda_log_date_format, handlers=[handler])
        handler.addFilter(LambdaLogFilter(app))


def configure_script_logging(*loggers):
    assert len(logging.getLogger().handlers) == 0, 'Logging is already configured.'
    _configure_non_app_logging(*loggers)


def get_test_logger(*names):
    return logging.getLogger(_test_logger_name(names))


def _test_logger_name(names):
    return '.'.join(('test', *names))


def configure_test_logging(*loggers):
    prefix = _test_logger_name('')
    expected = [(logger.name, True) for logger in loggers]
    actual = [(logger.name, logger.name.startswith(prefix)) for logger in loggers]
    assert actual == expected, actual
    _configure_non_app_logging(get_test_logger(), *loggers)


log_format = ' '.join([
    '%(asctime)s',
    '%(levelname)+7s',
    '%(threadName)s',
    '%(name)s:',
    '%(message)s'
])


def _configure_non_app_logging(*loggers):
    _configure_log_levels(*loggers)
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        handler = one(root_logger.handlers)
        root_formatter = logging.Formatter(log_format)
        handler.setFormatter(root_formatter)
    else:
        logging.basicConfig(format=log_format)


def _configure_log_levels(*loggers):
    azul_level_ = azul_log_level()
    root_level = root_log_level()
    logging.getLogger().setLevel(root_level)
    # Only log AWS request & response bodies when AZUL_DEBUG is 2
    azul_boto3_log.setLevel(root_level)
    es_log.setLevel(es_log_level())
    for logger in {*loggers, azul.log}:
        logger.setLevel(azul_level_)


def root_log_level():
    return [logging.WARN, logging.INFO, logging.DEBUG][azul.config.debug]


def azul_log_level():
    return [logging.INFO, logging.DEBUG, logging.DEBUG][azul.config.debug]


def es_log_level():
    return root_log_level()


def silent_es_log_level():
    return [logging.ERROR, logging.INFO, logging.DEBUG][azul.config.debug]


es_log = logging.getLogger('elasticsearch')
azul_boto3_log = logging.getLogger('azul.boto3')


@contextmanager
def silenced_es_logger():
    """
    Does nothing if AZUL_DEBUG is 2. Temporarily sets the level of the
    Elasticsearch logger to WARNING if AZUL_DEBUG is 1, or ERROR if it is 0.

    Use sparingly since it assumes that only the current thread uses the ES
    client. If other threads use the ES client concurrently, their logging will
    be affected, too.
    """
    if config.debug > 1:
        yield
    else:
        patched_log_level = silent_es_log_level()
        original_log_level = es_log.level
        try:
            es_log.setLevel(patched_log_level)
            assert es_log.level == patched_log_level
            yield
        finally:
            es_log.setLevel(original_log_level)
            assert es_log.level == original_log_level


def http_body_log_message(body_type: str,
                          body: bytes | bytearray | str | None,
                          *,
                          verbatim: bool = False,
                          ) -> str:
    if body is None:
        return f'… without {body_type} body'
    elif isinstance(body, (bytes, bytearray, str)):
        if verbatim:
            if isinstance(body, (bytes, bytearray)):
                body = body.decode(errors='ignore')
        else:
            body = trunc_ellipses(body, max_len=128)
        return f'… with {body_type} body {body!r}'
    else:
        return f'… with nonprintable body ({type(body)!r})'
