import logging
from typing import (
    Optional,
)

import attr
from more_itertools import (
    one,
)

import azul
from azul.chalice import (
    AzulChaliceApp,
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
    '%(name)s',
    '%(aws_request_id)s',
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
            # https://github.com/aws/aws-lambda-base-images/tree/python3.8
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
    for logger in loggers:
        assert logger.name.startswith(_test_logger_name(''))
    _configure_non_app_logging(get_test_logger(), *loggers)


log_format = ' '.join([
    '%(asctime)s',
    '%(levelname)-7s',
    '%(threadName)s:',
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
    es_logger = logging.getLogger('elasticsearch')
    for logger in {*loggers, azul.log, es_logger}:
        logger.setLevel(azul_level_)


def root_log_level():
    return [logging.WARN, logging.INFO, logging.DEBUG][azul.config.debug]


def azul_log_level():
    return [logging.INFO, logging.DEBUG, logging.DEBUG][azul.config.debug]
