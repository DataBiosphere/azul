from contextlib import (
    contextmanager,
)
import logging
from typing import (
    Any,
    Literal,
    Optional,
    TYPE_CHECKING,
)

import attr
from more_itertools import (
    one,
)

import azul
from azul.json import (
    json_head,
)
from azul.strings import (
    trunc_ellipses,
)
from azul.types import (
    JSON,
    reify,
)

if TYPE_CHECKING:
    from azul.chalice import (
        AzulChaliceApp,
    )


@attr.s(frozen=False, kw_only=False, auto_attribs=True)
class LambdaLogFilter(logging.Filter):
    app: Optional['AzulChaliceApp'] = None

    def filter(self, record):
        if self.app is None or self.app.lambda_context is None:
            record.aws_request_id = '00010ca1-b0ba-466f-8c58-dabbad000000'
        else:
            record.aws_request_id = self.app.lambda_context.aws_request_id

        # The boto3.resources.action logger logs request parameters verbatim at
        # level DEBUG, which is enabled when AZUL_DEBUG is 2. That logger is
        # used for all Boto3 resources, one of which is MultipartUpload, which
        # StorageService uses to upload parts to S3 during mirroring. The parts
        # are very large, causing a memory error when the log message is being
        # prepared in an AWS Lambda function with limited memory. We need to
        # truncate the parameters to a reasonable size.
        #
        if azul.config.debug > 1 and record.name == 'boto3.resources.action':

            def truncate(arg):
                if isinstance(arg, string_types):
                    return arg[:max_log_arg_len]
                elif isinstance(arg, dict):
                    return {k: truncate(v) for k, v in arg.items()}
                elif isinstance(arg, (tuple, list)):
                    return type(arg)(map(truncate, arg))
                else:
                    return arg

            record.args = truncate(record.args)

        return True


lambda_log_format = '\t'.join([
    '[%(levelname)s]',
    '%(asctime)s.%(msecs)03dZ',
    '%(aws_request_id)s',
    '%(name)s',
    '%(message)s'
])
lambda_log_date_format = '%Y-%m-%dT%H:%M:%S'


def configure_app_logging(app: 'AzulChaliceApp', *loggers):
    _configure_log_levels(app.log, *loggers)
    if not app.loaded_dynamically:
        # Environment is not unit test
        root_logger = logging.getLogger()
        if root_logger.hasHandlers():
            # If a handler is already present, we're running on AWS Lambda. See
            #
            # https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/3f43f4d0/awslambdaric/bootstrap.py#L454
            #
            # for details.
            #
            handler = one(root_logger.handlers)
            root_formatter = logging.Formatter(lambda_log_format, lambda_log_date_format)
            handler.setFormatter(root_formatter)
        else:
            # Otherwise, we're running `chalice local`
            handler = logging.StreamHandler()
            logging.basicConfig(format=lambda_log_format,
                                datefmt=lambda_log_date_format,
                                handlers=[handler])
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


es_log = logging.getLogger('opensearch')
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
    if azul.config.debug > 1:
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


json_body_types = reify(JSON)

string_types = str, bytes, bytearray

max_log_arg_len = 1024 * 1024


def http_body_log_message(kind: Literal['request', 'response'], body: Any) -> str:
    """
    Returns a log message suitable for logging the given HTTP request or
    response body. The level set in AZUL_DEBUG determines whether the body is
    logged in full, truncated, or if only its type and length (if known) are
    logged.

    :param kind: wether the given body represents a request or a response

    :param body: the request or response body to be logged
    """
    debug = azul.config.debug
    max_len = max_log_arg_len if debug > 1 else 1024
    assert debug >= 0, debug
    if body is None:
        return f'… without a {kind} body'
    elif isinstance(body, string_types):
        if debug == 0:
            return f'… with a {kind} body of length {len(body)} and type {type(body)!r}'
        elif len(body) <= max_len:
            return f'… with a {kind} body of length {len(body)} being {body !r}'
        else:
            # https://github.com/python/typing/discussions/1911
            prefix = trunc_ellipses(body, max_len)  # type: ignore[type-var]
            return f'… with a {kind} body of length {len(body)} starting in {prefix!r}'
    elif isinstance(body, json_body_types):
        if debug == 0:
            pass  # fall through to the default
        else:
            repr, is_complete = json_head(max_len, body)
            if is_complete:
                return f'… with a {kind} body of length {len(repr)} being {repr}'
            else:
                return f'… with a {kind} body starting in {repr}'
    return f'… with a {kind} body of type ({type(body)!r})'
