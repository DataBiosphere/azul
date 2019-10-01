import logging

from azul import config
from azul.chalice import AzulChaliceApp


def configure_app_logging(app: AzulChaliceApp, *loggers):
    app.debug = config.debug > 0
    _configure_log_levels(app.log, *loggers)


def configure_script_logging(*loggers):
    assert len(logging.getLogger().handlers) == 0, 'Logging is already configured.'
    _configure_non_app_logging(*loggers)


def configure_test_logging(*loggers):
    _configure_non_app_logging(*loggers)


def _configure_non_app_logging(*loggers):
    logging.basicConfig(format="%(asctime)s %(levelname)-7s %(threadName)s: %(message)s")
    _configure_log_levels(*loggers)


def _configure_log_levels(*loggers):
    logging.getLogger().setLevel([logging.WARN, logging.INFO, logging.DEBUG][config.debug])
    for logger in {*loggers, logging.getLogger('azul')}:
        logger.setLevel([logging.INFO, logging.DEBUG, logging.DEBUG][config.debug])
