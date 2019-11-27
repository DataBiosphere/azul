import logging

import azul
from azul.chalice import AzulChaliceApp


def configure_app_logging(app: AzulChaliceApp, *loggers):
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
    azul_level_ = azul_log_level()
    root_level = root_log_level()
    logging.getLogger().setLevel(root_level)
    for logger in {*loggers, azul.log}:
        logger.setLevel(azul_level_)


def root_log_level():
    return [logging.WARN, logging.INFO, logging.DEBUG][azul.config.debug]


def azul_log_level():
    return [logging.INFO, logging.DEBUG, logging.DEBUG][azul.config.debug]
