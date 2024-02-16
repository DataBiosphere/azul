"""
Extract information from Azul config and print to standard output.

Usage example: python -m azul 'docker.resolve_docker_image_for_launch("pycharm")'
"""
import logging
import sys

from azul import (
    config,
    docker,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)
configure_script_logging()
locals = dict(config=config, docker=docker)
expression = sys.argv[1]
result = str(eval(expression, dict(__builtins__={}), locals))
log.info('Expression str(%s) evaluated to %r', expression, result)
print(result)
