"""
Extract information from Azul config and print to standard output.

Usage example: python -m azul "config.docker_images['kibana']"
"""

import sys

from azul import (
    config,
)

print(eval(sys.argv[1], dict(__builtins__={}), dict(config=config)))
