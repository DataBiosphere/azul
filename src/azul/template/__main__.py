"""
Usage: python -m azul.template foo.json.template.py foo.json

Same as ``python foo.json.template.py foo.json`` but configures script logging
"""
import logging
import sys

from azul.logging import (
    configure_script_logging,
)
from azul.modules import (
    load_module,
)

# This module is the real __main__
#
assert __name__ == '__main__'

# Even though we don't directly use the logger here, we need to instantiate and
# configure it. If we called configure_script_logging() without passing the
# logger, any logger instantiated by the template script would not be considered
# an Azul logger
#
log = logging.getLogger(__name__)
configure_script_logging(log)

# Shift the arguments so that the output file name becomes sys.argv[1] as
# expected by the emit… functions in the sibling __init__.py
#
template = sys.argv.pop(1)

# Invoke the template script and pretend that its module name is also __main__
#
load_module(template, __name__)
