"""
Evaluate an expression after 'from azul import config, docker' and either print
the result or return it via the process exit status.
"""
import argparse
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
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('expression',
                    help='the Python expression to evaluate')
group = parser.add_mutually_exclusive_group()
for status in (True, False):
    lower = str(status).lower()
    group.add_argument('--' + lower, '-' + lower[0],
                       dest='status',
                       default=None,
                       action='store_' + lower,
                       help=f'do not print the result of the evaluation but instead '
                            f'exit with a status of 0 if the result is {status}-ish or '
                            f'a non-zero exit status otherwise.')
args = parser.parse_args(sys.argv[1:])
locals = dict(config=config, docker=docker)
result = eval(args.expression, dict(__builtins__={}), locals)
log.info('Expression %r evaluated to %r', args.expression, result)
if args.status is None:
    print(result)
else:
    sys.exit(0 if bool(result) == args.status else 1)
