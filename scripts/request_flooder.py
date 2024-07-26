"""
Command line utility to make repeated HEAD requests at a given rate
"""

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging
import sys
import time

from azul import (
    require,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.http import (
    http_client,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)

http = http_client(log)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--url',
                        required=True,
                        metavar='URL',
                        help='The URL to request, '
                             'e.g. https://<deployment>/index/summary.')
    parser.add_argument('--rate',
                        required=True,
                        metavar='RATE',
                        type=int,
                        help='The desired request rate in req/5min. Note: the '
                             'actual request rate can end up being a little '
                             'slower than the desired rate, more so at higher '
                             'rates.')
    parser.add_argument('--time',
                        metavar='TIME',
                        type=int,
                        default='300',
                        help='The length of the test in seconds')
    args = parser.parse_args(argv)
    require(args.url.startswith('http'))
    require(1 <= args.rate <= 3000,
            'Request rate must be between 1 and 3000')
    require(1 < args.time <= 3600,
            'Request time must be between 1 and 3600')
    return args


def head_url(url: str) -> int:
    response = http.request('HEAD', url)
    return response.status


def main(argv):
    args = parse_args(argv)
    sleep_delay = 5 * 60 / args.rate
    with ThreadPoolExecutor(max_workers=64) as tpe:
        futures = []
        start_time = time.time()
        end_time = start_time + args.time
        while time.time() < end_time:
            time.sleep(sleep_delay)
            futures.append(tpe.submit(head_url, args.url))
        for f in as_completed(futures):
            assert f.result() in [200, 429]

    actual_rate = len(futures) / (time.time() - start_time)
    log.info('Actual rate: %.2f req/sec (%.2f req/5min)',
             actual_rate, (5 * 60 * actual_rate))


if __name__ == '__main__':
    configure_script_logging()
    main(sys.argv[1:])
