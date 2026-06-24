"""
Command line utility to make repeated URL requests at a configurable rate
"""

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
import logging
import sys
import time

from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.http import (
    http_client,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--method',
                        metavar='METHOD',
                        type=str,
                        default='HEAD',
                        help='The request method to be used (HEAD, GET, or PUT).')
    parser.add_argument('--url',
                        required=True,
                        metavar='URL',
                        help='The URL to request, '
                             'e.g. https://<deployment>/index/summary.')
    parser.add_argument('--rate',
                        required=True,
                        metavar='RATE',
                        type=int,
                        help='The desired request rate, or number of requests '
                             'per DENOMINATOR. Note: the actual request rate '
                             'can end up being a little slower than the '
                             'rate specified, more so at higher rates.')
    parser.add_argument('--per',
                        metavar='DENOMINATOR',
                        type=int,
                        default='300',
                        help='The DENOMINATOR of the rate in seconds.')
    parser.add_argument('--duration',
                        metavar='SECONDS',
                        type=int,
                        default='300',
                        help='Total duration of the test in seconds.')
    args = parser.parse_args(argv)
    args.method = args.method.upper()
    assert args.method in ['HEAD', 'GET', 'PUT'], R(
        'Invalid request method', args.method)
    assert args.url.startswith('http')
    assert args.rate / args.per >= 1 / 300, R(
        'Rate must be at least 1 request per 300 seconds')
    assert args.rate / args.per <= 10, R(
        'Rate must be no more than 10 request per second')
    assert 1 < args.duration <= 3600, R(
        'Total duration must be between 1 and 3600 seconds')
    return args


http = http_client(log=log)


def request_url(method: str, url: str) -> int:
    response = http.request(method=method, url=url)
    return response.status


def main(argv):
    args = parse_args(argv)
    sleep_delay = args.per / args.rate
    log.info('Starting requests at a rate of %d requests per %d seconds for %d seconds',
             args.rate, args.per, args.duration)
    with ThreadPoolExecutor(max_workers=64) as tpe:
        futures = []
        start_time = time.time()
        end_time = start_time + args.duration
        while time.time() < end_time:
            time.sleep(sleep_delay)
            futures.append(tpe.submit(request_url, args.method, args.url))
        for f in as_completed(futures):
            assert f.result() in [200, 429]

    num_requests = len(futures)
    actual_rate = num_requests / (time.time() - start_time)
    log.info('Number of requests: %d', num_requests)
    log.info('Actual rate: %.2f req/sec (or %.2f req per %d sec)',
             actual_rate, (args.per * actual_rate), args.per)


if __name__ == '__main__':
    configure_script_logging()
    main(sys.argv[1:])
