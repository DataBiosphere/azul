"""
Copy all files from the public sources in a catalog to the current deployment's
mirroring bucket.
"""
import argparse
import logging
import sys

from azul import (
    CatalogName,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
    get_sources,
)
from azul.azulclient import (
    AzulClient,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def mirror_catalog(azul: AzulClient,
                   catalog: CatalogName,
                   source_globs: set[str],
                   wait: bool):
    fail_queue = config.mirror_queue.to_fail.name
    assert azul.is_queue_empty(fail_queue), R(
        'Cannot begin mirroring because a previous operation failed: '
        'there are still messages in the fail queue.',
        fail_queue)
    sources = azul.source_service.list_sources(catalog,
                                               config.ServiceAccount.indexer)
    if '*' not in source_globs:
        matching_sources = azul.matching_sources([catalog], source_globs)[catalog]
        sources = [src for src in sources if src.ref.spec in matching_sources]

    azul.mirror_service(catalog).mirror_sources(sources)

    if wait:
        azul.wait_for_mirroring()
        assert azul.is_queue_empty(fail_queue), R(
            'There are messages in the fail queue', fail_queue)


def main(args):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--catalog',
                        metavar='NAME',
                        choices=config.catalogs,
                        default=config.default_catalog,
                        help='The name of the catalog to mirror.')
    parser.add_argument('--sources',
                        nargs='+',
                        help='Limit mirroring to a subset of the configured sources. '
                             'Supports shell-style wildcards to match multiple sources per argument. '
                             'All sources must be public. If no values are passed, this argument will be set from the '
                             'environment variable ``azul_current_sources``. If that variable is unset, all sources in '
                             'the selected catalog will be used.')
    parser.add_argument('--mirror',
                        action='store_true',
                        help='Mirror files in the specified catalog and sources')
    parser.add_argument('--purge',
                        action='store_true',
                        help='Purge the mirror queue before taking any other action.')
    parser.add_argument('--no-wait',
                        action='store_false',
                        dest='wait',
                        help='Do not wait for queues to empty before exiting script.')
    args = parser.parse_args(args)
    assert config.enable_mirroring, R('Mirroring is not enabled')

    azul = AzulClient()
    if args.purge:
        azul.queues.purge_mirror()
    if args.mirror:
        mirror_catalog(azul, args.catalog, get_sources(args.sources), args.wait)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
