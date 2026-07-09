"""
Copy all files from a catalog to the current deployment's mirroring bucket.
"""
import argparse
import logging
import sys

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
    get_sources,
)
from azul.auth import (
    indexer_authentication,
)
from azul.azulclient import (
    AzulClient,
)
from azul.indexer.mirror_service import (
    MirrorService,
)
from azul.lib import (
    R,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def mirror(args, azul: AzulClient) -> None:
    fail_queue = config.mirror_queue.to_fail.name
    assert azul.is_queue_empty(fail_queue), R(
        'Cannot begin mirroring because a previous operation failed: '
        'there are still messages in the fail queue.',
        fail_queue)

    if args.purge:
        azul.queues.purge_mirror()
    if args.mark:
        MirrorService.mirror_catalogs(mark=True, it=False)
    else:
        catalog = config.default_catalog if args.catalog is None else args.catalog
        service = MirrorService.for_catalog(catalog)
        source_globs = get_sources(args.sources)
        sources = azul.source_service.list_sources(catalog, indexer_authentication)
        if '*' not in source_globs:
            matching = azul.matching_sources([catalog], source_globs)[catalog]
            sources = [s for s in sources if s.ref.spec in matching]
        service.mirror_sources(sources)

    if args.wait:
        azul.wait_for_mirroring()
        assert azul.is_queue_empty(fail_queue), R(
            'There are messages in the fail queue', fail_queue)


def sweep(args) -> None:
    MirrorService.sweep_catalogs(dry_run=args.dry_run, it=False)


def main(args):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=AzulArgumentHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    sp = subparsers.add_parser('mirror',
                               help='Mirror files to the mirror bucket.',
                               formatter_class=AzulArgumentHelpFormatter)
    sp.add_argument('--catalog',
                    metavar='NAME',
                    choices=config.catalogs,
                    default=None,
                    help='The name of the catalog to mirror.')
    sp.add_argument('--sources',
                    nargs='+',
                    help='Limit mirroring to a subset of the configured sources. '
                         'Supports shell-style wildcards to match multiple sources per argument. '
                         'If no values are passed, this argument will be set from the environment variable '
                         '``azul_current_sources``. If that variable is unset, all sources in '
                         'the selected catalog will be used.')
    sp.add_argument('--mark',
                    action='store_true',
                    help='Enable marking for garbage collection. '
                         'Mirrors all sources in all catalogs, overwriting info objects to update '
                         'their LastModified time. Mutually exclusive with --catalog and --sources.')
    sp.add_argument('--purge',
                    action='store_true',
                    help='Purge the mirror queue before taking any other action.')
    sp.add_argument('--no-wait',
                    action='store_false',
                    dest='wait',
                    help='Do not wait for queues to empty before exiting script.')

    sp = subparsers.add_parser('sweep',
                               help='Sweep garbage from mirror buckets.',
                               formatter_class=AzulArgumentHelpFormatter)
    sp.add_argument('--dry-run',
                    action='store_true',
                    help='Report garbage objects without deleting them.')

    args = parser.parse_args(args)
    assert config.enable_mirroring, R('Mirroring is not enabled')

    if args.command == 'mirror':
        if args.mark:
            assert args.catalog is None, R(
                '--mark is mutually exclusive with --catalog')
            assert args.sources is None, R(
                '--mark is mutually exclusive with --sources')
        azul = AzulClient()
        mirror(args, azul)
    elif args.command == 'sweep':
        sweep(args)
    else:
        parser.print_usage()


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
