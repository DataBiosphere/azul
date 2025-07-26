"""
Copy all files from the public sources in a catalog to the current deployment's
mirroring bucket.
"""
import argparse
import logging
import sys
from typing import (
    Iterable,
)

from azul import (
    CatalogName,
    R,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
    matching_sources,
)
from azul.azulclient import (
    AzulClient,
)
from azul.indexer import (
    SourceRef,
)
from azul.logging import (
    configure_script_logging,
)

log = logging.getLogger(__name__)


def mirror_catalog(azul: AzulClient,
                   catalog: CatalogName,
                   source_globs: set[str],
                   wait: bool):
    plugin = azul.repository_plugin(catalog)
    fail_queue = config.mirror_queue.to_fail.name
    assert azul.is_queue_empty(fail_queue), R(
        'Cannot begin mirroring because a previous operation failed: '
        'there are still messages in the fail queue.',
        fail_queue)
    public_sources_by_spec = {
        str(source.spec): source
        for source in plugin.list_sources(authentication=None)
    }
    sources: Iterable[SourceRef]
    if '*' in source_globs:
        sources = public_sources_by_spec.values()
    else:
        source_strs = matching_sources(azul.sources_by_catalog([catalog]),
                                       source_globs)[catalog]
        try:
            sources = {
                public_sources_by_spec[source]
                for source in source_strs
            }
        except KeyError as e:
            assert False, R(
                'Cannot mirror managed-access source', e.args[0])
    azul.remote_mirror(catalog, sources)
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
                        default=config.current_sources,
                        nargs='+',
                        help='Limit mirroring to a subset of the configured sources. '
                             'Supports shell-style wildcards to match multiple sources per argument. '
                             'All sources must be public.')
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
        mirror_catalog(azul, args.catalog, set(args.sources), args.wait)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
