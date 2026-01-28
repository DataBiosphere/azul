"""
Copy all files from the public sources in a catalog to the current deployment's
mirroring bucket.
"""
import argparse
import logging
import sys

from azul import (
    CatalogName,
    R,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
    get_sources,
)
from azul.azulclient import (
    AzulClient,
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
        source.spec: source
        for source in plugin.list_accessible_sources(authentication=None)
    }
    # When the user doesn't specify a source or provides "*" as a source glob,
    # we implicitly filter out managed-access sources. This lets us assert that
    # all sources matching the provided globs are public, without forcing the
    # user to manually specify every public source.
    if '*' in source_globs:
        source_specs = azul.repository_plugin(catalog).sources
        source_refs = {
            source: source_specs[spec]
            for spec, source in public_sources_by_spec.items()
        }
    else:
        source_specs = azul.matching_sources([catalog], source_globs)[catalog]
        try:
            source_refs = {
                public_sources_by_spec[spec]: cfg
                for spec, cfg in source_specs.items()
            }
        except KeyError as e:
            assert False, R(
                'Cannot mirror managed-access source', e.args[0])
    azul.mirror_service(catalog).mirror_sources(source_refs.items())

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
