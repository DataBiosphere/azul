"""
Download manifest and metadata for a given bundle from the given repository
source and store them as separate JSON files in the index test data directory.

Note: silently overwrites the destination file.
"""

import argparse
import json
import logging
import os
import sys

from more_itertools import (
    one,
)

from args import (
    AzulArgumentHelpFormatter,
)
from azul import (
    cache,
    config,
)
from azul.files import (
    write_file_atomically,
)
from azul.indexer import (
    Bundle,
    SourcedBundleFQID,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins import (
    RepositoryPlugin,
)

logger = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=AzulArgumentHelpFormatter)
    default_catalog = config.default_catalog
    plugin_cls = RepositoryPlugin.load(default_catalog)
    plugin = plugin_cls.create(default_catalog)
    if len(plugin.sources) == 1:
        source_arg = {'default': str(one(plugin.sources))}
    else:
        source_arg = {'required': True}
    parser.add_argument('--source', '-s',
                        **source_arg,
                        help='The repository source containing the bundle')
    parser.add_argument('--uuid', '-b',
                        required=True,
                        help='The UUID of the bundle to can.')
    parser.add_argument('--version', '-v',
                        help='The version of the bundle to can  (default: the latest version).')
    parser.add_argument('--output-dir', '-O',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the output directory (default: %(default)s).')
    args = parser.parse_args(argv)
    bundle = fetch_bundle(args.source, args.uuid, args.version)
    save_bundle(bundle, args.output_dir)


def fetch_bundle(source: str, bundle_uuid: str, bundle_version: str) -> Bundle:
    for catalog in config.catalogs:
        plugin = plugin_for(catalog)
        sources = set(map(str, plugin.sources))
        if source in sources:
            source = plugin.resolve_source(source)
            fqid = SourcedBundleFQID(source=source,
                                     uuid=bundle_uuid,
                                     version=bundle_version)
            bundle = plugin.fetch_bundle(fqid)
            logger.info('Fetched bundle %r version %r from catalog %r.',
                        fqid.uuid, fqid.version, catalog)
            return bundle
    raise ValueError('No repository using this source')


@cache
def plugin_for(catalog):
    return RepositoryPlugin.load(catalog).create(catalog)


def save_bundle(bundle: Bundle, output_dir: str) -> None:
    for obj, suffix in [(bundle.manifest, '.manifest.json'),
                        (bundle.metadata_files, '.metadata.json')]:
        path = os.path.join(output_dir, bundle.uuid + suffix)
        with write_file_atomically(path) as f:
            json.dump(obj, f, indent=4)
        logger.info('Successfully wrote %s', path)


if __name__ == '__main__':
    configure_script_logging(logger)
    main(sys.argv[1:])
