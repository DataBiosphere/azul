"""
Download the contents of a given bundle from the given repository source and
store it as a single JSON file.

Note: silently overwrites the destination file.
"""

import argparse
import base64
import hashlib
import json
import logging
import os
import struct
import sys
import uuid

from azul import (
    cache,
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.files import (
    write_file_atomically,
)
from azul.indexer import (
    Bundle,
    SourcedBundleFQIDJSON,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins import (
    RepositoryPlugin,
)
from azul.plugins.metadata.anvil.bundle import (
    AnvilBundle,
)
from azul.types import (
    AnyJSON,
    AnyMutableJSON,
)

log = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--source', '-s',
                        required=True,
                        help='The repository source containing the bundle')
    parser.add_argument('--uuid', '-b',
                        required=True,
                        help='The UUID of the bundle to can.')
    parser.add_argument('--version', '-v',
                        help='The version of the bundle to can. Required for HCA, ignored for AnVIL.')
    parser.add_argument('--output-dir', '-O',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the output directory (default: %(default)s).')
    parser.add_argument('--redaction-key', '-K',
                        help='Provide a key to redact confidential or sensitive information from the output files')
    args = parser.parse_args(argv)
    bundle = fetch_bundle(args.source, args.uuid, args.version)
    if args.redaction_key:
        redact_bundle(bundle, args.redaction_key.encode())
    save_bundle(bundle, args.output_dir)


def fetch_bundle(source: str, bundle_uuid: str, bundle_version: str) -> Bundle:
    for catalog in config.catalogs:
        plugin = plugin_for(catalog)
        try:
            source_ref = plugin.resolve_source(source)
        except Exception:
            log.debug('Skipping catalog %r (incompatible source)', catalog)
        else:
            log.debug('Searching for %r in catalog %r', source, catalog)
            for plugin_source_spec in plugin.sources:
                if source_ref.spec.eq_ignoring_prefix(plugin_source_spec):
                    fqid = SourcedBundleFQIDJSON(source=source_ref.to_json(),
                                                 uuid=bundle_uuid,
                                                 version=bundle_version)
                    fqid = plugin.bundle_fqid_from_json(fqid)
                    bundle = plugin.fetch_bundle(fqid)
                    log.info('Fetched bundle %r version %r from catalog %r.',
                             fqid.uuid, fqid.version, catalog)
                    return bundle
    raise ValueError(f'No repository using source {source!r}')


@cache
def plugin_for(catalog):
    return RepositoryPlugin.load(catalog).create(catalog)


def save_bundle(bundle: Bundle, output_dir: str) -> None:
    path = os.path.join(output_dir,
                        f'{bundle.uuid}.{bundle.canning_qualifier()}.json')
    with write_file_atomically(path) as f:
        json.dump(bundle.to_json(), f, indent=4)
    log.info('Successfully wrote %s', path)


redacted_entity_types = {
    'biosample',
    'diagnosis',
    'donor'
}


def redact_bundle(bundle: Bundle, key: bytes) -> None:
    if isinstance(bundle, AnvilBundle):
        for entity_ref, entity_metadata in bundle.entities.items():
            if entity_ref.entity_type in redacted_entity_types:
                bundle.entities[entity_ref] = redact_json(entity_metadata, key)
    else:
        raise RuntimeError('HCA bundles do not support redaction', type(bundle))


def redact_json(o: AnyJSON, key: bytes) -> AnyMutableJSON:
    """
    >>> key = b'bananas'
    >>> redact_json('sensitive', key)
    'redacted-AVm0tjOw'

    >>> redact_json('sensitive', key + b'plit')
    'redacted-+L3zz1rW'

    >>> redact_json(123, key)
    42027752232213208

    >>> redact_json(['sensitive', 'confidential'], key)
    ['redacted-AVm0tjOw', 'redacted-ayRbEUrY']

    >>> redact_json({'foo': {'bar': [123, 456]}}, key)
    {'foo': {'bar': [42027752232213208, 42180364622007796]}}
    """
    if o is None:
        return o
    elif isinstance(o, str):
        # Preserve foreign and primary keys
        try:
            uuid.UUID(o)
        except ValueError:
            o = base64.b64encode(hashlib.sha1(key + o.encode()).digest()[:6])
            return (b'redacted-' + o).decode()
        else:
            return o
    elif isinstance(o, (int, float)):
        o = struct.unpack('>Q', hashlib.sha1(key + str(o).encode()).digest()[:8])[0]
        return (o & 0xFFFFFFFFFFFF) + 42000000000000000
    elif isinstance(o, list):
        # Preserve sorted-ness from AnVIL repository plugin
        return sorted(redact_json(item, key) for item in o)
    elif isinstance(o, dict):
        return {
            # Preserve references to original dataset
            k: v if k == 'source_datarepo_row_ids' else redact_json(v, key)
            for k, v in o.items()
        }
    else:
        assert False, type(o)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
