"""
Download the contents of a given bundle from the given repository source and
store it as a single JSON file. Users are expected to be familiar with the
structure of the bundle FQIDs for the given source and provide the appropriate
attributes.

Note: silently overwrites the destination file.
"""

import argparse
import base64
from collections.abc import (
    Mapping,
)
import hashlib
import json
import logging
import os
import struct
import sys
import uuid

from azul import (
    config,
)
from azul.args import (
    AzulArgumentHelpFormatter,
)
from azul.indexer import (
    Bundle,
)
from azul.lib import (
    cache,
)
from azul.lib.files import (
    write_file_atomically,
)
from azul.lib.types import (
    AnyJSON,
    AnyMutableJSON,
    json_dict,
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

log = logging.getLogger(__name__)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=AzulArgumentHelpFormatter)
    parser.add_argument('--source', '-s',
                        required=True,
                        help='The repository source containing the bundle')
    parser.add_argument('--uuid', '-b',
                        help='The UUID of the bundle to can. Required for HCA. For AnVIL, '
                             'the UUID is derived, so pass the AnVIL options instead.')
    parser.add_argument('--version', '-v',
                        help='The version of the bundle to can. Required for HCA. AnVIL '
                             'bundles all share one version, which is used automatically.')
    parser.add_argument('--table-name',
                        help='The BigQuery table of the bundle to can. AnVIL only.')
    parser.add_argument('--batch-prefix',
                        help='The prefix defining the batch of rows to can. AnVIL only, '
                             'and only for bundles of a batched table.')
    parser.add_argument('--primary-key',
                        help='The primary key of the bundle entity. AnVIL only, and only '
                             'for bundles of a table that is not batched.')
    parser.add_argument('--output-dir', '-O',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the output directory (default: %(default)s).')
    parser.add_argument('--redaction-key', '-K',
                        help='Provide a key to redact confidential or sensitive information from the output files')
    args = parser.parse_args(argv)
    fqid_fields = parse_fqid_fields(parser, args)
    bundle = fetch_bundle(args.source, fqid_fields)
    if args.redaction_key:
        redact_bundle(bundle, args.redaction_key.encode())
    save_bundle(bundle, args.output_dir)


def parse_fqid_fields(parser: argparse.ArgumentParser,
                      args: argparse.Namespace
                      ) -> Mapping[str, str]:
    """
    The attributes identifying the requested bundle, as keyword arguments to
    the constructor of the repository's FQID class. An AnVIL bundle is
    identified by the table it is drawn from and either the batch or the bundle
    entity it contains, from which its UUID and version are derived. A bundle
    of any other repository is identified by its UUID and version.
    """
    anvil_fields = {
        field: value
        for field, value in [('table_name', args.table_name),
                             ('batch_prefix', args.batch_prefix),
                             ('primary_key', args.primary_key)]
        if value is not None
    }
    other_fields = {
        field: value
        for field, value in [('uuid', args.uuid), ('version', args.version)]
        if value is not None
    }
    if anvil_fields:
        if other_fields:
            parser.error('--uuid and --version are derived from the AnVIL '
                         'options, and must not be combined with them')
        return anvil_fields
    elif 'uuid' in other_fields:
        return other_fields
    else:
        parser.error('Either --uuid or the AnVIL options are required')


def fetch_bundle(source: str, fqid_args: Mapping[str, str]) -> Bundle:
    for catalog in config.catalogs:
        plugin = plugin_for(catalog)
        try:
            source_spec = plugin.parse_source(source)
        except Exception:
            log.debug('Skipping catalog %r (incompatible source)', catalog)
        else:
            source_ref = plugin.resolve_source(source_spec)
            log.debug('Searching for %r in catalog %r', source, catalog)
            if source_spec in plugin.sources:
                # Constructing, rather than deserializing, lets a plugin
                # derive the attributes that aren't given
                fqid = plugin.bundle_fqid_cls(source=source_ref, **fqid_args)
                bundle = plugin.fetch_bundle(fqid)
                log.info('Fetched bundle %r version %r from catalog %r.',
                         fqid.uuid, fqid.version, catalog)
                return bundle
    raise ValueError(f'No repository using source {source!r}')


@cache
def plugin_for(catalog) -> RepositoryPlugin:
    return RepositoryPlugin.load(catalog).create(catalog)


def save_bundle(bundle: Bundle, output_dir: str) -> None:
    file_name = f'{bundle.uuid}.{bundle.canning_qualifier()}.json'
    path = os.path.join(output_dir, file_name)
    bundle_json = bundle.to_json()
    # We can bundles without the FQID so that we can mock it during tests
    bundle_json.pop('fqid')
    with write_file_atomically(path) as f:
        json.dump(bundle_json, f, indent=4)
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
                bundle.entities[entity_ref] = json_dict(redact_json(entity_metadata, key))
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
        assert isinstance(o, int), o
        return (o & 0xFFFFFFFFFFFF) + 42000000000000000
    elif isinstance(o, list):
        return [redact_json(e, key) for e in o]
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
