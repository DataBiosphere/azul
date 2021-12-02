import argparse
from collections import (
    defaultdict,
)
from datetime import (
    datetime,
)
import json
import logging
from operator import (
    itemgetter,
)
import os
import sys
from typing import (
    Dict,
    Mapping,
    Optional,
)
import uuid

from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    config,
    dss,
)
from azul.files import (
    write_file_atomically,
)
from azul.indexer import (
    Bundle,
    Prefix,
    SimpleSourceSpec,
    SourcedBundleFQID,
)
from azul.indexer.document import (
    EntityID,
)
from azul.json import (
    copy_json,
    copy_jsons,
)
from azul.logging import (
    configure_script_logging,
)
from azul.plugins.repository import (
    tdr,
)
from azul.plugins.repository.dss import (
    DSSBundle,
    DSSSourceRef,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRSourceRef,
)
from azul.terra import (
    TDRSourceSpec,
)
from azul.types import (
    JSON,
    MutableJSON,
    MutableJSONs,
)
from indexer.test_tdr import (
    TestTDRPlugin,
)

log = logging.getLogger(__name__)


def file_paths(parent_dir: str,
               bundle_uuid: str
               ) -> Dict[str, Dict[str, str]]:
    def paths(*parts: str, ext: str = ''):
        return {
            part: os.path.join(parent_dir, f'{bundle_uuid}.{part}{ext}.json')
            for part in parts
        }

    return {
        'dss': paths('manifest', 'metadata'),
        'tdr': paths('tables', 'result', ext='.tdr')
    }


def find_concrete_type(bundle: Bundle, file_name: str) -> str:
    return bundle.metadata_files[file_name]['describedBy'].rsplit('/', 1)[1]


def find_file_name(bundle: Bundle, entity_id: EntityID):
    return one(k for k, v in bundle.metadata_files.items()
               if k != 'links.json' and v['provenance']['document_id'] == entity_id)


def find_manifest_entry(bundle: Bundle, entity_id: EntityID) -> MutableJSON:
    return one(e for e in bundle.manifest if e['uuid'] == entity_id)


def convert_version(version: str) -> str:
    dt = datetime.strptime(version,
                           dss.version_format)
    return tdr.Plugin.format_version(dt)


def content_length(content: JSON) -> int:
    return len(json.dumps(content).encode('UTF-8'))


def deterministic_uuid(bundle_uuid: str, *names: str) -> str:
    namespace = uuid.UUID(bundle_uuid)
    sep = '\0'
    for name in names:
        assert sep not in name, (sep, name)
    return str(uuid.uuid5(namespace, sep.join(names)))


def drs_path(snapshot_id: str, file_id: str) -> str:
    return f'v1_{snapshot_id}_{file_id}'


def drs_uri(drs_path: Optional[str]) -> Optional[str]:
    if drs_path is None:
        return None
    else:
        netloc = furl(TestTDRPlugin.mock_service_url).netloc
        return f'drs://{netloc}/{drs_path}'


def dss_bundle_to_tdr(bundle: Bundle, source: TDRSourceRef) -> TDRBundle:
    metadata = copy_json(bundle.metadata_files)

    # Order entities by UUID for consistency with Plugin output.
    entities_by_type: Mapping[str, MutableJSONs] = defaultdict(list)
    for k, v in bundle.metadata_files.items():
        if k != 'links.json':
            entity_type = k.rsplit('_', 1)[0]
            entities_by_type[entity_type].append(v)
    for (entity_type, entities) in entities_by_type.items():
        entities.sort(key=lambda e: e['provenance']['document_id'])
        for i, entity in enumerate(entities):
            name = f'{entity_type}_{i}.json'
            bundle.metadata_files[name] = entity
            manifest_entry = find_manifest_entry(bundle,
                                                 entity['provenance']['document_id'])
            manifest_entry['name'] = name

    bundle.manifest.sort(key=itemgetter('uuid'))

    links_json = metadata['links.json']
    links_json['schema_type'] = 'links'  # DCP/1 uses 'link_bundle'
    for link in links_json['links']:
        process_id = link.pop('process')
        link['process_id'] = process_id
        link['process_type'] = find_concrete_type(bundle, find_file_name(bundle, process_id))
        link['link_type'] = 'process_link'  # No supplementary files in DCP/1 bundles
        for component in ('input', 'output'):  # Protocols already in desired format
            del link[f'{component}_type']  # Replace abstract type with concrete types
            component_list = link[f'{component}s']
            component_list[:] = [
                {
                    f'{component}_id': component_id,
                    f'{component}_type': find_concrete_type(bundle, find_file_name(bundle, component_id))
                }
                for component_id in component_list
            ]

    manifest: MutableJSONs = copy_jsons(bundle.manifest)
    links_entry = None
    for entry in manifest:
        entry['version'] = convert_version(entry['version'])
        entry['is_stitched'] = False
        if entry['name'] == 'links.json':
            links_entry = entry
        if entry['indexed']:
            entity_json = metadata[entry['name']]
            # Size of the entity JSON in TDR, not the size of pretty-printed
            # output file.
            entry['size'] = content_length(entity_json)
            # Only include mandatory checksums
            del entry['sha1']
            del entry['s3_etag']
            entry['crc32c'] = ''
            entry['sha256'] = ''
        else:
            entry['drs_path'] = drs_path(source.id,
                                         deterministic_uuid(bundle.uuid, entry['uuid']))
    manifest.sort(key=itemgetter('uuid'))

    assert links_entry is not None
    # links.json has no FQID of its own in TDR since its FQID is used
    # for the entire bundle.
    links_entry['uuid'] = bundle.uuid
    return TDRBundle(fqid=SourcedBundleFQID(source=source,
                                            uuid=links_entry['uuid'],
                                            version=links_entry['version']),
                     manifest=manifest,
                     metadata_files=metadata)


class Entity:

    def __init__(self, bundle: TDRBundle, file_name: str):
        self.concrete_type = find_concrete_type(bundle, file_name)
        self.manifest_entry = one(e for e in bundle.manifest if e['name'] == file_name)
        self.metadata = bundle.metadata_files[file_name]

    def to_json_row(self) -> JSON:
        return {
            f'{self.concrete_type}_id': self.manifest_entry['uuid'],
            'version': self.manifest_entry['version'],
            'content': self.metadata
        }


class Links(Entity):

    def __init__(self, bundle: TDRBundle, file_name: str):
        assert file_name == 'links.json'
        super().__init__(bundle, file_name),
        self.project_id = bundle.metadata_files['project_0.json']['provenance']['document_id']

    def to_json_row(self) -> JSON:
        return dict(super().to_json_row(),
                    project_id=self.project_id)


class File(Entity):

    def __init__(self, bundle: TDRBundle, file_name: str):
        super().__init__(bundle, file_name)
        assert self.concrete_type.endswith('_file')
        self.file_manifest_entry = one(e for e in bundle.manifest
                                       if e['name'] == self.metadata['file_core']['file_name'])
        assert bundle.fqid.source.spec.is_snapshot
        assert self.file_manifest_entry['drs_path'] is not None

    def to_json_row(self) -> JSON:
        return dict(super().to_json_row(),
                    file_id=drs_uri(self.file_manifest_entry['drs_path']),
                    descriptor=dict(tdr.Checksums.from_json(self.file_manifest_entry).to_json(),
                                    file_name=self.file_manifest_entry['name'],
                                    file_version=self.file_manifest_entry['version'],
                                    file_id=self.file_manifest_entry['uuid'],
                                    content_type=self.file_manifest_entry['content-type'].split(';', 1)[0],
                                    size=self.file_manifest_entry['size']))


def dump_tables(bundle: TDRBundle) -> MutableJSON:
    tables = defaultdict(list)
    for file_name in bundle.metadata_files:
        if file_name == 'links.json':
            entity_cls = Links
        elif '_file_' in file_name:
            entity_cls = File
        else:
            entity_cls = Entity
        entity = entity_cls(bundle, file_name)
        tables[entity.concrete_type].append(entity)
    return {
        'tables': {
            entity_type: {
                'rows': [e.to_json_row() for e in entities]
            }
            for entity_type, entities in tables.items()
        }
    }


def add_supp_files(bundle: TDRBundle,
                   *,
                   num_files: int,
                   version: str
                   ) -> None:
    links_json = bundle.metadata_files['links.json']['links']
    links_manifest = one(e for e in bundle.manifest if e['name'] == 'links.json')
    project_id = bundle.metadata_files['project_0.json']['provenance']['document_id']

    # When the plugin retrieves entities from BigQuery, it sorts them by UUID
    # and enumerates them to generate the numerical component of the document
    # names (e.g. "2" in "organoid_2.json"). Thus, when UUIDs are
    # lexicographically sorted, documents names must be numerically sorted to
    # match the expected output.
    #
    # Since we cannot control the lexicographic order of the generated UUIDs,
    # the document names used here as hash inputs do not correspond to the
    # actual documents associated with the generated UUIDs. All that matters is
    # that they're unique.
    document_names = [f'supplementary_file_{i}.json' for i in range(num_files)]
    metadata_ids = sorted(
        deterministic_uuid(bundle.uuid, document_name)
        for document_name in document_names
    )

    for document_name, metadata_id in zip(document_names, metadata_ids):
        data_id = deterministic_uuid(bundle.uuid, metadata_id, 'data')
        drs_id = deterministic_uuid(bundle.uuid, metadata_id, 'drs')
        file_name = f'{metadata_id}_file_name.fmt'

        content = {
            'describedBy': 'https://schema.humancellatlas.org/type/file/2.2.0/supplementary_file/supplementary_file',
            'schema_type': 'file',
            'provenance': {
                'document_id': metadata_id
            },
            'file_core': {
                'file_name': file_name
            }
        }
        bundle.metadata_files[document_name] = content
        bundle.manifest.extend([{
            'name': document_name,
            'uuid': metadata_id,
            'version': version,
            'size': content_length(content),
            'indexed': True,
            'content-type': 'application/json; dcp-type="metadata/file"',
            'crc32c': '',
            'sha256': '',
            'is_stitched': False
        }, {
            'name': file_name,
            'uuid': data_id,
            'size': 1024,
            'indexed': False,
            'content-type': 'whatever format there are in; dcp-type=data',
            'version': version,
            'crc32c': '',
            'sha256': '',
            'is_stitched': False,
            'drs_path': drs_path(bundle.fqid.source.id, drs_id)
        }])
        links_json.append({
            'link_type': 'supplementary_file_link',
            'entity': {
                'entity_type': 'project',
                'entity_id': project_id
            },
            'files': [{
                'file_type': 'supplementary_file',
                'file_id': metadata_id
            }]
        })
    # Update size in manifest to include new links
    links_manifest['size'] = content_length(bundle.metadata_files['links.json'])
    bundle.manifest.sort(key=itemgetter('uuid'))


def main(argv):
    """
    Load a canned bundle from DCP/1 and write *.manifest.tdr and *.metadata.tdr
    files showing the desired output for DCP/2.
    """
    default_version = datetime(year=2021, month=1, day=17, hour=0)

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--bundle-uuid', '-b',
                        default=TestTDRPlugin.bundle_uuid,
                        help='The UUID of the existing DCP/1 canned bundle.')
    parser.add_argument('--source-id', '-s',
                        default=TestTDRPlugin.snapshot_id,
                        help='The UUID of the snapshot/dataset to contain the canned DCP/2 bundle.')
    parser.add_argument('--version', '-v',
                        default=tdr.Plugin.format_version(default_version),
                        help='The version for any mock entities synthesized by the script.')
    parser.add_argument('--input-dir', '-I',
                        default=os.path.join(config.project_root, 'test', 'indexer', 'data'),
                        help='The path to the input directory.')
    parser.add_argument('--mock-supplementary-files', '-S',
                        type=int,
                        default=0,
                        help='The number of mock supplementary files to add to the output.')
    args = parser.parse_args(argv)

    paths = file_paths(args.input_dir, args.bundle_uuid)

    log.debug('Reading canned bundle %r from %r', args.bundle_uuid, paths['dss'])
    with open(paths['dss']['manifest']) as f:
        manifest = json.load(f)
    with open(paths['dss']['metadata']) as f:
        metadata = json.load(f)

    dss_source = DSSSourceRef(id='',
                              spec=SimpleSourceSpec(prefix=Prefix.of_everything,
                                                    name=config.dss_endpoint))
    dss_bundle = DSSBundle(fqid=SourcedBundleFQID(source=dss_source,
                                                  uuid=args.bundle_uuid,
                                                  version=''),
                           manifest=manifest,
                           metadata_files=metadata)

    tdr_source = TDRSourceRef(id=args.source_id,
                              spec=TDRSourceSpec(prefix=Prefix.of_everything,
                                                 project='test_project',
                                                 name='test_name',
                                                 is_snapshot=True),
                              is_public=True)
    tdr_bundle = dss_bundle_to_tdr(dss_bundle, tdr_source)

    add_supp_files(tdr_bundle,
                   num_files=args.mock_supplementary_files,
                   version=args.version)

    log.debug('Writing converted bundle %r to %r', args.bundle_uuid, paths['tdr'])
    with write_file_atomically(paths['tdr']['result']) as f:
        json.dump({
            'manifest': tdr_bundle.manifest,
            'metadata': tdr_bundle.metadata_files
        }, f, indent=4)

    with write_file_atomically(paths['tdr']['tables']) as f:
        json.dump(dump_tables(tdr_bundle), f, indent=4)


if __name__ == '__main__':
    configure_script_logging(log)
    main(sys.argv[1:])
