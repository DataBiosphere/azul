from collections import (
    defaultdict,
)
from concurrent.futures import (
    ThreadPoolExecutor,
)
from itertools import (
    groupby,
    islice,
)
import json
import logging
from operator import (
    itemgetter,
)
from typing import (
    Any,
    ClassVar,
    Iterable,
    Mapping,
    Optional,
    Union,
    cast,
)

import attr
from furl import (
    furl,
)
from more_itertools import (
    one,
)

from azul import (
    JSON,
    RequirementError,
    config,
    require,
)
from azul.bigquery import (
    BigQueryRow,
    BigQueryRows,
    backtick,
)
from azul.drs import (
    RegularDRSURI,
)
from azul.indexer import (
    BundleFQID,
)
from azul.indexer.document import (
    EntityID,
    EntityReference,
    EntityType,
)
from azul.plugins.metadata.hca.bundle import (
    HCABundle,
)
from azul.plugins.repository.tdr import (
    TDRBundle,
    TDRBundleFQID,
    TDRPlugin,
)
from azul.terra import (
    TDRSourceRef,
    TDRSourceSpec,
)
from azul.types import (
    JSONs,
    is_optional,
)
from azul.uuids import (
    validate_uuid_prefix,
)

log = logging.getLogger(__name__)

Entities = set[EntityReference]
EntitiesByType = dict[EntityType, set[EntityID]]


@attr.s(frozen=True, auto_attribs=True)
class Links:
    project: EntityReference
    processes: Entities = attr.Factory(set)
    protocols: Entities = attr.Factory(set)
    inputs: Entities = attr.Factory(set)
    outputs: Entities = attr.Factory(set)
    supplementary_files: Entities = attr.Factory(set)

    @classmethod
    def from_json(cls, project: EntityReference, links_json: JSON) -> 'Links':
        """
        A `links.json` file, in a more accessible form.

        :param links_json: The contents of a `links.json` file.

        :param project: A reference to the project the given `links.json`
                        belongs to.
        """
        self = cls(project)
        for link in cast(JSONs, links_json['links']):
            link_type = link['link_type']
            if link_type == 'process_link':
                self.processes.add(EntityReference(entity_type=link['process_type'],
                                                   entity_id=link['process_id']))
                for category in ('input', 'output', 'protocol'):
                    plural = category + 's'
                    target = getattr(self, plural)
                    for entity in cast(JSONs, link[plural]):
                        target.add(EntityReference(entity_type=entity[category + '_type'],
                                                   entity_id=entity[category + '_id']))
            elif link_type == 'supplementary_file_link':
                associate = EntityReference(entity_type=link['entity']['entity_type'],
                                            entity_id=link['entity']['entity_id'])
                # For MVP, only project entities can have associated supplementary files.
                require(associate == project,
                        'Supplementary file must be associated with the current project',
                        project, associate)
                for entity in cast(JSONs, link['files']):
                    self.supplementary_files.add(
                        EntityReference(entity_type='supplementary_file',
                                        entity_id=entity['file_id']))
            else:
                raise RequirementError('Unexpected link_type', link_type)
        return self

    def all_entities(self) -> Entities:
        return set.union(*(value if isinstance(value, set) else {value}
                           for field, value in attr.asdict(self, recurse=False).items()))

    def dangling_inputs(self) -> Entities:
        return {
            input_
            for input_ in self.inputs
            if input_.entity_type.endswith('_file') and not (
                input_ in self.outputs or
                input_ in self.supplementary_files
            )
        }


@attr.s(auto_attribs=True, kw_only=True, frozen=True)
class Checksums:
    crc32c: str
    sha1: Optional[str] = None
    sha256: str
    s3_etag: Optional[str] = None

    def to_json(self) -> dict[str, str]:
        """
        >>> Checksums(crc32c='a', sha1='b', sha256='c', s3_etag=None).to_json()
        {'crc32c': 'a', 'sha1': 'b', 'sha256': 'c'}
        """
        return {k: v for k, v in attr.asdict(self).items() if v is not None}

    @classmethod
    def from_json(cls, json: JSON) -> 'Checksums':
        """
        >>> Checksums.from_json({'crc32c': 'a', 'sha256': 'c'})
        Checksums(crc32c='a', sha1=None, sha256='c', s3_etag=None)

        >>> Checksums.from_json({'crc32c': 'a', 'sha1':'b', 'sha256': 'c', 's3_etag': 'd'})
        Checksums(crc32c='a', sha1='b', sha256='c', s3_etag='d')

        >>> Checksums.from_json({'crc32c': 'a'})
        Traceback (most recent call last):
            ...
        ValueError: ('JSON property cannot be absent or null', 'sha256')
        """

        def extract_field(field: attr.Attribute) -> tuple[str, Any]:
            value = json.get(field.name)
            if value is None and not is_optional(field.type):
                raise ValueError('JSON property cannot be absent or null', field.name)
            return field.name, value

        return cls(**dict(map(extract_field, attr.fields(cls))))


class TDRHCABundle(HCABundle[TDRBundleFQID], TDRBundle):

    @classmethod
    def canning_qualifier(cls) -> str:
        return super().canning_qualifier() + '.hca'

    def add_entity(self,
                   *,
                   entity_key: str,
                   entity_type: EntityType,
                   entity_row: BigQueryRow,
                   is_stitched: bool
                   ) -> None:
        entity_id = entity_row[entity_type + '_id']
        self._add_manifest_entry(name=entity_key,
                                 uuid=entity_id,
                                 version=TDRPlugin.format_version(entity_row['version']),
                                 size=entity_row['content_size'],
                                 content_type='application/json',
                                 dcp_type=f'"metadata/{entity_row["schema_type"]}"',
                                 is_stitched=is_stitched)
        if entity_type.endswith('_file'):
            descriptor = json.loads(entity_row['descriptor'])
            self._add_manifest_entry(name=entity_row['file_name'],
                                     uuid=descriptor['file_id'],
                                     version=descriptor['file_version'],
                                     size=descriptor['size'],
                                     content_type=descriptor['content_type'],
                                     dcp_type='data',
                                     is_stitched=is_stitched,
                                     checksums=Checksums.from_json(descriptor),
                                     drs_uri=self._parse_drs_uri(entity_row['file_id'], descriptor))
        content = entity_row['content']
        self.metadata_files[entity_key] = (json.loads(content)
                                           if isinstance(content, str)
                                           else content)

    metadata_columns: ClassVar[set[str]] = {
        'version',
        'JSON_EXTRACT_SCALAR(content, "$.schema_type") AS schema_type',
        'BYTE_LENGTH(content) AS content_size',
        'content'
    }

    data_columns: ClassVar[set[str]] = metadata_columns | {
        'descriptor',
        'JSON_EXTRACT_SCALAR(content, "$.file_core.file_name") AS file_name',
        'file_id'
    }

    # `links_id` is omitted for consistency since the other sets do not include
    # the primary key
    links_columns: ClassVar[set[str]] = metadata_columns | {
        'project_id'
    }

    _suffix = 'tdr.'

    def _add_manifest_entry(self,
                            *,
                            name: str,
                            uuid: str,
                            version: str,
                            size: int,
                            content_type: str,
                            dcp_type: str,
                            is_stitched: bool,
                            checksums: Optional[Checksums] = None,
                            drs_uri: Optional[str] = None) -> None:
        # These requirements prevent mismatches in the DRS domain, and ensure
        # that changes to the column syntax don't go undetected.
        if drs_uri is not None:
            parsed = RegularDRSURI.parse(drs_uri)
            require(parsed.uri.netloc == config.tdr_service_url.netloc)
        self.manifest.append({
            'name': name,
            'uuid': uuid,
            'version': version,
            'content-type': f'{content_type}; dcp-type={dcp_type}',
            'size': size,
            'is_stitched': is_stitched,
            **(
                {
                    'indexed': True,
                    'crc32c': '',
                    'sha256': ''
                } if checksums is None else {
                    'indexed': False,
                    'drs_uri': drs_uri,
                    **checksums.to_json()
                }
            )
        })

    def _parse_drs_uri(self,
                       file_id: Optional[str],
                       descriptor: JSON
                       ) -> Optional[str]:
        # The file_id column is present for datasets, but is usually null, may
        # contain unexpected/unusable values, and NEVER produces usable DRS URLs,
        # so we avoid parsing the column altogether for datasets.
        if self.fqid.source.spec.is_snapshot:
            if file_id is None:
                try:
                    external_drs_uri = descriptor['drs_uri']
                except KeyError:
                    raise RequirementError('`file_id` is null and `drs_uri` '
                                           'is not set in file descriptor', descriptor)
                else:
                    # FIXME: Support non-null DRS URIs in file descriptors
                    #        https://github.com/DataBiosphere/azul/issues/3631
                    require(external_drs_uri is None,
                            'Non-null `drs_uri` in file descriptor', external_drs_uri)
                    return external_drs_uri
            else:
                return file_id
        else:
            return None


class Plugin(TDRPlugin[TDRHCABundle, TDRSourceSpec, TDRSourceRef, TDRBundleFQID]):

    def list_partitions(self,
                        source: TDRSourceRef
                        ) -> Mapping[str, int]:
        prefix = source.spec.prefix
        prefixes = [
            prefix.common + partition_prefix
            for partition_prefix in prefix.partition_prefixes()
        ]
        assert prefixes, prefix
        rows = self._run_sql(f'''
            SELECT prefix, COUNT(links_id) AS subgraph_count
            FROM {backtick(self._full_table_name(source.spec, 'links'))}
            JOIN UNNEST({prefixes}) AS prefix ON STARTS_WITH(links_id, prefix)
            GROUP BY prefix
        ''')
        return {row['prefix']: row['subgraph_count'] for row in rows}

    def _list_bundles(self,
                      source: TDRSourceRef,
                      prefix: str
                      ) -> list[TDRBundleFQID]:
        source_prefix = source.spec.prefix.common
        validate_uuid_prefix(source_prefix + prefix)
        current_bundles = self._query_latest_version(source.spec, f'''
            SELECT links_id, version
            FROM {backtick(self._full_table_name(source.spec, 'links'))}
            WHERE STARTS_WITH(links_id, '{source_prefix + prefix}')
        ''', group_by='links_id')
        return [
            TDRBundleFQID(source=source,
                          uuid=row['links_id'],
                          version=self.format_version(row['version']))
            for row in current_bundles
        ]

    def _query_latest_version(self,
                              source: TDRSourceSpec,
                              query: str,
                              group_by: str
                              ) -> list[BigQueryRow]:
        iter_rows = self._run_sql(query)
        key = itemgetter(group_by)
        groups = groupby(sorted(iter_rows, key=key), key=key)
        return [self._choose_one_version(source, group) for _, group in groups]

    def _choose_one_version(self,
                            source: TDRSourceSpec,
                            versioned_items: BigQueryRows
                            ) -> BigQueryRow:
        if source.is_snapshot:
            return one(versioned_items)
        else:
            return max(versioned_items, key=itemgetter('version'))

    def _emulate_bundle(self, bundle_fqid: TDRBundleFQID) -> TDRHCABundle:
        bundle = TDRHCABundle(fqid=bundle_fqid,
                              manifest=[],
                              metadata_files={})
        entities, root_entities, links_jsons = self._stitch_bundles(bundle)
        bundle.add_entity(entity_key='links.json',
                          entity_type='links',
                          entity_row=self._merge_links(links_jsons),
                          is_stitched=False)

        with ThreadPoolExecutor(max_workers=config.num_tdr_workers) as executor:
            futures = {
                entity_type: executor.submit(self._retrieve_entities,
                                             bundle.fqid.source.spec,
                                             entity_type,
                                             entity_ids)
                for entity_type, entity_ids in entities.items()
            }
            for entity_type, future in futures.items():
                e = future.exception()
                if e is None:
                    rows = future.result()
                    pk_column = entity_type + '_id'
                    rows.sort(key=itemgetter(pk_column))
                    for i, row in enumerate(rows):
                        is_stitched = EntityReference(entity_id=row[pk_column],
                                                      entity_type=entity_type) not in root_entities
                        bundle.add_entity(entity_key=f'{entity_type}_{i}.json',
                                          entity_type=entity_type,
                                          entity_row=row,
                                          is_stitched=is_stitched)
                else:
                    log.error('TDR worker failed to retrieve entities of type %r',
                              entity_type, exc_info=e)
                    raise e
        bundle.manifest.sort(key=itemgetter('uuid'))
        return bundle

    def _stitch_bundles(self,
                        root_bundle: 'TDRHCABundle'
                        ) -> tuple[EntitiesByType, Entities, list[JSON]]:
        """
        Recursively follow dangling inputs to collect entities from upstream
        bundles, ensuring that no bundle is processed more than once.
        """
        source = root_bundle.fqid.source
        entities: EntitiesByType = defaultdict(set)
        root_entities = None
        unprocessed: set[TDRBundleFQID] = {root_bundle.fqid}
        processed: set[TDRBundleFQID] = set()
        stitched_links: list[JSON] = []
        # Retrieving links in batches eliminates the risk of exceeding
        # BigQuery's maximum query size. Using a batches size 1000 appears to be
        # equally performant as retrieving the links without batching.
        batch_size = 1000
        while unprocessed:
            batch = set(islice(unprocessed, batch_size))
            links = self._retrieve_links(batch)
            processed.update(batch)
            unprocessed -= batch
            stitched_links.extend(links.values())
            all_dangling_inputs: set[EntityReference] = set()
            for links_id, links_json in links.items():
                project = EntityReference(entity_type='project',
                                          entity_id=links_json['project_id'])
                links = Links.from_json(project, links_json['content'])
                linked_entities = links.all_entities()
                dangling_inputs = links.dangling_inputs()
                if links_id == root_bundle.fqid:
                    assert root_entities is None
                    root_entities = linked_entities - dangling_inputs
                for entity in linked_entities:
                    entities[entity.entity_type].add(entity.entity_id)
                if dangling_inputs:
                    log.info('There are %i dangling inputs in bundle %r', len(dangling_inputs), links_id)
                    log.debug('Dangling inputs in bundle %r: %r', links_id, dangling_inputs)
                    all_dangling_inputs.update(dangling_inputs)
                else:
                    log.info('Bundle %r is self-contained', links_id)
            if all_dangling_inputs:
                upstream = self._find_upstream_bundles(source, all_dangling_inputs)
                unprocessed |= upstream - processed

        assert root_entities is not None
        processed.remove(root_bundle.fqid)
        if processed:
            arg = f': {processed!r}' if log.isEnabledFor(logging.DEBUG) else ''
            log.info('Stitched %i bundle(s)%s', len(processed), arg)
        return entities, root_entities, stitched_links

    def _retrieve_links(self,
                        links_ids: set[TDRBundleFQID]
                        ) -> dict[TDRBundleFQID, JSON]:
        """
        Retrieve links entities from BigQuery and parse the `content` column.
        :param links_ids: Which links entities to retrieve.
        """
        source = one({fqid.source.spec for fqid in links_ids})
        links = self._retrieve_entities(source, 'links', links_ids)
        links = {
            # Copy the values so we can reassign `content` below
            fqid: dict(one(links_json
                           for links_json in links
                           if links_json['links_id'] == fqid.uuid))
            for fqid in links_ids
        }
        for links_json in links.values():
            links_json['content'] = json.loads(links_json['content'])
        return links

    def _retrieve_entities(self,
                           source: TDRSourceSpec,
                           entity_type: EntityType,
                           entity_ids: Union[set[EntityID], set[BundleFQID]],
                           ) -> list[BigQueryRow]:
        """
        Efficiently retrieve multiple entities from BigQuery in a single query.

        :param source: Snapshot containing the entity table

        :param entity_type: The type of entity, corresponding to the table name

        :param entity_ids: For links, the fully qualified UUID and version of
                           each `links` entity. For other entities, just the UUIDs.
        """
        pk_column = entity_type + '_id'
        version_column = 'version'
        non_pk_columns = (
            TDRHCABundle.links_columns if entity_type == 'links'
            else TDRHCABundle.data_columns if entity_type.endswith('_file')
            else TDRHCABundle.metadata_columns
        )
        assert version_column in non_pk_columns
        table_name = backtick(self._full_table_name(source, entity_type))
        entity_id_type = one(set(map(type, entity_ids)))

        def quote(s):
            return f"'{s}'"

        if entity_type == 'links':
            assert issubclass(entity_id_type, BundleFQID), entity_id_type
            entity_ids = cast(set[BundleFQID], entity_ids)
            where_columns = (pk_column, version_column)
            where_values = (
                (quote(fqid.uuid), f'TIMESTAMP({quote(fqid.version)})')
                for fqid in entity_ids
            )
            expected = {fqid.uuid for fqid in entity_ids}
        else:
            assert issubclass(entity_id_type, str), (entity_type, entity_id_type)
            where_columns = (pk_column,)
            where_values = ((quote(entity_id),) for entity_id in entity_ids)
            expected = entity_ids
        query = f'''
            SELECT {', '.join({pk_column, *non_pk_columns})}
            FROM {table_name}
            WHERE {self._in(where_columns, where_values)}
        '''
        log.debug('Retrieving %i entities of type %r ...', len(entity_ids), entity_type)
        rows = self._query_latest_version(source, query, group_by=pk_column)
        log.debug('Retrieved %i entities of type %r', len(rows), entity_type)
        missing = expected - {row[pk_column] for row in rows}
        require(not missing,
                f'Required entities not found in {table_name}: {missing}')
        return rows

    def _in(self,
            columns: tuple[str, ...],
            values: Iterable[tuple[str, ...]]
            ) -> str:
        """
        >>> plugin = Plugin(sources=set())
        >>> plugin._in(('foo', 'bar'), [('"abc"', '123'), ('"def"', '456')])
        '(foo, bar) IN (("abc", 123), ("def", 456))'
        """

        def join(i):
            return '(' + ', '.join(i) + ')'

        return join(columns) + ' IN ' + join(map(join, values))

    def _find_upstream_bundles(self,
                               source: TDRSourceRef,
                               outputs: Entities) -> set[TDRBundleFQID]:
        """
        Search for bundles containing processes that produce the specified output
        entities.
        """
        output_ids = [output.entity_id for output in outputs]
        output_id = 'JSON_EXTRACT_SCALAR(link_output, "$.output_id")'
        rows = self._run_sql(f'''
            SELECT links_id, version, {output_id} AS output_id
            FROM {backtick(self._full_table_name(source.spec, 'links'))} AS links
                JOIN UNNEST(JSON_EXTRACT_ARRAY(links.content, '$.links')) AS content_links
                    ON JSON_EXTRACT_SCALAR(content_links, '$.link_type') = 'process_link'
                JOIN UNNEST(JSON_EXTRACT_ARRAY(content_links, '$.outputs')) AS link_output
                    ON {output_id} IN UNNEST({output_ids})
        ''')
        bundles = set()
        outputs_found = set()
        for row in rows:
            bundles.add(TDRBundleFQID(source=source,
                                      uuid=row['links_id'],
                                      version=self.format_version(row['version'])))
            outputs_found.add(row['output_id'])
        missing = set(output_ids) - outputs_found
        require(not missing,
                f'Dangling inputs not found in any bundle: {missing}')
        return bundles

    def _merge_links(self, links_jsons: JSONs) -> JSON:
        """
        Merge the links.json documents from multiple stitched bundles into a
        single document.
        """
        root, *stitched = links_jsons
        if stitched:
            merged = {
                'links_id': root['links_id'],
                'version': root['version']
            }
            for common_key in ('project_id', 'schema_type'):
                merged[common_key] = one({row[common_key] for row in links_jsons})
            source_contents = [row['content'] for row in links_jsons]
            # FIXME: Explicitly verify compatible schema versions for stitched subgraphs
            #        https://github.com/DataBiosphere/azul/issues/3215
            schema_type = 'links'
            schema_version = '3.0.0'
            schema_url = furl(url='https://schema.humancellatlas.org',
                              path=('system', schema_version, schema_type))
            merged_content = {
                'schema_type': schema_type,
                'schema_version': schema_version,
                'describedBy': str(schema_url),
                'links': sum((sc['links'] for sc in source_contents), start=[])
            }
            merged['content'] = merged_content  # Keep result of parsed JSON for reuse
            merged['content_size'] = len(json.dumps(merged_content))
            assert merged.keys() == one({
                frozenset(row.keys()) for row in links_jsons
            }), merged
            assert merged_content.keys() == one({
                frozenset(sc.keys()) for sc in source_contents
            }), merged_content
            return merged
        else:
            return root
